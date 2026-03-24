package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// --- Configuration ---
const (
	MinWorkers = 50
	MaxWorkers = 100
	UserAgent  = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
)

// --- Global State ---
var (
	workersCount int32 = 50
	isScraping   bool  = false
	stopChan     chan bool
	resultsFile  *os.File
	mu           sync.Mutex
	emailRegex   = regexp.MustCompile(`[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}`)
	badDomains   = []string{"example.com", "test.com", "domain.com", "localhost"}
)

// --- Data Structures ---
type ScrapingConfig struct {
	URLs     []string `json:"urls"`
	Workers  int      `json:"workers"`
	Timeout  int      `json:"timeout"`
}

type Stats struct {
	Processed int64  `json:"processed"`
	Found     int64  `json:"found"`
	Errors    int64  `json:"errors"`
	Speed     string `json:"speed"`
	Status    string `json:"status"`
}

var stats = Stats{}

// --- WebSocket Hub ---
var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()

	// Send initial stats
	sendStats(conn)

	// Listen for commands
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		var cmd map[string]interface{}
		json.Unmarshal(msg, &cmd)
		if action, ok := cmd["action"]; ok {
			if action == "start" {
				go startScraping(cmd, conn)
			} else if action == "stop" {
				stopScraping()
			}
		}
	}
}

func sendStats(conn *websocket.Conn) {
	mu.Lock()
	defer mu.Unlock()
	data, _ := json.Marshal(stats)
	conn.WriteMessage(websocket.TextMessage, data)
}

// --- Scraper Engine ---
func startScraping(cmd map[string]interface{}, conn *websocket.Conn) {
	if isScraping {
		return
	}

	// Parse Config
	urlsInterface, _ := cmd["urls"].([]interface{})
	var urls []string
	for _, u := range urlsInterface {
		urls = append(urls, u.(string))
	}
	
	workers := MinWorkers
	if w, ok := cmd["workers"].(float64); ok {
		workers = int(w)
		if workers < MinWorkers { workers = MinWorkers }
		if workers > MaxWorkers { workers = MaxWorkers }
	}
	atomic.StoreInt32(&workersCount, int32(workers))

	isScraping = true
	stopChan = make(chan bool)
	
	// Reset Stats
	atomic.StoreInt64(&stats.Processed, 0)
	atomic.StoreInt64(&stats.Found, 0)
	atomic.StoreInt64(&stats.Errors, 0)
	stats.Status = "Running"
	stats.Speed = "0 URLs/min"

	// Open Results File
	var err error
	resultsFile, err = os.Create(fmt.Sprintf("results_%d.csv", time.Now().Unix()))
	if err != nil {
		log.Println("Failed to create results file")
		return
	}
	defer resultsFile.Close()
	resultsFile.WriteString("URL,Emails,Facebook,Timestamp\n")

	// Worker Pool
	urlChan := make(chan string, len(urls))
	var wg sync.WaitGroup

	// Start Workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for url := range urlChan {
				select {
				case <-stopChan:
					return
				default:
					processURL(url, conn)
				}
			}
		}(i)
	}

	// Feed URLs
	startTime := time.Now()
	for _, url := range urls {
		select {
		case <-stopChan:
			break
		default:
			urlChan <- url
		}
	}
	close(urlChan)
	wg.Wait()

	// Finish
	elapsed := time.Since(startTime)
	speed := float64(stats.Processed) / elapsed.Minutes()
	stats.Speed = fmt.Sprintf("%.2f URLs/min", speed)
	stats.Status = "Completed"
	isScraping = false
	
	// Final Update
	sendStats(conn)
}

func stopScraping() {
	if isScraping {
		close(stopChan)
		isScraping = false
		stats.Status = "Stopped"
	}
}

func processURL(url string, conn *websocket.Conn) {
	// Normalize URL
	if !strings.HasPrefix(url, "http") {
		url = "https://" + url
	}

	// Request
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		atomic.AddInt64(&stats.Errors, 1)
		return
	}
	req.Header.Set("User-Agent", UserAgent)

	resp, err := client.Do(req)
	if err != nil {
		atomic.AddInt64(&stats.Errors, 1)
		return
	}
	defer resp.Body.Close()

	// Read Body (Limit to 1MB for speed)
	scanner := bufio.NewScanner(resp.Body)
	var body strings.Builder
	limit := 1024 * 1024 
	count := 0
	for scanner.Scan() && count < limit {
		line := scanner.Text()
		body.WriteString(line)
		count += len(line)
	}

	content := body.String()
	
	// Extract Emails
	emails := emailRegex.FindAllString(content, -1)
	var cleanEmails []string
	seen := make(map[string]bool)

	for _, email := range emails {
		email = strings.ToLower(strings.TrimSpace(email))
		if isValidEmail(email) && !seen[email] {
			seen[email] = true
			cleanEmails = append(cleanEmails, email)
		}
	}

	// Extract Facebook Links (Simple)
	var fbLinks []string
	if strings.Contains(content, "facebook.com") {
		fbRegex := regexp.MustCompile(`https?://(?:www\.)?facebook\.com/[a-zA-Z0-9.]+`)
		fbLinks = fbRegex.FindAllString(content, -1)
	}

	// Update Stats
	atomic.AddInt64(&stats.Processed, 1)
	atomic.AddInt64(&stats.Found, int64(len(cleanEmails)))

	// Save to File
	if len(cleanEmails) > 0 || len(fbLinks) > 0 {
		mu.Lock()
		if resultsFile != nil {
			resultsFile.WriteString(fmt.Sprintf("%s,\"%s\",\"%s\",\"%s\"\n", 
				url, strings.Join(cleanEmails, ";"), strings.Join(fbLinks, ";"), time.Now().Format(time.RFC3339)))
		}
		mu.Unlock()
	}

	// Real-time UI Update (Throttled)
	if conn != nil && stats.Processed % 10 == 0 {
		sendStats(conn)
	}
}

func isValidEmail(email string) bool {
	// Basic Syntax Check already done by Regex
	// Domain Blacklist Check
	for _, bad := range badDomains {
		if strings.Contains(email, bad) {
			return false
		}
	}
	// Must have @ and .
	if !strings.Contains(email, "@") || !strings.Contains(email, ".") {
		return false
	}
	return true
}

// --- Web Interface (Embedded) ---
func indexHandler(w http.ResponseWriter, r *http.Request) {
	html := `
<!DOCTYPE html>
<html>
<head>
    <title>🚀 TURBO Go Scraper</title>
    <style>
        body { font-family: 'Segoe UI', sans-serif; background: #1e1e1e; color: #fff; padding: 20px; }
        .container { max-width: 900px; margin: 0 auto; }
        h1 { color: #00ff88; }
        textarea { width: 100%; height: 150px; background: #2d2d2d; color: #fff; border: 1px solid #444; padding: 10px; }
        .controls { margin: 20px 0; display: flex; gap: 10px; align-items: center; }
        button { padding: 10px 20px; border: none; cursor: pointer; font-weight: bold; }
        .btn-start { background: #00ff88; color: #000; }
        .btn-stop { background: #ff4444; color: #fff; }
        .stats { background: #2d2d2d; padding: 15px; border-radius: 5px; margin-top: 20px; display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }
        .stat-box { text-align: center; }
        .stat-val { font-size: 1.5em; font-weight: bold; color: #00ff88; }
        .log { background: #000; color: #0f0; font-family: monospace; padding: 10px; height: 200px; overflow-y: scroll; margin-top: 20px; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 TURBO Go Scraper (Ultra Fast)</h1>
        <p>Enter URLs (one per line):</p>
        <textarea id="urls">github.com
python.org
stackoverflow.com</textarea>
        
        <div class="controls">
            <label>Threads: <input type="number" id="workers" value="50" min="50" max="100" style="width:50px"></label>
            <button class="btn-start" onclick="startScrape()">🚀 START TURBO</button>
            <button class="btn-stop" onclick="stopScrape()">⏹️ STOP</button>
            <span id="status">Status: Ready</span>
        </div>

        <div class="stats">
            <div class="stat-box"><div>Processed</div><div class="stat-val" id="s-processed">0</div></div>
            <div class="stat-box"><div>Emails Found</div><div class="stat-val" id="s-found">0</div></div>
            <div class="stat-box"><div>Errors</div><div class="stat-val" id="s-errors" style="color:#ff4444">0</div></div>
            <div class="stat-box"><div>Speed</div><div class="stat-val" id="s-speed">0</div></div>
        </div>

        <div class="log" id="log">Waiting to start...</div>
    </div>

    <script>
        let ws;
        function connect() {
            ws = new WebSocket('ws://' + window.location.host + '/ws');
            ws.onmessage = function(event) {
                const data = JSON.parse(event.data);
                document.getElementById('s-processed').innerText = data.processed;
                document.getElementById('s-found').innerText = data.found;
                document.getElementById('s-errors').innerText = data.errors;
                document.getElementById('s-speed').innerText = data.speed;
                document.getElementById('status').innerText = 'Status: ' + data.status;
                if(data.status === 'Completed') {
                    document.getElementById('log').innerHTML += '<br>✅ Scraping Finished!';
                }
            };
        }
        connect();

        function startScrape() {
            const urls = document.getElementById('urls').value.split('\n');
            const workers = parseInt(document.getElementById('workers').value);
            ws.send(JSON.stringify({action: 'start', urls: urls, workers: workers}));
            document.getElementById('log').innerHTML += '<br>🚀 Starting Turbo Scraping...';
        }
        function stopScrape() {
            ws.send(JSON.stringify({action: 'stop'}));
            document.getElementById('log').innerHTML += '<br>⏹️ Stopping...';
        }
    </script>
</body>
</html>
`
	w.Write([]byte(html))
}

func main() {
	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/ws", wsHandler)

	fmt.Println("🚀 TURBO Scraper Starting on http://localhost:8080")
	fmt.Println("🛑 Press Ctrl+C to stop server")
	
	// Open browser automatically (Windows)
	go func() {
		time.Sleep(1 * time.Second)
		http.Get("http://localhost:8080") 
	}()

	log.Fatal(http.ListenAndServe(":8080", nil))
}