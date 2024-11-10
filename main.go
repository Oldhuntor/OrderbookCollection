package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/mattn/go-sqlite3"
)

type SymbolInfo struct {
	Symbol    string `json:"symbol"`
	BaseCoin  string `json:"baseCoin"`
	QuoteCoin string `json:"quoteCoin"`
}

type SymbolsResponse struct {
	Code string       `json:"code"`
	Msg  string       `json:"msg"`
	Data []SymbolInfo `json:"data"`
}

type OrderBookMessage struct {
	Action string `json:"action"`
	Arg    struct {
		InstType string `json:"instType"`
		Channel  string `json:"channel"`
		InstId   string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		Bids      [][]string `json:"bids"`
		Asks      [][]string `json:"asks"`
		Checksum  int32      `json:"checksum"`
		Timestamp string     `json:"ts"`
	} `json:"data"`
}

var orderBookSnapshots = sync.Map{} // Map to store snapshots for multiple symbols

func calculateChecksum(bids, asks [][]string) int32 {
	sort.Slice(bids, func(i, j int) bool { return bids[i][0] > bids[j][0] })
	sort.Slice(asks, func(i, j int) bool { return asks[i][0] < asks[j][0] })

	var data []string
	for i := 0; i < 25; i++ {
		if i < len(bids) {
			data = append(data, fmt.Sprintf("%s:%s", bids[i][0], bids[i][1]))
		}
		if i < len(asks) {
			data = append(data, fmt.Sprintf("%s:%s", asks[i][0], asks[i][1]))
		}
	}
	checkString := strings.Join(data, ":")
	return int32(crc32.ChecksumIEEE([]byte(checkString)))
}

func fetchAvailableSymbols() ([]SymbolInfo, error) {
	url := "https://api.bitget.com/api/v2/spot/public/symbols"
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch symbols: %v", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			fmt.Printf("failed to close body: %v", err)
		}
	}(resp.Body)

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	var symbolsResponse SymbolsResponse
	if err := json.Unmarshal(body, &symbolsResponse); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %v", err)
	}

	if symbolsResponse.Code != "00000" {
		return nil, fmt.Errorf("API error: %s", symbolsResponse.Msg)
	}

	return symbolsResponse.Data, nil
}

func displaySymbols(symbols []SymbolInfo) {
	fmt.Println("Available Trading Pairs:")
	for i, symbol := range symbols {
		fmt.Printf("%d: %s (%s/%s)\n", i+1, symbol.Symbol, symbol.BaseCoin, symbol.QuoteCoin)
	}
}

func getUserSelection(symbols []SymbolInfo) []string {
	fmt.Println("\nEnter the numbers of the symbols you want to subscribe to, separated by commas (e.g., 1,3,5):")
	reader := bufio.NewReader(os.Stdin)
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)
	selections := strings.Split(input, ",")

	var selectedSymbols []string
	for _, sel := range selections {
		index, err := strconv.Atoi(strings.TrimSpace(sel))
		if err != nil || index < 1 || index > len(symbols) {
			fmt.Printf("Invalid selection: %s\n", sel)
			continue
		}
		selectedSymbols = append(selectedSymbols, symbols[index-1].Symbol)
	}
	return selectedSymbols
}

func updateSnapshot(symbol string, bids, asks [][]string) {
	value, _ := orderBookSnapshots.LoadOrStore(symbol, &OrderBookMessage{})
	snapshot := value.(*OrderBookMessage)
	mergeLevels(&snapshot.Data[0].Bids, bids, true)
	mergeLevels(&snapshot.Data[0].Asks, asks, false)
}

func mergeLevels(snapshot *[][]string, updates [][]string, isBid bool) {
	for _, update := range updates {
		price, amount := update[0], update[1]
		found := false
		for i, level := range *snapshot {
			if level[0] == price {
				found = true
				if amount == "0" {
					*snapshot = append((*snapshot)[:i], (*snapshot)[i+1:]...)
				} else {
					(*snapshot)[i][1] = amount
				}
				break
			}
		}
		if !found && amount != "0" {
			*snapshot = append(*snapshot, []string{price, amount})
		}
	}

	sort.SliceStable(*snapshot, func(i, j int) bool {
		if isBid {
			return (*snapshot)[i][0] > (*snapshot)[j][0]
		}
		return (*snapshot)[i][0] < (*snapshot)[j][0]
	})
}

func subscribeOrderBook(symbol string, db *sql.DB, wg *sync.WaitGroup) {
	defer wg.Done()
	url := "wss://ws.bitget.com/v2/ws/public"

	for {
		fmt.Printf("Connecting to WebSocket for %s...\n", symbol)
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			log.Printf("Failed to connect: %v. Retrying in 5 seconds...", err)
			time.Sleep(5 * time.Second)
			continue
		}
		defer func(conn *websocket.Conn) {
			err := conn.Close()
			if err != nil {
				fmt.Printf("Failed to close WebSocket connection: %v", err)
			}
		}(conn)

		go func() {
			for {
				err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(time.Second))
				if err != nil {
					log.Println("Ping error:", err)
					return
				}
				time.Sleep(30 * time.Second)
			}
		}()

		subscribeMessage := fmt.Sprintf(`{
			"op": "subscribe",
			"args": [{
				"instType": "SPOT",
				"channel": "books",
				"instId": "%s"
			}]
		}`, symbol)
		fmt.Println(subscribeMessage)
		if err := conn.WriteMessage(websocket.TextMessage, []byte(subscribeMessage)); err != nil {
			log.Printf("Subscription failed: %v", err)
			err := conn.Close()
			if err != nil {
				fmt.Printf("Failed to close connection: %v", err)
				return
			}
			time.Sleep(5 * time.Second)
			continue
		}

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("Read error: %v. Reconnecting...", err)
				err := conn.Close()
				if err != nil {
					fmt.Printf("Failed to close connection: %v", err)
					return
				}
				time.Sleep(5 * time.Second)
				break
			}

			var orderBook OrderBookMessage
			err = json.Unmarshal(message, &orderBook)
			if err != nil {
				log.Printf("JSON parse error: %v", err)
				continue
			}

			if len(orderBook.Data) > 0 {
				snapshotData := orderBook.Data[0]

				if orderBook.Action == "snapshot" {
					orderBookSnapshots.Store(symbol, &OrderBookMessage{
						Data: []struct {
							Bids      [][]string `json:"bids"`
							Asks      [][]string `json:"asks"`
							Checksum  int32      `json:"checksum"`
							Timestamp string     `json:"ts"`
						}{snapshotData},
					})

					// Save the initial snapshot to the database
					saveOrderBook(db, symbol, "snapshot", snapshotData.Bids, snapshotData.Asks, snapshotData.Checksum)
				} else if orderBook.Action == "update" {
					updateSnapshot(symbol, snapshotData.Bids, snapshotData.Asks)

					value, _ := orderBookSnapshots.Load(symbol)
					snapshot := value.(*OrderBookMessage)
					calculatedChecksum := calculateChecksum(snapshot.Data[0].Bids, snapshot.Data[0].Asks)
					fmt.Printf("Calculated checksum: %d, Expected checksum: %d\n", calculatedChecksum, snapshotData.Checksum)

					if calculatedChecksum == snapshotData.Checksum {
						fmt.Println("Checksum verified. Saving incremental update to database.")
						// Save the incremental update to the database
						saveOrderBook(db, symbol, "update", snapshotData.Bids, snapshotData.Asks, calculatedChecksum)
					} else {
						log.Println("Checksum mismatch. Skipping insertion.")
					}
				}
			}
		}
	}
}

func saveOrderBook(db *sql.DB, symbol, action string, bids, asks [][]string, checksum int32) {
	bidsData, _ := json.Marshal(bids)
	asksData, _ := json.Marshal(asks)

	// Create a table for each symbol if it doesn't exist
	_, err := db.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s_order_book (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			symbol TEXT,
			action TEXT,
			bids TEXT,
			asks TEXT,
			checksum INTEGER,
			timestamp INTEGER
		)`, symbol))
	if err != nil {
		log.Fatalf("Failed to create table for %s: %v", symbol, err)
	}

	_, err = db.Exec(fmt.Sprintf(`
		INSERT INTO %s_order_book (symbol, action, bids, asks, checksum, timestamp)
		VALUES (?, ?, ?, ?, ?, ?)`, symbol),
		symbol,
		action,
		string(bidsData),
		string(asksData),
		checksum,
		time.Now().Unix(),
	)
	if err != nil {
		log.Printf("Database insert error for %s: %v", symbol, err)
	}
}

func main() {
	db, err := sql.Open("sqlite3", "./orderbook.db")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			fmt.Printf("Failed to close connection: %v", err)
		}
	}(db)

	symbols, err := fetchAvailableSymbols()
	if err != nil {
		log.Fatalf("Error fetching symbols: %v", err)
	}
	displaySymbols(symbols)

	selectedSymbols := getUserSelection(symbols)
	if len(selectedSymbols) == 0 {
		log.Fatal("No valid symbols selected. Exiting.")
	}

	var wg sync.WaitGroup

	for _, symbol := range selectedSymbols {
		wg.Add(1)
		go subscribeOrderBook(symbol, db, &wg)
	}

	wg.Wait()
}
