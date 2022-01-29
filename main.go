package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

func ExpectOk(err error) {
	if err != nil {
		log.Fatalf("Unexpected fatal error: %s\n", err)
	}
}

type DBConfig struct {
	Hostname string `json:"hostname"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type Config struct {
	Port          int      `json:"port"`
	DB            DBConfig `json:"db"`
	CacheFilename string   `json:"cache_filename"`
}

func LoadConfig(path string) (*Config, error) {
	configFile, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer configFile.Close()

	byteData, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}

	var config Config
	err = json.Unmarshal(byteData, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func logCHError(query string, err error) {
	if exception, ok := err.(*ch.Exception); ok {
		log.Printf("CH error while executing '%s': [%d] %s\n", query, exception.Code, exception.Message)
	} else {
		log.Printf("CH error while executing '%s': %s\n", query, err)
	}
}

type LogbrokerServer struct {
	conn ch.Conn
	port int

	mu            sync.Mutex
	cacheFilename string
	cachedLogs    map[string]LogEntries
	signalChannel chan os.Signal
	ticker        *time.Ticker
}

func NewLogbrokerServer(config *Config) (*LogbrokerServer, error) {
	conn, err := ch.Open(&ch.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.DB.Hostname, config.DB.Port)},
		Auth: ch.Auth{
			Database: config.DB.Database,
			Username: config.DB.Username,
			Password: config.DB.Password,
		},
		DialTimeout:     time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		Compression: &ch.Compression{
			Method: ch.CompressionLZ4,
		},
		Settings: ch.Settings{
			"max_execution_time": 60,
		},
	})
	if err != nil {
		return nil, err
	}

	// ping CH
	context := ch.Context(context.Background())
	err = conn.Ping(context)
	if err != nil {
		logCHError("Ping", err)
		return nil, err
	}

	// load cache
	cache := make(map[string]LogEntries)
	if _, err := os.Stat(config.CacheFilename); err == nil {
		cacheFile, err := os.Open(config.CacheFilename)
		if err != nil {
			return nil, err
		}
		defer cacheFile.Close()

		byteData, err := ioutil.ReadAll(cacheFile)
		if err != nil {
			return nil, err
		}

		err = json.Unmarshal(byteData, &cache)
		if err != nil {
			return nil, err
		}
	}

	lb := &LogbrokerServer{
		conn:          conn,
		port:          config.Port,
		cacheFilename: config.CacheFilename,
		cachedLogs:    cache,
		signalChannel: make(chan os.Signal),
		ticker:        time.NewTicker(time.Second),
	}

	signal.Notify(lb.signalChannel, syscall.SIGINT, syscall.SIGTERM)

	go func(lb *LogbrokerServer) {
		for {
			select {
			case <-lb.ticker.C:
				lb.flushCache()
			case signal := <-lb.signalChannel:
				log.Printf("Received signal %s\n", signal)
				lb.flushCache()
				os.Exit(1)
			}
		}
	}(lb)

	return lb, nil
}

func (lb *LogbrokerServer) flushCache() {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	for tableName, entries := range lb.cachedLogs {
		context := ch.Context(context.Background())
		query := fmt.Sprintf(`INSERT INTO "%s"`, tableName)
		batch, err := lb.conn.PrepareBatch(context, query)
		if err != nil {
			logCHError("PrepareBatch", err)
			break
		}

		failed := false
		for _, entry := range entries {
			err = batch.AppendStruct(&entry)
			if err != nil {
				failed = true
				logCHError("AppendStruct", err)
				break
			}
		}
		if failed {
			break
		}

		err = batch.Send()
		if err != nil {
			logCHError("Send", err)
			break
		}

		delete(lb.cachedLogs, tableName)
	}

	if err := lb.persistCache(); err != nil {
		// nothing needs to be done in this case
		// as this is equivalent to the situation
		// where app crashes right before lb.persistCache() call
		log.Printf("Failed to persist cache after successful commit: %s\n", err)
	}
}

func (lb *LogbrokerServer) persistCache() error {
	cacheFile, err := os.OpenFile(lb.cacheFilename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	defer cacheFile.Close()
	byteData, err := json.Marshal(lb.cachedLogs)
	if err != nil {
		return err
	}

	return os.WriteFile(lb.cacheFilename, byteData, 0666)
}

func writeTypedResponse(w http.ResponseWriter, status int, contentType string, message string) {
	w.WriteHeader(status)
	w.Header().Set("Content-Type", contentType)
	w.Write([]byte(message))
}

func writeResponse(w http.ResponseWriter, status int, message string) {
	writeTypedResponse(w, status, "text/plain", message)
}

func (lb *LogbrokerServer) Healthcheck(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	writeResponse(w, http.StatusOK, "OK")
}

func isValidTableName(name string) bool {
	if !(unicode.IsLetter(rune(name[0])) || name[0] == '_') {
		return false
	}
	for i := 1; i < len(name); i++ {
		if !unicode.IsLetter(rune(name[i])) {
			return false
		}
	}
	return true
}

func (lb *LogbrokerServer) ShowCreateTable(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		writeResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	log.Printf("Received ShowCreateTable request")

	val, ok := req.URL.Query()["table_name"]
	if !ok || len(val) < 1 {
		writeResponse(w, http.StatusBadRequest, "No table_name in query")
		return
	}

	tableName := val[0]
	if !isValidTableName(tableName) {
		writeResponse(w, http.StatusBadRequest, "Invalid table_name")
		return
	}

	context := ch.Context(req.Context())
	query := fmt.Sprintf("SHOW CREATE TABLE %s;", tableName)
	rows, err := lb.conn.Query(context, query)
	if err != nil {
		logCHError(query, err)
		writeResponse(w, http.StatusInternalServerError, "Internal error")
		return
	}

	rows.Next()
	var res string
	err = rows.Scan(&res)

	if err != nil {
		logCHError("Scan", err)
	}

	writeResponse(w, http.StatusOK, res)

	log.Printf("Finished processing ShowCreateTable request")
}

func (lb *LogbrokerServer) RecreateTable(w http.ResponseWriter, req *http.Request) {
	log.Printf("Received RecreateTable request")

	val, ok := req.URL.Query()["table_name"]
	if !ok || len(val) < 1 {
		writeResponse(w, http.StatusBadRequest, "No table_name in query")
		return
	}

	tableName := val[0]
	if !isValidTableName(tableName) {
		writeResponse(w, http.StatusBadRequest, "Invalid table_name")
		return
	}

	context := ch.Context(req.Context())
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName)
	err := lb.conn.Exec(context, query)
	if err != nil {
		logCHError(query, err)
		writeResponse(w, http.StatusInternalServerError, "Internal error")
		return
	}

	query = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			a Int32,
			b String
		) ENGINE=Memory;
	`, tableName)
	err = lb.conn.Exec(context, query)
	if err != nil {
		logCHError(query, err)
		writeResponse(w, http.StatusInternalServerError, "Internal error")
		return
	}

	writeResponse(w, http.StatusOK, "OK")

	log.Printf("Finished processing RecreateTable request")
}

type LogEntry struct {
	A int32  `json:"a" ch:"a"`
	B string `json:"b" ch:"b"`
}

type LogEntries = []LogEntry

type TableLogs struct {
	TableName string
	Entries   LogEntries
}

type WriteLogRequest = []TableLogs

func (tl *TableLogs) UnmarshalJSON(data []byte) error {
	var metaData struct {
		TableName string          `json:"table_name"`
		Format    string          `json:"format"`
		Rows      json.RawMessage `json:"rows"`
	}

	err := json.Unmarshal(data, &metaData)
	if err != nil {
		return err
	}

	tl.TableName = metaData.TableName
	if metaData.Format == "json" {
		err = json.Unmarshal(metaData.Rows, &tl.Entries)
		return err
	} else if metaData.Format == "list" {
		var listEntries [][2]json.RawMessage
		err = json.Unmarshal(metaData.Rows, &listEntries)
		if err != nil {
			return err
		}

		for _, entry := range listEntries {
			var resEntry LogEntry
			err = json.Unmarshal(entry[0], &resEntry.A)
			if err != nil {
				return err
			}

			err = json.Unmarshal(entry[1], &resEntry.B)
			if err != nil {
				return err
			}

			tl.Entries = append(tl.Entries, resEntry)
		}
		return nil
	} else {
		return errors.New("Unknown format")
	}
}

func (lb *LogbrokerServer) WriteLog(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		writeResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	log.Printf("Received WriteLog request")

	rawBody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Printf("Failed to read body: %s\n", err)
		writeResponse(w, http.StatusInternalServerError, "Internal error")
		return
	}

	var parsedReq WriteLogRequest
	err = json.Unmarshal(rawBody, &parsedReq)
	if err != nil {
		log.Printf("Failed to decode request body: %s\n", err)
		writeResponse(w, http.StatusBadRequest, "Failed to decode request body")
		return
	}

	lb.mu.Lock()
	defer lb.mu.Unlock()

	for _, tableLogs := range parsedReq {
		lb.cachedLogs[tableLogs.TableName] = append(lb.cachedLogs[tableLogs.TableName], tableLogs.Entries...)
	}
	err = lb.persistCache()
	if err != nil {
		log.Printf("Failed to persist cache")
		writeResponse(w, http.StatusInternalServerError, "Internal error")
		return
	}

	// write fake CH response
	resBuilder := strings.Builder{}
	resBuilder.WriteRune('[')
	if len(parsedReq) > 0 {
		resBuilder.WriteString(`""`)
	}
	for i := 1; i < len(parsedReq); i++ {
		resBuilder.WriteString(`,""`)
	}
	resBuilder.WriteRune(']')

	writeResponse(w, http.StatusOK, resBuilder.String())

	log.Printf("Finished processing WriteLog request")
}

func (lb *LogbrokerServer) Serve() error {
	http.HandleFunc("/healthcheck", lb.Healthcheck)
	http.HandleFunc("/show_create_table", lb.ShowCreateTable)
	http.HandleFunc("/recreate_table", lb.RecreateTable)
	http.HandleFunc("/write_log", lb.WriteLog)

	address := fmt.Sprintf("0.0.0.0:%d", lb.port)
	log.Printf("Serving on %s\n", address)

	return http.ListenAndServe(address, nil)
}

func main() {
	configPath := flag.String("config-path", "config.json", "Path to configuration file")
	flag.Parse()

	config, err := LoadConfig(*configPath)
	ExpectOk(err)

	server, err := NewLogbrokerServer(config)
	ExpectOk(err)

	ExpectOk(server.Serve())
}
