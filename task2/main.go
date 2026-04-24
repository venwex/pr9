package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
)

type IdempotencyStore struct {
	db *sql.DB
}

func NewIdempotencyStore(db *sql.DB) *IdempotencyStore {
	return &IdempotencyStore{db: db}
}

func InitDB(db *sql.DB) error {
	query := `
	CREATE TABLE IF NOT EXISTS idempotency_keys (
		key TEXT PRIMARY KEY,
		status TEXT NOT NULL,
		status_code INT,
		response_body BYTEA,
		created_at TIMESTAMP NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMP NOT NULL DEFAULT NOW()
	);
	`

	_, err := db.Exec(query)
	return err
}

func (s *IdempotencyStore) TryStart(ctx context.Context, key string) (bool, error) {
	query := `
	INSERT INTO idempotency_keys (key, status)
	VALUES ($1, $2)
	ON CONFLICT (key) DO NOTHING;
	`

	result, err := s.db.ExecContext(ctx, query, key, StatusProcessing)
	if err != nil {
		return false, err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	return rows == 1, nil
}

func (s *IdempotencyStore) Get(ctx context.Context, key string) (status string, statusCode int, body []byte, err error) {
	query := `
	SELECT status, COALESCE(status_code, 0), COALESCE(response_body, '')
	FROM idempotency_keys
	WHERE key = $1;
	`

	err = s.db.QueryRowContext(ctx, query, key).Scan(&status, &statusCode, &body)
	return status, statusCode, body, err
}

func (s *IdempotencyStore) Complete(ctx context.Context, key string, statusCode int, body []byte) error {
	query := `
	UPDATE idempotency_keys
	SET status = $2,
	    status_code = $3,
	    response_body = $4,
	    updated_at = NOW()
	WHERE key = $1;
	`

	_, err := s.db.ExecContext(ctx, query, key, StatusCompleted, statusCode, body)
	return err
}

type ResponseRecorder struct {
	http.ResponseWriter
	statusCode int
	body       bytes.Buffer
}

func (r *ResponseRecorder) WriteHeader(code int) {
	r.statusCode = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *ResponseRecorder) Write(b []byte) (int, error) {
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}

func IdempotencyMiddleware(store *IdempotencyStore, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")

		if key == "" {
			http.Error(w, "missing Idempotency-Key", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		started, err := store.TryStart(ctx, key)
		if err != nil {
			http.Error(w, "database error", http.StatusInternalServerError)
			return
		}

		if !started {
			status, statusCode, body, err := store.Get(ctx, key)
			if err != nil {
				http.Error(w, "database error", http.StatusInternalServerError)
				return
			}

			if status == StatusProcessing {
				http.Error(w, "request is already processing", http.StatusConflict)
				return
			}

			if status == StatusCompleted {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(statusCode)
				w.Write(body)
				return
			}
		}

		recorder := &ResponseRecorder{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}

		next.ServeHTTP(recorder, r)

		err = store.Complete(ctx, key, recorder.statusCode, recorder.body.Bytes())
		if err != nil {
			fmt.Println("failed to save idempotency result:", err)
		}
	})
}

func paymentHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Processing started")

	time.Sleep(2 * time.Second)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success","amount":1000}`))

	fmt.Println("Processing completed")
}

func main() {
	dsn := "postgres://loan:loanpass@localhost:5432/loandb?sslmode=disable"

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	err = InitDB(db)
	if err != nil {
		panic(err)
	}

	store := NewIdempotencyStore(db)

	mux := http.NewServeMux()
	mux.HandleFunc("/pay", paymentHandler)

	server := httptest.NewServer(IdempotencyMiddleware(store, mux))
	defer server.Close()

	key := "loan-payment-123"

	// Чтобы при каждом запуске тест был чистым
	_, _ = db.Exec("DELETE FROM idempotency_keys WHERE key = $1", key)

	fmt.Println("Sending 10 simultaneous requests with the same Idempotency-Key...")

	var wg sync.WaitGroup

	for i := 1; i <= 10; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			req, err := http.NewRequest(http.MethodPost, server.URL+"/pay", nil)
			if err != nil {
				fmt.Println("request create error:", err)
				return
			}

			req.Header.Set("Idempotency-Key", key)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				fmt.Println("request error:", err)
				return
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)

			fmt.Printf("Request %d: status=%d body=%s\n", id, resp.StatusCode, string(body))
		}(i)
	}

	wg.Wait()

	fmt.Println()
	fmt.Println("Sending one more request after first operation completed...")

	req, err := http.NewRequest(http.MethodPost, server.URL+"/pay", nil)
	if err != nil {
		panic(err)
	}

	req.Header.Set("Idempotency-Key", key)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	fmt.Printf("Final request: status=%d body=%s\n", resp.StatusCode, string(body))
}
