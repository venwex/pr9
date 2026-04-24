package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"time"
)

const (
	MaxRetries = 5
	BaseDelay  = 500 * time.Millisecond
)

func IsRetryable(resp *http.Response, err error) bool {
	if err != nil {
		return true // считаю сетевую ошибку временной
	}

	if resp == nil {
		return false
	}

	switch resp.StatusCode {
	case 429, 500, 502, 503, 504:
		return true
	case 401, 404:
		return false
	default:
		return false
	}
}

func CalculateBackoff(attempt int) time.Duration {
	maxDelay := BaseDelay * time.Duration(1<<(attempt-1))

	// full jitter, случайная задержка от 0 до maxDelay
	return time.Duration(rand.Int63n(int64(maxDelay)))
}

func ExecutePayment(ctx context.Context, client *http.Client, url string) error {
	for attempt := 1; attempt <= MaxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
		if err != nil {
			return err
		}

		resp, err := client.Do(req)

		if resp != nil {
			defer resp.Body.Close()
		}

		if err == nil && resp.StatusCode == 200 {
			fmt.Printf("Attempt %d: Success!\n", attempt)
			return nil
		}

		if err != nil {
			fmt.Printf("Attempt %d failed: %v\n", attempt, err)
		} else {
			fmt.Printf("Attempt %d failed: status %d\n", attempt, resp.StatusCode)
		}

		if !IsRetryable(resp, err) {
			return fmt.Errorf("non-retryable error")
		}

		if attempt == MaxRetries {
			break
		}

		delay := CalculateBackoff(attempt)
		fmt.Printf("Attempt %d failed: waiting %v...\n", attempt, delay)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return fmt.Errorf("payment failed after retries")
}

func main() {
	counter := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counter++

		if counter <= 3 {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"success"}`)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := ExecutePayment(ctx, server.Client(), server.URL)
	if err != nil {
		fmt.Println("Payment failed:", err)
	}
}
