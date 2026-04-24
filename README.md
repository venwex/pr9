# Practice 9 Go solutions

This folder contains two separate runnable Go programs:

- `task1` - resilient HTTP client with retry filtering, exponential backoff, full jitter, and `context.Context` support.
- `task2` - idempotency middleware demo with concurrent duplicate requests and cached response replay.

## Run

```bash
go run ./task1
go run ./task2
```

## What is implemented

### Task 1
- `IsRetryable(resp, err)`
- `CalculateBackoff(attempt)` with exponential backoff + full jitter
- `ExecutePayment(ctx, amount)` with immediate stop on context cancellation/timeout
- `httptest` payment gateway simulation:
  - first 3 requests return `503`
  - 4th request returns `200` with `{"status":"success"}`

### Task 2
- `Idempotency-Key` validation
- `409 Conflict` for request already in progress
- cached response returned for completed duplicate request
- simulated long-running business logic with `time.Sleep(2 * time.Second)`
- concurrent "double-click" attack using goroutines + `sync.WaitGroup`

## Important note

Task 2 is implemented with an in-memory store so the project runs immediately without external dependencies.
The middleware logic is production-ready in structure, but if your instructor strictly requires Redis or a database,
you should replace `MemoryStore` with a `RedisStore` or `DBStore` that keeps the same behavior.
