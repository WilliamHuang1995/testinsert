package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsm/redislock" // New: Redis distributed lock
	_ "github.com/go-sql-driver/mysql"
	goRedis "github.com/redis/go-redis/v9" // New: go-redis/v9 client
)

const (
	dbUser     = "testuser"
	dbPassword = "testpassword"
	dbHost     = "127.0.0.1"
	dbPort     = "3306"
	dbName     = "testdb"

	redisAddr = "localhost:6379"

	initialBalance  = 10
	numTransactions = 9
	decrementAmount = 1

	logicalAccountID = 1

	spendableBalanceCache = "account:balance" // Base key for Redis
)

var (
	mariadbDB   *sql.DB
	redisClient *goRedis.Client   // Changed to goRedis.Client for v9
	redisLocker *redislock.Client // New: Redis locker from bsm/redislock
	ctx         = context.Background()
)

// Define custom errors mirroring production code for clarity
var ErrInsufficientSpendableBalance = errors.New("insufficient spendable balance")
var ErrLockNotObtained = errors.New("lock not obtained")

func main() {
	// --- Redis Client and Locker Setup ---
	redisClient = goRedis.NewClient(&goRedis.Options{
		Addr: redisAddr,
		DB:   0,
	})
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Successfully connected to Redis!")

	redisLocker = redislock.New(redisClient) // Initialize the locker with the go-redis client

	// --- MariaDB Connection Setup ---
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", dbUser, dbPassword, dbHost, dbPort, dbName)

	mariadbDB, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to open MariaDB connection: %v", err)
	}
	defer mariadbDB.Close()   // Ensure connection is closed
	defer redisClient.Close() // Ensure Redis connection is closed (handles multiple connections if pooled internally)

	err = mariadbDB.Ping()
	if err != nil {
		log.Fatalf("Failed to connect to MariaDB: %v", err)
	}
	log.Println("Successfully connected to MariaDB!")

	// Set connection pool settings for MariaDB
	mariadbDB.SetMaxOpenConns(20)
	mariadbDB.SetMaxIdleConns(10)
	mariadbDB.SetConnMaxLifetime(5 * time.Minute)
	mariadbDB.SetConnMaxIdleTime(5 * time.Minute)

	// --- Database and Redis Initial State Setup ---
	// Setup MariaDB: only transactions_history table (no accounts table)
	if err = setupTransactionHistoryTableOnly(); err != nil {
		log.Fatalf("Failed to set up MariaDB transactions history table: %v", err)
	}

	// Setup Redis: Set the initial balance for the account
	if err = setupRedisInitialBalance(logicalAccountID, initialBalance); err != nil {
		log.Fatalf("Failed to set up initial balance in Redis: %v", err)
	}

	log.Printf("--- Starting Simulation (Redis Lock, MariaDB for History) ---\n")
	log.Printf("Conceptual initial balance for account %d in Redis: %d.\n", logicalAccountID, initialBalance)
	log.Printf("Total transactions to perform: %d.\n", numTransactions)

	startTime := time.Now()

	// --- Concurrent Transaction Processing ---
	var wg sync.WaitGroup
	// Use MariaDB pool size as worker limit, as each worker needs a DB connection for history
	numWorkers := mariadbDB.Stats().MaxOpenConnections
	jobs := make(chan int, numTransactions) // Buffered channel to send transaction numbers to workers

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for txNum := range jobs {
				err := processRedisLedgerTransaction(txNum, logicalAccountID, decrementAmount)
				if err != nil {
					// Basic retry for transient errors like deadlock/invalid connection/lock not obtained
					if strings.Contains(err.Error(), "Deadlock found") || errors.Is(err, ErrLockNotObtained) || strings.Contains(err.Error(), "invalid connection") {
						log.Printf("Worker %d, Transaction %d failed (transient error): %v. Retrying...", workerID, txNum, err)
						time.Sleep(50 * time.Millisecond)                                             // Short delay before retry
						err = processRedisLedgerTransaction(txNum, logicalAccountID, decrementAmount) // Retry the transaction
						if err != nil {
							log.Printf("Worker %d, Transaction %d failed (after retry): %v", workerID, txNum, err)
						}
					} else {
						log.Printf("Worker %d, Transaction %d failed: %v", workerID, txNum, err)
					}
				}
			}
		}(w)
	}

	// Distribute transaction numbers to the job channel
	for i := 0; i < numTransactions; i++ {
		jobs <- (i + 1)
	}
	close(jobs) // Close the channel to signal workers no more jobs will be sent

	wg.Wait() // Wait for all worker goroutines to complete their jobs
	duration := time.Since(startTime)
	log.Printf("All transactions completed in %s\n", duration)

	// --- Final Balance Verification ---
	finalRedisBal, err := getRedisBalance(logicalAccountID)
	if err != nil {
		log.Fatalf("Failed to get final balance from Redis for account %d: %v", logicalAccountID, err)
	}

	fmt.Printf("\n--- Simulation Complete (Redis Lock, MariaDB for History) ---\n")
	fmt.Printf("Conceptual Initial Balance: %d\n", initialBalance)
	fmt.Printf("Number of transactions attempted: %d\n", numTransactions)
	fmt.Printf("Expected final balance: %d\n", initialBalance-numTransactions)
	fmt.Printf("Actual final balance from Redis: %d\n", finalRedisBal)

	if finalRedisBal == initialBalance-numTransactions {
		log.Println("Result: The final balance in Redis is correct!")
	} else {
		log.Println("Result: The final balance in Redis is INCORRECT. Balance calculation issues may have occurred.")
	}

	successfulTxCount, err := getTransactionHistoryCount(logicalAccountID)
	if err != nil {
		log.Printf("Failed to get total history count from MariaDB: %v", err)
	} else {
		log.Printf("Total successful transaction entries in MariaDB history for account %d: %d\n", logicalAccountID, successfulTxCount)
		// Check if history count matches the number of attempted transactions
		if successfulTxCount == numTransactions {
			log.Println("Result: MariaDB history count matches total transactions!")
		} else {
			log.Println("Result: MariaDB history count DOES NOT match total transactions. Check Go logs for failures during MariaDB insert.")
		}
	}
}

// setupRedisInitialBalance initializes the account's balance in Redis.
func setupRedisInitialBalance(accountID int, balance int) error {
	key := fmt.Sprintf("%s:%d", spendableBalanceCache, accountID)
	err := redisClient.Set(ctx, key, balance, 0).Err() // 0 expiry means no expiry
	if err != nil {
		return fmt.Errorf("failed to set initial balance in Redis for account %d: %w", accountID, err)
	}
	log.Printf("Redis initial balance for account %d set to %d.", accountID, balance)
	return nil
}

// setupTransactionHistoryTableOnly initializes only the transactions_history table in MariaDB.
func setupTransactionHistoryTableOnly() error {
	_, err := mariadbDB.Exec(`DROP TABLE IF EXISTS transactions_history;`)
	if err != nil {
		return fmt.Errorf("failed to drop transactions_history table: %w", err)
	}

	_, err = mariadbDB.Exec(`
		CREATE TABLE transactions_history (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			account_id INT NOT NULL,
			amount INT NOT NULL,
			running_balance INT NOT NULL,
			transaction_time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create transactions_history table: %w", err)
	}
	_, err = mariadbDB.Exec(`CREATE INDEX idx_transactions_history_account_id_time ON transactions_history(account_id, transaction_time DESC, id DESC);`)
	if err != nil {
		return fmt.Errorf("failed to create index on transactions_history: %w", err)
	}

	log.Println("MariaDB setup complete: 'transactions_history' table created.")
	return nil
}

// processRedisLedgerTransaction handles a single transaction, first updating Redis using bsm/redislock, then MariaDB.
func processRedisLedgerTransaction(txNum int, accountID int, debitAmount int) error {
	cacheKey := fmt.Sprintf("%s:%d", spendableBalanceCache, accountID)
	// lockKey := fmt.Sprintf("cop_spendable_balance_lock_%d", accountID) // Simplified lock key from production code

	var newBalance float64 // Will hold the balance after successful Redis update

	// Call the separate function that mirrors production's redisLock logic
	processedBalance, err := deductWithRedisLock(ctx, cacheKey, accountID, float64(debitAmount))
	if err != nil {
		// This will return ErrInsufficientSpendableBalance or ErrLockNotObtained directly
		return err
	}
	newBalance = processedBalance // Use the balance returned by deductWithRedisLock

	// Record the transaction in MariaDB history
	// This is done in a separate MariaDB transaction for durability.
	mariaDbTx, err := mariadbDB.Begin()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to begin MariaDB transaction: %w", txNum, err)
	}
	defer mariaDbTx.Rollback() // Ensure rollback on error if insert/commit fails

	_, err = mariaDbTx.Exec(
		"INSERT INTO transactions_history (account_id, amount, running_balance) VALUES (?, ?, ?)",
		accountID, -debitAmount, newBalance, // Use newBalance obtained from successful Redis operation
	)
	if err != nil {
		// IMPORTANT: In a real system, if MariaDB fails here after Redis succeeded,
		// you would need a robust reconciliation mechanism (e.g., compensating transaction in Redis,
		// a message queue for retries, or dedicated audit/recovery processes)
		// to handle the eventual consistency between Redis and MariaDB.
		return fmt.Errorf("transaction %d: failed to insert into MariaDB history: %w", txNum, err)
	}

	err = mariaDbTx.Commit()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to commit MariaDB history: %w", txNum, err)
	}
	return nil // Transaction successfully completed
}

// deductWithRedisLock mirrors the core logic from your production code's Deduct function's redisLock branch.
// It uses bsm/redislock for distributed locking, performs the balance deduction in Redis.
// Returns the new running balance after deduction, or an error.
func deductWithRedisLock(ctx context.Context, cacheKey string, merchantID int, amount float64) (float64, error) {
	// Retrieve current balance from Redis BEFORE acquiring lock (optimistic read for info logging, but check is inside lock)
	runningBalanceBeforeLock, err := redisClient.Get(ctx, cacheKey).Float64()
	if err != nil {
		if err == goRedis.Nil { // Key not found, might be the very first transaction after setup
			runningBalanceBeforeLock = float64(initialBalance) // Assume initial balance if not found in cache
			// Attempt to set the initial balance in cache if it's the very first hit
			if err := redisClient.Set(ctx, cacheKey, runningBalanceBeforeLock, 0).Err(); err != nil {
				return 0, fmt.Errorf("failed to set initial balance in cache for first hit: %w", err)
			}
		} else {
			log.Printf("deductWithRedisLock: fail to get spendable balance from cache before lock (merchantID %d): %v", merchantID, err)
			return 0, err // Return other Redis errors
		}
	}
	// log.Printf("deductWithRedisLock: Initial GET (before lock) for merchant %d, balance: %.2f", merchantID, runningBalanceBeforeLock)

	lockKey := fmt.Sprintf("cop_spendable_balance_lock_%d", merchantID) // Simplified lock key from production code
	var redisLock *redislock.Lock
	retryCount := 1
	const maxRetries = 3 // Mirroring production code's retry limit

	for retryCount <= maxRetries { // Retry loop for obtaining lock
		// Obtain a lock with a short TTL (100ms) and no retry strategy for `locker.Obtain` itself, as we do manual retries.
		lock, err := redisLocker.Obtain(ctx, lockKey, 100*time.Millisecond, nil)
		if err != nil {
			if errors.Is(err, redislock.ErrNotObtained) {
				log.Printf("deductWithRedisLock: fail to obtain redis lock for %s, retrying (%d/%d)", lockKey, retryCount, maxRetries)
				if retryCount == maxRetries {
					log.Printf("deductWithRedisLock: fail to obtain redis lock after %d retries for %s: %v", maxRetries, lockKey, err)
					return 0, ErrLockNotObtained // Return custom error
				}
				retryCount++
				time.Sleep(50 * time.Millisecond) // Small delay before retry
				continue                          // Continue to next retry attempt
			}
			return 0, fmt.Errorf("deductWithRedisLock: unexpected error obtaining lock: %w", err) // Other unexpected Redis errors
		}
		redisLock = lock // Lock successfully obtained
		break            // Exit retry loop
	}

	// Ensure the lock is released when the function exits
	defer func() {
		if redisLock != nil {
			if err := redisLock.Release(ctx); err != nil {
				// Log the error but don't return it, as the main operation might have succeeded.
				log.Printf("deductWithRedisLock: failed to release redis lock %s: %v", lockKey, err)
			} else {
				// log.Printf("deductWithRedisLock: lock %s released successfully", lockKey)
			}
		}
	}()

	// log.Printf("deductWithRedisLock: process inside lock start for %s", lockKey)

	// Re-read balance INSIDE the lock for the definitive check and update
	// This is crucial. The `GET` outside the lock is just for initial info/debug.
	currentBalanceInsideLock, err := redisClient.Get(ctx, cacheKey).Float64()
	if err != nil {
		if err == goRedis.Nil { // Should not happen if initial SET succeeded or first-hit logic is robust.
			currentBalanceInsideLock = float64(initialBalance)
		} else {
			return 0, fmt.Errorf("failed to get balance inside lock for %s: %w", cacheKey, err)
		}
	}
	// log.Printf("deductWithRedisLock: Balance inside lock for %s: %.2f", lockKey, currentBalanceInsideLock)

	if amount > currentBalanceInsideLock {
		log.Printf("deductWithRedisLock: insufficient spendable balance (%.2f) for amount (%.2f) for merchant %d", currentBalanceInsideLock, amount, merchantID)
		return 0, ErrInsufficientSpendableBalance // Return custom error
	}

	// Perform the atomic decrement within the lock
	newRunningBalance, err := redisClient.IncrByFloat(ctx, cacheKey, amount*-1).Result() // Decrement by amount * -1
	if err != nil {
		return 0, fmt.Errorf("failed to IncrByFloat for %s: %w", cacheKey, err)
	}

	// log.Printf("deductWithRedisLock: updated balance in cache for %s. New balance: %.2f", cacheKey, newRunningBalance)
	return newRunningBalance, nil // Return the new balance
}

// getRedisBalance retrieves the current balance for a specific account directly from Redis.
func getRedisBalance(accountID int) (int, error) {
	key := fmt.Sprintf("%s:%d", spendableBalanceCache, accountID)
	val, err := redisClient.Get(ctx, key).Result()
	if err != nil {
		if err == goRedis.Nil {
			return 0, fmt.Errorf("account %d balance not found in Redis", accountID)
		}
		return 0, fmt.Errorf("failed to get balance from Redis for account %d: %w", accountID, err)
	}
	// Redis stores floats. Convert to int for comparison with initialBalance.
	balanceFloat, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to convert Redis balance '%s' to float: %w", val, err)
	}
	return int(balanceFloat), nil // Convert to int as your constants are int
}

// getTransactionHistoryCount retrieves the total number of history entries for a specific account from MariaDB.
func getTransactionHistoryCount(accountID int) (int, error) {
	var count int
	err := mariadbDB.QueryRow("SELECT COUNT(*) FROM transactions_history WHERE account_id = ?", accountID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to query history count for account %d: %w", accountID, err)
	}
	return count, nil
}
