package main

import (
	"context" // For Redis context
	"database/sql"
	"fmt"
	"log"
	"strconv" // For converting int to string for Redis keys
	"strings" // For error string matching
	"sync"
	"time"

	"github.com/go-redis/redis/v8"     // Redis client
	_ "github.com/go-sql-driver/mysql" // MariaDB/MySQL driver
)

const (
	dbUser     = "testuser"     // Your MariaDB username
	dbPassword = "testpassword" // Your MariaDB password
	dbHost     = "127.0.0.1"    // Your MariaDB host (e.g., localhost or an IP)
	dbPort     = "3306"         // Your MariaDB port
	dbName     = "testdb"       // Your MariaDB database name

	redisAddr = "localhost:6379" // Redis server address

	initialBalance  = 1000 // The conceptual initial balance for the account
	numTransactions = 999  // Total number of transactions for the account
	decrementAmount = 1    // The amount to decrement in each transaction

	logicalAccountID = 1 // The ID of the single account being tested

	// Lua script for atomic conditional decrement in Redis
	// KEYS[1] = account balance key (e.g., "account:balance:1")
	// ARGV[1] = amount to decrement
	// ARGV[2] = initialBalance (used if key doesn't exist yet, for first transaction)
	redisConditionalDecrementScript = `
        local current_balance = redis.call('GET', KEYS[1])

        if current_balance == false then -- Key does not exist, treat as initial state
            current_balance = tonumber(ARGV[2])
        else
            current_balance = tonumber(current_balance)
        end

        local new_balance = current_balance - tonumber(ARGV[1])

        if new_balance >= 0 then
            redis.call('SET', KEYS[1], new_balance)
            return new_balance -- Return the new balance if successful
        else
            return -1 -- Indicate insufficient funds
        end
    `
)

// Global database connection and Redis client
var (
	mariadbDB   *sql.DB
	redisClient *redis.Client
	ctx         = context.Background() // Context for Redis operations
)

func main() {
	// --- Redis Client Setup ---
	redisClient = redis.NewClient(&redis.Options{
		Addr: redisAddr,
		DB:   0, // Default DB
	})
	_, err := redisClient.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	log.Println("Successfully connected to Redis!")

	// --- MariaDB Connection Setup ---
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", dbUser, dbPassword, dbHost, dbPort, dbName)

	mariadbDB, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to open MariaDB connection: %v", err)
	}
	defer mariadbDB.Close()   // Ensure connection is closed
	defer redisClient.Close() // Ensure Redis connection is closed

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

	log.Printf("--- Starting Simulation (Redis for Balance, MariaDB for History) ---\n")
	log.Printf("Conceptual initial balance for account %d in Redis: %d.\n", logicalAccountID, initialBalance)
	log.Printf("Total transactions to perform: %d.\n", numTransactions)

	startTime := time.Now()

	// --- Concurrent Transaction Processing ---
	var wg sync.WaitGroup
	numWorkers := mariadbDB.Stats().MaxOpenConnections // Use MariaDB pool size as worker limit
	jobs := make(chan int, numTransactions)            // Buffered channel for jobs

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for txNum := range jobs {
				err := processRedisLedgerTransaction(txNum, logicalAccountID, decrementAmount)
				if err != nil {
					// Basic retry for transient errors like deadlock/invalid connection
					if strings.Contains(err.Error(), "Deadlock found") || strings.Contains(err.Error(), "invalid connection") || strings.Contains(err.Error(), "OOM command not allowed when used memory > 'maxmemory'") {
						log.Printf("Worker %d, Transaction %d failed (transient error): %v. Retrying...", workerID, txNum, err)
						time.Sleep(50 * time.Millisecond)                                             // Short delay before retry
						err = processRedisLedgerTransaction(txNum, logicalAccountID, decrementAmount) // Retry
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

	for i := 0; i < numTransactions; i++ {
		jobs <- (i + 1)
	}
	close(jobs)

	wg.Wait()
	duration := time.Since(startTime)
	log.Printf("All transactions completed in %s\n", duration)

	// --- Final Balance Verification ---
	// Get final balance from Redis
	finalRedisBal, err := getRedisBalance(logicalAccountID)
	if err != nil {
		log.Fatalf("Failed to get final balance from Redis for account %d: %v", logicalAccountID, err)
	}

	fmt.Printf("\n--- Simulation Complete (Redis for Balance, MariaDB for History) ---\n")
	fmt.Printf("Conceptual Initial Balance: %d\n", initialBalance)
	fmt.Printf("Number of transactions attempted: %d\n", numTransactions)
	fmt.Printf("Expected final balance: %d\n", initialBalance-numTransactions)
	fmt.Printf("Actual final balance from Redis: %d\n", finalRedisBal)

	if finalRedisBal == initialBalance-numTransactions {
		log.Println("Result: The final balance in Redis is correct!")
	} else {
		log.Println("Result: The final balance in Redis is INCORRECT. Balance calculation issues may have occurred.")
	}

	// Check MariaDB history count
	successfulTxCount, err := getTransactionHistoryCount(logicalAccountID)
	if err != nil {
		log.Printf("Failed to get total history count from MariaDB: %v", err)
	} else {
		log.Printf("Total successful transaction entries in MariaDB history for account %d: %d\n", logicalAccountID, successfulTxCount)
		// Expected history count should match numTransactions if all succeeded.
		if successfulTxCount == numTransactions {
			log.Println("Result: MariaDB history count matches total transactions!")
		} else {
			log.Println("Result: MariaDB history count DOES NOT match total transactions. Check Go logs for failures.")
		}
	}
}

// setupRedisInitialBalance initializes the account's balance in Redis.
func setupRedisInitialBalance(accountID int, balance int) error {
	key := fmt.Sprintf("account:balance:%d", accountID)
	err := redisClient.Set(ctx, key, balance, 0).Err() // 0 expiry means no expiry
	if err != nil {
		return fmt.Errorf("failed to set initial balance in Redis for account %d: %w", accountID, err)
	}
	log.Printf("Redis initial balance for account %d set to %d.", accountID, balance)
	return nil
}

// setupTransactionHistoryTableOnly initializes only the transactions_history table in MariaDB.
func setupTransactionHistoryTableOnly() error {
	// Drop transactions_history table if it exists
	_, err := mariadbDB.Exec(`DROP TABLE IF EXISTS transactions_history;`)
	if err != nil {
		return fmt.Errorf("failed to drop transactions_history table: %w", err)
	}

	// Create transactions_history table with the 'running_balance' column
	// 'amount' here is the specific transaction's value (debit as negative)
	// 'running_balance' is the balance after this transaction.
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
	// Add a composite index for querying latest balance efficiently if needed (though Redis is source now)
	// This index is still useful for auditing history by account.
	_, err = mariadbDB.Exec(`CREATE INDEX idx_transactions_history_account_id_time ON transactions_history(account_id, transaction_time DESC, id DESC);`)
	if err != nil {
		return fmt.Errorf("failed to create index on transactions_history: %w", err)
	}

	log.Println("MariaDB setup complete: 'transactions_history' table created.")
	return nil
}

// processRedisLedgerTransaction handles a single transaction, first updating Redis, then MariaDB.
func processRedisLedgerTransaction(txNum int, accountID int, debitAmount int) error {
	redisKey := fmt.Sprintf("account:balance:%d", accountID)

	// Step 1: Atomically update balance in Redis using a Lua script
	// This script returns the new balance if successful, or -1 for insufficient funds.
	result, err := redisClient.Eval(ctx, redisConditionalDecrementScript, []string{redisKey}, debitAmount, initialBalance).Result()
	if err != nil {
		// Handle specific Redis errors, e.g., script execution error, connection issues
		return fmt.Errorf("transaction %d: Redis script execution failed: %w", txNum, err)
	}

	newRedisBalance, ok := result.(int64)
	if !ok {
		return fmt.Errorf("transaction %d: Redis script returned unexpected type: %T", txNum, result)
	}

	if newRedisBalance == -1 {
		// Insufficient funds as returned by the Lua script
		return fmt.Errorf("transaction %d: insufficient balance in Redis. Debit: %d", txNum, debitAmount)
	}

	// Step 2: If Redis update was successful, record the transaction in MariaDB history
	// This is done in a separate MariaDB transaction for durability.
	mariaDbTx, err := mariadbDB.Begin()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to begin MariaDB transaction: %w", txNum, err)
	}
	defer mariaDbTx.Rollback() // Ensure rollback on error

	// Insert into transactions_history using the new balance from Redis
	_, err = mariaDbTx.Exec(
		"INSERT INTO transactions_history (account_id, amount, running_balance) VALUES (?, ?, ?)",
		accountID, -debitAmount, newRedisBalance, // Store debit as negative
	)
	if err != nil {
		return fmt.Errorf("transaction %d: failed to insert into MariaDB history: %w", txNum, err)
	}

	err = mariaDbTx.Commit()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to commit MariaDB history: %w", txNum, err)
	}
	return nil
}

// getRedisBalance retrieves the current balance for a specific account directly from Redis.
func getRedisBalance(accountID int) (int, error) {
	key := fmt.Sprintf("account:balance:%d", accountID)
	val, err := redisClient.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, fmt.Errorf("account %d balance not found in Redis", accountID)
		}
		return 0, fmt.Errorf("failed to get balance from Redis for account %d: %w", accountID, err)
	}
	balance, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("failed to convert Redis balance '%s' to int: %w", val, err)
	}
	return balance, nil
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
