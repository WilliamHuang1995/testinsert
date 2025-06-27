package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings" // For error string matching
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql" // MariaDB/MySQL driver
)

const (
	dbUser     = "testuser"     // Your MariaDB username
	dbPassword = "testpassword" // Your MariaDB password
	dbHost     = "127.0.0.1"    // Your MariaDB host (e.g., localhost or an IP)
	dbPort     = "3306"         // Your MariaDB port
	dbName     = "testdb"       // Your MariaDB database name

	initialBalance  = 1000 // The initial balance for the account in the 'accounts' table
	numTransactions = 999  // Total number of transactions to simulate
	decrementAmount = 1    // The amount to decrement in each transaction

	logicalAccountID = 1 // The ID of the single account being tested
)

// db is the single database connection for the application
var db *sql.DB

// --- Asynchronous Balance Update Queue & Job Type ---
// BalanceUpdateJob represents a task to retroactively update a history entry's running_balance
type BalanceUpdateJob struct {
	HistoryID      int64 // The ID of the transactions_history record to update
	AccountID      int   // The account ID associated with the transaction
	RunningBalance int   // The correct running balance *at the time of the transaction*
}

// Channel for the asynchronous job queue
var balanceUpdateJobs chan BalanceUpdateJob

// WaitGroup for the background worker to ensure all jobs are processed before main exits
var workerWg sync.WaitGroup

func main() {
	// --- Database Connection Setup ---
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", dbUser, dbPassword, dbHost, dbPort, dbName)

	var err error
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close() // Ensure the connection is closed when main exits

	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	log.Println("Successfully connected to MariaDB!")

	// Set connection pool settings
	db.SetMaxOpenConns(20) // Maximum number of open connections
	db.SetMaxIdleConns(10) // Maximum number of idle connections
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute) // Close idle connections after this time

	// --- Database Schema Setup ---
	if err = setupDatabase(); err != nil {
		log.Fatalf("Failed to set up database: %v", err)
	}

	// --- Initialize Asynchronous Queue and Worker ---
	balanceUpdateJobs = make(chan BalanceUpdateJob, numTransactions) // Buffered channel
	workerWg.Add(1)                                                  // Add 1 for the single worker goroutine
	go balanceUpdateWorker()                                         // Start the background worker

	log.Printf("--- Starting Simulation (Accounts for Balance, Async History Update) ---\n")
	log.Printf("Initial balance for account %d set to %d.\n", logicalAccountID, initialBalance)
	log.Printf("Total transactions to perform: %d.\n", numTransactions)

	startTime := time.Now()

	// --- Concurrent Transaction Processing (Primary Path) ---
	var primaryTxWg sync.WaitGroup                     // Use a separate WG for primary transactions
	numPrimaryWorkers := db.Stats().MaxOpenConnections // Limit primary path workers by DB connections
	jobs := make(chan int, numTransactions)            // Job channel for primary transactions

	for w := 0; w < numPrimaryWorkers; w++ {
		primaryTxWg.Add(1)
		go func(workerID int) {
			defer primaryTxWg.Done()
			for txNum := range jobs {
				err := processTransactionAndQueueHistoryUpdate(txNum, logicalAccountID, decrementAmount)
				if err != nil {
					// Basic retry for transient errors
					if strings.Contains(err.Error(), "Deadlock found") || strings.Contains(err.Error(), "invalid connection") {
						log.Printf("Worker %d, Transaction %d failed (transient error): %v. Retrying...", workerID, txNum, err)
						time.Sleep(50 * time.Millisecond)                                                       // Short delay before retry
						err = processTransactionAndQueueHistoryUpdate(txNum, logicalAccountID, decrementAmount) // Retry
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

	// Distribute transaction numbers to primary job channel
	for i := 0; i < numTransactions; i++ {
		jobs <- (i + 1)
	}
	close(jobs) // Close primary job channel

	primaryTxWg.Wait() // Wait for all primary transaction processing to complete
	duration := time.Since(startTime)
	log.Printf("Primary transactions completed in %s\n", duration)

	// --- Signal background worker to finish and wait for it ---
	close(balanceUpdateJobs) // Close the job queue to signal the worker to exit
	workerWg.Wait()          // Wait for the background worker to finish processing all jobs
	log.Println("All background balance history updates processed.")

	// --- Final Balance Verification ---
	finalBal, err := getBalanceFromAccounts(logicalAccountID)
	if err != nil {
		log.Fatalf("Failed to get final balance from accounts table for account %d: %v", logicalAccountID, err)
	}

	fmt.Printf("\n--- Simulation Complete ---\n")
	fmt.Printf("Initial Balance for Account %d: %d\n", logicalAccountID, initialBalance)
	fmt.Printf("Number of transactions: %d\n", numTransactions)
	fmt.Printf("Expected final balance (assuming all succeed): %d\n", initialBalance-numTransactions)
	fmt.Printf("Actual final balance from Accounts table for Account %d: %d\n", logicalAccountID, finalBal)

	if finalBal == initialBalance-numTransactions {
		log.Println("Result: The final balance in 'accounts' table is correct!")
	} else {
		log.Println("Result: The final balance in 'accounts' table is INCORRECT. Concurrency issues may have occurred.")
	}

	// For history count, we expect it to match numTransactions after both phases complete.
	successfulHistoryEntries, err := getTransactionHistoryCount(logicalAccountID)
	if err != nil {
		log.Printf("Failed to get total history count: %v", err)
	} else {
		log.Printf("Total transaction history entries recorded: %d\n", successfulHistoryEntries)
		// Check if history count matches the number of attempted transactions.
		if successfulHistoryEntries == numTransactions {
			log.Println("Consistency Check: History count matches total transactions attempted.")
		} else {
			log.Println("Consistency Check: History count DOES NOT match total transactions attempted. (Look for errors in primary or worker logs)")
		}
	}

	// Check the running_balance in history (it should now be filled in)
	lastHistoryBalance, err := getLastRunningBalanceFromHistory(logicalAccountID)
	if err != nil {
		log.Printf("Failed to get last running balance from history: %v", err)
	} else {
		log.Printf("Last recorded running balance in history for account %d: %d\n", logicalAccountID, lastHistoryBalance)
		if lastHistoryBalance == finalBal {
			log.Println("Consistency Check: Last history balance matches current accounts balance after async update.")
		} else {
			log.Println("Consistency Check: Last history balance DOES NOT match current accounts balance! (Async update issue)")
		}
	}
}

// setupDatabase initializes both the 'accounts' table (for running balance)
// and 'transactions_history' table (for the immutable log).
func setupDatabase() error {
	// --- Setup 'accounts' table ---
	_, err := db.Exec(`DROP TABLE IF EXISTS accounts;`)
	if err != nil {
		return fmt.Errorf("failed to drop accounts table: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE accounts (
			id INT PRIMARY KEY,
			balance INT NOT NULL
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create accounts table: %w", err)
	}

	_, err = db.Exec(`
		INSERT INTO accounts (id, balance) VALUES (?, ?);
	`, logicalAccountID, initialBalance)
	if err != nil {
		return fmt.Errorf("failed to insert initial balance into accounts table: %w", err)
	}
	log.Println("'accounts' table created and initialized.")

	// --- Setup 'transactions_history' table (immutable log) ---
	// Its running_balance column will be updated asynchronously.
	_, err = db.Exec(`DROP TABLE IF EXISTS transactions_history;`)
	if err != nil {
		return fmt.Errorf("failed to drop transactions_history table: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE transactions_history (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			account_id INT NOT NULL,
			amount INT NOT NULL,
			-- running_balance will be updated asynchronously, so it can be NULL initially
			running_balance INT NULL,
			transaction_time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create transactions_history table: %w", err)
	}
	// Add an index on account_id and transaction_time for efficient querying of history.
	_, err = db.Exec(`CREATE INDEX idx_transactions_history_account_id_time ON transactions_history(account_id, transaction_time DESC, id DESC);`)
	if err != nil {
		return fmt.Errorf("failed to create index on transactions_history: %w", err)
	}

	log.Println("'transactions_history' table created.")
	return nil
}

// processTransactionAndQueueHistoryUpdate implements the synchronous steps (1, 2, 4)
// and queues the asynchronous step (3).
func processTransactionAndQueueHistoryUpdate(txNum int, accountID int, amount int) error {
	// Start a database transaction for atomicity across 'accounts' and 'transactions_history' tables.
	sqlTx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to begin SQL transaction: %w", txNum, err)
	}
	defer sqlTx.Rollback() // Ensure rollback on error or if not explicitly committed

	// Step 1: Update the running balance in the accounts table (synchronous, immediate check)
	result, err := sqlTx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ? AND balance >= ?", amount, accountID, amount)
	if err != nil {
		return fmt.Errorf("transaction %d: failed to update accounts table: %w", txNum, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to get rows affected from accounts update: %w", txNum, err)
	}

	if rowsAffected == 0 {
		// Insufficient funds, so no primary transaction happens. Rollback is implicit via defer.
		return fmt.Errorf("transaction %d: insufficient balance or account %d not found", txNum, accountID)
	}

	// Get the NEW current balance from 'accounts' after the update, within the same transaction.
	// This is the balance that should be retroactively put into history.
	var balanceAtTx int
	err = sqlTx.QueryRow("SELECT balance FROM accounts WHERE id = ?", accountID).Scan(&balanceAtTx)
	if err != nil {
		return fmt.Errorf("transaction %d: failed to read current balance after accounts update: %w", txNum, err)
	}

	// Step 2: Insert a record into the transactions_history table (initial log, running_balance is NULL)
	// We capture the insert ID to use it for the later asynchronous update.
	res, err := sqlTx.Exec(
		"INSERT INTO transactions_history (account_id, amount) VALUES (?, ?)",
		accountID, -amount, // Store debit as negative amount
	)
	if err != nil {
		return fmt.Errorf("transaction %d: failed to insert into transactions_history: %w", txNum, err)
	}

	// Get the ID of the newly inserted history record
	historyID, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to get last insert ID for history: %w", txNum, err)
	}

	// Step 4: Commit the main transaction
	err = sqlTx.Commit()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to commit primary transaction: %w", txNum, err)
	}

	// Step 3: Queue a job in the background to retroactively update the transaction history
	// This happens *after* the primary transaction commits, so it's truly asynchronous.
	balanceUpdateJobs <- BalanceUpdateJob{
		HistoryID:      historyID,
		AccountID:      accountID,
		RunningBalance: balanceAtTx, // The balance captured at the time of the transaction
	}

	return nil
}

// balanceUpdateWorker consumes jobs from the queue and updates the running_balance in transactions_history.
// This runs asynchronously in the background (Step 5).
func balanceUpdateWorker() {
	defer workerWg.Done() // Signal completion when the worker goroutine exits

	for job := range balanceUpdateJobs {
		// We use a new transaction here because this is a separate, asynchronous operation.
		// It's important to not share the parent's transaction.
		tx, err := db.Begin()
		if err != nil {
			log.Printf("Background Worker: Failed to begin transaction for job %v: %v", job, err)
			// In a real system, you'd log this error and potentially push the job to a dead-letter queue.
			continue
		}

		// Update the running_balance column in the specific history entry
		_, err = tx.Exec(
			"UPDATE transactions_history SET running_balance = ? WHERE id = ?",
			job.RunningBalance, job.HistoryID,
		)
		if err != nil {
			tx.Rollback() // Rollback if update fails
			log.Printf("Background Worker: Failed to update history ID %d with balance %d: %v", job.HistoryID, job.RunningBalance, err)
			// Again, dead-letter queue or retry logic needed here in production.
			continue
		}

		err = tx.Commit() // Commit the background update
		if err != nil {
			log.Printf("Background Worker: Failed to commit transaction for job %v: %v", job, err)
			// Dead-letter queue.
		}
	}
	log.Println("Background balance update worker finished.")
}

// getBalanceFromAccounts retrieves the current running balance for a specific account from the 'accounts' table.
func getBalanceFromAccounts(accountID int) (int, error) {
	var balance int
	err := db.QueryRow("SELECT balance FROM accounts WHERE id = ?", accountID).Scan(&balance)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("account %d not found in accounts table", accountID)
		}
		return 0, fmt.Errorf("failed to query balance for account %d: %w", accountID, err)
	}
	return balance, nil
}

// getTransactionHistoryCount retrieves the total number of history entries for a specific account.
func getTransactionHistoryCount(accountID int) (int, error) {
	var count int
	// Count entries regardless of whether running_balance is set
	err := db.QueryRow("SELECT COUNT(*) FROM transactions_history WHERE account_id = ?", accountID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to query history count for account %d: %w", accountID, err)
	}
	return count, nil
}

// getLastRunningBalanceFromHistory retrieves the running_balance of the most recent transaction for an account.
// It will only consider entries where running_balance is NOT NULL.
func getLastRunningBalanceFromHistory(accountID int) (int, error) {
	var balance sql.NullInt64 // Use sql.NullInt64 to handle potentially NULL running_balance
	err := db.QueryRow(
		"SELECT running_balance FROM transactions_history WHERE account_id = ? AND running_balance IS NOT NULL ORDER BY transaction_time DESC, id DESC LIMIT 1",
		accountID,
	).Scan(&balance)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("no history with calculated running balance found for account %d", accountID)
		}
		return 0, fmt.Errorf("failed to get last running balance from history for account %d: %w", accountID, err)
	}
	if !balance.Valid {
		return 0, fmt.Errorf("last history entry for account %d has NULL running_balance (should not happen if worker completed)", accountID)
	}
	return int(balance.Int64), nil
}
