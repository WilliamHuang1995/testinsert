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

	initialBalance  = 10 // The conceptual initial balance for the account
	numTransactions = 9  // Total number of transactions for the account
	decrementAmount = 1  // The amount to decrement in each transaction

	logicalAccountID = 1 // The ID of the single account being tested
)

// db is the single database connection for the application
var db *sql.DB

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

	// Set connection pool settings for this single database
	db.SetMaxOpenConns(20) // Maximum number of open connections
	db.SetMaxIdleConns(10) // Maximum number of idle connections
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute) // Close idle connections after this time

	// --- Database Schema Setup ---
	// Setup database: only transactions_history table, with a genesis entry
	if err = setupTransactionHistoryOnlyDatabase(); err != nil {
		log.Fatalf("Failed to set up database with only transaction history: %v", err)
	}

	log.Printf("--- Starting Simulation (History-Only Ledger) ---\n")
	log.Printf("Conceptual initial balance for account %d: %d.\n", logicalAccountID, initialBalance)
	log.Printf("Total transactions to perform: %d.\n", numTransactions)

	startTime := time.Now()

	// --- Concurrent Transaction Processing ---
	var wg sync.WaitGroup
	numWorkers := db.Stats().MaxOpenConnections // Use pool size as worker limit
	jobs := make(chan int, numTransactions)     // Buffered channel for jobs

	// Start worker goroutines
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for txNum := range jobs {
				err := processLedgerTransaction(txNum, logicalAccountID, decrementAmount) // Pass db implicit via global
				if err != nil {
					// Basic retry logic for transient errors like deadlock/invalid connection
					if strings.Contains(err.Error(), "Deadlock found") || strings.Contains(err.Error(), "invalid connection") {
						log.Printf("Worker %d, Transaction %d failed (transient error): %v. Retrying...", workerID, txNum, err)
						time.Sleep(50 * time.Millisecond)                                        // Short delay before retry
						err = processLedgerTransaction(txNum, logicalAccountID, decrementAmount) // Retry
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

	// Distribute transaction numbers to job channel
	for i := 0; i < numTransactions; i++ {
		jobs <- (i + 1)
	}
	close(jobs) // Close the channel to signal workers no more jobs will be sent

	wg.Wait() // Wait for all worker goroutines to complete their jobs
	duration := time.Since(startTime)
	log.Printf("All transactions completed in %s\n", duration)

	// --- Final Balance Verification ---
	// Get final balance by querying the latest from transaction history
	finalBal, err := getLatestRunningBalanceFromHistory(logicalAccountID)
	if err != nil {
		log.Fatalf("Failed to get final balance from history for account %d: %v", logicalAccountID, err)
	}

	fmt.Printf("\n--- Simulation Complete (History-Only Ledger) ---\n")
	fmt.Printf("Conceptual Initial Balance for Account %d: %d\n", logicalAccountID, initialBalance)
	fmt.Printf("Number of transactions attempted: %d\n", numTransactions)
	fmt.Printf("Expected final balance for Account %d: %d\n", logicalAccountID, initialBalance-numTransactions)
	fmt.Printf("Actual final balance from History for Account %d: %d\n", logicalAccountID, finalBal)

	// Calculate and print total successful entries, and check against expected
	successfulTxCount, err := getTransactionHistoryCount(logicalAccountID)
	if err != nil {
		log.Printf("Failed to get total history count: %v", err)
	} else {
		// Subtract 1 for the initial "genesis" entry
		actualSuccessfulDecrements := successfulTxCount - 1
		log.Printf("Total successful transaction entries in history for account %d: %d\n", logicalAccountID, actualSuccessfulDecrements)

		// A more robust check for this ledger-only model:
		// The final balance should equal initialBalance minus total successful decrements.
		if finalBal == initialBalance-(actualSuccessfulDecrements*decrementAmount) {
			log.Println("Result: History reflects correct total deductions and final balance!")
		} else {
			log.Println("Result: History MAY be inconsistent with final balance. Check logs for failures.")
		}
	}
}

// setupTransactionHistoryOnlyDatabase initializes only the transactions_history table
// with a starting "genesis" balance entry.
func setupTransactionHistoryOnlyDatabase() error {
	// Drop transactions_history table if it exists
	_, err := db.Exec(`DROP TABLE IF EXISTS transactions_history;`)
	if err != nil {
		return fmt.Errorf("failed to drop transactions_history table: %w", err)
	}

	// Create transactions_history table with the 'running_balance' column
	// Use BIGINT for primary key for high volumes
	// Use TIMESTAMP(6) for microseconds precision
	_, err = db.Exec(`
		CREATE TABLE transactions_history (
			id BIGINT AUTO_INCREMENT PRIMARY KEY,
			account_id INT NOT NULL,
			amount INT NOT NULL,               -- Amount of this specific transaction (e.g., -1 for a debit)
			running_balance INT NOT NULL,      -- Balance AFTER this transaction
			transaction_time TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create transactions_history table: %w", err)
	}
	// Add a composite index for querying latest balance efficiently
	_, err = db.Exec(`CREATE INDEX idx_transactions_history_account_id_time ON transactions_history(account_id, transaction_time DESC, id DESC);`)
	if err != nil {
		return fmt.Errorf("failed to create index on transactions_history: %w", err)
	}

	// Insert the initial "genesis" balance entry for the logicalAccountID
	// This record represents the starting point of the ledger.
	// For a genesis entry, the 'amount' could be the initial balance itself.
	_, err = db.Exec(`
		INSERT INTO transactions_history (account_id, amount, running_balance)
		VALUES (?, ?, ?);
	`, logicalAccountID, initialBalance, initialBalance)
	if err != nil {
		return fmt.Errorf("failed to insert genesis entry into transactions_history: %w", err)
	}
	log.Println("Database setup complete: 'transactions_history' table created with genesis entry.")
	return nil
}

// processLedgerTransaction handles a single transaction in a history-only ledger model.
// It queries for the latest balance, performs application-side calculation, and inserts a new record.
func processLedgerTransaction(txNum int, accountID int, debitAmount int) error {
	tx, err := db.Begin() // Start a transaction for atomicity of read-calculate-write
	if err != nil {
		return fmt.Errorf("transaction %d: failed to begin transaction: %w", txNum, err)
	}
	defer tx.Rollback() // Ensure rollback on error or if not explicitly committed

	var latestRunningBalance int
	// Step 1: Query for the latest running balance for the account_id.
	// Use FOR UPDATE to lock the relevant index records to prevent race conditions during this read-modify-write cycle.
	err = tx.QueryRow(
		"SELECT running_balance FROM transactions_history WHERE account_id = ? ORDER BY transaction_time DESC, id DESC LIMIT 1 FOR UPDATE",
		accountID,
	).Scan(&latestRunningBalance)

	// Determine the base balance for the calculation.
	// If no prior entries (shouldn't happen if genesis is always inserted first), use initialBalance.
	currentBaseBalance := initialBalance // Default if no rows found (e.g., first ever transaction for an account)
	if err != nil {
		if err == sql.ErrNoRows {
			// This case should ideally not happen for logicalAccountID=1 if genesis is inserted.
			// It would happen for other accountIDs if they don't have a genesis entry.
			log.Printf("Transaction %d: No prior history found for account %d. Starting from conceptual initial balance.", txNum, accountID)
		} else {
			return fmt.Errorf("transaction %d: failed to query latest running balance: %w", txNum, err)
		}
	} else {
		currentBaseBalance = latestRunningBalance
	}

	// Step 2: Application-side calculation and check
	newRunningBalance := currentBaseBalance - debitAmount
	if newRunningBalance < 0 { // Check if balance would drop below zero
		return fmt.Errorf("transaction %d: insufficient balance. Current: %d, Debit: %d. New balance would be %d", txNum, currentBaseBalance, debitAmount, newRunningBalance)
	}

	// Step 3: Create a new entry back into transaction history with the updated running balance
	// Store the debit as a negative amount in the 'amount' column.
	_, err = tx.Exec(
		"INSERT INTO transactions_history (account_id, amount, running_balance) VALUES (?, ?, ?)",
		accountID, -debitAmount, newRunningBalance,
	)
	if err != nil {
		return fmt.Errorf("transaction %d: failed to insert new history entry: %w", txNum, err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to commit: %w", txNum, err)
	}
	return nil
}

// getLatestRunningBalanceFromHistory retrieves the running_balance of the most recent transaction for a specific account.
func getLatestRunningBalanceFromHistory(accountID int) (int, error) {
	var balance int
	// Order by transaction_time DESC (most recent first) and then id DESC (to break ties for same timestamp)
	err := db.QueryRow("SELECT running_balance FROM transactions_history WHERE account_id = ? ORDER BY transaction_time DESC, id DESC LIMIT 1", accountID).Scan(&balance)
	if err != nil {
		if err == sql.ErrNoRows {
			// If no entries are found (e.g., table empty or account never had a transaction)
			return 0, fmt.Errorf("no history found for account %d", accountID)
		}
		return 0, fmt.Errorf("failed to get last running balance from history for account %d: %w", accountID, err)
	}
	return balance, nil
}

// getTransactionHistoryCount retrieves the total number of history entries for a specific account.
func getTransactionHistoryCount(accountID int) (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM transactions_history WHERE account_id = ?", accountID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to query history count for account %d: %w", accountID, err)
	}
	return count, nil
}
