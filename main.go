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
	dbUser          = "testuser"     // Your MariaDB username
	dbPassword      = "testpassword" // Your MariaDB password
	dbHost          = "127.0.0.1"    // Your MariaDB host (e.g., localhost or an IP)
	dbPort          = "3306"         // Your MariaDB port
	dbName          = "testdb"       // Your MariaDB database name
	initialBalance  = 1000
	numTransactions = 999
	decrementAmount = 1 // The amount to decrement in each transaction

	logicalAccountID = 1 // The ID of the single account being tested for running balance
)

func main() {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", dbUser, dbPassword, dbHost, dbPort, dbName)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	log.Println("Successfully connected to MariaDB!")

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute) // Good practice

	if err = setupDatabase(db); err != nil {
		log.Fatalf("Failed to set up database: %v", err)
	}

	log.Printf("--- Starting Simulation with History Table and Running Balance --- \n")
	log.Printf("Initial balance for account %d set to %d.\n", logicalAccountID, initialBalance)
	log.Printf("Total transactions to perform: %d.\n", numTransactions)

	startTime := time.Now()

	var wg sync.WaitGroup
	numWorkers := db.Stats().MaxOpenConnections
	jobs := make(chan int, numTransactions)

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for txNum := range jobs {
				err := decrementBalanceWithHistory(db, txNum, logicalAccountID, decrementAmount)
				if err != nil {
					// Basic retry for transient errors
					if strings.Contains(err.Error(), "Deadlock found") || strings.Contains(err.Error(), "invalid connection") {
						log.Printf("Worker %d, Transaction %d failed (transient error): %v. Retrying...", workerID, txNum, err)
						time.Sleep(50 * time.Millisecond)
						err = decrementBalanceWithHistory(db, txNum, logicalAccountID, decrementAmount)
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

	finalBal, err := getBalance(db, logicalAccountID)
	if err != nil {
		log.Fatalf("Failed to get final balance for account %d: %v", logicalAccountID, err)
	}

	fmt.Printf("\n--- Simulation Complete ---\n")
	fmt.Printf("Initial Balance for Account %d: %d\n", logicalAccountID, initialBalance)
	fmt.Printf("Number of transactions: %d\n", numTransactions)
	fmt.Printf("Expected final balance for Account %d: %d\n", logicalAccountID, initialBalance-numTransactions)
	fmt.Printf("Actual final balance for Account %d: %d\n", logicalAccountID, finalBal)

	if finalBal == initialBalance-numTransactions {
		log.Println("Result: The final balance in 'accounts' table is correct!")
	} else {
		log.Println("Result: The final balance in 'accounts' table is INCORRECT. Concurrency issues may have occurred.")
	}

	count, err := getTransactionHistoryCount(db, logicalAccountID)
	if err != nil {
		log.Printf("Failed to get transaction history count: %v", err)
	} else {
		log.Printf("Total transaction history entries for account %d: %d\n", logicalAccountID, count)
	}

	// Verify the last recorded running balance in history
	lastHistoryBalance, err := getLastRunningBalanceFromHistory(db, logicalAccountID)
	if err != nil {
		log.Printf("Failed to get last running balance from history: %v", err)
	} else {
		log.Printf("Last recorded running balance in history for account %d: %d\n", logicalAccountID, lastHistoryBalance)
		if lastHistoryBalance == finalBal {
			log.Println("Consistency Check: Last history balance matches current running balance.")
		} else {
			log.Println("Consistency Check: Last history balance DOES NOT match current running balance! (This indicates an issue)")
		}
	}
}

// setupDatabase initializes the accounts and transactions_history tables.
func setupDatabase(db *sql.DB) error {
	// Drop accounts table if it exists
	_, err := db.Exec(`DROP TABLE IF EXISTS accounts;`)
	if err != nil {
		return fmt.Errorf("failed to drop accounts table: %w", err)
	}

	// Create accounts table (for running balance)
	_, err = db.Exec(`
		CREATE TABLE accounts (
			id INT PRIMARY KEY,
			balance INT NOT NULL
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create accounts table: %w", err)
	}

	// Insert initial balance for the logicalAccountID
	_, err = db.Exec(`
		INSERT INTO accounts (id, balance) VALUES (?, ?);
	`, logicalAccountID, initialBalance)
	if err != nil {
		return fmt.Errorf("failed to insert initial balance into accounts table: %w", err)
	}

	// Drop transactions_history table if it exists
	_, err = db.Exec(`DROP TABLE IF EXISTS transactions_history;`)
	if err != nil {
		return fmt.Errorf("failed to drop transactions_history table: %w", err)
	}

	// Create transactions_history table with the new 'running_balance' column
	_, err = db.Exec(`
		CREATE TABLE transactions_history (
			id INT AUTO_INCREMENT PRIMARY KEY,
			account_id INT NOT NULL,
			amount INT NOT NULL,
			running_balance INT NOT NULL,      -- NEW COLUMN
			transaction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create transactions_history table: %w", err)
	}
	// Add an index on account_id and transaction_time for faster lookups/ordering
	_, err = db.Exec(`CREATE INDEX idx_transactions_history_account_id_time ON transactions_history(account_id, transaction_time DESC);`)
	if err != nil {
		return fmt.Errorf("failed to create index on transactions_history: %w", err)
	}

	log.Println("Database setup complete: 'accounts' and 'transactions_history' tables created.")
	return nil
}

// decrementBalanceWithHistory updates the running balance and inserts a history record atomically.
func decrementBalanceWithHistory(db *sql.DB, txNum int, accountID int, amount int) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to begin transaction: %w", txNum, err)
	}
	defer tx.Rollback() // Rollback on error or if not committed

	// Step 1: Update the running balance in the accounts table
	result, err := tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ? AND balance >= ?", amount, accountID, amount)
	if err != nil {
		return fmt.Errorf("transaction %d: failed to update accounts table: %w", txNum, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to get rows affected from accounts update: %w", txNum, err)
	}

	if rowsAffected == 0 {
		// This means the WHERE condition (balance >= amount) was not met.
		return fmt.Errorf("transaction %d: insufficient balance or account %d not found", txNum, accountID)
	}

	// Step 2: Get the NEW current balance AFTER the update (within the same transaction)
	var currentBalance int
	err = tx.QueryRow("SELECT balance FROM accounts WHERE id = ?", accountID).Scan(&currentBalance)
	if err != nil {
		return fmt.Errorf("transaction %d: failed to read current balance after update: %w", txNum, err)
	}

	// Step 3: Insert a record into the transactions_history table with the running balance
	_, err = tx.Exec("INSERT INTO transactions_history (account_id, amount, running_balance) VALUES (?, ?, ?)", accountID, amount, currentBalance)
	if err != nil {
		return fmt.Errorf("transaction %d: failed to insert into transactions_history: %w", txNum, err)
	}

	// Step 4: Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to commit: %w", txNum, err)
	}
	return nil
}

// getBalance retrieves the current running balance for a specific account from the accounts table.
func getBalance(db *sql.DB, accountID int) (int, error) {
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
func getTransactionHistoryCount(db *sql.DB, accountID int) (int, error) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM transactions_history WHERE account_id = ?", accountID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to query history count for account %d: %w", accountID, err)
	}
	return count, nil
}

// getLastRunningBalanceFromHistory retrieves the running_balance of the most recent transaction for an account.
func getLastRunningBalanceFromHistory(db *sql.DB, accountID int) (int, error) {
	var balance int
	err := db.QueryRow("SELECT running_balance FROM transactions_history WHERE account_id = ? ORDER BY transaction_time DESC, id DESC LIMIT 1", accountID).Scan(&balance)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("no history found for account %d", accountID)
		}
		return 0, fmt.Errorf("failed to get last running balance from history for account %d: %w", accountID, err)
	}
	return balance, nil
}
