package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3" // SQLite driver
)

const (
	dbName          = "balance_atomic.db"
	initialBalance  = 100
	decrementAmount = 1  // The amount to subtract in each transaction
	numTransactions = 99 // Trying to decrement 99 times
)

func main() {
	// 1. Clean up previous database file if it exists
	os.Remove(dbName)

	// 2. Open the database connection
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?_journal=WAL&_timeout=5000", dbName))
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	// 3. Create table and initialize balance
	if err = setupDatabase(db); err != nil {
		log.Fatalf("Failed to set up database: %v", err)
	}

	// 4. Run concurrent transactions
	var wg sync.WaitGroup
	successfulTx := make(chan bool, numTransactions) // Channel to count successful transactions

	for i := 0; i < numTransactions; i++ {
		wg.Add(1)
		go func(txNum int) {
			defer wg.Done()
			err := decrementBalanceAtomic(db, txNum, decrementAmount, successfulTx)
			if err != nil {
				log.Printf("Transaction %d failed: %v", txNum, err)
			}
		}(i + 1)
	}

	// Wait for all transactions to complete
	wg.Wait()
	close(successfulTx)

	// Count successful transactions
	countSuccessful := 0
	for success := range successfulTx {
		if success {
			countSuccessful++
		}
	}

	// 5. Check final balance
	finalBal, err := getBalance(db)
	if err != nil {
		log.Fatalf("Failed to get final balance: %v", err)
	}

	fmt.Printf("\n--- Simulation Complete ---\n")
	fmt.Printf("Initial Balance: %d\n", initialBalance)
	fmt.Printf("Number of attempted transactions: %d\n", numTransactions)
	fmt.Printf("Number of successful transactions: %d\n", countSuccessful)
	fmt.Printf("Expected final balance: %d\n", initialBalance-countSuccessful) // Expected based on successful ones
	fmt.Printf("Actual final balance: %d\n", finalBal)

	if finalBal == initialBalance-countSuccessful {
		fmt.Println("Result: The final balance is correct based on successful decrements!")
	} else {
		fmt.Println("Result: The final balance is INCORRECT. Concurrency issues may have occurred.")
	}
	fmt.Printf("Final balance should be > 0. Is it %t\n", finalBal > 0)
}

// setupDatabase initializes the table and sets the initial balance.
func setupDatabase(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS accounts (
			id INTEGER PRIMARY KEY,
			balance INTEGER NOT NULL
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	_, err = db.Exec(`
		INSERT INTO accounts (id, balance) VALUES (1, ?);
	`, initialBalance)
	if err != nil {
		return fmt.Errorf("failed to insert initial balance: %w", err)
	}
	log.Printf("Database setup complete. Initial balance set to %d.\n", initialBalance)
	return nil
}

// decrementBalanceAtomic decrements the balance by 'amount' only if the result is > 0.
// It also sends a signal to a channel if the transaction was successful.
func decrementBalanceAtomic(db *sql.DB, txNum int, amount int, successChan chan<- bool) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction %d: %w", txNum, err)
	}
	defer tx.Rollback() // Rollback on error or if not committed

	res, err := tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = 1 AND balance - ? > 0", amount, amount)
	if err != nil {
		return fmt.Errorf("transaction %d: failed to update balance: %w", txNum, err)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to get rows affected: %w", txNum, err)
	}

	if rowsAffected == 0 {
		// This means the WHERE condition was not met (balance was not sufficient)
		// Or the row ID didn't exist (less likely given our setup)
		// log.Printf("Transaction %d: update skipped, insufficient balance or condition not met.\n", txNum) // Uncomment for verbose output
		successChan <- false
	} else {
		// Update was successful
		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("transaction %d: failed to commit: %w", txNum, err)
		}
		// fmt.Printf("Transaction %d committed successfully.\n", txNum) // Uncomment for verbose output
		successChan <- true
	}
	return nil
}

// getBalance retrieves the current balance from the database.
func getBalance(db *sql.DB) (int, error) {
	var balance int
	err := db.QueryRow("SELECT balance FROM accounts WHERE id = 1").Scan(&balance)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, fmt.Errorf("account not found")
		}
		return 0, fmt.Errorf("failed to query balance: %w", err)
	}
	return balance, nil
}
