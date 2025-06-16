package main

import (
	"database/sql"
	"fmt"
	"log"
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
)

func main() {
	// Construct the DSN (Data Source Name) for MariaDB
	// format: [username[:password]@][protocol[(address)]]/dbname[?param1=value1&paramN=valueN]
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", dbUser, dbPassword, dbHost, dbPort, dbName)

	// Open the database connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to open database connection: %v", err)
	}
	defer db.Close()

	// Ping the database to ensure connection is established
	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	log.Println("Successfully connected to MariaDB!")

	// Set connection pool settings
	db.SetMaxOpenConns(20)                 // Maximum number of open connections
	db.SetMaxIdleConns(10)                 // Maximum number of idle connections
	db.SetConnMaxLifetime(5 * time.Minute) // Connections can live for 5 minutes

	// Setup database: drop existing table, create new one, and initialize balance
	if err = setupDatabase(db); err != nil {
		log.Fatalf("Failed to set up database: %v", err)
	}

	log.Printf("--- Starting Simulation ---\n")
	log.Printf("Initial balance set to %d.\n", initialBalance)
	// Run concurrent transactions
	var wg sync.WaitGroup
	for i := 0; i < numTransactions; i++ {
		wg.Add(1)
		go func(txNum int) {
			defer wg.Done()
			err := decrementBalance(db, txNum, decrementAmount)
			if err != nil {
				log.Printf("Transaction %d failed: %v", txNum, err)
			}
		}(i + 1)
	}

	// Wait for all transactions to complete
	wg.Wait()

	// Check final balance
	finalBal, err := getBalance(db)
	if err != nil {
		log.Fatalf("Failed to get final balance: %v", err)
	}

	fmt.Printf("\n--- Simulation Complete ---\n")
	fmt.Printf("Initial Balance: %d\n", initialBalance)
	fmt.Printf("Number of transactions: %d\n", numTransactions)
	fmt.Printf("Expected final balance: %d\n", initialBalance-numTransactions)
	fmt.Printf("Actual final balance: %d\n", finalBal)

	if finalBal == initialBalance-numTransactions {
		log.Println("Result: The final balance is correct!")
	} else {
		log.Println("Result: The final balance is INCORRECT. Concurrency issues may have occurred.")
	}
}

// setupDatabase initializes the table and sets the initial balance for MariaDB.
func setupDatabase(db *sql.DB) error {
	// Drop table if it exists to ensure a clean start
	_, err := db.Exec(`DROP TABLE IF EXISTS accounts;`)
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// Create table
	_, err = db.Exec(`
		CREATE TABLE accounts (
			id INT PRIMARY KEY,
			balance INT NOT NULL
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert initial balance
	_, err = db.Exec(`
		INSERT INTO accounts (id, balance) VALUES (1, ?);
	`, initialBalance)
	if err != nil {
		return fmt.Errorf("failed to insert initial balance: %w", err)
	}
	log.Printf("Database setup complete. Initial balance set to %d.\n", initialBalance)
	return nil
}

// decrementBalance decrements the balance by 'amount' within a transaction for MariaDB.
func decrementBalance(db *sql.DB, txNum int, amount int) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction %d: %w", txNum, err)
	}
	defer tx.Rollback() // Rollback on error or if not committed

	// Use the atomic UPDATE with a WHERE clause to ensure balance >= amount
	result, err := tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = 1 AND balance >= ?", amount, amount)
	if err != nil {
		return fmt.Errorf("transaction %d: failed to update balance: %w", txNum, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to get rows affected: %w", txNum, err)
	}

	if rowsAffected == 0 {
		// This means the WHERE condition (balance >= amount) was not met.
		// So, the balance was insufficient or the account row didn't exist.
		// For this simulation, we consider it a "failure" for the decrement.
		return fmt.Errorf("transaction %d: insufficient balance or account not found", txNum)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("transaction %d: failed to commit: %w", txNum, err)
	}
	// fmt.Printf("Transaction %d committed successfully.\n", txNum) // Uncomment for verbose output
	return nil
}

// getBalance retrieves the current balance from the database for MariaDB.
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
