package main

import (
	"database/sql"
	"fmt"
	"log"
	"strconv" // Needed for converting int to string for dbName
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql" // MariaDB/MySQL driver
)

const (
	dbUser          = "testuser"      // Your MariaDB username
	dbPassword      = "testpassword"  // Your MariaDB password
	dbHost          = "127.0.0.1"     // Your MariaDB host (e.g., localhost or an IP)
	dbPort          = "3306"          // Your MariaDB port
	baseDbName      = "testdb_shard_" // Base name for sharded databases (e.g., testdb_shard_0, testdb_shard_1)
	numShards       = 4               // Number of database shards
	initialBalance  = 1000000         // Initial balance for THE single logical account
	numTransactions = 999999          // Total number of transactions for THE single logical account
	decrementAmount = 1               // The amount to decrement in each transaction

	// We are now simulating ONE logical account (ID=1) whose transactions are spread.
	// So, the actual account ID in each DB will be 1.
	logicalAccountID = 1 // The single logical account ID that is 'hot'
)

// Global slice to hold connections to each shard
var shards []*sql.DB

func main() {
	// Initialize connections to all shards
	shards = make([]*sql.DB, numShards)
	for i := 0; i < numShards; i++ {
		shardDbName := baseDbName + strconv.Itoa(i) // e.g., "testdb_shard_0", "testdb_shard_1"
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", dbUser, dbPassword, dbHost, dbPort, shardDbName)

		db, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Fatalf("Failed to open database connection for shard %d (%s): %v", i, shardDbName, err)
		}

		err = db.Ping()
		if err != nil {
			log.Fatalf("Failed to connect to database shard %d (%s): %v", i, shardDbName, err)
		}
		log.Printf("Successfully connected to MariaDB Shard %d (%s)!", i, shardDbName)

		// Set connection pool settings for each shard
		db.SetMaxOpenConns(50) // Adjusted based on previous conversation
		db.SetMaxIdleConns(10) // Adjusted based on previous conversation
		db.SetConnMaxLifetime(5 * time.Minute)
		db.SetConnMaxIdleTime(5 * time.Minute)

		shards[i] = db
	}
	defer closeShardConnections() // Ensure all connections are closed on exit

	// Setup databases on all shards (create tables and initial balance for logicalAccountID)
	// Each shard will have logicalAccountID, but with an initial balance that's a fraction of the total
	if err := setupShardsForSingleLogicalAccount(); err != nil {
		log.Fatalf("Failed to set up shards for single logical account: %v", err)
	}

	log.Printf("--- Starting Sharded Simulation for Logical Account %d ---\n", logicalAccountID)
	log.Printf("Initial total balance (conceptual): %d distributed over %d shards.\n", initialBalance, numShards)
	log.Printf("Total transactions to perform: %d.\n", numTransactions)

	startTime := time.Now()

	// Run concurrent transactions, distributing them across shards based on transaction number
	var wg sync.WaitGroup
	for i := 0; i < numTransactions; i++ {
		wg.Add(1)
		// For a single logical account, its transactions are distributed across shards.
		// The shard is chosen based on the transaction number, not the account ID itself.
		shardIndexForTx := i % numShards
		go func(txNum int, targetShardIndex int) {
			defer wg.Done()
			// This function will now decrement logicalAccountID on the determined shard
			err := decrementBalanceOnSpecificShard(targetShardIndex, logicalAccountID, decrementAmount)
			if err != nil {
				// Log errors more verbosely here to debug if it gets stuck
				log.Printf("Transaction %d (on Shard %d) failed for Account %d: %v", txNum, targetShardIndex, logicalAccountID, err)
			}
		}(i+1, shardIndexForTx)
	}

	wg.Wait()
	duration := time.Since(startTime)
	log.Printf("All transactions completed in %s\n", duration)

	// Verify final total balance by summing balances from all shards for logicalAccountID
	fmt.Printf("\n--- Verifying Total Balance for Logical Account %d ---\n", logicalAccountID)
	totalFinalBalance := 0
	var sumWg sync.WaitGroup
	var sumMutex sync.Mutex // Protect totalFinalBalance
	var sumErrs = make(chan error, numShards)

	for i := 0; i < numShards; i++ {
		sumWg.Add(1)
		go func(shardIndex int) {
			defer sumWg.Done()
			db := shards[shardIndex]
			var balance int
			err := db.QueryRow("SELECT balance FROM accounts WHERE id = ?", logicalAccountID).Scan(&balance)
			if err != nil {
				if err == sql.ErrNoRows {
					log.Printf("Account %d not found on shard %d.", logicalAccountID, shardIndex)
					sumErrs <- nil // Not an error that prevents sum
					return
				}
				sumErrs <- fmt.Errorf("failed to query balance for logical account %d on shard %d: %w", logicalAccountID, shardIndex, err)
				return
			}
			sumMutex.Lock()
			totalFinalBalance += balance
			sumMutex.Unlock()
		}(i)
	}
	sumWg.Wait()
	close(sumErrs)

	for err := range sumErrs {
		if err != nil {
			log.Fatalf("Error during final balance verification: %v", err)
		}
	}

	expectedFinalBalance := initialBalance - numTransactions
	fmt.Printf("Initial Balance (conceptual): %d\n", initialBalance)
	fmt.Printf("Number of transactions: %d\n", numTransactions)
	fmt.Printf("Expected final total balance: %d\n", expectedFinalBalance)
	fmt.Printf("Actual final total balance: %d\n", totalFinalBalance)

	if totalFinalBalance == expectedFinalBalance {
		log.Println("Result: The final total balance is correct!")
	} else {
		log.Println("Result: The final total balance is INCORRECT. Concurrency issues may have occurred across shards.")
	}

	fmt.Printf("\n--- Sharded Simulation Complete for Logical Account %d ---\n", logicalAccountID)
}

// closeShardConnections iterates through all shard connections and closes them.
func closeShardConnections() {
	for i, db := range shards {
		if db != nil {
			err := db.Close()
			if err != nil {
				log.Printf("Error closing connection to shard %d: %v", i, err)
			}
		}
	}
	log.Println("All shard connections closed.")
}

// setupShardsForSingleLogicalAccount initializes the database schema and a fraction
// of the initial balance on each shard for the single logicalAccountID.
func setupShardsForSingleLogicalAccount() error {
	var wg sync.WaitGroup
	var errs = make(chan error, numShards)

	balancePerShard := initialBalance / numShards
	remainderBalance := initialBalance % numShards // To handle uneven division

	for i := 0; i < numShards; i++ {
		wg.Add(1)
		go func(shardIndex int, db *sql.DB) {
			defer wg.Done()
			shardDbName := baseDbName + strconv.Itoa(shardIndex)
			log.Printf("Setting up shard %d (%s) for logical account %d...", shardIndex, shardDbName, logicalAccountID)

			// Drop table if it exists
			_, err := db.Exec(`DROP TABLE IF EXISTS accounts;`)
			if err != nil {
				errs <- fmt.Errorf("shard %d: failed to drop table: %w", shardIndex, err)
				return
			}

			// Create table
			_, err = db.Exec(`
				CREATE TABLE accounts (
					id INT PRIMARY KEY,
					balance INT NOT NULL
				);
			`)
			if err != nil {
				errs <- fmt.Errorf("shard %d: failed to create table: %w", shardIndex, err)
				return
			}

			// Insert logicalAccountID with its portion of the initial balance
			currentShardBalance := balancePerShard
			if shardIndex == 0 { // Add remainder to the first shard
				currentShardBalance += remainderBalance
			}

			_, err = db.Exec(`
				INSERT INTO accounts (id, balance) VALUES (?, ?);
			`, logicalAccountID, currentShardBalance)
			if err != nil {
				errs <- fmt.Errorf("shard %d: failed to insert initial balance for account %d: %w", shardIndex, logicalAccountID, err)
				return
			}
			log.Printf("Shard %d setup complete. Account %d balance: %d.", shardIndex, logicalAccountID, currentShardBalance)
		}(i, shards[i])
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			return err
		}
	}
	log.Println("All shards initialized for single logical account.")
	return nil
}

// decrementBalanceOnSpecificShard decrements the balance for logicalAccountID on a specific shard.
func decrementBalanceOnSpecificShard(shardIndex int, accountID int, amount int) error {
	db := shards[shardIndex] // Use the specific shard connection

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("shard %d, account %d: failed to begin transaction: %w", shardIndex, accountID, err)
	}
	defer tx.Rollback()

	// Use the atomic UPDATE with a WHERE clause to ensure balance >= amount
	// IMPORTANT: Now we are decrementing a portion of the balance on each shard.
	// This approach is *not* ACID-compliant for the total logical balance without
	// a distributed transaction coordinator. If a decrement on one shard fails,
	// the overall logical balance will be off. This test demonstrates scaling
	// individual shard operations.
	result, err := tx.Exec("UPDATE accounts SET balance = balance - ? WHERE id = ? AND balance >= ?", amount, accountID, amount)
	if err != nil {
		return fmt.Errorf("shard %d, account %d: failed to update balance: %w", shardIndex, accountID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("shard %d, account %d: failed to get rows affected: %w", shardIndex, accountID, err)
	}

	if rowsAffected == 0 {
		// This means the WHERE condition (balance >= amount) was not met.
		// For a single logical account distributed, this means that specific shard's portion
		// of the balance ran out.
		return fmt.Errorf("shard %d, account %d: insufficient balance on this shard or account not found", shardIndex, accountID)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("shard %d, account %d: failed to commit: %w", shardIndex, accountID, err)
	}
	return nil
}
