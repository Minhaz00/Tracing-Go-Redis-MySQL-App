package config

import (
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
)

func InitDB() (*sql.DB, error) {
    db, err := sql.Open("mysql", "my_user:my_password@tcp(localhost:3306)/my_db")
    if err != nil {
        return nil, err
    }
    
    // Create the users table if it doesn't exist
    if err := createUsersTable(db); err != nil {
        return nil, err
    }
    
    return db, nil
}

func createUsersTable(db *sql.DB) error {
    query := `
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(255) NOT NULL UNIQUE,
        email VARCHAR(255) NOT NULL
    )
    `
    
    _, err := db.Exec(query)
    return err
}