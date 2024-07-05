package main

import (
    "context"
    "database/sql"
    "encoding/json"
    "log"
    "net/http"
    "time"

    "go-mysql-redis-app/config"
    "go-mysql-redis-app/models"
    "go-mysql-redis-app/tracing"

    "github.com/gin-gonic/gin"
    "github.com/go-redis/redis/v8"
    "go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

var db *sql.DB
var redisClient *redis.Client

func main() {
    cleanup := tracing.InitTracer()
    defer cleanup()

    var err error
    db, err = config.InitDB()
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    redisClient = config.InitRedis()
    defer redisClient.Close()

    r := gin.Default()
    r.Use(otelgin.Middleware("go-mysql-redis-app"))

    r.GET("/", func(c *gin.Context) {
        c.String(http.StatusOK, "Hello World!")
    })

    r.GET("/user", getUsers)
    r.GET("/user/:username", getUser)
    r.POST("/user", createUser)
    r.PUT("/user/:username", updateUser)
    r.DELETE("/user/:username", deleteUser)

    r.Run(":5000")
}

func getUsers(c *gin.Context) {
    rows, err := db.Query("SELECT id, username, email FROM users")
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }
    defer rows.Close()

    var users []models.User
    for rows.Next() {
        var user models.User
        if err := rows.Scan(&user.ID, &user.Username, &user.Email); err != nil {
            c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
            return
        }
        users = append(users, user)
    }

    c.JSON(http.StatusOK, users)
}

func getUser(c *gin.Context) {
    username := c.Param("username")

    // Try to get from Redis
    val, err := redisClient.Get(context.Background(), username).Result()
    if err == nil {
        var user models.User
        json.Unmarshal([]byte(val), &user)
        c.JSON(http.StatusOK, user)
        return
    }

    // If not in Redis, get from MySQL
    var user models.User
    err = db.QueryRow("SELECT id, username, email FROM users WHERE username = ?", username).Scan(&user.ID, &user.Username, &user.Email)
    if err != nil {
        if err == sql.ErrNoRows {
            c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
        } else {
            c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        }
        return
    }

    // Cache in Redis
    userJSON, _ := json.Marshal(user)
    redisClient.Set(context.Background(), username, userJSON, time.Hour)

    c.JSON(http.StatusOK, user)
}

func createUser(c *gin.Context) {
    var user models.User
    if err := c.ShouldBindJSON(&user); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    result, err := db.Exec("INSERT INTO users (username, email) VALUES (?, ?)", user.Username, user.Email)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    id, _ := result.LastInsertId()
    user.ID = int(id)

    c.JSON(http.StatusCreated, user)
}

func updateUser(c *gin.Context) {
    username := c.Param("username")
    var user models.User
    if err := c.ShouldBindJSON(&user); err != nil {
        c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
        return
    }

    _, err := db.Exec("UPDATE users SET email = ? WHERE username = ?", user.Email, username)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    // Invalidate Redis cache
    redisClient.Del(context.Background(), username)

    c.JSON(http.StatusOK, user)
}

func deleteUser(c *gin.Context) {
    username := c.Param("username")

    result, err := db.Exec("DELETE FROM users WHERE username = ?", username)
    if err != nil {
        c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
        return
    }

    rowsAffected, _ := result.RowsAffected()
    if rowsAffected == 0 {
        c.JSON(http.StatusNotFound, gin.H{"error": "User not found"})
        return
    }

    // Invalidate Redis cache
    redisClient.Del(context.Background(), username)

    c.Status(http.StatusNoContent)
}