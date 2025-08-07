package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Product struct {
	ID       int      `json:"id"`
	Name     string   `json:"name"`
	Category string   `json:"category"`
	Price    float64  `json:"price"`
	Reviews  []string `json:"reviews,omitempty"`
}

var (
	rdb     *redis.Client
	mysqlDB *sql.DB
	mongoDB *mongo.Database
	ctx     = context.Background()
)

func initRedis() {
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		fmt.Println("❌ Could not connect to Redis:", err)
		return
	}

	fmt.Println("✅ Connected to Redis:", pong)
}

func initMySQL() {
	var err error
	mysqlDB, err = sql.Open("mysql", "root:2010@tcp(localhost:3306)/flipkart")
	if err != nil {
		log.Fatal("MySQL connection failed:", err)
	}
}

func initMongo() {
	clientOpts := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		log.Fatal("MongoDB connection failed:", err)
	}
	mongoDB = client.Database("flipkart")
}

func fetchFromMySQL(keyword string) ([]Product, error) {
	query := "SELECT id, name, category, price FROM products WHERE name LIKE ? LIMIT 10"
	rows, err := mysqlDB.Query(query, "%"+keyword+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var products []Product
	for rows.Next() {
		var p Product
		if err := rows.Scan(&p.ID, &p.Name, &p.Category, &p.Price); err != nil {
			continue
		}
		products = append(products, p)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("Row iteration error: %w", err)
	}

	return products, nil
}

func fetchMetadataFromMongo(productID int) ([]string, error) {
	coll := mongoDB.Collection("product_reviews")
	var result struct {
		ProductID int      `bson:"product_id"`
		Reviews   []string `bson:"reviews"`
	}
	filter := map[string]interface{}{"product_id": productID}
	err := coll.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		return nil, err
	}
	return result.Reviews, nil
}

func searchHandler(c *gin.Context) {
	keyword := c.Query("q")
	if keyword == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing search keyword"})
		return
	}

	// Check cache
	cacheKey := "search:" + keyword
	//rdb.Set(ctx, cacheKey, "Akhil", 0)
	cached, err := rdb.Get(ctx, cacheKey).Result()
	if err == redis.Nil {
		// Cache miss: Key does not exist
		fmt.Println("Cache miss")
	} else if err != nil {
		// Actual error while connecting to Redis
		fmt.Println("Redis error:", err)
	} else {
		// Cache hit
		fmt.Println("Cache hit")
		c.Data(http.StatusOK, "application/json", []byte(cached))
		return
	}

	// Cache miss - query MySQL
	fmt.Println(keyword)
	products, err := fetchFromMySQL(keyword)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	// Attach reviews from MongoDB
	for i := range products {
		reviews, _ := fetchMetadataFromMongo(products[i].ID)
		products[i].Reviews = reviews
	}

	// Serialize and cache response
	response, _ := json.Marshal(products)
	rdb.Set(ctx, cacheKey, response, 10*time.Minute)

	c.JSON(http.StatusOK, products)
}

func main() {
	initRedis()
	initMySQL()
	initMongo()

	r := gin.Default()
	r.GET("/search", searchHandler)
	r.Run(":8080")
}
