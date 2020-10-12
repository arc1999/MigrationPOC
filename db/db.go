package db

import (
	"MigrationPOC/model"
	"context"
	"github.com/jinzhu/gorm"
	"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"time"

	// mysql dialect
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

var mysqldb *gorm.DB
var mongodb *mongo.Database

//Init -
func init() {
	err := godotenv.Load(".env")

	if err != nil {
		log.Fatalf("Error loading .env file")
	}
	dbURL := os.Getenv("SQL_DATABASE_URL")
	log.Println(dbURL)
	mysqldb, err = gorm.Open("mysql", dbURL)
	if err != nil {
		log.Errorln("in side error")
		log.Fatalln(err.Error())
	}
	log.Println(mysqldb)
	mysqldb.AutoMigrate(model.ICD{})
	log.Println("My sql connection done")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(
		os.Getenv("DATA_MONGODB_URI"),
	))

	if err != nil {
		log.Fatal(err)
	}
	mongodb = client.Database(os.Getenv("DATA_MONGODB_DATABASE"))
	log.Println("MongoDb Connected")
}

//GetDB - return Db instance
func GetMysqlDB() *gorm.DB {
	return mysqldb
}

//GetDB - return Db instance
func GetMongoDB() *mongo.Database {
	return mongodb
}
