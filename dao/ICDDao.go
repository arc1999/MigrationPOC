package dao

import (
	"MigrationPOC/db"
	"MigrationPOC/model"
	"context"
	log "github.com/sirupsen/logrus"
	gormbulk "github.com/t-tiger/gorm-bulk-insert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
)

type ICDDao struct {

}
func (d ICDDao)Paginate(startid int64,nperpage int64)  ([]model.ICDMongo, int64,error) {

	filter := bson.M{"id": bson.M{"$gt": startid}}
	options := options.Find()
	options.SetLimit(nperpage)
	db := db.GetMongoDB()
	cur, err := db.Collection(os.Getenv("DATA_MONGODB_DATABASE")).Find(context.TODO(), filter, options)
	if err != nil {
		return nil,0, err
	}
	defer cur.Close(context.TODO())
	var jobs []model.ICDMongo
	var id int64
	for cur.Next(context.TODO()) {
		var job model.ICDMongo

		err := cur.Decode(&job)
		if err != nil {
			return nil,0, err
		}
		id = job.ID
		jobs = append(jobs, job)
	}
	return jobs,id, nil
}
func (d ICDDao) GetCount() (int64,error){
	db := db.GetMongoDB()
	return db.Collection(os.Getenv("DATA_MONGODB_DATABASE")).CountDocuments(context.TODO(),bson.M{})
}
func (d ICDDao)  BulkInsert(Entity []model.ICD,nperpage int64)  error {
	sqldb := db.GetMysqlDB()
	b := make([]interface{}, len(Entity))
	for i := range Entity {
		b[i] = Entity[i]
	}
	err := gormbulk.BulkInsert(sqldb, b, int(nperpage))
	if err != nil {
		log.Printf("error in saving ICD")
		return err
	}
	return nil
}