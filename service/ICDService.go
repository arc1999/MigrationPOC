package service

import (
	 "MigrationPOC/dao"
	"MigrationPOC/transformer"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)
var d dao.ICDDao
type ICDService struct {

}

func (is ICDService) Migrate()  {
	totaldoc,err := d.GetCount()
	if err != nil {
		log.Fatal(err)
	}
	perpage:= os.Getenv("N_PER_PAGE")
	nperpage,err := strconv.ParseInt(perpage,10,64)
	if err != nil {
		log.Fatal(err)
	}

	var i int64
	var id int64
	for i*nperpage <totaldoc{
		micds,nextid,err := d.Paginate(id,nperpage)
		if err != nil {
			log.Fatal(err)
		}
		icds := transformer.Transform(micds)
		err = d.BulkInsert(icds,nperpage)
		if err != nil {
			log.Fatal(err)
		}
		id =nextid
		i++
	}
}