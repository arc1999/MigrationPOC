package model

import "time"

type ICDMongo struct {
	ID        int64     `json:"id" gorm:"column:id;type:bigint;"`
	DateCreated           time.Time `json:"dateCreated"`
	DateUpdated           time.Time `json:"dateUpdated"`
	CreatedBy             int64     `json:"createdBy"`
	UpdatedBy             int64     `json:"updatedBy"`
	CreatedByName         *string    `json:"createdByName"`
	UpdatedByName         *string    `json:"updatedByName"`
	Code                  string    `json:"code" validate:"required"`
	DiseaseChiefComplaint string    `json:"diseaseChiefComplaint" validate:"required"`
	IcdCode               *string    `json:"icdCode"`
	CommonTerms           []string  	`json:"commonTerms"`
}
