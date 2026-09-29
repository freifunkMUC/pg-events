module github.com/freifunkMUC/pg-events/example

go 1.26.0

// the example always builds against the library in this repository
replace github.com/freifunkMUC/pg-events => ../

require (
	github.com/freifunkMUC/pg-events v0.0.0
	github.com/sirupsen/logrus v1.10.2
	gorm.io/driver/postgres v1.6.3
	gorm.io/gorm v1.31.2
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.10.0 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/lib/pq v1.12.3 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)
