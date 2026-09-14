module github.com/freifunkMUC/pg-events/example

go 1.26.0

// the example always builds against the library in this repository
replace github.com/freifunkMUC/pg-events => ../

require (
	github.com/freifunkMUC/pg-events v0.0.0
	github.com/jinzhu/gorm v1.9.16
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.10.2
)

require (
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/lib/pq v1.12.3 // indirect
	golang.org/x/sys v0.48.0 // indirect
)
