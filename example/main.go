package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/freifunkMUC/pg-events/pkg/pgevents"
)

type ExampleTable struct {
	ID        uint      `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func openGorm(connectionString string) (*gorm.DB, error) {
	db, err := gorm.Open(postgres.Open(connectionString), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	// Migrate the schema
	if err := db.AutoMigrate(&ExampleTable{}); err != nil {
		return nil, fmt.Errorf("failed to migrate the example table: %w", err)
	}

	return db, nil
}

func main() {
	logrus.SetLevel(logrus.DebugLevel)

	// The context bounds getting started, not the listener's life: it runs
	// until Close.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	connectionString := "host=localhost port=5432 sslmode=disable dbname=postgres user=postgres password=development"

	db, err := openGorm(connectionString)
	if err != nil {
		logrus.Fatal(err)
	}

	listener, err := pgevents.OpenListener(ctx, connectionString)
	if err != nil {
		logrus.Fatal(err)
	}
	defer func() { _ = listener.Close() }()

	if err := listener.Attach(ctx, "example_tables"); err != nil {
		logrus.Fatal(err)
	}

	listener.OnEvent(func(event *pgevents.TableEvent) {
		row := &ExampleTable{}
		if err := json.Unmarshal([]byte(event.Data), row); err == nil {
			fmt.Println(row.CreatedAt)
		}
	})

	listener.OnReconnect(func() {
		fmt.Println("reconnected")
	})

	for i := 0; ; i++ {
		r := db.Save(&ExampleTable{
			Name:      fmt.Sprintf("example-row-%d", i),
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		})
		if r.Error != nil {
			logrus.Error(r.Error)
		}
		time.Sleep(5 * time.Second)
	}
}
