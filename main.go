package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gabriel-vasile/mimetype"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func walkDir(dir string, rabbitChannel *amqp.Channel, dateLimit time.Time, wg *sync.WaitGroup) {
	defer wg.Done()

	visit := func(path string, f os.FileInfo, err error) error {
		if f.IsDir() && path != dir {
			wg.Add(1)
			go walkDir(path, rabbitChannel, dateLimit, wg)
			return filepath.SkipDir
		}
		if f.Mode().IsRegular() {
			modTime := f.ModTime()
			if !modTime.After(dateLimit) {
				return nil
			}

			mime, _ := mimetype.DetectFile(path)

			body := fmt.Sprintf(
				"{\"path\": \"%s\", \"updated_at\": \"%s\", \"mime\": \"%s\"}",
				path,
				modTime.Local().String(),
				mime.String(),
			)

			rabbitChannel.Publish(
				"",
				"walks",
				false,
				false,
				amqp.Publishing{
					DeliveryMode: amqp.Transient,
					ContentType:  "application/json",
					Body:         []byte(body),
				})
		}
		return nil
	}

	filepath.Walk(dir, visit)
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	root := "/home/ido"
	dateLimit := time.Now().Add(time.Hour * -1 * 24 * 365 * 10)

	var conn *amqp.Connection
	var err error
	for i := 0; i < 5; i++ {
		conn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
		if err == nil {
			break
		} else {
			log.Error().Msg("Could not connect to rabbitmq, retrying in 5 seconds...")
			time.Sleep(time.Second * 5)
		}
	}

	if err != nil {
		log.Fatal().Msg("Could never reach rabbitmq")
	}
	defer conn.Close()

	rabbitChannel, err := conn.Channel()
	if err != nil {
		log.Fatal().Msg("Could not create channel")
	}
	defer rabbitChannel.Close()

	_, err = rabbitChannel.QueueDeclare(
		"walks", // name
		true,    // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		log.Fatal().Msg("Could not create queue")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	walkDir(root, rabbitChannel, dateLimit, &wg)
	wg.Wait()
}
