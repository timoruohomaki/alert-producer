package main

// for more details, see:
// https://github.com/Azure/azure-sdk-for-go/blob/main/sdk/messaging/azeventhubs/example_producing_events_test.go

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables from .env file
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Retrieve environment variables for Event Hub
	connectionString := os.Getenv("EVENTHUB_CONNECTION_STRING")
	eventHubName := os.Getenv("EVENTHUB_NAME")

	if connectionString == "" || eventHubName == "" {
		log.Fatal("Event Hub connection string or name is not set in .env file")
	}

	// posting

	build := 4
	thisHost, _ := os.Hostname()
	buildEnv := os.Getenv("BUILD_ENV")

	fmt.Println()
	fmt.Println("=============================================")
	fmt.Println("=  Starting EventHub Alert Producer...      =")
	fmt.Println("=============================================")
	fmt.Println("  Build version:    ", strconv.Itoa(build))
	fmt.Println("  Host name:        ", thisHost)
	fmt.Println("  Target Event Hub: ", eventHubName)
	fmt.Println("  Environment:      ", buildEnv)
	fmt.Println("=============================================")
	fmt.Println()

	// Read the first JSON object from the file
	message, err := readFirstJSONObject("alerts.json")
	if err != nil {
		log.Fatalf("Failed to read JSON message: %v", err)
	}

	// Create Event Hub client
	client, err := azeventhubs.NewProducerClientFromConnectionString(connectionString, eventHubName, nil)

	if err != nil {
		log.Fatalf("Failed to create Event Hub client: %v", err)
	}

	defer client.Close(context.Background())

	// Send JSON message to Event Hub

	fmt.Println("Ready to send message batch to Event Hub.")

	err = sendMessageBatchToEventHub(client, message)

	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	fmt.Println("Message sent to Event Hub successfully.")
}

// ====== func main ends here =======

// readFirstJSONObject reads the first JSON object from an array in a JSON file

func readFirstJSONObject(filename string) ([]byte, error) {
	fileContent, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", filename, err)
	}

	// Parse the JSON content to an array of generic objects
	var jsonData []map[string]interface{}
	if err := json.Unmarshal(fileContent, &jsonData); err != nil {
		return nil, fmt.Errorf("file content is not a valid JSON array: %w", err)
	}

	// Check if the array has at least one object
	if len(jsonData) == 0 {
		return nil, fmt.Errorf("no objects found in JSON array")
	}

	// Marshal the first object back to JSON format
	// TODO: eventually will send one object on every run or something
	firstObject, err := json.Marshal(jsonData[0])
	if err != nil {
		return nil, fmt.Errorf("failed to marshal first JSON object: %w", err)
	}

	return firstObject, nil
}

// sendMessageToEventHub sends a JSON message to Azure Event Hub

func sendMessageBatchToEventHub(client *azeventhubs.ProducerClient, message []byte) error {

	// create event batch

	events := createEventsForSend()

	fmt.Println("Creating a batch with", len(events), "events.")

	// create batch object

	newBatchOptions := &azeventhubs.EventDataBatchOptions{}

	fmt.Println("Options set.")

	batch, err := client.NewEventDataBatch(context.Background(), newBatchOptions)

	if err != nil {
		fmt.Println("Failed to create new data batch: ", err)
		panic(err)
	}

	fmt.Println("Batch created.")

	fmt.Println("Producer sending %s events", len(events))

	for i := 0; i < len(events); i++ {

		err = batch.AddEventData(events[i], nil)

		if err != nil {
			fmt.Println("Failed to add event data to batch: ", err)
			panic(err)
		}
	}

	// Send the message batch to Event Hub
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = client.SendEventDataBatch(ctx, batch, nil)

	if err != nil {
		return fmt.Errorf("failed to send event batch: %w", err)
	}

	return nil

}

func createEventsForSend() []*azeventhubs.EventData {

	return []*azeventhubs.EventData{
		{
			Body: []byte("hello"),
		},
		{
			Body: []byte("world"),
		},
	}

}
