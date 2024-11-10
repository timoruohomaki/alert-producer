package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
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
	err = sendMessageToEventHub(client, message)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	fmt.Println("Message sent to Event Hub successfully.")
}

// readFirstJSONObject reads the first JSON object from an array in a JSON file
func readFirstJSONObject(filename string) ([]byte, error) {
	fileContent, err := ioutil.ReadFile(filename)
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
	firstObject, err := json.Marshal(jsonData[0])
	if err != nil {
		return nil, fmt.Errorf("failed to marshal first JSON object: %w", err)
	}

	return firstObject, nil
}

// sendMessageToEventHub sends a JSON message to Azure Event Hub
func sendMessageToEventHub(client *azeventhubs.Client, message []byte) error {
	// Create a sender to send messages
	sender, err := client.NewProducerClient(nil)
	if err != nil {
		return fmt.Errorf("failed to create producer client: %w", err)
	}
	defer sender.Close(context.Background())

	// Create the message batch
	batch, err := sender.NewEventDataBatch(nil)
	if err != nil {
		return fmt.Errorf("failed to create event data batch: %w", err)
	}

	// Add the JSON message to the batch
	err = batch.AddEventData(&azeventhubs.EventData{
		Body: message,
	}, nil)
	if err != nil {
		return fmt.Errorf("failed to add event data: %w", err)
	}

	// Send the message batch to Event Hub
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = sender.SendEventBatch(ctx, batch, nil)
	if err != nil {
		return fmt.Errorf("failed to send event batch: %w", err)
	}

	return nil
}
