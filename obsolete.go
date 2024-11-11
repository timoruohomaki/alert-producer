package obsolete

// area for snippets not actually used anywhere

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs"
)

func sendMessageToEventHub(client *azeventhubs.ProducerClient, message []byte) error {

	// old code to be replaced

	// Create a sender to send messages
	sender, err := client.New(nil)
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
