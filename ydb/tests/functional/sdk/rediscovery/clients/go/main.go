package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

func probe() error {
	endpoint, database := os.Getenv("YDB_ENDPOINT"), os.Getenv("YDB_DATABASE")
	if endpoint == "" || database == "" {
		return fmt.Errorf("YDB_ENDPOINT and YDB_DATABASE must be set")
	}
	ctx := context.Background()
	driver, err := ydb.Open(ctx, "grpc://"+endpoint+database,
		ydb.WithAnonymousCredentials(), ydb.WithDialTimeout(8*time.Second))
	if err != nil {
		return err
	}
	defer driver.Close(ctx)

	var request func(context.Context) error
	switch mode := os.Getenv("YDB_PROBE"); mode {
	case "", "scheme":
		request = func(callCtx context.Context) error {
			_, err := driver.Scheme().ListDirectory(callCtx, database)
			return err
		}
	case "topic-write":
		writer, err := driver.Topic().StartWriter(database+"/topic",
			topicoptions.WithWriterProducerID("probe"),
			topicoptions.WithWriterCodec(topictypes.CodecRaw),
			topicoptions.WithWriterWaitServerAck(true))
		if err != nil {
			return err
		}
		defer func() {
			closeCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			_ = writer.Close(closeCtx)
		}()
		request = func(callCtx context.Context) error {
			return writer.Write(callCtx, topicwriter.Message{Data: bytes.NewReader([]byte("probe"))})
		}
	default:
		return fmt.Errorf("unsupported YDB_PROBE %q", mode)
	}

	var lastErr error
	retryDeadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(retryDeadline) {
		callCtx, cancel := context.WithTimeout(ctx, min(2*time.Second, time.Until(retryDeadline)))
		lastErr = request(callCtx)
		cancel()
		if lastErr == nil {
			fmt.Println("OK")
			retryDeadline = time.Now().Add(8 * time.Second)
		} else {
			log.Print(lastErr)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return lastErr
}

func main() {
	if err := probe(); err != nil {
		log.Fatal(err)
	}
}
