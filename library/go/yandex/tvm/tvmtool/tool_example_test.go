package tvmtool_test

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/library/go/core/log/zap"
	"github.com/ydb-platform/ydb/library/go/yandex/tvm"
	"github.com/ydb-platform/ydb/library/go/yandex/tvm/tvmtool"
)

func ExampleNewClient() {
	zlog, err := zap.New(zap.ConsoleConfig(log.DebugLevel))
	if err != nil {
		panic(err)
	}

	tvmClient, err := tvmtool.NewClient(
		"http://localhost:9000",
		tvmtool.WithAuthToken("auth-token"),
		tvmtool.WithSrc("my-cool-app"),
		tvmtool.WithLogger(zlog),
	)
	if err != nil {
		panic(err)
	}

	ticket, err := tvmClient.GetServiceTicketForAlias(context.Background(), "black-box")
	if err != nil {
		retryable := false
		if tvmErr, ok := err.(*tvm.Error); ok {
			retryable = tvmErr.Retriable
		}

		zlog.Fatal(
			"failed to get service ticket",
			log.String("alias", "black-box"),
			log.Error(err),
			log.Bool("retryable", retryable),
		)
	}
	fmt.Printf("ticket: %s\n", ticket)
}

func ExampleNewClient_backgroundServiceTicketsUpdate() {
	zlog, err := zap.New(zap.ConsoleConfig(log.DebugLevel))
	if err != nil {
		panic(err)
	}

	bgCtx, bgCancel := context.WithCancel(context.Background())
	defer bgCancel()

	tvmClient, err := tvmtool.NewClient(
		"http://localhost:9000",
		tvmtool.WithAuthToken("auth-token"),
		tvmtool.WithSrc("my-cool-app"),
		tvmtool.WithLogger(zlog),
		tvmtool.WithBackgroundUpdate(bgCtx),
	)
	if err != nil {
		panic(err)
	}

	ticket, err := tvmClient.GetServiceTicketForAlias(context.Background(), "black-box")
	if err != nil {
		retryable := false
		if tvmErr, ok := err.(*tvm.Error); ok {
			retryable = tvmErr.Retriable
		}

		zlog.Fatal(
			"failed to get service ticket",
			log.String("alias", "black-box"),
			log.Error(err),
			log.Bool("retryable", retryable),
		)
	}
	fmt.Printf("ticket: %s\n", ticket)
}
