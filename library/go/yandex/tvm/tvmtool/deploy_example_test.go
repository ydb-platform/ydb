package tvmtool_test

import (
	"context"
	"fmt"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/library/go/yandex/tvm"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmtool"
)

func ExampleNewDeployClient_simple() {
	zlog, err := zap.New(zap.ConsoleConfig(log.DebugLevel))
	if err != nil {
		panic(err)
	}

	tvmClient, err := tvmtool.NewDeployClient(tvmtool.WithLogger(zlog))
	if err != nil {
		panic(err)
	}

	ticket, err := tvmClient.GetServiceTicketForAlias(context.TODO(), "black-box")
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

func ExampleNewDeployClient_custom() {
	zlog, err := zap.New(zap.ConsoleConfig(log.DebugLevel))
	if err != nil {
		panic(err)
	}

	tvmClient, err := tvmtool.NewDeployClient(
		tvmtool.WithSrc("second_app"),
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
