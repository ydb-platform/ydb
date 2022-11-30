package cachedtvm_test

import (
	"context"
	"fmt"
	"time"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/library/go/yandex/tvm/cachedtvm"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmtool"
)

func ExampleNewClient_checkServiceTicket() {
	zlog, err := zap.New(zap.ConsoleConfig(log.DebugLevel))
	if err != nil {
		panic(err)
	}

	tvmClient, err := tvmtool.NewAnyClient(tvmtool.WithLogger(zlog))
	if err != nil {
		panic(err)
	}

	cachedTvmClient, err := cachedtvm.NewClient(
		tvmClient,
		cachedtvm.WithCheckServiceTicket(1*time.Minute, 1000),
	)
	if err != nil {
		panic(err)
	}
	defer cachedTvmClient.Close()

	ticketInfo, err := cachedTvmClient.CheckServiceTicket(context.TODO(), "3:serv:....")
	if err != nil {
		panic(err)
	}

	fmt.Println("ticket info: ", ticketInfo.LogInfo)
}
