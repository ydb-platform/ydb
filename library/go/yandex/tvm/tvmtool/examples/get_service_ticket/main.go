package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/zap"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmtool"
)

var (
	baseURI = "http://localhost:3000"
	dst     = "dst"
)

func main() {
	flag.StringVar(&baseURI, "tool-uri", baseURI, "TVM tool uri")
	flag.StringVar(&dst, "dst", dst, "Destination TVM app (must be configured in tvm-tool)")
	flag.Parse()

	zlog, err := zap.New(zap.ConsoleConfig(log.DebugLevel))
	if err != nil {
		panic(err)
	}

	auth := os.Getenv("TVMTOOL_LOCAL_AUTHTOKEN")
	if auth == "" {
		zlog.Fatal("Please provide tvm-tool auth in env[TVMTOOL_LOCAL_AUTHTOKEN]")
		return
	}

	tvmClient, err := tvmtool.NewClient(
		baseURI,
		tvmtool.WithAuthToken(auth),
		tvmtool.WithLogger(zlog),
	)
	if err != nil {
		zlog.Fatal("failed create tvm client", log.Error(err))
		return
	}
	defer tvmClient.Close()

	ticket, err := tvmClient.GetServiceTicketForAlias(context.Background(), dst)
	if err != nil {
		zlog.Fatal("failed to get tvm ticket", log.String("dst", dst), log.Error(err))
		return
	}

	fmt.Printf("ticket: %s\n", ticket)
}
