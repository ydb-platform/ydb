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
	baseURI    = "http://localhost:3000"
	srvTicket  string
	userTicket string
)

func main() {
	flag.StringVar(&baseURI, "tool-uri", baseURI, "TVM tool uri")
	flag.StringVar(&srvTicket, "srv", "", "service ticket to check")
	flag.StringVar(&userTicket, "usr", "", "user ticket to check")
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

	fmt.Printf("------ Check service ticket ------\n\n")
	srvCheck, err := tvmClient.CheckServiceTicket(context.Background(), srvTicket)
	if err != nil {
		fmt.Printf("Failed\nTicket: %s\nError: %s\n", srvCheck, err)
	} else {
		fmt.Printf("OK\nInfo: %s\n", srvCheck)
	}

	if userTicket == "" {
		return
	}

	fmt.Printf("\n------ Check user ticket result ------\n\n")

	usrCheck, err := tvmClient.CheckUserTicket(context.Background(), userTicket)
	if err != nil {
		fmt.Printf("Failed\nTicket: %s\nError: %s\n", usrCheck, err)
		return
	}
	fmt.Printf("OK\nInfo: %s\n", usrCheck)
}
