package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type Opts struct {
	Port     uint16
	KeyFile  string
	CertFile string
}

func handler(writer http.ResponseWriter, request *http.Request) {
	res := "pong.my"

	writer.Header().Set("Content-Type", "text/plain")
	writer.WriteHeader(http.StatusOK)

	_, _ = writer.Write([]byte(res))
}

func runServer(opts *Opts) error {
	mainMux := http.NewServeMux()
	mainMux.Handle("/ping", http.HandlerFunc(handler))

	server := &http.Server{
		Addr:     fmt.Sprintf("localhost:%d", opts.Port),
		Handler:  mainMux,
		ErrorLog: log.New(os.Stdout, "", log.LstdFlags),
	}

	return server.ListenAndServeTLS(opts.CertFile, opts.KeyFile)
}

func markFlagRequired(flags *pflag.FlagSet, names ...string) {
	for _, n := range names {
		name := n
		if err := cobra.MarkFlagRequired(flags, name); err != nil {
			panic(err)
		}
	}
}

func main() {
	opts := Opts{}

	cmd := cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			return runServer(&opts)
		},
	}

	flags := cmd.Flags()
	flags.Uint16Var(&opts.Port, "port", 0, "")
	flags.StringVar(&opts.KeyFile, "keyfile", "", "path to key file")
	flags.StringVar(&opts.CertFile, "certfile", "", "path to cert file")

	markFlagRequired(flags, "port", "keyfile", "certfile")

	if err := cmd.Execute(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Exit with err: %s", err)
		os.Exit(1)
	}
}
