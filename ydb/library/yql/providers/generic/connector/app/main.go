package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/client"
	"github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/app/server"
)

var rootCmd = &cobra.Command{
	Use:   "connector",
	Short: "Connector for external data sources",
	// Run: func(cmd *cobra.Command, args []string) {},
}

func init() {
	rootCmd.AddCommand(server.Cmd)
	rootCmd.AddCommand(client.Cmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
