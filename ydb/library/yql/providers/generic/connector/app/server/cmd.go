package server

import (
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
)

var Cmd = &cobra.Command{
	Use:   "server",
	Short: "Connector server",
	Run: func(cmd *cobra.Command, args []string) {
		if err := run(cmd, args); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

const configFlag = "config"

func init() {
	Cmd.Flags().StringP(configFlag, "c", "", "path to server config file")

	if err := Cmd.MarkFlagRequired(configFlag); err != nil {
		log.Fatal(err)
	}
}
