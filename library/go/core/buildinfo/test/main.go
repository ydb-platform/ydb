package main

import (
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/buildinfo"
)

func main() {
	if buildinfo.Info.ProgramVersion != "" {
		fmt.Print(buildinfo.Info.ProgramVersion)
	} else {
		fmt.Printf("ProgramVersion is not available\n")
	}
}
