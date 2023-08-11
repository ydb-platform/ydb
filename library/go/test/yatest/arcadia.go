//go:build arcadia
// +build arcadia

package yatest

import (
	"os"
)

func doInit() {
	initTestContext()
}

func init() {
	if val := os.Getenv("YA_TEST_CONTEXT_FILE"); val != "" {
		if _, err := os.Stat(val); err == nil {
			lazyInit()
		}
	}
}
