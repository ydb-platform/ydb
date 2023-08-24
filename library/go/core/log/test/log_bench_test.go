package test

import (
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb/library/go/core/log"
)

func BenchmarkOutput(b *testing.B) {
	for _, loggerInput := range loggersToTest {
		for _, count := range []int{0, 1, 2, 5} {
			logger, err := loggerInput.factory(log.DebugLevel)
			if err != nil {
				b.Fatalf("failed to create logger: %s", b.Name())
			}
			b.Run(fmt.Sprintf("%s fields %d", loggerInput.name, count), func(b *testing.B) {
				benchmarkFields(b, logger, count)
			})
		}
	}
}

func benchmarkFields(b *testing.B, logger log.Logger, count int) {
	flds := genFields(count)

	for n := 0; n < b.N; n++ {
		logger.Debug(msg, flds...)
	}
}

func genFields(count int) []log.Field {
	flds := make([]log.Field, 0, count)
	for ; count > 0; count-- {
		flds = append(flds, log.String(key, value))
	}

	return flds
}
