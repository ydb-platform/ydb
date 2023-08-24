package golog

import (
	canal_log "github.com/siddontang/go-log/log"
	"github.com/ydb-platform/ydb/library/go/core/log"
)

func SetLevel(level log.Level) {
	switch level {
	case log.DebugLevel:
		canal_log.SetLevel(canal_log.LevelDebug)
	case log.ErrorLevel:
		canal_log.SetLevel(canal_log.LevelError)
	case log.FatalLevel:
		canal_log.SetLevel(canal_log.LevelFatal)
	case log.InfoLevel:
		canal_log.SetLevel(canal_log.LevelInfo)
	case log.TraceLevel:
		canal_log.SetLevel(canal_log.LevelTrace)
	}
}
