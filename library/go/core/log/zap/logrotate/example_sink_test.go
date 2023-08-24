//go:build linux || darwin
// +build linux darwin

package logrotate_test

import (
	"net/url"
	"path/filepath"
	"syscall"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/library/go/core/log/zap"
	"github.com/ydb-platform/ydb/library/go/core/log/zap/logrotate"
	uberzap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func Example_simpleUsage() {
	// Basic usage, when you don't need any custom preferences is quite easy.
	// register our logrotate sink and force it to reopen files on sighup(remember to check for errors)
	_ = logrotate.RegisterLogrotateSink(syscall.SIGHUP)
	// create zap logger as usual, using `logrotate://` instead of omitting it or using `file://`
	cfg := zap.JSONConfig(log.DebugLevel)
	logPath, _ := filepath.Abs("./example.log")
	cfg.OutputPaths = []string{"logrotate://" + logPath}
	logger, _ := zap.New(cfg)
	// That's all, when your process receives SIGHUP file will be reopened
	logger.Debug("this log should be reopened by SIGHUP")
}

func Example_namedUsage() {
	// Note: each scheme can be registered only once and can not be unregistered
	// If you want to provide custom unused scheme name(remember to check for errors):
	_ = logrotate.RegisterNamedLogrotateSink("rotate-usr1", syscall.SIGUSR1)
	// Now we create logger using that cheme
	cfg := zap.JSONConfig(log.DebugLevel)
	logPath, _ := filepath.Abs("./example.log")
	cfg.OutputPaths = []string{"rotate-usr1://" + logPath}
	logger, _ := zap.New(cfg)
	// Now file will be reopened by SIGUSR1
	logger.Debug("this log should be reopened by SIGHUP")
}

func Example_standaloneUsage() {
	// If you don't want to register scheme, or use custom logging core you can do this(remember to check for errors):
	u, _ := url.ParseRequestURI("/tmp/example.log")
	sink, _ := logrotate.NewLogrotateSink(u, syscall.SIGHUP)

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{MessageKey: "msg"})
	core := zapcore.NewCore(encoder, sink, uberzap.NewAtomicLevel())
	logger := uberzap.New(core)
	// Now file will be reopened by SIGHUP
	logger.Debug("this log should be reopened by SIGHUP")
}
