//go:build linux || darwin
// +build linux darwin

package logrotate

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb/library/go/core/log"
	"github.com/ydb-platform/ydb/library/go/core/log/zap"
)

func TestLogrotateSink(t *testing.T) {
	testLogFilename := "test.log"
	testDir := "testLogrotate"

	// use test dir in default temp files location
	tempDir, err := os.MkdirTemp("", testDir)
	require.NoError(t, err, "failed to create temporary directory %s", testDir)

	testLogPath := filepath.Join(tempDir, testLogFilename)

	defer func() {
		_ = os.RemoveAll(tempDir)
	}() // clean up

	err = RegisterLogrotateSink(syscall.SIGUSR1)
	require.NoError(t, err, "failed to register sink")

	// Double registration is not allowed
	err = RegisterLogrotateSink(syscall.SIGUSR1)
	require.Error(t, err)

	cfg := zap.JSONConfig(log.DebugLevel)
	cfg.OutputPaths = []string{"logrotate://" + testLogPath}
	logger, err := zap.New(cfg)
	require.NoError(t, err, "failed to create logger")

	testLogFile, err := os.OpenFile(testLogPath, os.O_RDONLY, 0)
	require.NoError(t, err, "expected logger to create file: %v", err)
	defer func() {
		_ = testLogFile.Close()
	}()

	// test write to file
	logger.Debug("test")
	logger.Debug("test")

	err = os.Rename(testLogPath, testLogPath+".rotated")
	require.NoError(t, err, "failed to rename file")

	err = syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	require.NoError(t, err, "failed to send signal to self, %v", err)

	// There is an essential race that we can not control of delivering signal,
	// so we just wait enough here
	time.Sleep(time.Second)

	logger.Debug("test")
	logger.Debug("test")
	logger.Debug("test")

	// Reopen file to sync content
	err = syscall.Kill(syscall.Getpid(), syscall.SIGUSR1)
	require.NoError(t, err, "failed to send signal to self, %v", err)
	time.Sleep(time.Second)

	requireLineCount(t, testLogPath, 3)
	requireLineCount(t, testLogPath+".rotated", 2)
}

func requireLineCount(t *testing.T, path string, lines int) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	require.NoError(t, err, "failed to open log file for reading")
	defer func() { _ = file.Close() }()
	dataRead, err := io.ReadAll(file)
	require.NoError(t, err, "failed to read log file")
	require.Equal(t, lines, strings.Count(string(dataRead), "\n"))
}
