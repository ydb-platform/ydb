package log_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb/library/go/core/log"
)

var levelsToTest = []struct {
	name  string
	level log.Level
}{
	{name: log.TraceString, level: log.TraceLevel},
	{name: log.DebugString, level: log.DebugLevel},
	{name: log.InfoString, level: log.InfoLevel},
	{name: log.WarnString, level: log.WarnLevel},
	{name: log.ErrorString, level: log.ErrorLevel},
	{name: log.FatalString, level: log.FatalLevel},
}

func TestLevels(t *testing.T) {
	for _, levelInput := range levelsToTest {
		t.Run("Convert "+levelInput.name, func(t *testing.T) {
			levelFromLevelString, err := log.ParseLevel(levelInput.name)
			require.NoError(t, err)
			require.Equal(t, levelInput.level, levelFromLevelString)

			levelStringFromLevel := levelInput.level.String()
			require.Equal(t, levelInput.name, levelStringFromLevel)

			levelFromLevelStringFromLevel, err := log.ParseLevel(levelStringFromLevel)
			require.NoError(t, err)
			require.Equal(t, levelInput.level, levelFromLevelStringFromLevel)
		})
	}
}

func TestLevel_MarshalText(t *testing.T) {
	level := log.DebugLevel
	_, err := level.MarshalText()
	require.NoError(t, err)

	level = log.Level(100500)
	_, err = level.MarshalText()
	require.Error(t, err)

	level = log.Level(-1)
	_, err = level.MarshalText()
	require.Error(t, err)
}
