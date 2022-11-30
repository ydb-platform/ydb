package tooltest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"a.yandex-team.ru/library/go/core/log"
	"a.yandex-team.ru/library/go/core/log/nop"
	"a.yandex-team.ru/library/go/yandex/tvm/tvmauth"
)

type testLogger struct {
	nop.Logger

	msgs []string
}

func (l *testLogger) Info(msg string, fields ...log.Field) {
	l.msgs = append(l.msgs, msg)
}

func TestLogger(t *testing.T) {
	var l testLogger

	c, err := tvmauth.NewToolClient(recipeToolOptions(t), &l)
	require.NoError(t, err)
	defer c.Destroy()

	time.Sleep(time.Second)

	require.NotEmpty(t, l.msgs)
}
