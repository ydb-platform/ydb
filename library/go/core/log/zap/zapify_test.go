package zap

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb/library/go/core/log"
	"go.uber.org/zap/zapcore"
)

// Simple test, that all type of fields are correctly zapified.
// Maybe we also need some test that checks resulting zap.Field type also.
func TestZapifyField(t *testing.T) {
	fileds := []log.Field{
		log.Nil("test"),
		log.String("test", "test"),
		log.Binary("test", []byte("test")),
		log.Bool("test", true),
		log.Int("test", -42),
		log.UInt("test", 42),
		log.Float64("test", 0.42),
		log.Time("test", time.Now()),
		log.Duration("test", time.Second),
		log.Error(fmt.Errorf("test")),
		log.Array("test", []uint32{42}),
		log.Any("test", struct{ ID uint32 }{ID: 42}),
		log.Reflect("test", struct{ ID uint32 }{ID: 42}),
	}
	for _, field := range fileds {
		assert.NotPanics(t, func() {
			zapifyField(field)
		})
	}
}

func TestZapifyAny(t *testing.T) {
	f := zapifyField(log.Any("test", struct{ ID uint32 }{ID: 42}))
	assert.Equal(t, zapcore.ReflectType, f.Type)
}

func TestZapifyReflect(t *testing.T) {
	f := zapifyField(log.Any("test", struct{ ID uint32 }{ID: 42}))
	assert.Equal(t, zapcore.ReflectType, f.Type)
}

type stringer struct{}

func (*stringer) String() string {
	return "hello"
}

func TestZapifyStringer(t *testing.T) {
	f0 := zapifyField(log.Any("test", &stringer{}))
	assert.Equal(t, zapcore.StringerType, f0.Type)

	f1 := zapifyField(log.Reflect("test", &stringer{}))
	assert.Equal(t, zapcore.ReflectType, f1.Type)
}
