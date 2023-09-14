package zap

import (
	"context"

	"github.com/ydb-platform/ydb/library/go/core/log/ctxlog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ctxField struct {
	ctx context.Context
}

// MarshalLogObject implements zapcore.ObjectMarshaler to append context fields directly to encoder in a lazy manner
func (c ctxField) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	fields := ctxlog.ContextFields(c.ctx)
	for _, f := range fields {
		zapifyField(f).AddTo(encoder)
	}
	return nil
}

// Context creates a log field from context - all fields bound with ctxlog.WithFields will be added.
func Context(ctx context.Context) zap.Field {
	return zap.Field{
		Key:       "",
		Type:      zapcore.InlineMarshalerType,
		Interface: ctxField{ctx: ctx},
	}
}
