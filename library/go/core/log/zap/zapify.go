package zap

import (
	"fmt"

	"github.com/ydb-platform/ydb/library/go/core/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ZapifyLevel turns interface log level to zap log level
func ZapifyLevel(level log.Level) zapcore.Level {
	switch level {
	case log.TraceLevel:
		return zapcore.DebugLevel
	case log.DebugLevel:
		return zapcore.DebugLevel
	case log.InfoLevel:
		return zapcore.InfoLevel
	case log.WarnLevel:
		return zapcore.WarnLevel
	case log.ErrorLevel:
		return zapcore.ErrorLevel
	case log.FatalLevel:
		return zapcore.FatalLevel
	default:
		// For when new log level is not added to this func (most likely never).
		panic(fmt.Sprintf("unknown log level: %d", level))
	}
}

// UnzapifyLevel turns zap log level to interface log level.
func UnzapifyLevel(level zapcore.Level) log.Level {
	switch level {
	case zapcore.DebugLevel:
		return log.DebugLevel
	case zapcore.InfoLevel:
		return log.InfoLevel
	case zapcore.WarnLevel:
		return log.WarnLevel
	case zapcore.ErrorLevel:
		return log.ErrorLevel
	case zapcore.FatalLevel, zapcore.DPanicLevel, zapcore.PanicLevel:
		return log.FatalLevel
	default:
		// For when new log level is not added to this func (most likely never).
		panic(fmt.Sprintf("unknown log level: %d", level))
	}
}

// nolint: gocyclo
func zapifyField(field log.Field) zap.Field {
	switch field.Type() {
	case log.FieldTypeNil:
		return zap.Reflect(field.Key(), nil)
	case log.FieldTypeString:
		return zap.String(field.Key(), field.String())
	case log.FieldTypeBinary:
		return zap.Binary(field.Key(), field.Binary())
	case log.FieldTypeBoolean:
		return zap.Bool(field.Key(), field.Bool())
	case log.FieldTypeSigned:
		return zap.Int64(field.Key(), field.Signed())
	case log.FieldTypeUnsigned:
		return zap.Uint64(field.Key(), field.Unsigned())
	case log.FieldTypeFloat:
		return zap.Float64(field.Key(), field.Float())
	case log.FieldTypeTime:
		return zap.Time(field.Key(), field.Time())
	case log.FieldTypeDuration:
		return zap.Duration(field.Key(), field.Duration())
	case log.FieldTypeError:
		return zap.NamedError(field.Key(), field.Error())
	case log.FieldTypeArray:
		return zap.Any(field.Key(), field.Interface())
	case log.FieldTypeAny:
		return zap.Any(field.Key(), field.Interface())
	case log.FieldTypeReflect:
		return zap.Reflect(field.Key(), field.Interface())
	case log.FieldTypeByteString:
		return zap.ByteString(field.Key(), field.Binary())
	default:
		// For when new field type is not added to this func
		panic(fmt.Sprintf("unknown field type: %d", field.Type()))
	}
}

func zapifyFields(fields ...log.Field) []zapcore.Field {
	zapFields := make([]zapcore.Field, 0, len(fields))
	for _, field := range fields {
		zapFields = append(zapFields, zapifyField(field))
	}

	return zapFields
}
