package encoders

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

const (
	// EncoderNameKV is the encoder name to use for zap config
	EncoderNameKV = "kv"
)

const (
	// We use ' for quote symbol instead of " so that it doesn't interfere with %q of fmt package
	stringQuoteSymbol = '\''
	kvArraySeparator  = ','
)

var kvPool = sync.Pool{New: func() interface{} {
	return &kvEncoder{}
}}

func getKVEncoder() *kvEncoder {
	return kvPool.Get().(*kvEncoder)
}

type kvEncoder struct {
	cfg            zapcore.EncoderConfig
	pool           buffer.Pool
	buf            *buffer.Buffer
	openNamespaces int

	// for encoding generic values by reflection
	reflectBuf *buffer.Buffer
	reflectEnc *json.Encoder
}

// NewKVEncoder constructs kv encoder
func NewKVEncoder(cfg zapcore.EncoderConfig) (zapcore.Encoder, error) {
	return newKVEncoder(cfg), nil
}

func newKVEncoder(cfg zapcore.EncoderConfig) *kvEncoder {
	pool := buffer.NewPool()
	return &kvEncoder{
		cfg:  cfg,
		pool: pool,
		buf:  pool.Get(),
	}
}

func (enc *kvEncoder) addElementSeparator() {
	if enc.buf.Len() == 0 {
		return
	}

	enc.buf.AppendByte(' ')
}

func (enc *kvEncoder) addKey(key string) {
	enc.addElementSeparator()
	enc.buf.AppendString(key)
	enc.buf.AppendByte('=')
}

func (enc *kvEncoder) appendFloat(val float64, bitSize int) {
	enc.appendArrayItemSeparator()
	switch {
	case math.IsNaN(val):
		enc.buf.AppendString(`"NaN"`)
	case math.IsInf(val, 1):
		enc.buf.AppendString(`"+Inf"`)
	case math.IsInf(val, -1):
		enc.buf.AppendString(`"-Inf"`)
	default:
		enc.buf.AppendFloat(val, bitSize)
	}
}

func (enc *kvEncoder) AddArray(key string, arr zapcore.ArrayMarshaler) error {
	enc.addKey(key)
	return enc.AppendArray(arr)
}

func (enc *kvEncoder) AddObject(key string, obj zapcore.ObjectMarshaler) error {
	enc.addKey(key)
	return enc.AppendObject(obj)
}

func (enc *kvEncoder) AddBinary(key string, val []byte) {
	enc.AddString(key, base64.StdEncoding.EncodeToString(val))
}

func (enc *kvEncoder) AddByteString(key string, val []byte) {
	enc.addKey(key)
	enc.AppendByteString(val)
}

func (enc *kvEncoder) AddBool(key string, val bool) {
	enc.addKey(key)
	enc.AppendBool(val)
}

func (enc *kvEncoder) AddComplex128(key string, val complex128) {
	enc.addKey(key)
	enc.AppendComplex128(val)
}

func (enc *kvEncoder) AddDuration(key string, val time.Duration) {
	enc.addKey(key)
	enc.AppendDuration(val)
}

func (enc *kvEncoder) AddFloat64(key string, val float64) {
	enc.addKey(key)
	enc.AppendFloat64(val)
}

func (enc *kvEncoder) AddInt64(key string, val int64) {
	enc.addKey(key)
	enc.AppendInt64(val)
}

func (enc *kvEncoder) resetReflectBuf() {
	if enc.reflectBuf == nil {
		enc.reflectBuf = enc.pool.Get()
		enc.reflectEnc = json.NewEncoder(enc.reflectBuf)
	} else {
		enc.reflectBuf.Reset()
	}
}

func (enc *kvEncoder) AddReflected(key string, obj interface{}) error {
	enc.resetReflectBuf()
	err := enc.reflectEnc.Encode(obj)
	if err != nil {
		return err
	}
	enc.reflectBuf.TrimNewline()
	enc.addKey(key)
	_, err = enc.buf.Write(enc.reflectBuf.Bytes())
	return err
}

func (enc *kvEncoder) OpenNamespace(key string) {
	enc.addKey(key)
	enc.buf.AppendByte('{')
	enc.openNamespaces++
}

func (enc *kvEncoder) AddString(key, val string) {
	enc.addKey(key)
	enc.AppendString(val)
}

func (enc *kvEncoder) AddTime(key string, val time.Time) {
	enc.addKey(key)
	enc.AppendTime(val)
}

func (enc *kvEncoder) AddUint64(key string, val uint64) {
	enc.addKey(key)
	enc.AppendUint64(val)
}

func (enc *kvEncoder) appendArrayItemSeparator() {
	last := enc.buf.Len() - 1
	if last < 0 {
		return
	}

	switch enc.buf.Bytes()[last] {
	case '[', '{', '=':
		return
	default:
		enc.buf.AppendByte(kvArraySeparator)
	}
}

func (enc *kvEncoder) AppendArray(arr zapcore.ArrayMarshaler) error {
	enc.appendArrayItemSeparator()
	enc.buf.AppendByte('[')
	err := arr.MarshalLogArray(enc)
	enc.buf.AppendByte(']')
	return err
}

func (enc *kvEncoder) AppendObject(obj zapcore.ObjectMarshaler) error {
	enc.appendArrayItemSeparator()
	enc.buf.AppendByte('{')
	err := obj.MarshalLogObject(enc)
	enc.buf.AppendByte('}')
	return err
}

func (enc *kvEncoder) AppendBool(val bool) {
	enc.appendArrayItemSeparator()
	enc.buf.AppendBool(val)
}

func (enc *kvEncoder) AppendByteString(val []byte) {
	enc.appendArrayItemSeparator()
	_, _ = enc.buf.Write(val)
}

func (enc *kvEncoder) AppendComplex128(val complex128) {
	enc.appendArrayItemSeparator()
	r, i := real(val), imag(val)

	enc.buf.AppendByte('"')
	// Because we're always in a quoted string, we can use strconv without
	// special-casing NaN and +/-Inf.
	enc.buf.AppendFloat(r, 64)
	enc.buf.AppendByte('+')
	enc.buf.AppendFloat(i, 64)
	enc.buf.AppendByte('i')
	enc.buf.AppendByte('"')
}

func (enc *kvEncoder) AppendDuration(val time.Duration) {
	cur := enc.buf.Len()
	enc.cfg.EncodeDuration(val, enc)
	if cur == enc.buf.Len() {
		// User-supplied EncodeDuration is a no-op. Fall back to nanoseconds to keep
		// JSON valid.
		enc.AppendInt64(int64(val))
	}
}

func (enc *kvEncoder) AppendInt64(val int64) {
	enc.appendArrayItemSeparator()
	enc.buf.AppendInt(val)
}

func (enc *kvEncoder) AppendReflected(val interface{}) error {
	enc.appendArrayItemSeparator()
	enc.resetReflectBuf()
	err := enc.reflectEnc.Encode(val)
	if err != nil {
		return err
	}
	enc.reflectBuf.TrimNewline()
	enc.addElementSeparator()
	_, err = enc.buf.Write(enc.reflectBuf.Bytes())
	return err
}

func (enc *kvEncoder) AppendString(val string) {
	enc.appendArrayItemSeparator()
	var quotes bool
	if strings.ContainsAny(val, " =[]{}") {
		quotes = true
	}

	if quotes {
		enc.buf.AppendByte(stringQuoteSymbol)
	}
	enc.buf.AppendString(val)
	if quotes {
		enc.buf.AppendByte(stringQuoteSymbol)
	}
}

func (enc *kvEncoder) AppendTime(val time.Time) {
	cur := enc.buf.Len()
	enc.cfg.EncodeTime(val, enc)
	if cur == enc.buf.Len() {
		// User-supplied EncodeTime is a no-op. Fall back to nanos since epoch to keep
		// output JSON valid.
		enc.AppendInt64(val.UnixNano())
	}
}

func (enc *kvEncoder) AppendUint64(val uint64) {
	enc.appendArrayItemSeparator()
	enc.buf.AppendUint(val)
}

func (enc *kvEncoder) AddComplex64(k string, v complex64) { enc.AddComplex128(k, complex128(v)) }
func (enc *kvEncoder) AddFloat32(k string, v float32)     { enc.AddFloat64(k, float64(v)) }
func (enc *kvEncoder) AddInt(k string, v int)             { enc.AddInt64(k, int64(v)) }
func (enc *kvEncoder) AddInt32(k string, v int32)         { enc.AddInt64(k, int64(v)) }
func (enc *kvEncoder) AddInt16(k string, v int16)         { enc.AddInt64(k, int64(v)) }
func (enc *kvEncoder) AddInt8(k string, v int8)           { enc.AddInt64(k, int64(v)) }
func (enc *kvEncoder) AddUint(k string, v uint)           { enc.AddUint64(k, uint64(v)) }
func (enc *kvEncoder) AddUint32(k string, v uint32)       { enc.AddUint64(k, uint64(v)) }
func (enc *kvEncoder) AddUint16(k string, v uint16)       { enc.AddUint64(k, uint64(v)) }
func (enc *kvEncoder) AddUint8(k string, v uint8)         { enc.AddUint64(k, uint64(v)) }
func (enc *kvEncoder) AddUintptr(k string, v uintptr)     { enc.AddUint64(k, uint64(v)) }
func (enc *kvEncoder) AppendComplex64(v complex64)        { enc.AppendComplex128(complex128(v)) }
func (enc *kvEncoder) AppendFloat64(v float64)            { enc.appendFloat(v, 64) }
func (enc *kvEncoder) AppendFloat32(v float32)            { enc.appendFloat(float64(v), 32) }
func (enc *kvEncoder) AppendInt(v int)                    { enc.AppendInt64(int64(v)) }
func (enc *kvEncoder) AppendInt32(v int32)                { enc.AppendInt64(int64(v)) }
func (enc *kvEncoder) AppendInt16(v int16)                { enc.AppendInt64(int64(v)) }
func (enc *kvEncoder) AppendInt8(v int8)                  { enc.AppendInt64(int64(v)) }
func (enc *kvEncoder) AppendUint(v uint)                  { enc.AppendUint64(uint64(v)) }
func (enc *kvEncoder) AppendUint32(v uint32)              { enc.AppendUint64(uint64(v)) }
func (enc *kvEncoder) AppendUint16(v uint16)              { enc.AppendUint64(uint64(v)) }
func (enc *kvEncoder) AppendUint8(v uint8)                { enc.AppendUint64(uint64(v)) }
func (enc *kvEncoder) AppendUintptr(v uintptr)            { enc.AppendUint64(uint64(v)) }

func (enc *kvEncoder) Clone() zapcore.Encoder {
	clone := enc.clone()
	_, _ = clone.buf.Write(enc.buf.Bytes())
	return clone
}

func (enc *kvEncoder) clone() *kvEncoder {
	clone := getKVEncoder()
	clone.cfg = enc.cfg
	clone.openNamespaces = enc.openNamespaces
	clone.pool = enc.pool
	clone.buf = enc.pool.Get()
	return clone
}

// nolint: gocyclo
func (enc *kvEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	final := enc.clone()
	if final.cfg.TimeKey != "" && final.cfg.EncodeTime != nil {
		final.addElementSeparator()
		final.buf.AppendString(final.cfg.TimeKey + "=")
		final.cfg.EncodeTime(ent.Time, final)
	}
	if final.cfg.LevelKey != "" && final.cfg.EncodeLevel != nil {
		final.addElementSeparator()
		final.buf.AppendString(final.cfg.LevelKey + "=")
		final.cfg.EncodeLevel(ent.Level, final)
	}
	if ent.LoggerName != "" && final.cfg.NameKey != "" {
		nameEncoder := final.cfg.EncodeName

		if nameEncoder == nil {
			// Fall back to FullNameEncoder for backward compatibility.
			nameEncoder = zapcore.FullNameEncoder
		}

		final.addElementSeparator()
		final.buf.AppendString(final.cfg.NameKey + "=")
		nameEncoder(ent.LoggerName, final)
	}
	if ent.Caller.Defined && final.cfg.CallerKey != "" && final.cfg.EncodeCaller != nil {
		final.addElementSeparator()
		final.buf.AppendString(final.cfg.CallerKey + "=")
		final.cfg.EncodeCaller(ent.Caller, final)
	}

	if enc.buf.Len() > 0 {
		final.addElementSeparator()
		_, _ = final.buf.Write(enc.buf.Bytes())
	}

	// Add the message itself.
	if final.cfg.MessageKey != "" {
		final.addElementSeparator()
		final.buf.AppendString(final.cfg.MessageKey + "=")
		final.AppendString(ent.Message)
	}

	// Add any structured context.
	for _, f := range fields {
		f.AddTo(final)
	}

	// If there's no stacktrace key, honor that; this allows users to force
	// single-line output.
	if ent.Stack != "" && final.cfg.StacktraceKey != "" {
		final.buf.AppendByte('\n')
		final.buf.AppendString(ent.Stack)
	}

	if final.cfg.LineEnding != "" {
		final.buf.AppendString(final.cfg.LineEnding)
	} else {
		final.buf.AppendString(zapcore.DefaultLineEnding)
	}
	return final.buf, nil
}
