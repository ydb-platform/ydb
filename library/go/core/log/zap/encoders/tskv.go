package encoders

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/ydb-platform/ydb/library/go/core/xerrors"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

const (
	// EncoderNameKV is the encoder name to use for zap config
	EncoderNameTSKV = "tskv"
)

const (
	tskvLineEnding       = '\n'
	tskvElementSeparator = '\t'
	tskvKVSeparator      = '='
	tskvMark             = "tskv"
	tskvArrayStart       = '['
	tskvArrayEnd         = ']'
	tskvArraySeparator   = ','
)

var tskvKeyEscapeRules = []string{
	`\`, `\\`,
	"\t", "\\t",
	"\n", "\\n",
	"\r", `\r`,
	"\x00", `\0`,
	"=", `\=`,
}

var tskvValueEscapeRules = []string{
	`\`, `\\`,
	"\t", "\\t",
	"\n", `\n`,
	"\r", `\r`,
	"\x00", `\0`,
}

type tskvEscaper struct {
	keyReplacer   *strings.Replacer
	valueReplacer *strings.Replacer
}

func newTSKVEscaper() tskvEscaper {
	return tskvEscaper{
		keyReplacer:   strings.NewReplacer(tskvKeyEscapeRules...),
		valueReplacer: strings.NewReplacer(tskvValueEscapeRules...),
	}
}

func (esc *tskvEscaper) escapeKey(key string) string {
	return esc.keyReplacer.Replace(key)
}

func (esc *tskvEscaper) escapeValue(val string) string {
	return esc.valueReplacer.Replace(val)
}

func hexEncode(val []byte) []byte {
	dst := make([]byte, hex.EncodedLen(len(val)))
	hex.Encode(dst, val)
	return dst
}

var tskvPool = sync.Pool{New: func() interface{} {
	return &tskvEncoder{}
}}

func getTSKVEncoder() *tskvEncoder {
	return tskvPool.Get().(*tskvEncoder)
}

type tskvEncoder struct {
	cfg  zapcore.EncoderConfig
	pool buffer.Pool
	buf  *buffer.Buffer

	// for encoding generic values by reflection
	reflectBuf *buffer.Buffer
	reflectEnc *json.Encoder

	tskvEscaper tskvEscaper
}

// NewKVEncoder constructs tskv encoder
func NewTSKVEncoder(cfg zapcore.EncoderConfig) (zapcore.Encoder, error) {
	return newTSKVEncoder(cfg), nil
}

func newTSKVEncoder(cfg zapcore.EncoderConfig) *tskvEncoder {
	pool := buffer.NewPool()
	return &tskvEncoder{
		cfg:         cfg,
		pool:        pool,
		buf:         pool.Get(),
		tskvEscaper: newTSKVEscaper(),
	}
}

func (enc *tskvEncoder) appendElementSeparator() {
	if enc.buf.Len() == 0 {
		return
	}

	enc.buf.AppendByte(tskvElementSeparator)
}

func (enc *tskvEncoder) appendArrayItemSeparator() {
	last := enc.buf.Len() - 1
	if last < 0 {
		return
	}

	switch enc.buf.Bytes()[last] {
	case tskvArrayStart, tskvKVSeparator:
		return
	default:
		enc.buf.AppendByte(tskvArraySeparator)
	}
}

func (enc *tskvEncoder) safeAppendKey(key string) {
	enc.appendElementSeparator()
	enc.buf.AppendString(enc.tskvEscaper.escapeKey(key))
	enc.buf.AppendByte(tskvKVSeparator)
}

func (enc *tskvEncoder) safeAppendString(val string) {
	enc.buf.AppendString(enc.tskvEscaper.escapeValue(val))
}

func (enc *tskvEncoder) appendFloat(val float64, bitSize int) {
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

func (enc *tskvEncoder) AddArray(key string, arr zapcore.ArrayMarshaler) error {
	enc.safeAppendKey(key)
	return enc.AppendArray(arr)
}

func (enc *tskvEncoder) AddObject(key string, obj zapcore.ObjectMarshaler) error {
	enc.safeAppendKey(key)
	return enc.AppendObject(obj)
}

func (enc *tskvEncoder) AddBinary(key string, val []byte) {
	enc.AddByteString(key, val)
}

func (enc *tskvEncoder) AddByteString(key string, val []byte) {
	enc.safeAppendKey(key)
	enc.AppendByteString(val)
}

func (enc *tskvEncoder) AddBool(key string, val bool) {
	enc.safeAppendKey(key)
	enc.AppendBool(val)
}

func (enc *tskvEncoder) AddComplex128(key string, val complex128) {
	enc.safeAppendKey(key)
	enc.AppendComplex128(val)
}

func (enc *tskvEncoder) AddDuration(key string, val time.Duration) {
	enc.safeAppendKey(key)
	enc.AppendDuration(val)
}

func (enc *tskvEncoder) AddFloat64(key string, val float64) {
	enc.safeAppendKey(key)
	enc.AppendFloat64(val)
}

func (enc *tskvEncoder) AddInt64(key string, val int64) {
	enc.safeAppendKey(key)
	enc.AppendInt64(val)
}

func (enc *tskvEncoder) resetReflectBuf() {
	if enc.reflectBuf == nil {
		enc.reflectBuf = enc.pool.Get()
		enc.reflectEnc = json.NewEncoder(enc.reflectBuf)
	} else {
		enc.reflectBuf.Reset()
	}
}

func (enc *tskvEncoder) AddReflected(key string, obj interface{}) error {
	enc.resetReflectBuf()
	err := enc.reflectEnc.Encode(obj)
	if err != nil {
		return err
	}
	enc.reflectBuf.TrimNewline()
	enc.safeAppendKey(key)
	enc.safeAppendString(enc.reflectBuf.String())
	return err
}

// OpenNamespace is not supported due to tskv format design
// See AppendObject() for more details
func (enc *tskvEncoder) OpenNamespace(key string) {
	panic("TSKV encoder does not support namespaces")
}

func (enc *tskvEncoder) AddString(key, val string) {
	enc.safeAppendKey(key)
	enc.safeAppendString(val)
}

func (enc *tskvEncoder) AddTime(key string, val time.Time) {
	enc.safeAppendKey(key)
	enc.AppendTime(val)
}

func (enc *tskvEncoder) AddUint64(key string, val uint64) {
	enc.safeAppendKey(key)
	enc.AppendUint64(val)
}

func (enc *tskvEncoder) AppendArray(arr zapcore.ArrayMarshaler) error {
	enc.appendArrayItemSeparator()
	enc.buf.AppendByte(tskvArrayStart)
	err := arr.MarshalLogArray(enc)
	enc.buf.AppendByte(tskvArrayEnd)
	return err
}

// TSKV format does not support hierarchy data so we can't log Objects here
// The only thing we can do is to implicitly use fmt.Stringer interface
//
// ObjectMarshaler interface requires MarshalLogObject method
// from within MarshalLogObject you only have access to ObjectEncoder methods (AddString, AddBool ...)
// so if you call AddString then object log will be split by \t sign
// but \t is key-value separator and tskv doesn't have another separators
// e.g
// json encoded: objLogFieldName={"innerObjKey1":{"innerObjKey2":"value"}}
// tskv encoded: objLogFieldName={	\tinnerObjKey1={	\tinnerObjKey2=value}}
func (enc *tskvEncoder) AppendObject(obj zapcore.ObjectMarshaler) error {
	var err error

	enc.appendArrayItemSeparator()
	enc.buf.AppendByte('{')
	stringerObj, ok := obj.(fmt.Stringer)
	if !ok {
		err = xerrors.Errorf("fmt.Stringer implementation required due to marshall into tskv format")
	} else {
		enc.safeAppendString(stringerObj.String())
	}
	enc.buf.AppendByte('}')

	return err
}

func (enc *tskvEncoder) AppendBool(val bool) {
	enc.appendArrayItemSeparator()
	enc.buf.AppendBool(val)
}

func (enc *tskvEncoder) AppendByteString(val []byte) {
	enc.appendArrayItemSeparator()
	_, _ = enc.buf.Write(hexEncode(val))
}

func (enc *tskvEncoder) AppendComplex128(val complex128) { // TODO
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

func (enc *tskvEncoder) AppendDuration(val time.Duration) {
	cur := enc.buf.Len()
	enc.cfg.EncodeDuration(val, enc)
	if cur == enc.buf.Len() {
		// User-supplied EncodeDuration is a no-op. Fall back to nanoseconds
		enc.AppendInt64(int64(val))
	}
}

func (enc *tskvEncoder) AppendInt64(val int64) {
	enc.appendArrayItemSeparator()
	enc.buf.AppendInt(val)
}

func (enc *tskvEncoder) AppendReflected(val interface{}) error {
	enc.appendArrayItemSeparator()

	enc.resetReflectBuf()
	err := enc.reflectEnc.Encode(val)
	if err != nil {
		return err
	}
	enc.reflectBuf.TrimNewline()
	enc.safeAppendString(enc.reflectBuf.String())
	return nil
}

func (enc *tskvEncoder) AppendString(val string) {
	enc.appendArrayItemSeparator()
	enc.safeAppendString(val)
}

func (enc *tskvEncoder) AppendTime(val time.Time) {
	cur := enc.buf.Len()
	enc.cfg.EncodeTime(val, enc)
	if cur == enc.buf.Len() {
		// User-supplied EncodeTime is a no-op. Fall back to nanos since epoch to keep output tskv valid.
		enc.AppendInt64(val.Unix())
	}
}

func (enc *tskvEncoder) AppendUint64(val uint64) {
	enc.appendArrayItemSeparator()
	enc.buf.AppendUint(val)
}

func (enc *tskvEncoder) AddComplex64(k string, v complex64) { enc.AddComplex128(k, complex128(v)) }
func (enc *tskvEncoder) AddFloat32(k string, v float32)     { enc.AddFloat64(k, float64(v)) }
func (enc *tskvEncoder) AddInt(k string, v int)             { enc.AddInt64(k, int64(v)) }
func (enc *tskvEncoder) AddInt32(k string, v int32)         { enc.AddInt64(k, int64(v)) }
func (enc *tskvEncoder) AddInt16(k string, v int16)         { enc.AddInt64(k, int64(v)) }
func (enc *tskvEncoder) AddInt8(k string, v int8)           { enc.AddInt64(k, int64(v)) }
func (enc *tskvEncoder) AddUint(k string, v uint)           { enc.AddUint64(k, uint64(v)) }
func (enc *tskvEncoder) AddUint32(k string, v uint32)       { enc.AddUint64(k, uint64(v)) }
func (enc *tskvEncoder) AddUint16(k string, v uint16)       { enc.AddUint64(k, uint64(v)) }
func (enc *tskvEncoder) AddUint8(k string, v uint8)         { enc.AddUint64(k, uint64(v)) }
func (enc *tskvEncoder) AddUintptr(k string, v uintptr)     { enc.AddUint64(k, uint64(v)) }
func (enc *tskvEncoder) AppendComplex64(v complex64)        { enc.AppendComplex128(complex128(v)) }
func (enc *tskvEncoder) AppendFloat64(v float64)            { enc.appendFloat(v, 64) }
func (enc *tskvEncoder) AppendFloat32(v float32)            { enc.appendFloat(float64(v), 32) }
func (enc *tskvEncoder) AppendInt(v int)                    { enc.AppendInt64(int64(v)) }
func (enc *tskvEncoder) AppendInt32(v int32)                { enc.AppendInt64(int64(v)) }
func (enc *tskvEncoder) AppendInt16(v int16)                { enc.AppendInt64(int64(v)) }
func (enc *tskvEncoder) AppendInt8(v int8)                  { enc.AppendInt64(int64(v)) }
func (enc *tskvEncoder) AppendUint(v uint)                  { enc.AppendUint64(uint64(v)) }
func (enc *tskvEncoder) AppendUint32(v uint32)              { enc.AppendUint64(uint64(v)) }
func (enc *tskvEncoder) AppendUint16(v uint16)              { enc.AppendUint64(uint64(v)) }
func (enc *tskvEncoder) AppendUint8(v uint8)                { enc.AppendUint64(uint64(v)) }
func (enc *tskvEncoder) AppendUintptr(v uintptr)            { enc.AppendUint64(uint64(v)) }

func (enc *tskvEncoder) Clone() zapcore.Encoder {
	clone := enc.clone()
	_, _ = clone.buf.Write(enc.buf.Bytes())
	return clone
}

func (enc *tskvEncoder) clone() *tskvEncoder {
	clone := getTSKVEncoder()
	clone.cfg = enc.cfg
	clone.pool = enc.pool
	clone.buf = enc.pool.Get()
	clone.tskvEscaper = enc.tskvEscaper
	return clone
}

// nolint: gocyclo
func (enc *tskvEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	final := enc.clone()
	final.AppendString(tskvMark)

	if final.cfg.TimeKey != "" && final.cfg.EncodeTime != nil {
		final.safeAppendKey(final.cfg.TimeKey)
		final.cfg.EncodeTime(ent.Time, final)
	}
	if final.cfg.LevelKey != "" && final.cfg.EncodeLevel != nil {
		final.safeAppendKey(final.cfg.LevelKey)
		final.cfg.EncodeLevel(ent.Level, final)
	}
	if ent.LoggerName != "" && final.cfg.NameKey != "" {
		nameEncoder := final.cfg.EncodeName

		if nameEncoder == nil {
			// Fall back to FullNameEncoder for backward compatibility.
			nameEncoder = zapcore.FullNameEncoder
		}

		final.safeAppendKey(final.cfg.NameKey)
		nameEncoder(ent.LoggerName, final)
	}
	if ent.Caller.Defined && final.cfg.CallerKey != "" && final.cfg.EncodeCaller != nil {
		final.safeAppendKey(final.cfg.CallerKey)
		final.cfg.EncodeCaller(ent.Caller, final)
	}

	if enc.buf.Len() > 0 {
		final.appendElementSeparator()
		_, _ = final.buf.Write(enc.buf.Bytes())
	}

	// Add the message itself.
	if final.cfg.MessageKey != "" {
		final.safeAppendKey(final.cfg.MessageKey)
		final.safeAppendString(ent.Message)
	}

	// Add any structured context.
	for _, f := range fields {
		f.AddTo(final)
	}

	if ent.Stack != "" && final.cfg.StacktraceKey != "" {
		final.safeAppendKey(final.cfg.StacktraceKey)
		final.safeAppendString(ent.Stack)
	}

	if final.cfg.LineEnding != "" {
		final.buf.AppendString(final.cfg.LineEnding)
	} else {
		final.buf.AppendByte(tskvLineEnding)
	}

	return final.buf, nil
}
