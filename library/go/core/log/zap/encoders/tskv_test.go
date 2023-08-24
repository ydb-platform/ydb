package encoders

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

func TestTSKVEscaper(t *testing.T) {
	tests := []struct {
		input         string
		expectedKey   string
		expectedValue string
		desc          string
	}{
		{
			input:         "plain text$ no need to escape",
			expectedKey:   "plain text$ no need to escape",
			expectedValue: "plain text$ no need to escape",
			desc:          "test without escape",
		}, {
			input:         "test escape\tab",
			expectedKey:   `test escape\tab`,
			expectedValue: `test escape\tab`,
			desc:          "escape tab",
		},
		{
			input:         "\ntest es\\cape\t\t a\rll char\x00s in string=",
			expectedKey:   `\ntest es\\cape\t\t a\rll char\0s in string\=`,
			expectedValue: `\ntest es\\cape\t\t a\rll char\0s in string=`,
			desc:          "escape all chars",
		},
	}
	esc := newTSKVEscaper()
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			cur := esc.escapeKey(tt.input)
			assert.Equal(t, tt.expectedKey, cur, "Incorrect escaped TSKV key.")
		})

		t.Run(tt.desc, func(t *testing.T) {
			cur := esc.escapeValue(tt.input)
			assert.Equal(t, tt.expectedValue, cur, "Incorrect escaped TSKV value.")
		})
	}
}

type noJSON struct{}

func (nj noJSON) MarshalJSON() ([]byte, error) {
	return nil, errors.New("no")
}

type nonloggable struct{}

func (l nonloggable) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return nil
}

type loggable struct {
	bool bool
	spec string
}

func (l loggable) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	return nil
}

func (l loggable) String() string {

	return fmt.Sprintf("loggable%s=%t%s", l.spec, l.bool, l.spec)
}

func (l loggable) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	if !l.bool {
		return errors.New("can't marshal")
	}
	enc.AppendBool(l.bool)
	return nil
}

type loggables int

func (ls loggables) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	l := loggable{true, ""}
	for i := 0; i < int(ls); i++ {
		if err := enc.AppendObject(l); err != nil {
			return err
		}
	}
	return nil
}

func getCommonTestConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:     "M",
		LevelKey:       "L",
		TimeKey:        "T",
		NameKey:        "N",
		CallerKey:      "C",
		StacktraceKey:  "S",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.EpochTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

func getSpecCharsTestConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:     "M\t",
		LevelKey:       "L\n",
		TimeKey:        "T\r",
		NameKey:        "N\x00",
		CallerKey:      "C=",
		StacktraceKey:  "S\\",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.EpochTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

func getRawTskvEncoder() *tskvEncoder {
	pool := buffer.NewPool()
	cfg := getCommonTestConfig()
	return &tskvEncoder{
		cfg:         cfg,
		pool:        pool,
		buf:         pool.Get(),
		tskvEscaper: newTSKVEscaper(),
	}

}

func assertOutput(t testing.TB, expected string, f func(encoder zapcore.Encoder)) {
	enc := getRawTskvEncoder()
	f(enc)
	assert.Equal(t, expected, enc.buf.String(), "Unexpected encoder output after adding.")

	enc.buf.Reset()
	enc.AddString("foo", "bar")
	f(enc)
	expectedPrefix := `foo=bar`
	if expected != "" {
		// If we expect output, it should be tab-separated from the previous
		// field.
		expectedPrefix += "\t"
	}
	assert.Equal(t, expectedPrefix+expected, enc.buf.String(), "Unexpected encoder output after adding as a second field.")
}

func TestTSKVEncoderObjectFields(t *testing.T) {
	tests := []struct {
		desc     string
		expected string
		f        func(encoder zapcore.Encoder)
	}{
		{"binary", `k=61623132`, func(e zapcore.Encoder) { e.AddBinary("k", []byte("ab12")) }},
		{"binary esc ", `k\n=61623132`, func(e zapcore.Encoder) { e.AddBinary("k\n", []byte("ab12")) }},
		{"bool", `k=true`, func(e zapcore.Encoder) { e.AddBool("k", true) }},
		{"bool", `k\t=false`, func(e zapcore.Encoder) { e.AddBool("k\t", false) }},

		{"byteString", `k=765c`, func(e zapcore.Encoder) { e.AddByteString(`k`, []byte(`v\`)) }},
		{"byteString esc", `k\t=61623132`, func(e zapcore.Encoder) { e.AddByteString("k\t", []byte("ab12")) }},
		{"byteString empty val", `k=`, func(e zapcore.Encoder) { e.AddByteString("k", []byte{}) }},
		{"byteString nil val", `k=`, func(e zapcore.Encoder) { e.AddByteString("k", nil) }},

		{"complex128", `k="1+2i"`, func(e zapcore.Encoder) { e.AddComplex128("k", 1+2i) }},
		{"complex128 esc", `k\t="1+2i"`, func(e zapcore.Encoder) { e.AddComplex128("k\t", 1+2i) }},
		{"complex64", `k="1+2i"`, func(e zapcore.Encoder) { e.AddComplex64("k", 1+2i) }},
		{"complex64 esc", `k\t="1+2i"`, func(e zapcore.Encoder) { e.AddComplex64("k\t", 1+2i) }},

		{"duration", `k$=0.000000001`, func(e zapcore.Encoder) { e.AddDuration("k$", 1) }},
		{"duration esc", `k\t=0.000000001`, func(e zapcore.Encoder) { e.AddDuration("k\t", 1) }},

		{"float64", `k=1`, func(e zapcore.Encoder) { e.AddFloat64("k", 1.0) }},
		{"float64 esc", `k\t=1`, func(e zapcore.Encoder) { e.AddFloat64("k\t", 1.0) }},
		{"float64", `k=10000000000`, func(e zapcore.Encoder) { e.AddFloat64("k", 1e10) }},
		{"float64", `k="NaN"`, func(e zapcore.Encoder) { e.AddFloat64("k", math.NaN()) }},
		{"float64", `k="+Inf"`, func(e zapcore.Encoder) { e.AddFloat64("k", math.Inf(1)) }},
		{"float64", `k="-Inf"`, func(e zapcore.Encoder) { e.AddFloat64("k", math.Inf(-1)) }},

		{"float32", `k=1`, func(e zapcore.Encoder) { e.AddFloat32("k", 1.0) }},
		{"float32", `k\t=1`, func(e zapcore.Encoder) { e.AddFloat32("k\t", 1.0) }},
		{"float32", `k=10000000000`, func(e zapcore.Encoder) { e.AddFloat32("k", 1e10) }},
		{"float32", `k="NaN"`, func(e zapcore.Encoder) { e.AddFloat32("k", float32(math.NaN())) }},
		{"float32", `k="+Inf"`, func(e zapcore.Encoder) { e.AddFloat32("k", float32(math.Inf(1))) }},
		{"float32", `k="-Inf"`, func(e zapcore.Encoder) { e.AddFloat32("k", float32(math.Inf(-1))) }},

		{"int", `k=42`, func(e zapcore.Encoder) { e.AddInt("k", 42) }},
		{"int esc", `k\t=42`, func(e zapcore.Encoder) { e.AddInt("k\t", 42) }},
		{"int64", `k=42`, func(e zapcore.Encoder) { e.AddInt64("k", 42) }},
		{"int32", `k=42`, func(e zapcore.Encoder) { e.AddInt32("k", 42) }},
		{"int16", `k=42`, func(e zapcore.Encoder) { e.AddInt16("k", 42) }},
		{"int8", `k=42`, func(e zapcore.Encoder) { e.AddInt8("k", 42) }},

		{"string", `k=v$`, func(e zapcore.Encoder) { e.AddString("k", "v$") }},
		{"string esc", `k\t=v\\`, func(e zapcore.Encoder) { e.AddString("k\t", `v\`) }},
		{"string", `k=`, func(e zapcore.Encoder) { e.AddString("k", "") }},

		{"time", `k=1`, func(e zapcore.Encoder) { e.AddTime("k", time.Unix(1, 0)) }},
		{"time esc", `k\t=1`, func(e zapcore.Encoder) { e.AddTime("k\t", time.Unix(1, 0)) }},

		{"uint", `k=42`, func(e zapcore.Encoder) { e.AddUint("k", 42) }},
		{"uint esc", `k\t=42`, func(e zapcore.Encoder) { e.AddUint("k\t", 42) }},
		{"uint64", `k=42`, func(e zapcore.Encoder) { e.AddUint64("k", 42) }},
		{"uint32", `k=42`, func(e zapcore.Encoder) { e.AddUint32("k", 42) }},
		{"uint16", `k=42`, func(e zapcore.Encoder) { e.AddUint16("k", 42) }},
		{"uint8", `k=42`, func(e zapcore.Encoder) { e.AddUint8("k", 42) }},
		{"uintptr", `k=42`, func(e zapcore.Encoder) { e.AddUintptr("k", 42) }},
		{
			desc:     "object (success)",
			expected: `k={loggable=true}`,
			f: func(e zapcore.Encoder) {
				assert.NoError(t, e.AddObject("k", loggable{true, ""}), "Unexpected error calling AddObject.")
			},
		},
		{
			desc:     "object esc (success)",
			expected: `k={loggable\t=true\t}`,
			f: func(e zapcore.Encoder) {
				assert.NoError(t, e.AddObject("k", loggable{true, "\t"}), "Unexpected error calling AddObject.")
			},
		},
		{
			desc:     "object (error)",
			expected: `k={}`,
			f: func(e zapcore.Encoder) {
				assert.Error(t, e.AddObject("k", nonloggable{}), "Expected an error calling AddObject.")
			},
		},
		{
			desc:     "array (with nested object)",
			expected: `loggables=[{loggable=true},{loggable=true}]`,
			f: func(e zapcore.Encoder) {
				assert.NoError(
					t,
					e.AddArray("loggables", loggables(2)),
					"Unexpected error calling AddObject with nested ArrayMarshalers.",
				)
			},
		},
		{
			desc:     "array (success)",
			expected: `k=[true]`,
			f: func(e zapcore.Encoder) {
				assert.NoError(t, e.AddArray(`k`, loggable{true, ""}), "Unexpected error calling MarshalLogArray.")
			},
		},
		{
			desc:     "array esc (success)",
			expected: `k\t=[true]`,
			f: func(e zapcore.Encoder) {
				assert.NoError(t, e.AddArray("k\t", loggable{true, ""}), "Unexpected error calling MarshalLogArray.")
			},
		},
		{
			desc:     "array (error)",
			expected: `k=[]`,
			f: func(e zapcore.Encoder) {
				assert.Error(t, e.AddArray("k", loggable{false, ""}), "Expected an error calling MarshalLogArray.")
			},
		},
		{
			desc:     "reflect enc (success)",
			expected: `k\t={"aee":"l=l","bee":123,"cee":0.9999,"dee":[{"key":"p\\ni","val":3.141592653589793},{"key":"tau=","val":6.283185307179586}]}`,
			f: func(e zapcore.Encoder) {
				type bar struct {
					Key string  `json:"key"`
					Val float64 `json:"val"`
				}

				type foo struct {
					A string  `json:"aee"`
					B int     `json:"bee"`
					C float64 `json:"cee"`
					D []bar   `json:"dee"`
				}

				assert.NoError(t, e.AddReflected("k\t", foo{
					A: "l=l",
					B: 123,
					C: 0.9999,
					D: []bar{
						{"p\ni", 3.141592653589793},
						{"tau=", 6.283185307179586},
					},
				}), "Unexpected error JSON-serializing a map.")
			},
		},
		{
			desc:     "reflect (failure)",
			expected: "",
			f: func(e zapcore.Encoder) {
				assert.Error(t, e.AddReflected("k", noJSON{}), "Unexpected success JSON-serializing a noJSON.")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			assertOutput(t, tt.expected, tt.f)
		})
	}
}

func TestTskvEncoderOpenNamespace(t *testing.T) {
	enc := getRawTskvEncoder()
	assert.PanicsWithValue(t, "TSKV encoder does not support namespaces", func() { enc.OpenNamespace("test") })
}

func TestTSKVEncoderArrays(t *testing.T) {
	tests := []struct {
		desc     string
		expected string // expect f to be called twice
		f        func(zapcore.ArrayEncoder)
	}{
		{"bool", `[true,true]`, func(e zapcore.ArrayEncoder) { e.AppendBool(true) }},
		{"byteString", `[6b,6b]`, func(e zapcore.ArrayEncoder) { e.AppendByteString([]byte("k")) }},
		{"byteString", `[6b5c,6b5c]`, func(e zapcore.ArrayEncoder) { e.AppendByteString([]byte(`k\`)) }},
		{"complex128", `["1+2i","1+2i"]`, func(e zapcore.ArrayEncoder) { e.AppendComplex128(1 + 2i) }},
		{"complex64", `["1+2i","1+2i"]`, func(e zapcore.ArrayEncoder) { e.AppendComplex64(1 + 2i) }},
		{"durations", `[0.000000002,0.000000002]`, func(e zapcore.ArrayEncoder) { e.AppendDuration(2) }},
		{"float64", `[3.14,3.14]`, func(e zapcore.ArrayEncoder) { e.AppendFloat64(3.14) }},
		{"float32", `[3.14,3.14]`, func(e zapcore.ArrayEncoder) { e.AppendFloat32(3.14) }},
		{"int", `[42,42]`, func(e zapcore.ArrayEncoder) { e.AppendInt(42) }},
		{"int64", `[42,42]`, func(e zapcore.ArrayEncoder) { e.AppendInt64(42) }},
		{"int32", `[42,42]`, func(e zapcore.ArrayEncoder) { e.AppendInt32(42) }},
		{"int16", `[42,42]`, func(e zapcore.ArrayEncoder) { e.AppendInt16(42) }},
		{"int8", `[42,42]`, func(e zapcore.ArrayEncoder) { e.AppendInt8(42) }},
		{"string", `[k,k]`, func(e zapcore.ArrayEncoder) { e.AppendString("k") }},
		{"string", `[k\\,k\\]`, func(e zapcore.ArrayEncoder) { e.AppendString(`k\`) }},
		{"times", `[1,1]`, func(e zapcore.ArrayEncoder) { e.AppendTime(time.Unix(1, 0)) }},
		{"uint", `[42,42]`, func(e zapcore.ArrayEncoder) { e.AppendUint(42) }},
		{"uint64", `[42,42]`, func(e zapcore.ArrayEncoder) { e.AppendUint64(42) }},
		{"uint32", `[42,42]`, func(e zapcore.ArrayEncoder) { e.AppendUint32(42) }},
		{"uint16", `[42,42]`, func(e zapcore.ArrayEncoder) { e.AppendUint16(42) }},
		{"uint8", `[42,42]`, func(e zapcore.ArrayEncoder) { e.AppendUint8(42) }},
		{"uintptr", `[42,42]`, func(e zapcore.ArrayEncoder) { e.AppendUintptr(42) }},
		{
			desc:     "arrays (success)",
			expected: `[[true],[true]]`,
			f: func(arr zapcore.ArrayEncoder) {
				assert.NoError(t, arr.AppendArray(zapcore.ArrayMarshalerFunc(func(inner zapcore.ArrayEncoder) error {
					inner.AppendBool(true)
					return nil
				})), "Unexpected error appending an array.")
			},
		},
		{
			desc:     "arrays (error)",
			expected: `[[true],[true]]`,
			f: func(arr zapcore.ArrayEncoder) {
				assert.Error(t, arr.AppendArray(zapcore.ArrayMarshalerFunc(func(inner zapcore.ArrayEncoder) error {
					inner.AppendBool(true)
					return errors.New("fail")
				})), "Expected an error appending an array.")
			},
		},
		{
			desc:     "objects (success)",
			expected: `[{loggable=true},{loggable=true}]`,
			f: func(arr zapcore.ArrayEncoder) {
				assert.NoError(t, arr.AppendObject(loggable{true, ""}), "Unexpected error appending an object.")
			},
		},
		{
			desc:     "objects esc (success)",
			expected: `[{loggable\t=true\t},{loggable\t=true\t}]`,
			f: func(arr zapcore.ArrayEncoder) {
				assert.NoError(t, arr.AppendObject(loggable{true, "\t"}), "Unexpected error appending an object.")
			},
		},
		{
			desc:     "objects (error: fmt.Stringer not implemented)",
			expected: `[{},{}]`,
			f: func(arr zapcore.ArrayEncoder) {
				assert.Error(t, arr.AppendObject(nonloggable{}), "Expected an error appending an object.")
			},
		},
		{
			desc:     "reflect (success)",
			expected: `[{"foo":5},{"foo":5}]`,
			f: func(arr zapcore.ArrayEncoder) {
				assert.NoError(
					t,
					arr.AppendReflected(map[string]int{"foo": 5}),
					"Unexpected an error appending an object with reflection.",
				)
			},
		},
		{
			desc:     "reflect esc (success)",
			expected: `[{"foo\\t":5},{"foo\\t":5}]`,
			f: func(arr zapcore.ArrayEncoder) {
				assert.NoError(
					t,
					arr.AppendReflected(map[string]int{"foo\t": 5}),
					"Unexpected an error appending an object with reflection.",
				)
			},
		},
		{
			desc:     "reflect (error)",
			expected: `[]`,
			f: func(arr zapcore.ArrayEncoder) {
				assert.Error(
					t,
					arr.AppendReflected(noJSON{}),
					"Unexpected an error appending an object with reflection.",
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			f := func(enc zapcore.Encoder) error {
				return enc.AddArray("array", zapcore.ArrayMarshalerFunc(func(arr zapcore.ArrayEncoder) error {
					tt.f(arr)
					tt.f(arr)
					return nil
				}))
			}
			assertOutput(t, `array=`+tt.expected, func(enc zapcore.Encoder) {
				err := f(enc)
				assert.NoError(t, err, "Unexpected error adding array to JSON encoder.")
			})
		})
	}
}

func TestTSKVEncodeEntry(t *testing.T) {
	entryTime := time.Date(2019, 7, 13, 15, 33, 42, 99, time.UTC)

	tests := []struct {
		desc     string
		expected string
		cnf      zapcore.EncoderConfig
		ent      zapcore.Entry
		fields   []zapcore.Field
	}{
		{
			desc: "entry without escape",
			expected: `tskv	T=1563032022	L=info	M=text here
`,
			cnf: getCommonTestConfig(),
			ent: zapcore.Entry{
				Time:    entryTime,
				Message: "text here",
			},
			fields: []zapcore.Field{},
		},
		{
			desc: "all fields entry without escape",
			expected: `tskv	T=1563032022	L=debug	N=bob	C=foo.go:42	M=text here	S=fake-stack
`,
			cnf: getCommonTestConfig(),
			ent: zapcore.Entry{
				Level:      zapcore.DebugLevel,
				Time:       entryTime,
				LoggerName: "bob",
				Message:    "text here",
				Caller:     zapcore.EntryCaller{Defined: true, File: "foo.go", Line: 42},
				Stack:      "fake-stack",
			},
			fields: []zapcore.Field{},
		},
		{
			desc: "entry with escaped field names",
			expected: `tskv	T\r=1563032022	L\n=debug	N\0=bob	C\==foo.go:42	M\t=text here	S\\=fake-stack
`,
			cnf: getSpecCharsTestConfig(),
			ent: zapcore.Entry{
				Level:      zapcore.DebugLevel,
				Time:       entryTime,
				LoggerName: "bob",
				Message:    "text here",
				Caller:     zapcore.EntryCaller{Defined: true, File: "foo.go", Line: 42},
				Stack:      "fake-stack",
			},
			fields: []zapcore.Field{},
		},
		{
			desc: "entry message escape",
			expected: `tskv	T=1563032022	L=info	M=t\\ex=t\0he\r\tre\n
`,
			cnf: getCommonTestConfig(),
			ent: zapcore.Entry{
				Time:    entryTime,
				Message: "t\\ex=t\x00he\r\tre\n",
			},
			fields: []zapcore.Field{},
		},
		{
			desc: "entry multi-line stack escape",
			expected: `tskv	T=1563032022	L=info	M=	S=fake-st\rack\n\tlevel2\n\tlevel1
`,
			cnf: getCommonTestConfig(),
			ent: zapcore.Entry{
				Time:  entryTime,
				Stack: "fake-st\rack\n\tlevel2\n\tlevel1",
			},
			fields: []zapcore.Field{},
		},
		{
			desc: "entry multi-line caller escape",
			expected: `tskv	T=1563032022	L=info	C=fo\to.go:42	M=
`,
			cnf: getCommonTestConfig(),
			ent: zapcore.Entry{
				Time:   entryTime,
				Caller: zapcore.EntryCaller{Defined: true, File: "fo\to.go", Line: 42},
			},
			fields: []zapcore.Field{},
		},
		{
			desc: "entry multi-line logger escape",
			expected: `tskv	T=1563032022	L=info	N=b\0b	M=
`,
			cnf: getCommonTestConfig(),
			ent: zapcore.Entry{
				Time:       entryTime,
				LoggerName: "b\x00b",
			},
			fields: []zapcore.Field{},
		},
		{
			desc: "entry with additional zap fields",
			expected: `tskv	T=1563032022	L=info	M=	so=passes	answer=42	common_pie=3.14	` +
				`reflect={"loggable":"yes"}	bytes_array=0001020309	bool=true	complex="0+1i"
`,
			cnf: getCommonTestConfig(),
			ent: zapcore.Entry{
				Time: entryTime,
			},
			fields: []zapcore.Field{
				zap.String("so", "passes"),
				zap.Int("answer", 42),
				zap.Float64("common_pie", 3.14),
				zap.Reflect("reflect", map[string]string{"loggable": "yes"}),
				zap.Binary("bytes_array", []byte{0, 1, 2, 3, '\t'}),
				zap.Bool("bool", true),
				zap.Complex128("complex", 1i)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			enc, err := NewTSKVEncoder(tt.cnf)
			if err != nil {
				panic(err)
			}

			buf, err := enc.EncodeEntry(tt.ent, tt.fields)
			if assert.NoError(t, err, "Unexpected TSKV encoding error.") {
				assert.Equal(t, tt.expected, buf.String(), "Incorrect encoded TSKV entry.")
			}
			buf.Free()
		})
	}
}

func TestTskvEncoderLoggerWithMethod(t *testing.T) {
	entryTime := time.Date(2019, 7, 13, 15, 33, 42, 99, time.UTC)

	enc := getRawTskvEncoder()
	enc.AddString("Permanent", "message")
	enc.Clone()
	tt := struct {
		desc     string
		expected string
		ent      zapcore.Entry
	}{
		desc: "entry without escape",
		expected: `tskv	T=1563032022	L=info	Permanent=message	M=text here
`,
		ent: zapcore.Entry{
			Time:    entryTime,
			Message: "text here",
		},
	}

	for i := 0; i < 3; i++ {
		t.Run(tt.desc, func(t *testing.T) {
			buf, err := enc.EncodeEntry(tt.ent, []zapcore.Field{})
			if assert.NoError(t, err, "Unexpected TSKV encoding error.") {
				assert.Equal(t, tt.expected, buf.String(), "Incorrect encoded TSKV entry.")
			}
		})
	}
}
