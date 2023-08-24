package encoders

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestKVEncodeEntry(t *testing.T) {
	type bar struct {
		Key string `json:"key"`

		Val float64 `json:"val"`
	}

	type foo struct {
		A string  `json:"aee"`
		B int     `json:"bee"`
		C float64 `json:"cee"`
		D []bar   `json:"dee"`
	}

	tests := []struct {
		desc     string
		expected string
		ent      zapcore.Entry
		fields   []zapcore.Field
	}{
		{
			desc: "info entry with some fields",
			expected: `T=2018-06-19T16:33:42.000Z L=info N=bob M='lob law' so=passes answer=42 common_pie=3.14 ` +
				`such={"aee":"lol","bee":123,"cee":0.9999,"dee":[{"key":"pi","val":3.141592653589793},` +
				`{"key":"tau","val":6.283185307179586}]}
`,
			ent: zapcore.Entry{
				Level:      zapcore.InfoLevel,
				Time:       time.Date(2018, 6, 19, 16, 33, 42, 99, time.UTC),
				LoggerName: "bob",
				Message:    "lob law",
			},
			fields: []zapcore.Field{
				zap.String("so", "passes"),
				zap.Int("answer", 42),
				zap.Float64("common_pie", 3.14),
				zap.Reflect("such", foo{
					A: "lol",
					B: 123,
					C: 0.9999,
					D: []bar{
						{"pi", 3.141592653589793},
						{"tau", 6.283185307179586},
					},
				}),
			},
		},
		{
			desc: "info entry with array fields",
			expected: `T=2020-06-26T11:13:42.000Z L=info N=alice M='str array' env=test ` +
				`intarray=[-5,-7,0,-12] ` +
				`uintarray=[1,2,3,4,5] ` +
				`strarray=[funny,bunny] ` +
				`book=['Alice's Adventures in Wonderland','Lewis Carroll',26-11-1865] ` +
				`floatarray=[3.14,-2.17,0.0000000000000000000000000000000000662607]` + "\n",
			ent: zapcore.Entry{
				Level:      zapcore.InfoLevel,
				Time:       time.Date(2020, 6, 26, 11, 13, 42, 0, time.UTC),
				LoggerName: "alice",
				Message:    "str array",
			},
			fields: []zapcore.Field{
				zap.String("env", "test"),
				zap.Ints("intarray", []int{-5, -7, 0, -12}),
				zap.Uints("uintarray", []uint{1, 2, 3, 4, 5}),
				zap.Strings("strarray", []string{"funny", "bunny"}),
				zap.Strings("book", []string{"Alice's Adventures in Wonderland", "Lewis Carroll", "26-11-1865"}),
				zap.Float32s("floatarray", []float32{3.14, -2.17, 0.662607015e-34}),
			},
		},
		{
			desc:     "corner cases of arrays",
			expected: "T=2020-06-26T12:13:42.000Z L=info N=zorg M='str array' cornerequal=['hello=',world] cornerbracket=['is[',jail,']'] cornerbraces=['is{',exit,'}']\n",
			ent: zapcore.Entry{
				Level:      zapcore.InfoLevel,
				Time:       time.Date(2020, 6, 26, 12, 13, 42, 0, time.UTC),
				LoggerName: "zorg",
				Message:    "str array",
			},
			fields: []zapcore.Field{
				zap.Strings("cornerequal", []string{"hello=", "world"}),
				zap.Strings("cornerbracket", []string{"is[", "jail", "]"}),
				zap.Strings("cornerbraces", []string{"is{", "exit", "}"}),
			},
		},
	}

	enc, _ := NewKVEncoder(zapcore.EncoderConfig{
		MessageKey:     "M",
		LevelKey:       "L",
		TimeKey:        "T",
		NameKey:        "N",
		CallerKey:      "C",
		StacktraceKey:  "S",
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	})

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			buf, err := enc.EncodeEntry(tt.ent, tt.fields)
			if assert.NoError(t, err, "Unexpected KV encoding error.") {
				assert.Equal(t, tt.expected, buf.String(), "Incorrect encoded KV entry.")
			}
			buf.Free()
		})
	}
}
