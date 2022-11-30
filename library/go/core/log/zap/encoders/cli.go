package encoders

import (
	"sync"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

const (
	// EncoderNameCli is the encoder name to use for zap config
	EncoderNameCli = "cli"
)

var cliPool = sync.Pool{New: func() interface{} {
	return &cliEncoder{}
}}

func getCliEncoder() *cliEncoder {
	return cliPool.Get().(*cliEncoder)
}

type cliEncoder struct {
	*kvEncoder
}

// NewCliEncoder constructs cli encoder
func NewCliEncoder(cfg zapcore.EncoderConfig) (zapcore.Encoder, error) {
	return newCliEncoder(cfg), nil
}

func newCliEncoder(cfg zapcore.EncoderConfig) *cliEncoder {
	return &cliEncoder{
		kvEncoder: newKVEncoder(cfg),
	}
}

func (enc *cliEncoder) Clone() zapcore.Encoder {
	clone := enc.clone()
	_, _ = clone.buf.Write(enc.buf.Bytes())
	return clone
}

func (enc *cliEncoder) clone() *cliEncoder {
	clone := getCliEncoder()
	clone.kvEncoder = getKVEncoder()
	clone.cfg = enc.cfg
	clone.openNamespaces = enc.openNamespaces
	clone.pool = enc.pool
	clone.buf = enc.pool.Get()
	return clone
}

func (enc *cliEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	final := enc.clone()

	// Direct write because we do not want to quote message in cli mode
	final.buf.AppendString(ent.Message)

	// Add any structured context.
	for _, f := range fields {
		f.AddTo(final)
	}

	// If there's no stacktrace key, honor that; this allows users to force
	// single-line output.
	if ent.Stack != "" && final.cfg.StacktraceKey != "" {
		final.buf.AppendByte('\n')
		final.AppendString(ent.Stack)
	}

	if final.cfg.LineEnding != "" {
		final.AppendString(final.cfg.LineEnding)
	} else {
		final.AppendString(zapcore.DefaultLineEnding)
	}
	return final.buf, nil
}
