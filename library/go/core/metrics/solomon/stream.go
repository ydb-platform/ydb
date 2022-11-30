package solomon

import (
	"context"
	"encoding/json"
	"io"

	"a.yandex-team.ru/library/go/core/xerrors"
)

const HeaderSize = 24

type StreamFormat string

func (r *Registry) StreamJSON(ctx context.Context, w io.Writer) (written int, err error) {
	cw := newCompressedWriter(w, CompressionNone)

	if ctx.Err() != nil {
		return written, xerrors.Errorf("streamJSON context error: %w", ctx.Err())
	}
	_, err = cw.Write([]byte("{\"metrics\":["))
	if err != nil {
		return written, xerrors.Errorf("write metrics failed: %w", err)
	}

	first := true
	r.metrics.Range(func(_, s interface{}) bool {
		if ctx.Err() != nil {
			err = xerrors.Errorf("streamJSON context error: %w", ctx.Err())
			return false
		}

		// write trailing comma
		if !first {
			_, err = cw.Write([]byte(","))
			if err != nil {
				err = xerrors.Errorf("write metrics failed: %w", err)
				return false
			}
		}

		var b []byte

		b, err = json.Marshal(s)
		if err != nil {
			err = xerrors.Errorf("marshal metric failed: %w", err)
			return false
		}

		// write metric json
		_, err = cw.Write(b)
		if err != nil {
			err = xerrors.Errorf("write metric failed: %w", err)
			return false
		}

		first = false
		return true
	})
	if err != nil {
		return written, err
	}

	if ctx.Err() != nil {
		return written, xerrors.Errorf("streamJSON context error: %w", ctx.Err())
	}
	_, err = cw.Write([]byte("]}"))
	if err != nil {
		return written, xerrors.Errorf("write metrics failed: %w", err)
	}

	if ctx.Err() != nil {
		return written, xerrors.Errorf("streamJSON context error: %w", ctx.Err())
	}
	err = cw.Close()
	if err != nil {
		return written, xerrors.Errorf("close failed: %w", err)
	}

	return cw.(*noCompressionWriteCloser).written, nil
}

func (r *Registry) StreamSpack(ctx context.Context, w io.Writer, compression CompressionType) (int, error) {
	metrics, err := r.Gather()
	if err != nil {
		return 0, err
	}
	return NewSpackEncoder(ctx, compression, metrics).Encode(w)
}
