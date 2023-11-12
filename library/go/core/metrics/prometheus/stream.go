package prometheus

import (
	"context"
	"fmt"
	"io"

	"github.com/prometheus/common/expfmt"
)

const (
	StreamText    expfmt.Format = expfmt.FmtText
	StreamCompact expfmt.Format = expfmt.FmtProtoCompact
)

func (r *Registry) Stream(_ context.Context, w io.Writer) (int, error) {
	metrics, err := r.Gather()
	if err != nil {
		return 0, fmt.Errorf("cannot gather metrics: %w", err)
	}

	enc := expfmt.NewEncoder(w, r.streamFormat)
	for _, mf := range metrics {
		if err := enc.Encode(mf); err != nil {
			return 0, fmt.Errorf("cannot encode metric family: %w", err)
		}
	}

	// prometheus encoder does not report how much bytes have been written
	// so we indicate it by returning -1 instead
	return -1, nil
}
