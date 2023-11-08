package collect

import (
	"context"

	"github.com/ydb-platform/ydb/library/go/core/metrics"
)

type Func func(ctx context.Context, r metrics.Registry, c metrics.CollectPolicy)
