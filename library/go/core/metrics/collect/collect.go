package collect

import (
	"context"

	"a.yandex-team.ru/library/go/core/metrics"
)

type Func func(ctx context.Context, r metrics.Registry, c metrics.CollectPolicy)
