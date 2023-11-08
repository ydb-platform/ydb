package all

import (
	_ "github.com/ydb-platform/ydb/library/go/blockcodecs/blockbrotli"
	_ "github.com/ydb-platform/ydb/library/go/blockcodecs/blocklz4"
	_ "github.com/ydb-platform/ydb/library/go/blockcodecs/blocksnappy"
	_ "github.com/ydb-platform/ydb/library/go/blockcodecs/blockzstd"
)
