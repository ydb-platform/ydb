package certs

import (
	"github.com/ydb-platform/ydb/library/go/core/resource"
)

func InternalCAs() []byte {
	return resource.Get("/certifi/internal.pem")
}

func CommonCAs() []byte {
	return resource.Get("/certifi/common.pem")
}
