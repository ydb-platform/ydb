package certs

import (
	"a.yandex-team.ru/library/go/core/resource"
)

func InternalCAs() []byte {
	return resource.Get("/certifi/internal.pem")
}

func CommonCAs() []byte {
	return resource.Get("/certifi/common.pem")
}
