package utils

import (
	"fmt"

	api_common "github.com/ydb-platform/ydb/ydb/library/yql/providers/generic/connector/api/common"
)

func EndpointToString(ep *api_common.TEndpoint) string {
	return fmt.Sprintf("%s:%d", ep.GetHost(), ep.GetPort())
}
