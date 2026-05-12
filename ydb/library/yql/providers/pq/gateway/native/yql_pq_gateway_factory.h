#pragma once

#include "yql_pq_gateway_services.h"

#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h>

namespace NYql {

IPqGatewayFactory::TPtr CreatePqNativeGatewayFactory(const TPqGatewayServices& services);

} // namespace NYql
