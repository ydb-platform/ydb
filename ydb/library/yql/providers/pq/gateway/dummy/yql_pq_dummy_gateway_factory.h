#pragma once

#include <ydb/library/yql/providers/pq/gateway/abstract/yql_pq_gateway.h>

namespace NYql {

IPqGatewayFactory::TPtr CreatePqFileGatewayFactory(IPqGateway::TPtr pqGateway);

} // namespace NYql
