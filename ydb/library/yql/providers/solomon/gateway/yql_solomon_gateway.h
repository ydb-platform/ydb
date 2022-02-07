#pragma once

#include <ydb/library/yql/providers/solomon/provider/yql_solomon_gateway.h>

namespace NYql {

class TSolomonGatewayConfig;

ISolomonGateway::TPtr CreateSolomonGateway(const TSolomonGatewayConfig& config);

} // namespace NYql
