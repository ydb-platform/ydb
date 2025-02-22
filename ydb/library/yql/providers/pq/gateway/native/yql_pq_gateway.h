#pragma once

#include <yql/essentials/providers/common/metrics/metrics_registry.h>
#include <ydb/library/yql/providers/pq/cm_client/client.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_gateway.h>

#include <ydb/library/yql/providers/common/token_accessor/client/factory.h>

#include <ydb-cpp-sdk/client/driver/driver.h>

#include <util/generic/ptr.h>

namespace NKikimr::NMiniKQL {
class IFunctionRegistry;
} // namespace NKikimr::NMiniKQL

namespace NYql {

IPqGateway::TPtr CreatePqNativeGateway(const TPqGatewayServices& services);

IPqGatewayFactory::TPtr CreatePqNativeGatewayFactory();

} // namespace NYql
