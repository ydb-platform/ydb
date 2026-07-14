#pragma once

#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>

namespace NYql {

namespace NProto {
class TDqConfig;
}

TIntrusivePtr<IDqGateway> CreateDqGateway(const TString& host, int port);
TIntrusivePtr<IDqGateway> CreateDqGateway(const NProto::TDqConfig& config);

} // namespace NYql
