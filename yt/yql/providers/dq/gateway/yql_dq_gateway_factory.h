#pragma once

#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>

namespace NYql {

TIntrusivePtr<IDqGateway> CreateDqGateway(const TString& host, int port);

} // namespace NYql
