#pragma once

#include <ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>
#include <ydb/library/yql/providers/yt/gateway/native/yql_yt_native.h>

namespace NYql {

IYtGateway::TPtr CreateYtMixedGateway(const TYtNativeServices& services);

} // namspace NYql
