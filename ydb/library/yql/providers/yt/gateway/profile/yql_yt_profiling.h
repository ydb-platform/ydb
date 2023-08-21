#pragma once

#include <ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>

namespace NYql {

IYtGateway::TPtr CreateYtProfilingGateway(IYtGateway::TPtr slave);

} // namspace NYql
