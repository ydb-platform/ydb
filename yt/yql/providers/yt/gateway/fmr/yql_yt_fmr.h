#pragma once

#include <yt/yql/providers/yt/provider/yql_yt_forwarding_gateway.h>

namespace NYql {

IYtGateway::TPtr CreateYtFmrGateway(IYtGateway::TPtr slave);

} // namspace NYql
