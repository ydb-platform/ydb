#pragma once

#include <yt/yql/providers/yt/lib/tvm_client/tvm_client.h>

namespace NYql {

ITvmClient::TPtr CreateDummyTvmClient();

} // namespace NYql
