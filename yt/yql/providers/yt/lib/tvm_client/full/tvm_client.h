#pragma once

#include <yt/yql/providers/yt/lib/tvm_client/tvm_client.h>
#include <yt/yql/providers/yt/lib/tvm_client/proto/tvm_client.pb.h>

namespace NYql {

ITvmClient::TPtr CreateTvmClient(const TYtTvmConfig& config);

}; // namespace NYql
