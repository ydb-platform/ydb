#pragma once

#include <src/client/impl/ydb_internal/internal_header.h>

#include <ydb-cpp-sdk/client/types/status_codes.h>
#include <ydb-cpp-sdk/client/types/ydb.h>

#include <ydb-cpp-sdk/library/yql_common/issue/yql_issue.h>

#include <ydb-cpp-sdk/library/grpc/client/grpc_client_low.h>


namespace NYdb {

// Other callbacks
using TSimpleCb = std::function<void()>;
using TErrorCb = std::function<void(NYdbGrpc::TGrpcStatus&)>;

struct TBalancingSettings {
    EBalancingPolicy Policy;
    std::string PolicyParams;
};

} // namespace NYdb
