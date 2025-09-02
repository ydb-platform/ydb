#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/internal_header.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/ydb.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/issue/yql_issue.h>

#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>


namespace NYdb::inline Dev {

// Other callbacks
using TSimpleCb = std::function<void()>;
using TErrorCb = std::function<void(NYdbGrpc::TGrpcStatus&)>;

struct TBalancingSettings {
    EBalancingPolicy Policy;
    std::string PolicyParams;
};

} // namespace NYdb
