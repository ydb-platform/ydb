#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/db_driver_state/state.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/plain_status/status.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

namespace NYdb::inline Dev {

using NYdbGrpc::IQueueClientContext;

class TGRpcConnectionsImpl;

template<typename TResponse>
using TResponseCb = std::function<void(TResponse*, TPlainStatus status)>;
using TDeferredOperationCb = std::function<void(Ydb::Operations::Operation*, TPlainStatus status)>;

class TDeferredAction {
public:
    TDeferredAction(
        const std::string& operationId,
        TDeferredOperationCb&& userCb,
        TGRpcConnectionsImpl* connection,
        std::shared_ptr<IQueueClientContext> context,
        TDeadline::Duration delay,
        TDeadline globalDeadline,
        TDbDriverStatePtr dbState,
        const std::string& endpoint);

    void Start();

private:
    void Complete(bool ok);

    TDeferredOperationCb UserResponseCb_;
    TGRpcConnectionsImpl* Connection_;
    std::shared_ptr<IQueueClientContext> Context_;
    TDeadline Deadline_;
    TDeadline::Duration NextDelay_;
    TDeadline GlobalDeadline_;
    TDbDriverStatePtr DbDriverState_;
    std::string OperationId_;
    std::string Endpoint_;
};

} // namespace NYdb
