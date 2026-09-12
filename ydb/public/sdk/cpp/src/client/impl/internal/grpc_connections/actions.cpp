#define INCLUDE_YDB_INTERNAL_H
#include "actions.h"
#include "grpc_connections.h"

#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>

using namespace std::chrono_literals;

namespace NYdb::inline Dev {

constexpr TDeadline::Duration MAX_DEFERRED_CALL_DELAY = 10s;

TDeferredAction::TDeferredAction(const std::string& operationId,
    TDeferredOperationCb&& userCb,
    TGRpcConnectionsImpl* connection,
    std::shared_ptr<IQueueClientContext> context,
    TDeadline::Duration delay,
    TDeadline globalDeadline,
    TDbDriverStatePtr dbState,
    const std::string& endpoint)
    : UserResponseCb_(std::move(userCb))
    , Connection_(connection)
    , Context_(std::move(context))
    , Deadline_(std::min(globalDeadline, TDeadline::AfterDuration(delay)))
    , NextDelay_(std::min(delay * 2, MAX_DEFERRED_CALL_DELAY))
    , GlobalDeadline_(globalDeadline)
    , DbDriverState_(std::move(dbState))
    , OperationId_(operationId)
    , Endpoint_(endpoint)
{
}

void TDeferredAction::Start() {
    auto context = Context_;
    const auto deadline = Deadline_;
    GetSdkRuntime().ScheduleCallback(deadline,
        [action = std::move(*this)](bool ok) mutable { action.Complete(ok); },
        std::move(context));
}

void TDeferredAction::Complete(bool ok) {
    if (!ok) {
        NYdbGrpc::TGrpcStatus status = {"Deferred timer interrupted", -1, true};
        DbDriverState_->StatCollector.IncDiscoveryFailDueTransportError();
        TPlainStatus plainStatus(status, Endpoint_, {});
        if (!Endpoint_.empty()) {
            plainStatus.Issues.AddIssue(NYdb::NIssue::TIssue("Grpc error response on endpoint " + Endpoint_));
        }
        Connection_->PostToResponseQueue(
            [callback = std::move(UserResponseCb_), status = std::move(plainStatus)]() mutable {
                auto runningCallback = std::move(callback);
                runningCallback(nullptr, std::move(status));
            });
        return;
    }

    Ydb::Operations::GetOperationRequest request;
    request.set_id(TStringType{OperationId_});
    TRpcRequestSettings settings;
    settings.PreferredEndpoint = TEndpointKey(Endpoint_, 0);
    settings.Deadline = GlobalDeadline_;
    Connection_->RunDeferred<Ydb::Operation::V1::OperationService, Ydb::Operations::GetOperationRequest, Ydb::Operations::GetOperationResponse>(
        std::move(request),
        std::move(UserResponseCb_),
        &Ydb::Operation::V1::OperationService::Stub::AsyncGetOperation,
        DbDriverState_, NextDelay_, settings, true, std::move(Context_));
}

} // namespace NYdb
