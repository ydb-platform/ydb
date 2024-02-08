#define INCLUDE_YDB_INTERNAL_H
#include "actions.h"
#include "grpc_connections.h"

#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>

namespace NYdb {

constexpr TDuration MAX_DEFERRED_CALL_DELAY = TDuration::Seconds(10); // The max delay between GetOperation calls for one operation

TSimpleCbResult::TSimpleCbResult(
        TSimpleCb&& cb,
        TGRpcConnectionsImpl* connections,
        std::shared_ptr<IQueueClientContext> context)
    : TGenericCbHolder<TSimpleCb>(std::move(cb), connections, std::move(context))
{ }

void TSimpleCbResult::Process(void*) {
    UserResponseCb_();
    delete this;
}

TDeferredAction::TDeferredAction(const TStringType& operationId,
        TDeferredOperationCb&& userCb,
        TGRpcConnectionsImpl* connection,
        std::shared_ptr<IQueueClientContext> context,
        TDuration delay,
        TDbDriverStatePtr dbState,
        const TStringType& endpoint)
    : TAlarmActionBase(std::move(userCb), connection, std::move(context))
    , NextDelay_(Min(delay * 2, MAX_DEFERRED_CALL_DELAY))
    , DbDriverState_(dbState)
    , OperationId_(operationId)
    , Endpoint_(endpoint)
{
    Deadline_ = gpr_time_add(
        gpr_now(GPR_CLOCK_MONOTONIC),
        gpr_time_from_micros(delay.MicroSeconds(), GPR_TIMESPAN));
}

void TDeferredAction::OnAlarm() {
    Y_ABORT_UNLESS(Connection_);

    Ydb::Operations::GetOperationRequest getOperationRequest;
    getOperationRequest.set_id(OperationId_);

    TRpcRequestSettings settings;
    settings.PreferredEndpoint = TEndpointKey(Endpoint_, 0);
    
    Connection_->RunDeferred<Ydb::Operation::V1::OperationService, Ydb::Operations::GetOperationRequest, Ydb::Operations::GetOperationResponse>(
        std::move(getOperationRequest),
        std::move(UserResponseCb_),
        &Ydb::Operation::V1::OperationService::Stub::AsyncGetOperation,
        DbDriverState_,
        NextDelay_,
        settings,
        true,
        std::move(Context_));
    }

void TDeferredAction::OnError() {
    Y_ABORT_UNLESS(Connection_);
    NYdbGrpc::TGrpcStatus status = {"Deferred timer interrupted", -1, true};
    DbDriverState_->StatCollector.IncDiscoveryFailDueTransportError();

    auto resp = new TGRpcErrorResponse<Ydb::Operations::Operation>(
        std::move(status),
        std::move(UserResponseCb_),
        Connection_,
        std::move(Context_),
        Endpoint_);
    Connection_->EnqueueResponse(resp);
}

TPeriodicAction::TPeriodicAction(
    TPeriodicCb&& userCb,
    TGRpcConnectionsImpl* connection,
    std::shared_ptr<NYdbGrpc::IQueueClientContext> context,
    TDuration period)
    : TAlarmActionBase(std::move(userCb), connection, std::move(context))
    , Period_(period)
{
    Deadline_ = gpr_time_add(
        gpr_now(GPR_CLOCK_MONOTONIC),
        gpr_time_from_micros(Period_.MicroSeconds(), GPR_TIMESPAN));
}

void TPeriodicAction::OnAlarm() {
    NYql::TIssues issues;
    if (!UserResponseCb_(std::move(issues), EStatus::SUCCESS)) {
        return;
    }

    auto ctx = Connection_->CreateContext();
    if (!ctx)
        return;
    Context_ = ctx;

    auto action = MakeIntrusive<TPeriodicAction>(
        std::move(UserResponseCb_),
        Connection_,
        Context_,
        Period_);
    action->Start();
}

void TPeriodicAction::OnError() {
    NYql::TIssues issues;
    issues.AddIssue(NYql::TIssue("Deferred timer interrupted"));
    UserResponseCb_(std::move(issues), EStatus::CLIENT_INTERNAL_ERROR);
}

} // namespace NYdb
