#define INCLUDE_YDB_INTERNAL_H
#include "actions.h"
#include "grpc_connections.h"

#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>

using namespace std::chrono_literals;

namespace NYdb::inline Dev {

constexpr TDeadline::Duration MAX_DEFERRED_CALL_DELAY = 10s; // The max delay between GetOperation calls for one operation

TDeferredAction::TDeferredAction(const std::string& operationId,
        TDeferredOperationCb&& userCb,
        TGRpcConnectionsImpl* connection,
        std::shared_ptr<IQueueClientContext> context,
        TDeadline::Duration delay,
        TDeadline globalDeadline,
        TDbDriverStatePtr dbState,
        const std::string& endpoint)
    : TAlarmActionBase(std::move(userCb), connection, std::move(context))
    , NextDelay_(std::min(delay * 2, MAX_DEFERRED_CALL_DELAY))
    , GlobalDeadline_(globalDeadline)
    , DbDriverState_(dbState)
    , OperationId_(operationId)
    , Endpoint_(endpoint)
{
    Deadline_ = std::min(GlobalDeadline_, TDeadline::AfterDuration(delay));
}

void TDeferredAction::OnAlarm() {
    Y_ABORT_UNLESS(Connection_);
    auto self = TPtr(this);
    Connection_->PostCallback(
        [self](bool stopped) mutable {
            if (stopped) {
                self->UserResponseCb_(nullptr, MakeClientStoppedStatus());
                self->Context_.reset();
                return;
            }
            if (self->Context_->IsCancelled()) {
                self->UserResponseCb_(nullptr, MakeClientStoppedStatus());
                self->Context_.reset();
                return;
            }

            Ydb::Operations::GetOperationRequest getOperationRequest;
            getOperationRequest.set_id(TStringType{self->OperationId_});

            TRpcRequestSettings settings;
            settings.PreferredEndpoint = TEndpointKey(self->Endpoint_, 0);
            settings.Deadline = self->GlobalDeadline_;

            self->Connection_->RunDeferred<
                Ydb::Operation::V1::OperationService,
                Ydb::Operations::GetOperationRequest,
                Ydb::Operations::GetOperationResponse>(
                std::move(getOperationRequest),
                std::move(self->UserResponseCb_),
                &Ydb::Operation::V1::OperationService::Stub::AsyncGetOperation,
                self->DbDriverState_,
                self->NextDelay_,
                settings,
                true,
                std::move(self->Context_));
        });
}

void TDeferredAction::OnError() {
    Y_ABORT_UNLESS(Connection_);
    NYdbGrpc::TGrpcStatus status = {"Deferred timer interrupted", -1, true};
    DbDriverState_->StatCollector.IncDiscoveryFailDueTransportError();

    TPlainStatus plainStatus(status, Endpoint_, {});
    if (!Endpoint_.empty()) {
        plainStatus.Issues.AddIssue(NYdb::NIssue::TIssue(
            "Grpc error response on endpoint " + Endpoint_));
    }
    Connection_->RunResponseCallback<Ydb::Operations::Operation>(
        std::move(UserResponseCb_), nullptr, std::move(plainStatus));
}

TPeriodicAction::TPeriodicAction(
    TPeriodicCb&& userCb,
    TGRpcConnectionsImpl* connection,
    std::shared_ptr<NYdbGrpc::IQueueClientContext> context,
    TDeadline::Duration period)
    : TAlarmActionBase(std::move(userCb), connection, std::move(context))
    , Period_(period)
{
    Deadline_ = TDeadline::AfterDuration(period);
}

void TPeriodicAction::OnAlarm() {
    auto self = TIntrusivePtr<TPeriodicAction>(this);
    Connection_->PostCallback(
        [self](bool stopped) mutable {
            if (stopped) {
                self->OnStopped();
                return;
            }
            NYdb::NIssue::TIssues issues;
            const auto status = self->Context_->IsCancelled()
                                    ? EStatus::CLIENT_CANCELLED
                                    : EStatus::SUCCESS;
            if (!self->UserResponseCb_(std::move(issues), status) || status != EStatus::SUCCESS) {
                self->Context_.reset();
                return;
            }

            auto context = self->Connection_->CreateContext();
            if (!context) {
                return;
            }

            auto action = MakeIntrusive<TPeriodicAction>(
                std::move(self->UserResponseCb_),
                self->Connection_,
                std::move(context),
                self->Period_);
            action->Start();
        });
}

void TPeriodicAction::OnError() {
    auto self = TIntrusivePtr<TPeriodicAction>(this);
    Connection_->PostCallback(
        [self](bool stopped) mutable {
            if (stopped) {
                self->OnStopped();
                return;
            }
            NYdb::NIssue::TIssues issues;
            const auto status = self->Context_->IsCancelled()
                                    ? EStatus::CLIENT_CANCELLED
                                    : EStatus::CLIENT_INTERNAL_ERROR;
            if (status == EStatus::CLIENT_INTERNAL_ERROR) {
                issues.AddIssue(NYdb::NIssue::TIssue("Deferred timer interrupted"));
            }
            self->UserResponseCb_(std::move(issues), status);
            self->Context_.reset();
        });
}

void TPeriodicAction::OnStopped() {
    UserResponseCb_(NYdb::NIssue::TIssues{}, EStatus::CLIENT_CANCELLED);
    Context_.reset();
}

TDelayedAction::TDelayedAction(
    TDelayedCb&& userCb,
    TGRpcConnectionsImpl* connection,
    std::shared_ptr<IQueueClientContext> context,
    TDeadline deadline)
    : TAlarmActionBase(std::move(userCb), connection, std::move(context))
{
    Deadline_ = deadline;
}

void TDelayedAction::OnAlarm() {
    UserResponseCb_(true);
}

void TDelayedAction::OnError() {
    UserResponseCb_(false);
}

} // namespace NYdb::inline Dev
