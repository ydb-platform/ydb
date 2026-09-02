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
        : TAlarmActionBase(std::move(userCb), connection, std::move(context),
                           std::min(globalDeadline, TDeadline::AfterDuration(delay)))
        , NextDelay_(std::min(delay * 2, MAX_DEFERRED_CALL_DELAY))
        , GlobalDeadline_(globalDeadline)
        , DbDriverState_(dbState)
        , OperationId_(operationId)
        , Endpoint_(endpoint) {
    }

    void TDeferredAction::OnAlarm() {
        Y_ABORT_UNLESS(Connection_);
        Connection_->PostCallback(
            [self = TPtr(this)]() mutable {
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
            std::move(UserResponseCb_), std::move(plainStatus));
    }

    TPeriodicAction::TPeriodicAction(
        TPeriodicCb&& userCb,
        TGRpcConnectionsImpl* connection,
        std::shared_ptr<NYdbGrpc::IQueueClientContext> context,
        TDeadline::Duration period)
        : TAlarmActionBase(std::move(userCb), connection, std::move(context), TDeadline::AfterDuration(period))
        , Period_(period) {
    }

    void TPeriodicAction::OnAlarm() {
        Connection_->PostCallback(
            [self = TIntrusivePtr<TPeriodicAction>(this)]() mutable {
                if (self->Context_->IsCancelled()) {
                    self->UserResponseCb_(NYdb::NIssue::TIssues{}, EStatus::CLIENT_CANCELLED);
                    self->Context_.reset();
                    return;
                }
                if (!self->UserResponseCb_(NYdb::NIssue::TIssues{}, EStatus::SUCCESS)) {
                    self->Context_.reset();
                    return;
                }

                auto context = self->Connection_->CreateContext();
                if (!context) {
                    return;
                }

                MakeIntrusive<TPeriodicAction>(
                    std::move(self->UserResponseCb_),
                    self->Connection_,
                    std::move(context),
                    self->Period_)
                    ->Start();
            });
    }

    void TPeriodicAction::OnError() {
        Connection_->PostCallback(
            [self = TIntrusivePtr<TPeriodicAction>(this)]() mutable {
                if (self->Context_->IsCancelled()) {
                    self->UserResponseCb_(NYdb::NIssue::TIssues{}, EStatus::CLIENT_CANCELLED);
                    self->Context_.reset();
                    return;
                }
                NYdb::NIssue::TIssues issues;
                issues.AddIssue(NYdb::NIssue::TIssue("Deferred timer interrupted"));
                self->UserResponseCb_(std::move(issues), EStatus::CLIENT_INTERNAL_ERROR);
                self->Context_.reset();
            });
    }

    void TDelayedAction::OnAlarm() {
        UserResponseCb_(true);
    }

    void TDelayedAction::OnError() {
        UserResponseCb_(false);
    }

} // namespace NYdb::inline Dev
