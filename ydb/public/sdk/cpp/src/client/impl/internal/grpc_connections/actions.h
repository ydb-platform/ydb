#pragma once

#include <ydb/public/sdk/cpp/src/client/impl/internal/internal_header.h>

#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/common/types.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/db_driver_state/state.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/plain_status/status.h>

#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <grpcpp/alarm.h>

#include <memory>

namespace NYdb::inline Dev {

using NYdbGrpc::IQueueClientContext;
using NYdbGrpc::IQueueClientEvent;

class TGRpcConnectionsImpl;

template<typename TResponse>
using TResponseCb = std::function<void(TResponse*, TPlainStatus status)>;
using TDeferredOperationCb = std::function<void(Ydb::Operations::Operation*, TPlainStatus status)>;
using TDelayedCb = std::function<void(bool ok)>;

inline TPlainStatus MakeClientStoppedStatus() {
    return TPlainStatus(EStatus::CLIENT_CANCELLED, "Client is stopped");
}

template<typename TCb>
class TAlarmActionBase
    : public TThrRefBase
    , private IQueueClientEvent
{
public:
    using TPtr = TIntrusivePtr<TAlarmActionBase<TCb>>;

    TAlarmActionBase(
            TCb&& userCb,
            TGRpcConnectionsImpl* connections,
            std::shared_ptr<IQueueClientContext> context,
            TDeadline deadline)
        : Deadline_(deadline)
        , UserResponseCb_(std::move(userCb))
        , Connection_(connections)
        , Context_(std::move(context))
    {}

    virtual void OnAlarm() = 0;
    virtual void OnError() = 0;

    void Start() {
        Y_ABORT_UNLESS(this->Context_, "Missing shared context");
        auto context = this->Context_->CreateContext();
        if (!context) {
            OnError();
            return;
        }
        LocalContext_ = context;
        Alarm_.Set(this->Context_->CompletionQueue(), Deadline_, PrepareTag());
        context->SubscribeCancel([self = TPtr(this)] {
            self->Stop();
        });
    }

    void Stop() {
        Alarm_.Cancel();
    }

private:
    IQueueClientEvent* PrepareTag() {
        Ref();
        return this;
    }

    bool Execute(bool ok) override {
        LocalContext_.reset();

        if (ok) {
            OnAlarm();
        } else {
            OnError();
        }

        return false;
    }

    void Destroy() override {
        UnRef();
    }

protected:
    TDeadline Deadline_;
    TCb UserResponseCb_;
    TGRpcConnectionsImpl* Connection_;
    std::shared_ptr<IQueueClientContext> Context_;

private:
    grpc::Alarm Alarm_;
    std::shared_ptr<IQueueClientContext> LocalContext_;
};

class TDeferredAction
    : public TAlarmActionBase<TDeferredOperationCb>
{
public:
    using TPtr = TIntrusivePtr<TDeferredAction>;

    TDeferredAction(
        const std::string& operationId,
        TDeferredOperationCb&& userCb,
        TGRpcConnectionsImpl* connection,
        std::shared_ptr<IQueueClientContext> context,
        TDeadline::Duration delay,
        TDeadline globalDeadline,
        TDbDriverStatePtr dbState,
        const std::string& endpoint);

    void OnAlarm() override;
    void OnError() override;

private:
    TDeadline::Duration NextDelay_;
    TDeadline GlobalDeadline_;

    TDbDriverStatePtr DbDriverState_;
    const std::string OperationId_;
    const std::string Endpoint_;
};

class TPeriodicAction
    : public TAlarmActionBase<TPeriodicCb>
{
public:
    TPeriodicAction(
        TPeriodicCb&& userCb,
        TGRpcConnectionsImpl* connection,
        std::shared_ptr<IQueueClientContext> context,
        TDeadline::Duration period);

    void OnAlarm() override;
    void OnError() override;
private:
    TDeadline::Duration Period_;
};

class TDelayedAction
    : public TAlarmActionBase<TDelayedCb>
{
public:
    using TAlarmActionBase::TAlarmActionBase;

    void OnAlarm() override;
    void OnError() override;
};

} // namespace NYdb
