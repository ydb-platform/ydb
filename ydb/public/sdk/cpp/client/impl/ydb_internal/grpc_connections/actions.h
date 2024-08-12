#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>

#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/common/types.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/db_driver_state/state.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/plain_status/status.h>

#include <ydb/library/grpc/client/grpc_client_low.h>

#include <util/thread/pool.h>

#include <grpcpp/alarm.h>

namespace NYdb {

using NYdbGrpc::IQueueClientContext;
using NYdbGrpc::IQueueClientEvent;

class TGRpcConnectionsImpl;
struct TPlainStatus;


template<typename TResponse>
using TResponseCb = std::function<void(TResponse*, TPlainStatus status)>;
using TDeferredOperationCb = std::function<void(Ydb::Operations::Operation*, TPlainStatus status)>;

template<typename TCb>
class TGenericCbHolder {
protected:
    TGenericCbHolder(
            TCb&& userCb,
            TGRpcConnectionsImpl* connections,
            std::shared_ptr<IQueueClientContext> context)
        : UserResponseCb_(std::move(userCb))
        , Connection_(connections)
        , Context_(std::move(context))
    {}

    TCb UserResponseCb_;
    TGRpcConnectionsImpl* Connection_;
    std::shared_ptr<IQueueClientContext> Context_;
};

template<typename TCb>
class TAlarmActionBase
    : public TThrRefBase
    , public TGenericCbHolder<TCb>
    , private IQueueClientEvent
{
public:
    using TPtr = TIntrusivePtr<TAlarmActionBase<TCb>>;
    using TGenericCbHolder<TCb>::TGenericCbHolder;

    virtual void OnAlarm() = 0;
    virtual void OnError() = 0;

    void Start() {
        Y_ABORT_UNLESS(this->Context_, "Missing shared context");
        auto context = this->Context_->CreateContext();
        {
            std::lock_guard lock(Mutex_);
            LocalContext_ = context;
            Alarm_.Set(this->Context_->CompletionQueue(), Deadline_, PrepareTag());
        }
        context->SubscribeStop([self = TPtr(this)] {
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
        {
            std::lock_guard lock(Mutex_);
            LocalContext_.reset();
        }

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
    gpr_timespec Deadline_ = {};

private:
    std::mutex Mutex_;
    grpc::Alarm Alarm_;
    std::shared_ptr<IQueueClientContext> LocalContext_;
};

template<typename TResponse>
class TGRpcErrorResponse
    : public TGenericCbHolder<TResponseCb<TResponse>>
    , public IObjectInQueue
{
public:
    TGRpcErrorResponse(
            NYdbGrpc::TGrpcStatus&& status,
            TResponseCb<TResponse>&& userCb,
            TGRpcConnectionsImpl* connections,
            std::shared_ptr<IQueueClientContext> context,
            const std::string& endpoint)
        : TGenericCbHolder<TResponseCb<TResponse>>(std::move(userCb), connections, std::move(context))
        , GRpcStatus_(std::move(status))
        , Endpoint_(endpoint)
    { }

    void Process(void*) override {
        TPlainStatus status(GRpcStatus_, Endpoint_, {});

        if (!Endpoint_.empty()) {
            TStringType msg = "Grpc error response on endpoint ";
            msg += Endpoint_;
            status.Issues.AddIssue(NYql::TIssue(msg));
        }

        this->UserResponseCb_(nullptr, status);
        delete this;
    }

private:
    NYdbGrpc::TGrpcStatus GRpcStatus_;
    std::string Endpoint_;
};

template<typename TResponse>
class TResult
    : public TGenericCbHolder<TResponseCb<TResponse>>
    , public IObjectInQueue
{
public:
    TResult(
            TResponse&& response,
            NYdbGrpc::TGrpcStatus&& status,
            TResponseCb<TResponse>&& userCb,
            TGRpcConnectionsImpl* connections,
            std::shared_ptr<IQueueClientContext> context,
            const std::string& endpoint,
            std::multimap<TStringType, TStringType>&& metadata)
        : TGenericCbHolder<TResponseCb<TResponse>>(std::move(userCb), connections, std::move(context))
        , Response_(std::move(response))
        , GRpcStatus_(std::move(status))
        , Endpoint_(endpoint)
        , Metadata_(std::move(metadata)) {}

    void Process(void*) override {
        this->UserResponseCb_(&Response_, TPlainStatus{GRpcStatus_, Endpoint_, std::move(Metadata_)});
        delete this;
    }

private:
    TResponse Response_;
    NYdbGrpc::TGrpcStatus GRpcStatus_;
    const std::string Endpoint_;
    std::multimap<TStringType, TStringType> Metadata_;
};

class TSimpleCbResult
    : public TGenericCbHolder<TSimpleCb>
    , public IObjectInQueue
{
public:
    TSimpleCbResult(
        TSimpleCb&& cb,
        TGRpcConnectionsImpl* connections,
        std::shared_ptr<IQueueClientContext> context);
    void Process(void*) override;
};

////////////////////////////////////////////////////////////////////////////////

class TDeferredAction
    : public TAlarmActionBase<TDeferredOperationCb>
{
public:
    using TPtr = TIntrusivePtr<TDeferredAction>;

    TDeferredAction(
        const TStringType& operationId,
        TDeferredOperationCb&& userCb,
        TGRpcConnectionsImpl* connection,
        std::shared_ptr<IQueueClientContext> context,
        TDuration timeout,
        TDbDriverStatePtr dbState,
        const TStringType& endpoint);

    void OnAlarm() override;
    void OnError() override;

private:
    TDuration NextDelay_;
    TDbDriverStatePtr DbDriverState_;
    const TStringType OperationId_;
    const TStringType Endpoint_;
};

class TPeriodicAction
    : public TAlarmActionBase<TPeriodicCb>
{
public:
    TPeriodicAction(
        TPeriodicCb&& userCb,
        TGRpcConnectionsImpl* connection,
        std::shared_ptr<IQueueClientContext> context,
        TDuration period);

    void OnAlarm() override;
    void OnError() override;
private:
    TDuration Period_;
};

} // namespace NYdb
