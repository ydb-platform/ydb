#pragma once

#include <ydb/public/sdk/cpp/client/impl/ydb_internal/kqp_session_common/kqp_session_common.h>
#include <ydb/public/sdk/cpp/client/ydb_types/core_facility/core_facility.h>

#include <util/generic/map.h>

namespace NYdb {

class TOperation;
namespace NSessionPool {

class IGetSessionCtx : private TNonCopyable {
public:
    virtual ~IGetSessionCtx() = default;
    // Transfer ownership to ctx
    virtual void ReplySessionToUser(TKqpSessionCommon* session) = 0;
    virtual void ReplyError(TStatus status) = 0;
    virtual void ReplyNewSession() = 0;
};

//How often run session pool keep alive check
constexpr TDuration PERIODIC_ACTION_INTERVAL = TDuration::Seconds(5);
constexpr TDuration MAX_WAIT_SESSION_TIMEOUT = TDuration::Seconds(5); //Max time to wait session
constexpr ui64 PERIODIC_ACTION_BATCH_SIZE = 10; //Max number of tasks to perform during one interval
constexpr TDuration CREATE_SESSION_INTERNAL_TIMEOUT = TDuration::Seconds(2); //Timeout for createSession call inside session pool

TStatus GetStatus(const TOperation& operation);
TStatus GetStatus(const TStatus& status);

TDuration RandomizeThreshold(TDuration duration);
bool IsSessionCloseRequested(const TStatus& status);

template<typename TResponse>
NThreading::TFuture<TResponse> InjectSessionStatusInterception(
        std::shared_ptr<::NYdb::TKqpSessionCommon> impl, NThreading::TFuture<TResponse> asyncResponse,
        bool updateTimeout,
        TDuration timeout,
        std::function<void(const TResponse&, TKqpSessionCommon&)> cb = {})
{
    auto promise = NThreading::NewPromise<TResponse>();
    asyncResponse.Subscribe([impl, promise, cb, updateTimeout, timeout](NThreading::TFuture<TResponse> future) mutable {
        Y_ABORT_UNLESS(future.HasValue());

        // TResponse can hold refcounted user provided data (TSession for example)
        // and we do not want to have copy of it (for example it can cause delay dtor call)
        // so using move semantic here is mandatory.
        // Also we must reset captured shared pointer to session impl
        TResponse value = std::move(future.ExtractValue());

        const TStatus& status = GetStatus(value);
        // Exclude CLIENT_RESOURCE_EXHAUSTED from transport errors which can cause to session disconnect
        // since we have guarantee this request wasn't been started to execute.

        if (status.IsTransportError() && status.GetStatus() != EStatus::CLIENT_RESOURCE_EXHAUSTED) {
            impl->MarkBroken();
        } else if (status.GetStatus() == EStatus::SESSION_BUSY) {
            impl->MarkBroken();
        } else if (status.GetStatus() == EStatus::BAD_SESSION) {
            impl->MarkBroken();
        } else if (IsSessionCloseRequested(status)) {
            impl->MarkAsClosing();
        } else {
            // NOTE: About GetState and lock
            // Simultanious call multiple requests on the same session make no sence, due to server limitation.
            // But user can perform this call, right now we do not protect session from this, it may cause
            // raise on session state if respoise is not success.
            // It should not be a problem - in case of this race we close session
            // or put it in to settler.
            if (updateTimeout && status.GetStatus() != EStatus::CLIENT_RESOURCE_EXHAUSTED) {
                impl->ScheduleTimeToTouch(RandomizeThreshold(timeout), impl->GetState() == TKqpSessionCommon::EState::S_ACTIVE);
            }
        }
        if (cb) {
            cb(value, *impl);
        }
        impl.reset();
        promise.SetValue(std::move(value));
    });
    return promise.GetFuture();
}

class TSessionPool {
private:
    class TWaitersQueue {
    public:
        TWaitersQueue(ui32 maxQueueSize, TDuration maxWaitSessionTimeout=MAX_WAIT_SESSION_TIMEOUT);

        // returns true and gets ownership if queue size less than limit
        // otherwise returns false and doesn't not touch ctx
        bool TryPush(std::unique_ptr<IGetSessionCtx>& p);
        std::unique_ptr<IGetSessionCtx> TryGet();
        void GetOld(TInstant now, TVector<std::unique_ptr<IGetSessionCtx>>& oldWaiters);
        ui32 Size() const;

    private:
        const ui32 MaxQueueSize_;
        const TDuration MaxWaitSessionTimeout_;
        TMultiMap<TInstant, std::unique_ptr<IGetSessionCtx>> Waiters_;
    };
public:
    using TKeepAliveCmd = std::function<void(TKqpSessionCommon* s)>;
    using TDeletePredicate = std::function<bool(TKqpSessionCommon* s, size_t sessionsCount)>;
    TSessionPool(ui32 maxActiveSessions);

    // Extracts session from pool or creates new one ising given ctx
    void GetSession(std::unique_ptr<IGetSessionCtx> ctx);

    // Returns true if session returned to pool successfully
    bool ReturnSession(TKqpSessionCommon* impl, bool active);

    // Returns trun if has waiter and scheduled to create new session
    // too feed it
    bool CheckAndFeedWaiterNewSession(bool active);

    TPeriodicCb CreatePeriodicTask(std::weak_ptr<ISessionClient> weakClient, TKeepAliveCmd&& cmd, TDeletePredicate&& predicate);
    i64 GetActiveSessions() const;
    i64 GetActiveSessionsLimit() const;
    i64 GetCurrentPoolSize() const;
    void DecrementActiveCounter();
    void IncrementActiveCounterUnsafe();

    void Drain(std::function<bool(std::unique_ptr<TKqpSessionCommon>&&)> cb, bool close);
    void SetStatCollector(NSdkStats::TStatCollector::TSessionPoolStatCollector collector);

private:
    void UpdateStats();
    static void ReplySessionToUser(TKqpSessionCommon* session, std::unique_ptr<IGetSessionCtx> ctx);

    mutable std::mutex Mtx_;
    bool Closed_;

    TMultiMap<TInstant, std::unique_ptr<TKqpSessionCommon>> Sessions_;
    TWaitersQueue WaitersQueue_;

    i64 ActiveSessions_;
    const ui32 MaxActiveSessions_;
    NSdkStats::TSessionCounter ActiveSessionsCounter_;
    NSdkStats::TSessionCounter InPoolSessionsCounter_;
    NSdkStats::TSessionCounter SessionWaiterCounter_;
    NSdkStats::TAtomicCounter<::NMonitoring::TRate> FakeSessionsCounter_;
};

}
}
