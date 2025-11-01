#pragma once

#include "kqp_session_common.h"

#include <ydb/public/sdk/cpp/src/client/types/core_facility/core_facility.h>


namespace NYdb::inline Dev {

class TOperation;
namespace NSessionPool {

class IGetSessionCtx : private TNonCopyable {
public:
    virtual ~IGetSessionCtx() = default;
    // Transfer ownership to ctx
    virtual void ReplySessionToUser(TKqpSessionCommon* session) = 0;
    virtual void ReplyError(TStatus status) = 0;
    virtual void ReplyNewSession() = 0;
    virtual void ScheduleOnDeadlineWaiterCleanup() = 0;
    virtual TDeadline GetDeadline() const = 0;
};

//How often run session pool keep alive check
constexpr TDeadline::Duration PERIODIC_ACTION_INTERVAL = std::chrono::seconds(5);
constexpr TDeadline::Duration MAX_WAIT_SESSION_TIMEOUT = std::chrono::seconds(5); // Max time to wait session
constexpr std::uint64_t PERIODIC_ACTION_BATCH_SIZE = 10; // Max number of tasks to perform during one interval

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

        if (status.IsTransportError()
            && status.GetStatus() != EStatus::CLIENT_RESOURCE_EXHAUSTED && status.GetStatus() != EStatus::CLIENT_OUT_OF_RANGE)
        {
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

class TSessionPool : public IServerCloseHandler {
private:
    class TWaitersQueue {
    public:
        TWaitersQueue(std::uint32_t maxQueueSize);

        // returns true and gets ownership if queue size less than limit
        // otherwise returns false and doesn't not touch ctx
        bool TryPush(std::unique_ptr<IGetSessionCtx>& p);
        std::unique_ptr<IGetSessionCtx> TryGet();
        void GetOld(TDeadline deadline, std::vector<std::unique_ptr<IGetSessionCtx>>& oldWaiters);
        std::uint32_t Size() const;

    private:
        const std::uint32_t MaxQueueSize_;
        std::multimap<TDeadline, std::unique_ptr<IGetSessionCtx>> Waiters_;
    };
public:
    using TKeepAliveCmd = std::function<void(TKqpSessionCommon* s)>;
    using TDeletePredicate = std::function<bool(TKqpSessionCommon* s, size_t sessionsCount)>;
    TSessionPool(std::uint32_t maxActiveSessions);

    // Extracts session from pool or creates new one ising given ctx
    void GetSession(std::unique_ptr<IGetSessionCtx> ctx);

    // Returns true if session returned to pool successfully
    bool ReturnSession(TKqpSessionCommon* impl, bool active);

    // Returns trun if has waiter and scheduled to create new session
    // too feed it
    bool CheckAndFeedWaiterNewSession(bool active);

    void ClearOldWaiters();

    TPeriodicCb CreatePeriodicTask(std::weak_ptr<ISessionClient> weakClient, TKeepAliveCmd&& cmd, TDeletePredicate&& predicate);
    std::int64_t GetActiveSessions() const;
    std::int64_t GetActiveSessionsLimit() const;
    std::int64_t GetCurrentPoolSize() const;
    void DecrementActiveCounter();
    void IncrementActiveCounterUnsafe();

    void Drain(std::function<bool(std::unique_ptr<TKqpSessionCommon>&&)> cb, bool close);
    void SetStatCollector(NSdkStats::TStatCollector::TSessionPoolStatCollector collector);

    void OnCloseSession(const TKqpSessionCommon*, std::shared_ptr<ISessionClient> client) override;

private:
    void UpdateStats();
    static void ReplySessionToUser(TKqpSessionCommon* session, std::unique_ptr<IGetSessionCtx> ctx);

    mutable std::mutex Mtx_;
    bool Closed_;

    std::multimap<TInstant, std::unique_ptr<TKqpSessionCommon>> Sessions_;
    TWaitersQueue WaitersQueue_;

    std::int64_t ActiveSessions_;
    const std::uint32_t MaxActiveSessions_;
    NSdkStats::TSessionCounter ActiveSessionsCounter_;
    NSdkStats::TSessionCounter InPoolSessionsCounter_;
    NSdkStats::TSessionCounter SessionWaiterCounter_;
    NSdkStats::TAtomicCounter<::NMonitoring::TRate> FakeSessionsCounter_;
};

}
}
