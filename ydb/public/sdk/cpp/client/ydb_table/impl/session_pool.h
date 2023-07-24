#pragma once

#include "client_session.h"

#include <ydb/public/sdk/cpp/client/ydb_types/core_facility/core_facility.h>

#include <util/generic/map.h>


namespace NYdb {

namespace NSessionPool {

class IGetSessionCtx : private TNonCopyable {
public:
    virtual ~IGetSessionCtx() = default;
    // Transfer ownership to ctx
    virtual void ReplySessionToUser(TKqpSessionCommon* session) = 0;
    virtual void ReplyError(TStatus status) = 0;
    virtual void ReplyNewSession() = 0;
};

}

namespace NTable {

using namespace NThreading;
using namespace NSessionPool;

constexpr TDuration MAX_WAIT_SESSION_TIMEOUT = TDuration::Seconds(5); //Max time to wait session
constexpr ui64 PERIODIC_ACTION_BATCH_SIZE = 10; //Max number of tasks to perform during one interval
constexpr TDuration CREATE_SESSION_INTERNAL_TIMEOUT = TDuration::Seconds(2); //Timeout for createSession call inside session pool

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
    using TKeepAliveCmd = std::function<void(TSession session)>;
    using TDeletePredicate = std::function<bool(TSession::TImpl* session, TTableClient::TImpl* client, size_t sessionsCount)>;
    TSessionPool(ui32 maxActiveSessions);

    // Extracts session from pool or creates new one ising given ctx
    void GetSession(std::unique_ptr<IGetSessionCtx> ctx);

    // Returns true if session returned to pool successfully
    bool ReturnSession(TKqpSessionCommon* impl, bool active);

    // Returns trun if has waiter and scheduled to create new session
    // too feed it
    bool CheckAndFeedWaiterNewSession(bool active);

    TPeriodicCb CreatePeriodicTask(std::weak_ptr<TTableClient::TImpl> weakClient, TKeepAliveCmd&& cmd, TDeletePredicate&& predicate);
    i64 GetActiveSessions() const;
    i64 GetActiveSessionsLimit() const;
    i64 GetCurrentPoolSize() const;
    void DecrementActiveCounter();
    void IncrementActiveCounterUnsafe();

    void Drain(std::function<bool(std::unique_ptr<TSession::TImpl>&&)> cb, bool close);
    void SetStatCollector(NSdkStats::TStatCollector::TSessionPoolStatCollector collector);

private:
    void UpdateStats();
    static void ReplySessionToUser(TKqpSessionCommon* session, std::unique_ptr<IGetSessionCtx> ctx);

    mutable std::mutex Mtx_;
    bool Closed_;

    TMultiMap<TInstant, std::unique_ptr<TSession::TImpl>> Sessions_;
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
