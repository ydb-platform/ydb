#pragma once

#include "client_session.h"

#include <ydb/public/sdk/cpp/client/ydb_types/core_facility/core_facility.h>

#include <util/generic/map.h>


namespace NYdb {
namespace NTable {

using namespace NThreading;


constexpr TDuration MAX_WAIT_SESSION_TIMEOUT = TDuration::Seconds(5); //Max time to wait session
constexpr ui64 PERIODIC_ACTION_BATCH_SIZE = 10; //Max number of tasks to perform during one interval
constexpr TDuration CREATE_SESSION_INTERNAL_TIMEOUT = TDuration::Seconds(2); //Timeout for createSession call inside session pool


class TSessionPool {
    typedef TAsyncCreateSessionResult
        (*TAwareSessonProvider)
            (std::shared_ptr<TTableClient::TImpl> client, const TCreateSessionSettings& settings);
private:
    class TWaitersQueue {
    public:
        TWaitersQueue(ui32 maxQueueSize, TDuration maxWaitSessionTimeout=MAX_WAIT_SESSION_TIMEOUT);

        bool TryPush(NThreading::TPromise<TCreateSessionResult>& p);
        TMaybe<NThreading::TPromise<TCreateSessionResult>> TryGet();
        void GetOld(TInstant now, TVector<NThreading::TPromise<TCreateSessionResult>>& oldWaiters);
        ui32 Size() const;

    private:
        const ui32 MaxQueueSize_;
        const TDuration MaxWaitSessionTimeout_;
        TMultiMap<TInstant, NThreading::TPromise<TCreateSessionResult>> Waiters_;
    };
public:
    using TKeepAliveCmd = std::function<void(TSession session)>;
    using TDeletePredicate = std::function<bool(TSession::TImpl* session, TTableClient::TImpl* client, size_t sessionsCount)>;
    TSessionPool(ui32 maxActiveSessions);
    // TAwareSessonProvider:
    // function is called if session pool is empty,
    // this is used for additional total session count limitation
    TAsyncCreateSessionResult GetSession(
        std::shared_ptr<TTableClient::TImpl> client,
        const TCreateSessionSettings& settings);
    // Returns true if session returned to pool successfully
    bool ReturnSession(TSession::TImpl* impl, bool active, std::shared_ptr<TTableClient::TImpl> client);
    // Returns trun if has waiter and scheduled to create new session
    // too feed it
    bool CheckAndFeedWaiterNewSession(std::shared_ptr<TTableClient::TImpl> client, bool active);
    TPeriodicCb CreatePeriodicTask(std::weak_ptr<TTableClient::TImpl> weakClient, TKeepAliveCmd&& cmd, TDeletePredicate&& predicate);
    i64 GetActiveSessions() const;
    i64 GetActiveSessionsLimit() const;
    i64 GetCurrentPoolSize() const;
    void DecrementActiveCounter();
    void IncrementActiveCounterUnsafe();

    void Drain(std::function<bool(std::unique_ptr<TSession::TImpl>&&)> cb, bool close);
    void SetStatCollector(NSdkStats::TStatCollector::TSessionPoolStatCollector collector);

    static void CreateFakeSession(NThreading::TPromise<TCreateSessionResult>& promise,
        std::shared_ptr<TTableClient::TImpl> client);
private:
    void UpdateStats();
    void MakeSessionPromiseFromSession(
        TSession::TImpl* session,
        NThreading::TPromise<TCreateSessionResult>& promise,
        std::shared_ptr<TTableClient::TImpl> client
    );

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
