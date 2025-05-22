#pragma once

#include "defs.h"

#include "events.h"
#include "quoter_resource_tree.h"
#include "schema.h"

#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/counters_kesus.pb.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/scheduler_cookie.h>

#include <util/generic/hash.h>

namespace NKikimr {
namespace NKesus {

class TKesusTablet : public TActor<TKesusTablet>, public NTabletFlatExecutor::TTabletExecutedFlat {
private:
    using Schema = TKesusSchema;
    using TTxBase = NTabletFlatExecutor::TTransactionBase<TKesusTablet>;

    struct TTxInitSchema;
    struct TTxInit;
    struct TTxSelfCheck;

    struct TTxDummy;

    struct TTxConfigGet;
    struct TTxConfigSet;

    struct TTxSessionAttach;
    struct TTxSessionDestroy;
    struct TTxSessionDetach;
    struct TTxSessionTimeout;
    struct TTxSessionsDescribe;

    struct TTxSemaphoreAcquire;
    struct TTxSemaphoreCreate;
    struct TTxSemaphoreDescribe;
    struct TTxSemaphoreDelete;
    struct TTxSemaphoreRelease;
    struct TTxSemaphoreTimeout;
    struct TTxSemaphoreUpdate;

    struct TTxQuoterResourceAdd;
    struct TTxQuoterResourceUpdate;
    struct TTxQuoterResourceDelete;
    struct TTxQuoterResourceDescribe;
    class TQuoterResourceSink;

    static constexpr ui64 QUOTER_TICK_PROCESSING_WAKEUP_TAG = 1;

    static constexpr size_t MAX_SESSIONS_LIMIT = 1000000; // 1 million
    static constexpr size_t MAX_DESCRIPTION_SIZE = 1024;
    static constexpr size_t MAX_PROTECTION_KEY_SIZE = 16;

    static constexpr size_t MAX_SEMAPHORE_NAME = 1024; // 1KB
    static constexpr size_t MAX_SEMAPHORE_DATA = 1024 * 1024; // 1MB
    static constexpr size_t MAX_SEMAPHORES_LIMIT = 1000000; // 1 million

    static constexpr TDuration MIN_SELF_CHECK_PERIOD = TDuration::MilliSeconds(500);
    static constexpr TDuration MAX_SELF_CHECK_PERIOD = TDuration::Seconds(10);
    static constexpr TDuration MIN_SESSION_GRACE_PERIOD = TDuration::Seconds(1);
    static constexpr TDuration MAX_SESSION_GRACE_PERIOD = TDuration::Seconds(30);

    static constexpr TDuration MAX_SESSION_TIMEOUT = TDuration::Minutes(30);
    static constexpr TDuration MAX_ACQUIRE_TIMEOUT = TDuration::Days(1);

    struct THtmlRenderer;

    struct TDelayedEvent {
        const TActorId Recipient;
        const ui64 Cookie;
        THolder<IEventBase> Event;

        TDelayedEvent(const TActorId& recipient, ui64 cookie, IEventBase* event)
            : Recipient(recipient)
            , Cookie(cookie)
            , Event(event)
        {}
    };

    struct TProxyInfo {
        TActorId ActorID;
        ui64 Generation = 0;
        TActorId InterconnectSession;
        THashSet<ui64> AttachedSessions;
    };

    struct TSemaphoreInfo;

    struct TSemaphoreOwnerInfo {
        ui64 OrderId;
        ui64 SessionId;
        ui64 Count;
        TString Data;
    };

    struct TSemaphoreWaiterInfo {
        ui64 OrderId;
        ui64 SessionId;
        ui64 TimeoutMillis;
        ui64 Count;
        TString Data;
        TSchedulerCookieHolder TimeoutCookie;
        TInstant ScheduledTimeoutDeadline;
    };

    struct TSessionInfo {
        ui64 Id;
        ui64 TimeoutMillis;
        TString Description;
        TString ProtectionKey;
        THashMap<ui64, TSemaphoreOwnerInfo> OwnedSemaphores;
        THashMap<ui64, TSemaphoreWaiterInfo> WaitingSemaphores;
        TSchedulerCookieHolder TimeoutCookie;
        TInstant ScheduledTimeoutDeadline;

        TProxyInfo* OwnerProxy;
        ui64 OwnerCookie = 0;
        THashMap<TSemaphoreInfo*, ui64> SemaphoreWaitCookie;
        THashMap<TSemaphoreInfo*, ui64> SemaphoreWatchCookie;

        TActorId LastOwnerProxy;
        ui64 LastOwnerSeqNo = 0;

        void ClearWatchCookies();

        bool DetachProxy() {
            SemaphoreWaitCookie.clear();
            ClearWatchCookies();
            if (OwnerProxy) {
                OwnerProxy->AttachedSessions.erase(Id);
                OwnerProxy = nullptr;
                OwnerCookie = 0;
                return true;
            } else {
                return false;
            }
        }

        void AttachProxy(TProxyInfo* proxy, ui64 cookie, ui64 seqNo) {
            Y_ABORT_UNLESS(proxy);
            if (OwnerProxy != proxy) {
                if (OwnerProxy) {
                    OwnerProxy->AttachedSessions.erase(Id);
                }
                OwnerProxy = proxy;
                OwnerProxy->AttachedSessions.insert(Id);
            }
            OwnerProxy = proxy;
            OwnerCookie = cookie;
            SemaphoreWaitCookie.clear();
            ClearWatchCookies();
            LastOwnerProxy = OwnerProxy->ActorID;
            LastOwnerSeqNo = seqNo;
            TimeoutCookie.Detach();
        }

        template<class Callback>
        void ConsumeSemaphoreWaitCookie(TSemaphoreInfo* semaphore, Callback&& callback) {
            auto it = SemaphoreWaitCookie.find(semaphore);
            if (it != SemaphoreWaitCookie.end()) {
                ui64 cookie = it->second;
                SemaphoreWaitCookie.erase(it);
                callback(cookie);
            }
        }

        ui64 RemoveSemaphoreWatchCookie(TSemaphoreInfo* semaphore) {
            auto it = SemaphoreWatchCookie.find(semaphore);
            Y_ABORT_UNLESS(it != SemaphoreWatchCookie.end());
            ui64 cookie = it->second;
            SemaphoreWatchCookie.erase(it);
            return cookie;
        }
    };

    struct TSemaphoreInfo {
        ui64 Id;
        TString Name;
        TString Data;
        ui64 Limit;
        ui64 Count;
        bool Ephemeral;

        THashSet<TSemaphoreOwnerInfo*> Owners;
        TMap<ui64, TSemaphoreWaiterInfo*> Waiters;

        THashSet<TSessionInfo*> DataWatchers;
        THashSet<TSessionInfo*> OwnersWatchers;

        bool IsEmpty() const {
            return Owners.empty() && Waiters.empty();
        }

        ui64 GetFirstOrderId() const {
            if (!Waiters.empty()) {
                return Waiters.begin()->first;
            }
            return 0;
        }

        bool CanAcquire(ui64 count) const {
            Y_ABORT_UNLESS(count <= Limit);
            return count <= (Limit - Count);
        }

        void NotifyWatchers(TVector<TDelayedEvent>& events, bool dataChanged, bool ownerChanged);
    };

    struct TQuoterResourceSessionsAccumulator {
        void Accumulate(const TActorId& recipient, ui64 resourceId, double amount, const NKikimrKesus::TStreamingQuoterResource* props);
        void Sync(const TActorId& recipient, ui64 resourceId, ui32 lastReportId, double amount);
        void SendAll(const TActorContext& ctx, ui64 tabletId);

        struct TSendInfo {
            THolder<TEvKesus::TEvResourcesAllocated> Event;
            THashMap<ui64, size_t> ResIdIndex;
        };

        struct TSendSyncInfo {
            THolder<TEvKesus::TEvSyncResources> Event;
            THashMap<ui64, size_t> ResIdIndex;
        };

        THashMap<TActorId, TSendInfo> SendInfos;
        THashMap<TActorId, TSendSyncInfo> SendSyncInfos;
    };

    struct TEvPrivate {
        enum EEv {
            EvSessionTimeout = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvAcquireSemaphoreTimeout,
            EvSelfCheckStart,
            EvSelfCheckTimeout,

            EvEnd
        };

        static_assert(EvEnd <= EventSpaceEnd(TKikimrEvents::ES_PRIVATE),
            "expected EvEnd <= EventSpaceEnd(TKikimrEvents::ES_PRIVATE)");

        struct TEvSessionTimeout : public TEventLocal<TEvSessionTimeout, EvSessionTimeout> {
            const ui64 SessionId;
            TSchedulerCookieHolder Cookie;

            TEvSessionTimeout(ui64 sessionId, ISchedulerCookie* cookie)
                : SessionId(sessionId)
                , Cookie(cookie)
            {}
        };

        struct TEvAcquireSemaphoreTimeout : public TEventLocal<TEvAcquireSemaphoreTimeout, EvAcquireSemaphoreTimeout> {
            const ui64 SessionId;
            const ui64 SemaphoreId;
            TSchedulerCookieHolder Cookie;

            TEvAcquireSemaphoreTimeout(ui64 sessionId, ui64 semaphoreId, ISchedulerCookie* cookie)
                : SessionId(sessionId)
                , SemaphoreId(semaphoreId)
                , Cookie(cookie)
            {}
        };

        struct TEvSelfCheckStart : public TEventLocal<TEvSelfCheckStart, EvSelfCheckStart> {
            const TInstant Deadline;

            explicit TEvSelfCheckStart(TInstant deadline)
                : Deadline(deadline)
            {}
        };

        struct TEvSelfCheckTimeout : public TEventLocal<TEvSelfCheckTimeout, EvSelfCheckTimeout> {
            TSchedulerCookieHolder Cookie;

            explicit TEvSelfCheckTimeout(ISchedulerCookie* cookie)
                : Cookie(cookie)
            {}
        };
    };

private:
    TString KesusPath;
    ui64 ConfigVersion;
    ui64 NextSessionId;
    ui64 NextSemaphoreId;
    ui64 NextSemaphoreOrderId;
    TDuration SelfCheckPeriod;
    TDuration SessionGracePeriod;
    Ydb::Coordination::ConsistencyMode ReadConsistencyMode;
    Ydb::Coordination::ConsistencyMode AttachConsistencyMode;
    THashMap<ui64, TSessionInfo> Sessions;
    THashMap<ui64, TSemaphoreInfo> Semaphores;
    THashMap<TString, TSemaphoreInfo*> SemaphoresByName;
    THashMap<TActorId, TProxyInfo> Proxies;
    THashMap<ui32, THashSet<TProxyInfo*>> ProxiesByNode;
    THashMap<ui64, ui64> SessionsTxCount;

    // Quoter support
    ui64 NextQuoterResourceId;
    TQuoterResources QuoterResources;
    TInstant NextQuoterTickTime = TInstant::Max();
    TTickProcessorQueue QuoterTickProcessorQueue;
    TQuoterResourceSessionsAccumulator QuoterResourceSessionsAccumulator;
    Ydb::Coordination::RateLimiterCountersMode RateLimiterCountersMode;
    bool QuoterTickProcessingIsScheduled = false;

    // Counters support
    THolder<TTabletCountersBase> TabletCountersPtr;
    TTabletCountersBase* TabletCounters;

    // Self check support
    ui64 SelfCheckCounter = 0;
    bool SelfCheckPending = false;

    ui64 StrictMarkerCounter = 0;

public:
    TKesusTablet(const TActorId& tablet, TTabletStorageInfo* info);
    virtual ~TKesusTablet();

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KESUS_TABLET_ACTOR;
    }

private:
    void OnDetach(const TActorContext& ctx) override;
    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) override;
    void OnActivateExecutor(const TActorContext& ctx) override;
    void DefaultSignalTabletActive(const TActorContext& ctx) override;
    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext& ctx) override;

private:
    NTabletFlatExecutor::ITransaction* CreateTxInitSchema();
    NTabletFlatExecutor::ITransaction* CreateTxInit();

private:
    void ResetState();
    void ResetCounters();

    bool UseStrictRead() const {
        return ReadConsistencyMode == Ydb::Coordination::CONSISTENCY_MODE_STRICT;
    }

    bool UseStrictAttach() const {
        return AttachConsistencyMode == Ydb::Coordination::CONSISTENCY_MODE_STRICT;
    }

    void PersistSysParam(NIceDb::TNiceDb& db, ui64 id, const TString& value);
    void PersistDeleteSession(NIceDb::TNiceDb& db, ui64 sessionId);
    void PersistDeleteSessionSemaphore(NIceDb::TNiceDb& db, ui64 sessionId, ui64 semaphoreId);
    void PersistDeleteSemaphore(NIceDb::TNiceDb& db, ui64 semaphoreId);

    void PersistStrictMarker(NIceDb::TNiceDb& db);

    void DoDeleteSession(
        NIceDb::TNiceDb& db, TSessionInfo* session, TVector<TDelayedEvent>& events);
    void DoDeleteSemaphore(
        NIceDb::TNiceDb& db, TSemaphoreInfo* semaphore, TVector<TDelayedEvent>& events);
    void DoDeleteSessionSemaphore(
        NIceDb::TNiceDb& db, TSemaphoreInfo* semaphore, TSemaphoreOwnerInfo* owner, TVector<TDelayedEvent>& events);
    void DoDeleteSessionSemaphore(
        NIceDb::TNiceDb& db, TSemaphoreInfo* semaphore, TSemaphoreWaiterInfo* waiter, TVector<TDelayedEvent>& events);
    void DoProcessSemaphoreQueue(
        TSemaphoreInfo* semaphore, TVector<TDelayedEvent>& events, bool ownersChanged = false);

private:
    bool ScheduleSelfCheck(const TActorContext& ctx);
    bool ScheduleWaiterTimeout(ui64 semaphoreId, TSemaphoreWaiterInfo* waiter, const TActorContext& ctx);
    bool ScheduleSessionTimeout(TSessionInfo* session, const TActorContext& ctx, TDuration gracePeriod = TDuration::Zero());
    void ClearProxy(TProxyInfo* proxy, const TActorContext& ctx);
    void ForgetProxy(TProxyInfo* proxy);
    void ScheduleQuoterTick();

private:
    void VerifyKesusPath(const TString& kesusPath);

    void Handle(TEvents::TEvUndelivered::TPtr& ev);
    void Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev);
    void Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev);
    void Handle(TEvents::TEvWakeup::TPtr& ev);

    void Handle(TEvKesus::TEvDummyRequest::TPtr& ev);
    void Handle(TEvKesus::TEvSetConfig::TPtr& ev);
    void Handle(TEvKesus::TEvGetConfig::TPtr& ev);
    void Handle(TEvKesus::TEvDescribeSemaphore::TPtr& ev);
    void Handle(TEvKesus::TEvDescribeProxies::TPtr& ev);
    void Handle(TEvKesus::TEvDescribeSessions::TPtr& ev);
    void Handle(TEvKesus::TEvRegisterProxy::TPtr& ev);
    void Handle(TEvKesus::TEvUnregisterProxy::TPtr& ev);
    void Handle(TEvKesus::TEvAttachSession::TPtr& ev);
    void Handle(TEvKesus::TEvDetachSession::TPtr& ev);
    void Handle(TEvKesus::TEvDestroySession::TPtr& ev);
    void Handle(TEvKesus::TEvAcquireSemaphore::TPtr& ev);
    void Handle(TEvKesus::TEvReleaseSemaphore::TPtr& ev);
    void Handle(TEvKesus::TEvCreateSemaphore::TPtr& ev);
    void Handle(TEvKesus::TEvUpdateSemaphore::TPtr& ev);
    void Handle(TEvKesus::TEvDeleteSemaphore::TPtr& ev);

    // Quoter API
    void Handle(TEvKesus::TEvDescribeQuoterResources::TPtr& ev);
    void Handle(TEvKesus::TEvAddQuoterResource::TPtr& ev);
    void Handle(TEvKesus::TEvUpdateQuoterResource::TPtr& ev);
    void Handle(TEvKesus::TEvDeleteQuoterResource::TPtr& ev);
    // Quoter runtime
    void Handle(TEvKesus::TEvSubscribeOnResources::TPtr& ev);
    void Handle(TEvKesus::TEvUpdateConsumptionState::TPtr& ev);
    void Handle(TEvKesus::TEvAccountResources::TPtr& ev);
    void Handle(TEvKesus::TEvReportResources::TPtr& ev);
    void Handle(TEvKesus::TEvResourcesAllocatedAck::TPtr& ev);
    void Handle(TEvKesus::TEvGetQuoterResourceCounters::TPtr& ev);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev);
    void HandleQuoterTick();

    void Handle(TEvPrivate::TEvSessionTimeout::TPtr& ev);
    void Handle(TEvPrivate::TEvAcquireSemaphoreTimeout::TPtr& ev);
    void Handle(TEvPrivate::TEvSelfCheckStart::TPtr& ev);
    void Handle(TEvPrivate::TEvSelfCheckTimeout::TPtr& ev);

    // Ignored messages for compatibility
    void HandleIgnored();

private:
    STFUNC(StateInit);
    STFUNC(StateWork);

private:
    void AddSessionTx(ui64 sessionId) {
        ++SessionsTxCount[sessionId];
    }

    ui64 GetSessionTxCount(ui64 sessionId) {
        return SessionsTxCount.Value(sessionId, 0);
    }

    void RemoveSessionTx(ui64 sessionId) {
        auto it = SessionsTxCount.find(sessionId);
        Y_ABORT_UNLESS(it != SessionsTxCount.end());
        if (!--it->second) {
            SessionsTxCount.erase(it);
        }
    }

    bool IsSessionFastPathAllowed(ui64 sessionId) {
        return sessionId > 0 && sessionId < NextSessionId && GetSessionTxCount(sessionId) == 0;
    }
};

}
}
