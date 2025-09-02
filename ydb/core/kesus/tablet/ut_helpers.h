#pragma once

#include "tablet.h"
#include "events.h"

#include <ydb/core/erasure/erasure.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NKesus {

enum ELockMode {
    LOCK_MODE_EXCLUSIVE,
    LOCK_MODE_SHARED,
};

struct TTestContext {
    TTabletTypes::EType TabletType;
    ui64 TabletId;
    THolder<TTestActorRuntime> Runtime;
    THashMap<std::tuple<TActorId, ui64>, TActorId> ProxyClients;

    TTestContext();

    void Setup(ui32 nodeCount = 1, bool useRealThreads = false);
    void Finalize();

    virtual void SetupLogging();
    virtual void SetupTabletServices();

    // Sleeps for millis milliseconds using fake time
    void Sleep(ui64 millis);

    // Doesn't sleep unlike NKikimr::RebootTablet
    void RebootTablet();

    // Returns tablet actor for direct manipulation
    TActorId GetTabletActorId();

    // Extremely pedantic version of GrabEdgeEvent
    template<class TEvent>
    THolder<TEvent> ExpectEdgeEvent(const TActorId& actor) {
        return Runtime->GrabEdgeEvent<TEvent>(actor)->Release();
    }

    // Extremely pedantic version of GrabEdgeEvent
    template<class TEvent>
    THolder<TEvent> ExpectEdgeEvent(const TActorId& actor, ui64 cookie) {
        auto ev = Runtime->GrabEdgeEvent<TEvent>(actor);
        UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, cookie);
        return ev->Release();
    }

    template<class TEvent>
    void ExpectNoEdgeEvent(const TActorId& actor, TDuration duration) {
        auto edge = Runtime->AllocateEdgeActor();
        Runtime->EnableScheduleForActor(edge);
        Runtime->Schedule(new IEventHandle(edge, edge, new TEvent()), duration);
        auto event = Runtime->GrabEdgeEvent<TEvent>({actor, edge});
        UNIT_ASSERT_VALUES_UNEQUAL_C(event->Recipient, actor, "Unexpected event " << event->Get()->ToString());
    }

    // Sends payload to tablet from edge, fresh pipe every time
    void SendFromEdge(const TActorId& edge, IEventBase* payload, ui64 cookie = 0);

    template <class TEvent>
    void SendFromEdge(const TActorId& edge, THolder<TEvent> payload, ui64 cookie = 0) {
        SendFromEdge(edge, payload.Release(), cookie);
    }

    // Sends payload to tablet from proxy (caches pipe per proxy/generation pair)
    void SendFromProxy(const TActorId& proxy, ui64 generation, IEventBase* payload, ui64 cookie = 0);

    // set/get config requests
    NKikimrKesus::TEvGetConfigResult GetConfig();
    NKikimrKesus::TEvSetConfigResult SetConfig(ui64 txId, const Ydb::Coordination::Config& config, ui64 version, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    // Makes a dummy request using this proxy/generation pair
    void SyncProxy(const TActorId& proxy, ui64 generation, bool useTransactions = false);

    // Registers a proxy/generation pair with the tablet
    ui64 SendRegisterProxy(const TActorId& proxy, ui64 generation);
    NKikimrKesus::TEvRegisterProxyResult RegisterProxy(const TActorId& proxy, ui64 generation);
    void MustRegisterProxy(const TActorId& proxy, ui64 generation, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    // Unregisters a proxy/generation pair from the tablet
    NKikimrKesus::TEvUnregisterProxyResult UnregisterProxy(const TActorId& proxy, ui64 generation);
    void MustUnregisterProxy(const TActorId& proxy, ui64 generation, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    // Session attachment helpers
    void SendAttachSession(
        ui64 cookie, const TActorId& proxy, ui64 generation, ui64 sessionId,
        ui64 timeoutMillis = 0, const TString& description = TString(), ui64 seqNo = 0,
        const TString& key = TString());
    NKikimrKesus::TEvAttachSessionResult NextAttachSessionResult(
        ui64 cookie, const TActorId& proxy, ui64 generation);
    ui64 ExpectAttachSessionResult(
        ui64 cookie, const TActorId& proxy, ui64 generation, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);
    NKikimrKesus::TEvAttachSessionResult AttachSession(
        const TActorId& proxy, ui64 generation, ui64 sessionId,
        ui64 timeoutMillis = 0, const TString& description = TString(), ui64 seqNo = 0,
        const TString& key = TString());
    ui64 MustAttachSession(
        const TActorId& proxy, ui64 generation, ui64 sessionId,
        ui64 timeoutMillis = 0, const TString& description = TString(), ui64 seqNo = 0,
        const TString& key = TString());

    // Session detachment helpers
    NKikimrKesus::TEvDetachSessionResult DetachSession(const TActorId& proxy, ui64 generation, ui64 sessionId);
    void MustDetachSession(const TActorId& proxy, ui64 generation, ui64 sessionId, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    // Session destruction helpers
    NKikimrKesus::TEvDestroySessionResult DestroySession(const TActorId& proxy, ui64 generation, ui64 sessionId);
    void MustDestroySession(const TActorId& proxy, ui64 generation, ui64 sessionId, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    // Lock acquire helpers
    void SendAcquireLock(
        ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId,
        const TString& lockName, ELockMode mode,
        ui64 timeoutMillis = 0, const TString& data = TString());
    bool ExpectAcquireLockResult(ui64 reqId, const TActorId& proxy, ui64 generation,
        Ydb::StatusIds::StatusCode status);
    void ExpectAcquireLockResult(ui64 reqId, const TActorId& proxy, ui64 generation, bool acquired = true);

    // Lock release helpers
    bool MustReleaseLock(
        ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId, const TString& lockName,
        Ydb::StatusIds::StatusCode status);
    void MustReleaseLock(
        ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId, const TString& lockName, bool released = true);

    // Semaphore helpers
    void CreateSemaphore(
        const TString& name, ui64 limit, const TString& data, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);
    void CreateSemaphore(const TString& name, ui64 limit, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        CreateSemaphore(name, limit, TString(), status);
    }

    void UpdateSemaphore(
        const TString& name, const TString& data, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    void DeleteSemaphore(const TString& name, bool force, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);
    void DeleteSemaphore(const TString& name, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        DeleteSemaphore(name, false, status);
    }

    void SessionCreateSemaphore(
        ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId,
        const TString& name, ui64 limit, const TString& data, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    void SessionUpdateSemaphore(
        ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId,
        const TString& name, const TString& data, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    void SessionDeleteSemaphore(
        ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId,
        const TString& name, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    // Semaphore acquire helpers
    void SendAcquireSemaphore(
        ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId,
        const TString& name, ui64 count,
        ui64 timeoutMillis = 0, const TString& data = TString());
    bool ExpectAcquireSemaphoreResult(ui64 reqId, const TActorId& proxy, ui64 generation,
        Ydb::StatusIds::StatusCode status);
    void ExpectAcquireSemaphoreResult(ui64 reqId, const TActorId& proxy, ui64 generation, bool acquired = true);
    void ExpectAcquireSemaphorePending(ui64 reqId, const TActorId& proxy, ui64 generation);

    // Semaphore release helpers
    bool MustReleaseSemaphore(
        ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId, const TString& name,
        Ydb::StatusIds::StatusCode status);
    void MustReleaseSemaphore(
        ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId, const TString& name, bool released = true);

    // Describes and verifies info about proxies from the tablet

    struct TSimpleProxyInfo {
        ui64 Generation;
        THashSet<ui64> AttachedSessions;
    };

    THashMap<TActorId, TSimpleProxyInfo> DescribeProxies();
    void VerifyProxyRegistered(const TActorId& proxy, ui64 generation);
    void VerifyProxyNotRegistered(const TActorId& proxy);
    void VerifyProxyHasSessions(const TActorId& proxy, ui64 generation, const THashSet<ui64>& expectedSessions);

    // Describes and verifies info about sessions on the tablet

    struct TSimpleSessionInfo {
        ui64 TimeoutMillis;
        TString Description;
        TActorId OwnerProxy;
    };

    THashMap<ui64, TSimpleSessionInfo> DescribeSessions();
    void VerifySessionNotFound(ui64 sessionId);
    void VerifySessionExists(ui64 sessionId);

    // Describes and verifies info about locks

    struct TSimpleLockDescription {
        ui64 ExclusiveOwner = 0;
        THashSet<ui64> SharedOwners;
        THashMap<ui64, ELockMode> Waiters;
    };

    TSimpleLockDescription DescribeLock(const TString& lockName, bool includeWaiters = false);
    void VerifyLockNotFound(const TString& lockName);
    void VerifyLockExclusive(const TString& lockName, ui64 sessionId);
    void VerifyLockShared(const TString& lockName, const THashSet<ui64>& sessionIds);
    void VerifyLockWaiters(const TString& lockName, const THashSet<ui64>& sessionIds);

    // Describes and verifies info about semaphores

    struct TSimpleSemaphoreDescription {
        ui64 Limit;
        TString Data;
        bool Ephemeral;
        THashMap<ui64, ui64> Owners;
        THashMap<ui64, ui64> Waiters;
        bool WatchAdded;
    };

    TSimpleSemaphoreDescription DescribeSemaphore(const TString& name, bool includeWaiters = false);
    void VerifySemaphoreNotFound(const TString& name);
    void VerifySemaphoreOwners(const TString& name, const THashSet<ui64>& sessionIds, bool ephemeral = false);
    void VerifySemaphoreWaiters(const TString& name, const THashSet<ui64>& sessionIds, bool ephemeral = false);

    struct TDescribeSemaphoreChanges {
        bool DataChanged;
        bool OwnersChanged;
    };

    void SendSessionDescribeSemaphore(
        ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId,
        const TString& name, bool watchData = false, bool watchOwners = false);
    TSimpleSemaphoreDescription ExpectDescribeSemaphoreResult(
        ui64 reqId, const TActorId& proxy, ui64 generation,
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);
    TDescribeSemaphoreChanges ExpectDescribeSemaphoreChanged(ui64 reqId, const TActorId& proxy, ui64 generation);

    // Quoter
    THolder<TEvKesus::TEvDescribeQuoterResourcesResult> VerifyDescribeQuoterResources(
        const NKikimrKesus::TEvDescribeQuoterResources& req,
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);
    THolder<TEvKesus::TEvDescribeQuoterResourcesResult> VerifyDescribeQuoterResources(
        const std::vector<ui64>& resourceIds,
        const std::vector<TString>& resourcePaths,
        bool recursive,
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);
    NKikimrKesus::TEvDescribeQuoterResourcesResult DescribeQuoterResources(
        const std::vector<ui64>& resourceIds,
        const std::vector<TString>& resourcePaths,
        bool recursive);

    ui64 AddQuoterResource(const NKikimrKesus::TStreamingQuoterResource& resource, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);
    ui64 AddQuoterResource(const TString& resourcePath, const NKikimrKesus::THierarchicalDRRResourceConfig& config, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);
    ui64 AddQuoterResource(const TString& resourcePath, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        return AddQuoterResource(resourcePath, NKikimrKesus::THierarchicalDRRResourceConfig(), status);
    }
    ui64 AddQuoterResource(const TString& resourcePath, double maxUnitsPerSecond, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        cfg.SetMaxUnitsPerSecond(maxUnitsPerSecond);
        return AddQuoterResource(resourcePath, cfg, status);
    }

    void UpdateQuoterResource(const NKikimrKesus::TStreamingQuoterResource& resource, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);
    void UpdateQuoterResource(const TString& resourcePath, const NKikimrKesus::THierarchicalDRRResourceConfig& config, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);
    void UpdateQuoterResource(ui64 resourceId, const NKikimrKesus::THierarchicalDRRResourceConfig& config, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);
    void UpdateQuoterResource(ui64 resourceId, double maxUnitsPerSecond, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        NKikimrKesus::THierarchicalDRRResourceConfig cfg;
        cfg.SetMaxUnitsPerSecond(maxUnitsPerSecond);
        return UpdateQuoterResource(resourceId, cfg, status);
    }

    void DeleteQuoterResource(const NKikimrKesus::TEvDeleteQuoterResource& req, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);
    void DeleteQuoterResource(const TString& resourcePath, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);
    void DeleteQuoterResource(ui64 resourceId, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    struct TResourceConsumingInfo {
        TResourceConsumingInfo(const TString& path, bool consume, double amount = 0.0, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);
        TResourceConsumingInfo(ui64 id, bool consume, double amount = 0.0, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

        TString Path;
        ui64 Id = 0;
        bool Consume;
        double Amount;
        Ydb::StatusIds::StatusCode ExpectedStatus;
    };

    struct TResourceAccountInfo {
        ui64 Id = 0;
        TInstant Start;
        TDuration Interval;
        std::vector<double> Amount;

        TResourceAccountInfo(ui64 id, TInstant start, TDuration interval, std::vector<double>&& amount)
            : Id(id)
            , Start(start)
            , Interval(interval)
            , Amount(std::move(amount))
        {}
    };

    NKikimrKesus::TEvSubscribeOnResourcesResult SubscribeOnResources(const TActorId& client, const TActorId& edge, const std::vector<TResourceConsumingInfo>& info);
    NKikimrKesus::TEvSubscribeOnResourcesResult SubscribeOnResource(const TActorId& client, const TActorId& edge, const TString& path, bool startConsuming, double amount = 0.0, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    void UpdateConsumptionState(const TActorId& client, const TActorId& edge, const std::vector<TResourceConsumingInfo>& info);
    void UpdateConsumptionState(const TActorId& client, const TActorId& edge, ui64 id, bool consume, double amount = 0.0, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS);

    void AccountResources(const TActorId& client, const TActorId& edge, const std::vector<TResourceAccountInfo>& info);
    void AccountResources(const TActorId& client, const TActorId& edge, ui64 id, TInstant start, TDuration interval, std::vector<double>&& amount);

    NKikimrKesus::TEvGetQuoterResourceCountersResult GetQuoterResourceCounters();
};

}
}
