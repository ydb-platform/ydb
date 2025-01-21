#include "ut_helpers.h"

#include <ydb/core/metering/metering.h>

#include <ydb/library/actors/core/event_pb.h>

#include <algorithm>

namespace NKikimr {
namespace NKesus {

// Write metering events into memory only
class TFakeMetering : public TActor<TFakeMetering> {
    std::vector<TString> Jsons;

public:
    explicit TFakeMetering()
        : TActor<TFakeMetering>(&TFakeMetering::StateWork)
    {}

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
            HFunc(NMetering::TEvMetering::TEvWriteMeteringJson, HandleWriteMeteringJson);
        default:
            HandleUnexpectedEvent(ev);
            break;
        }
    }

    void HandlePoisonPill(const TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Die(ctx);
    }

    void HandleWriteMeteringJson(
        const NMetering::TEvMetering::TEvWriteMeteringJson::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ctx);

        LOG_DEBUG_S(ctx, NKikimrServices::KESUS_PROXY,
                    "tests -- TFakeMetering got TEvMetering::TEvWriteMeteringJson");

        const auto* msg = ev->Get();

        Jsons.push_back(msg->MeteringJson);
    }

    void HandleUnexpectedEvent(STFUNC_SIG)
    {
        ALOG_DEBUG(NKikimrServices::KESUS_PROXY,
                    "TFakeMetering:"
                        << " unhandled event type: " << ev->GetTypeRewrite()
                        << " event: " << ev->ToString());
    }
};

NActors::TActorId CreateFakeMetering(NActors::TTestActorRuntime &runtime) {
    NActors::TActorId actorId = runtime.Register(new TFakeMetering());
    runtime.RegisterService(NMetering::MakeMeteringServiceID(), actorId);
    return NMetering::MakeMeteringServiceID();
}

TTestContext::TTestContext()
    : TabletType(TTabletTypes::Kesus)
    , TabletId(MakeTabletID(false, 1))
{}

void TTestContext::Setup(ui32 nodeCount, bool useRealThreads) {
    ProxyClients.clear();
    Runtime.Reset(new TTestBasicRuntime(nodeCount, useRealThreads));

    SetupLogging();
    SetupTabletServices();

    TActorId bootstrapper = CreateTestBootstrapper(*Runtime,
        CreateTestTabletInfo(TabletId, TabletType, TErasureType::ErasureNone),
        &CreateKesusTablet);
    Runtime->EnableScheduleForActor(bootstrapper);
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTablet::EvBoot);
        Runtime->DispatchEvents(options);
    }

    CreateFakeMetering(*Runtime);
}

void TTestContext::Finalize() {
    Runtime.Reset(nullptr);
    ProxyClients.clear();
}

void TTestContext::SetupLogging() {
    Runtime->SetLogPriority(NKikimrServices::KESUS_TABLET, NLog::PRI_TRACE);
}

void TTestContext::SetupTabletServices() {
    ::NKikimr::SetupTabletServices(*Runtime);
}

void TTestContext::Sleep(ui64 millis) {
    TActorId sender = Runtime->AllocateEdgeActor();
    Runtime->Schedule(new IEventHandle(sender, sender, new TEvents::TEvWakeup()), TDuration::MilliSeconds(millis), 0);
    ExpectEdgeEvent<TEvents::TEvWakeup>(sender, 0);
}

void TTestContext::RebootTablet() {
    ui32 nodeIndex = 0;
    TActorId sender = Runtime->AllocateEdgeActor(nodeIndex);
    ForwardToTablet(*Runtime, TabletId, sender, new TEvents::TEvPoisonPill(), nodeIndex);
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTablet::EvBoot);
        Runtime->DispatchEvents(options);
    }
    InvalidateTabletResolverCache(*Runtime, TabletId, nodeIndex);
    ProxyClients.clear();
}

TActorId TTestContext::GetTabletActorId() {
    ui64 cookie = RandomNumber<ui64>();
    TActorId edge = Runtime->AllocateEdgeActor();
    SendFromEdge(edge, new TEvKesus::TEvDummyRequest(), cookie);
    TAutoPtr<IEventHandle> handle;
    Runtime->GrabEdgeEvent<TEvKesus::TEvDummyResponse>(handle);
    Y_ABORT_UNLESS(handle);
    UNIT_ASSERT_VALUES_EQUAL(handle->Recipient, edge);
    UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, cookie);
    return handle->Sender;
}

void TTestContext::SendFromEdge(const TActorId& edge, IEventBase* payload, ui64 cookie) {
    ui32 nodeIndex = edge.NodeId() - Runtime->GetNodeId(0);
    Runtime->SendToPipe(
        TabletId,
        edge,
        payload,
        nodeIndex,
        GetPipeConfigWithRetries(),
        TActorId(),
        cookie);
}

void TTestContext::SendFromProxy(const TActorId& proxy, ui64 generation, IEventBase* payload, ui64 cookie) {
    ui32 nodeIndex = proxy.NodeId() - Runtime->GetNodeId(0);
    TActorId& clientId = ProxyClients[std::tie(proxy, generation)];
    if (!clientId) {
        clientId = Runtime->ConnectToPipe(TabletId, proxy, nodeIndex, GetPipeConfigWithRetries());
    }
    Runtime->SendToPipe(
        TabletId,
        proxy,
        payload,
        nodeIndex,
        GetPipeConfigWithRetries(),
        clientId,
        cookie);
}

NKikimrKesus::TEvGetConfigResult TTestContext::GetConfig() {
    const ui64 cookie = RandomNumber<ui64>();
    const auto edge = Runtime->AllocateEdgeActor();
    SendFromEdge(edge, new TEvKesus::TEvGetConfig(), cookie);

    auto result = ExpectEdgeEvent<TEvKesus::TEvGetConfigResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetConfig().path(), result->Record.GetPath(), "Record: " << result->Record);
    return result->Record;
}

NKikimrKesus::TEvSetConfigResult TTestContext::SetConfig(ui64 txId, const Ydb::Coordination::Config& config, ui64 version, Ydb::StatusIds::StatusCode status) {
    const ui64 cookie = RandomNumber<ui64>();
    const auto edge = Runtime->AllocateEdgeActor();
    SendFromEdge(edge, new TEvKesus::TEvSetConfig(txId, config, version), cookie);

    auto result = ExpectEdgeEvent<TEvKesus::TEvSetConfigResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetError().GetStatus(), status, "Record: " << result->Record);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetTxId(), txId, "Record: " << result->Record);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetTabletId(), TabletId, "Record: " << result->Record);
    return result->Record;
}

void TTestContext::SyncProxy(const TActorId& proxy, ui64 generation, bool useTransactions) {
    ui64 cookie = RandomNumber<ui64>();
    SendFromProxy(proxy, generation, new TEvKesus::TEvDummyRequest(useTransactions), cookie);
    ExpectEdgeEvent<TEvKesus::TEvDummyResponse>(proxy, cookie);
}

ui64 TTestContext::SendRegisterProxy(const TActorId& proxy, ui64 generation) {
    ui64 cookie = RandomNumber<ui64>();
    SendFromProxy(proxy, generation, new TEvKesus::TEvRegisterProxy("", generation), cookie);
    return cookie;
}

NKikimrKesus::TEvRegisterProxyResult TTestContext::RegisterProxy(const TActorId& proxy, ui64 generation) {
    ui64 cookie = SendRegisterProxy(proxy, generation);
    auto result = ExpectEdgeEvent<TEvKesus::TEvRegisterProxyResult>(proxy, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetProxyGeneration(), generation);
    return result->Record;
}

void TTestContext::MustRegisterProxy(const TActorId& proxy, ui64 generation, Ydb::StatusIds::StatusCode status) {
    auto result = RegisterProxy(proxy, generation);
    UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetStatus(), status);
}

NKikimrKesus::TEvUnregisterProxyResult TTestContext::UnregisterProxy(const TActorId& proxy, ui64 generation) {
    ui64 cookie = RandomNumber<ui64>();
    SendFromProxy(proxy, generation, new TEvKesus::TEvUnregisterProxy("", generation), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvUnregisterProxyResult>(proxy, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetProxyGeneration(), generation);
    return result->Record;
}

void TTestContext::MustUnregisterProxy(const TActorId& proxy, ui64 generation, Ydb::StatusIds::StatusCode status) {
    auto result = UnregisterProxy(proxy, generation);
    UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetStatus(), status);
}

void TTestContext::SendAttachSession(
    ui64 cookie, const TActorId& proxy, ui64 generation, ui64 sessionId,
    ui64 timeoutMillis, const TString& description, ui64 seqNo,
    const TString& key)
{
    SendFromProxy(proxy, generation, new TEvKesus::TEvAttachSession("", generation, sessionId, timeoutMillis, description, seqNo, key), cookie);
}

NKikimrKesus::TEvAttachSessionResult TTestContext::NextAttachSessionResult(
    ui64 cookie, const TActorId& proxy, ui64 generation)
{
    auto result = ExpectEdgeEvent<TEvKesus::TEvAttachSessionResult>(proxy, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetProxyGeneration(), generation);
    return result->Record;
}

ui64 TTestContext::ExpectAttachSessionResult(
    ui64 cookie, const TActorId& proxy, ui64 generation, Ydb::StatusIds::StatusCode status)
{
    auto result = NextAttachSessionResult(cookie, proxy, generation);
    UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetStatus(), status);
    return result.GetSessionId();
}

NKikimrKesus::TEvAttachSessionResult TTestContext::AttachSession(
    const TActorId& proxy, ui64 generation, ui64 sessionId,
    ui64 timeoutMillis, const TString& description, ui64 seqNo,
    const TString& key)
{
    ui64 cookie = RandomNumber<ui64>();
    SendAttachSession(cookie, proxy, generation, sessionId, timeoutMillis, description, seqNo, key);
    return NextAttachSessionResult(cookie, proxy, generation);
}

ui64 TTestContext::MustAttachSession(
    const TActorId& proxy, ui64 generation, ui64 sessionId,
    ui64 timeoutMillis, const TString& description, ui64 seqNo,
    const TString& key)
{
    ui64 cookie = RandomNumber<ui64>();
    SendAttachSession(cookie, proxy, generation, sessionId, timeoutMillis, description, seqNo, key);
    return ExpectAttachSessionResult(cookie, proxy, generation);
}

NKikimrKesus::TEvDetachSessionResult TTestContext::DetachSession(const TActorId& proxy, ui64 generation, ui64 sessionId) {
    ui64 cookie = RandomNumber<ui64>();
    SendFromProxy(proxy, generation, new TEvKesus::TEvDetachSession("", generation, sessionId), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvDetachSessionResult>(proxy, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetProxyGeneration(), generation);
    return result->Record;
}

void TTestContext::MustDetachSession(const TActorId& proxy, ui64 generation, ui64 sessionId, Ydb::StatusIds::StatusCode status) {
    auto result = DetachSession(proxy, generation, sessionId);
    UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetStatus(), status);
}

NKikimrKesus::TEvDestroySessionResult TTestContext::DestroySession(const TActorId& proxy, ui64 generation, ui64 sessionId) {
    ui64 cookie = RandomNumber<ui64>();
    SendFromProxy(proxy, generation, new TEvKesus::TEvDestroySession("", generation, sessionId), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvDestroySessionResult>(proxy, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetProxyGeneration(), generation);
    return result->Record;
}

void TTestContext::MustDestroySession(const TActorId& proxy, ui64 generation, ui64 sessionId, Ydb::StatusIds::StatusCode status) {
    auto result = DestroySession(proxy, generation, sessionId);
    UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetStatus(), status);
}

void TTestContext::SendAcquireLock(
    ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId,
    const TString& lockName, ELockMode mode,
    ui64 timeoutMillis, const TString& data)
{
    ui64 count;
    switch (mode) {
        case LOCK_MODE_EXCLUSIVE:
            count = Max<ui64>();
            break;
        case LOCK_MODE_SHARED:
            count = 1;
            break;
        default:
            Y_ABORT("Unexpected lock mode %d", mode);
    }
    SendFromProxy(proxy, generation, new TEvKesus::TEvAcquireSemaphore("", generation, sessionId, lockName, count, timeoutMillis, data, true), reqId);
}

bool TTestContext::ExpectAcquireLockResult(ui64 reqId, const TActorId& proxy, ui64 generation, Ydb::StatusIds::StatusCode status) {
    auto result = ExpectEdgeEvent<TEvKesus::TEvAcquireSemaphoreResult>(proxy, reqId);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetProxyGeneration(), generation);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
    return result->Record.GetAcquired();
}

void TTestContext::ExpectAcquireLockResult(ui64 reqId, const TActorId& proxy, ui64 generation, bool acquired) {
    UNIT_ASSERT_VALUES_EQUAL(ExpectAcquireLockResult(reqId, proxy, generation, Ydb::StatusIds::SUCCESS), acquired);
}

bool TTestContext::MustReleaseLock(ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId, const TString& lockName, Ydb::StatusIds::StatusCode status) {
    SendFromProxy(proxy, generation, new TEvKesus::TEvReleaseSemaphore("", generation, sessionId, lockName), reqId);
    auto result = ExpectEdgeEvent<TEvKesus::TEvReleaseSemaphoreResult>(proxy, reqId);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetProxyGeneration(), generation);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
    return result->Record.GetReleased();
}

void TTestContext::MustReleaseLock(ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId, const TString& lockName, bool released) {
    UNIT_ASSERT_VALUES_EQUAL(MustReleaseLock(reqId, proxy, generation, sessionId, lockName, Ydb::StatusIds::SUCCESS), released);
}

void TTestContext::CreateSemaphore(const TString& name, ui64 limit, const TString& data, Ydb::StatusIds::StatusCode status) {
    ui64 cookie = RandomNumber<ui64>();
    auto edge = Runtime->AllocateEdgeActor();
    SendFromEdge(edge, new TEvKesus::TEvCreateSemaphore("", name, limit, data), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvCreateSemaphoreResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
}

void TTestContext::SessionCreateSemaphore(
        ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId,
        const TString& name, ui64 limit, const TString& data, Ydb::StatusIds::StatusCode status)
{
    auto event = new TEvKesus::TEvCreateSemaphore("", name, limit, data);
    event->Record.SetProxyGeneration(generation);
    event->Record.SetSessionId(sessionId);
    SendFromProxy(proxy, generation, event, reqId);
    auto result = ExpectEdgeEvent<TEvKesus::TEvCreateSemaphoreResult>(proxy, reqId);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetProxyGeneration(), generation);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
}

void TTestContext::UpdateSemaphore(const TString& name, const TString& data, Ydb::StatusIds::StatusCode status) {
    ui64 cookie = RandomNumber<ui64>();
    auto edge = Runtime->AllocateEdgeActor();
    SendFromEdge(edge, new TEvKesus::TEvUpdateSemaphore("", name, data), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvUpdateSemaphoreResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
}

void TTestContext::SessionUpdateSemaphore(
        ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId,
        const TString& name, const TString& data, Ydb::StatusIds::StatusCode status)
{
    auto event = new TEvKesus::TEvUpdateSemaphore("", name, data);
    event->Record.SetProxyGeneration(generation);
    event->Record.SetSessionId(sessionId);
    SendFromProxy(proxy, generation, event, reqId);
    auto result = ExpectEdgeEvent<TEvKesus::TEvUpdateSemaphoreResult>(proxy, reqId);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetProxyGeneration(), generation);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
}

void TTestContext::DeleteSemaphore(const TString& name, bool force, Ydb::StatusIds::StatusCode status) {
    ui64 cookie = RandomNumber<ui64>();
    auto edge = Runtime->AllocateEdgeActor();
    SendFromEdge(edge, new TEvKesus::TEvDeleteSemaphore("", name, force), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvDeleteSemaphoreResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
}

void TTestContext::SessionDeleteSemaphore(
        ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId,
        const TString& name, Ydb::StatusIds::StatusCode status)
{
    auto event = new TEvKesus::TEvDeleteSemaphore("", name, false);
    event->Record.SetProxyGeneration(generation);
    event->Record.SetSessionId(sessionId);
    SendFromProxy(proxy, generation, event, reqId);
    auto result = ExpectEdgeEvent<TEvKesus::TEvDeleteSemaphoreResult>(proxy, reqId);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetProxyGeneration(), generation);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
}

void TTestContext::SendAcquireSemaphore(
    ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId,
    const TString& name, ui64 count,
    ui64 timeoutMillis, const TString& data)
{
    SendFromProxy(proxy, generation, new TEvKesus::TEvAcquireSemaphore("", generation, sessionId, name, count, timeoutMillis, data), reqId);
}

bool TTestContext::ExpectAcquireSemaphoreResult(ui64 reqId, const TActorId& proxy, ui64 generation, Ydb::StatusIds::StatusCode status) {
    auto result = ExpectEdgeEvent<TEvKesus::TEvAcquireSemaphoreResult>(proxy, reqId);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetProxyGeneration(), generation);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
    return result->Record.GetAcquired();
}

void TTestContext::ExpectAcquireSemaphoreResult(ui64 reqId, const TActorId& proxy, ui64 generation, bool acquired) {
    UNIT_ASSERT_VALUES_EQUAL(ExpectAcquireSemaphoreResult(reqId, proxy, generation, Ydb::StatusIds::SUCCESS), acquired);
}

void TTestContext::ExpectAcquireSemaphorePending(ui64 reqId, const TActorId& proxy, ui64 generation) {
    auto result = ExpectEdgeEvent<TEvKesus::TEvAcquireSemaphorePending>(proxy, reqId);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetProxyGeneration(), generation);
}

bool TTestContext::MustReleaseSemaphore(
    ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId, const TString& name,
    Ydb::StatusIds::StatusCode status)
{
    SendFromProxy(proxy, generation, new TEvKesus::TEvReleaseSemaphore("", generation, sessionId, name), reqId);
    auto result = ExpectEdgeEvent<TEvKesus::TEvReleaseSemaphoreResult>(proxy, reqId);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetProxyGeneration(), generation);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
    return result->Record.GetReleased();
}

void TTestContext::MustReleaseSemaphore(
    ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId, const TString& name, bool released)
{
    UNIT_ASSERT_VALUES_EQUAL(MustReleaseSemaphore(reqId, proxy, generation, sessionId, name, Ydb::StatusIds::SUCCESS), released);
}

THashMap<TActorId, TTestContext::TSimpleProxyInfo> TTestContext::DescribeProxies() {
    ui64 cookie = RandomNumber<ui64>();
    TActorId edge = Runtime->AllocateEdgeActor();
    SendFromEdge(edge, new TEvKesus::TEvDescribeProxies(""), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvDescribeProxiesResult>(edge, cookie);
    THashMap<TActorId, TSimpleProxyInfo> proxies;
    for (const auto& proxyDesc : result->Record.GetProxies()) {
        TActorId proxyId = ActorIdFromProto(proxyDesc.GetActorID());
        auto& proxyInfo = proxies[proxyId];
        proxyInfo.Generation = proxyDesc.GetGeneration();
        for (ui64 sessionId : proxyDesc.GetAttachedSessions()) {
            proxyInfo.AttachedSessions.insert(sessionId);
        }
    }
    return proxies;
}

void TTestContext::VerifyProxyRegistered(const TActorId& proxy, ui64 generation) {
    auto proxies = DescribeProxies();
    UNIT_ASSERT_C(proxies.contains(proxy), "Proxy " << proxy << " is not registered");
    const auto& proxyInfo = proxies.at(proxy);
    UNIT_ASSERT_VALUES_EQUAL(proxyInfo.Generation, generation);
}

void TTestContext::VerifyProxyNotRegistered(const TActorId& proxy) {
    auto proxies = DescribeProxies();
    UNIT_ASSERT_C(!proxies.contains(proxy), "Proxy " << proxy << " is registered");
}

void TTestContext::VerifyProxyHasSessions(const TActorId& proxy, ui64 generation, const THashSet<ui64>& expectedSessions) {
    auto proxies = DescribeProxies();
    UNIT_ASSERT_C(proxies.contains(proxy), "Proxy " << proxy << " is not registered");
    const auto& proxyInfo = proxies.at(proxy);
    UNIT_ASSERT_VALUES_EQUAL(proxyInfo.Generation, generation);
    for (ui64 sessionId : expectedSessions) {
        UNIT_ASSERT_C(proxyInfo.AttachedSessions.contains(sessionId), "Proxy " << proxy << " does not own session " << sessionId);
    }
    for (ui64 sessionId : proxyInfo.AttachedSessions) {
        UNIT_ASSERT_C(expectedSessions.contains(sessionId), "Proxy " << proxy << " owns unexpected session " << sessionId);
    }
}

THashMap<ui64, TTestContext::TSimpleSessionInfo> TTestContext::DescribeSessions() {
    ui64 cookie = RandomNumber<ui64>();
    TActorId edge = Runtime->AllocateEdgeActor();
    SendFromEdge(edge, new TEvKesus::TEvDescribeSessions(""), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvDescribeSessionsResult>(edge, cookie);
    THashMap<ui64, TSimpleSessionInfo> sessions;
    for (const auto& sessionInfo : result->Record.GetSessions()) {
        Y_ABORT_UNLESS(!sessions.contains(sessionInfo.GetSessionId()));
        auto& session = sessions[sessionInfo.GetSessionId()];
        session.TimeoutMillis = sessionInfo.GetTimeoutMillis();
        session.Description = sessionInfo.GetDescription();
        session.OwnerProxy = ActorIdFromProto(sessionInfo.GetOwnerProxy());
    }
    return sessions;
}

void TTestContext::VerifySessionNotFound(ui64 sessionId) {
    auto sessions = DescribeSessions();
    UNIT_ASSERT_C(!sessions.contains(sessionId), "Session " << sessionId << " actually exist");
}

void TTestContext::VerifySessionExists(ui64 sessionId) {
    auto sessions = DescribeSessions();
    UNIT_ASSERT_C(sessions.contains(sessionId), "Session " << sessionId << " does not exist");
}

TTestContext::TSimpleLockDescription TTestContext::DescribeLock(const TString& lockName, bool includeWaiters) {
    ui64 cookie = RandomNumber<ui64>();
    TActorId edge = Runtime->AllocateEdgeActor();
    auto request = new TEvKesus::TEvDescribeSemaphore("", lockName);
    request->Record.SetIncludeOwners(true);
    request->Record.SetIncludeWaiters(includeWaiters);
    SendFromEdge(edge, request, cookie);
    auto event = ExpectEdgeEvent<TEvKesus::TEvDescribeSemaphoreResult>(edge, cookie);
    TSimpleLockDescription result;
    if (event->Record.GetError().GetStatus() != Ydb::StatusIds::NOT_FOUND) {
        UNIT_ASSERT_VALUES_EQUAL(event->Record.GetError().GetStatus(), Ydb::StatusIds::SUCCESS);
        const auto& desc = event->Record.GetSemaphoreDescription();
        UNIT_ASSERT_C(desc.ephemeral(), "Lock " << lockName << " is not ephemeral");
        for (const auto& owner : desc.owners()) {
            switch (owner.count()) {
                case Max<ui64>():
                    UNIT_ASSERT_C(result.ExclusiveOwner == 0, "Lock " << lockName << " has multiple exclusive owners");
                    result.ExclusiveOwner = owner.session_id();
                    break;
                case 1:
                    result.SharedOwners.insert(owner.session_id());
                    break;
                default:
                    Y_ABORT("Unexpected count %lu", owner.count());
            }
        }
        UNIT_ASSERT_C(result.ExclusiveOwner != 0 || !result.SharedOwners.empty(), "Lock " << lockName << " is not locked (but exists)");
        UNIT_ASSERT_C(result.ExclusiveOwner == 0 || result.SharedOwners.empty(), "Lock " << lockName << " is both exclusive and shared locked");
        for (const auto& waiter : desc.waiters()) {
            ELockMode mode;
            switch (waiter.count()) {
                case Max<ui64>():
                    mode = LOCK_MODE_EXCLUSIVE;
                    break;
                case 1:
                    mode = LOCK_MODE_SHARED;
                    break;
                default:
                    Y_ABORT("Unexpected count %lu", waiter.count());
            }
            result.Waiters[waiter.session_id()] = mode;
        }
    }
    return result;
}

void TTestContext::VerifyLockNotFound(const TString& lockName) {
    auto result = DescribeLock(lockName);
    UNIT_ASSERT_C(result.ExclusiveOwner == 0 && result.SharedOwners.empty(), "Lock " << lockName << " is currently locked");
}

void TTestContext::VerifyLockExclusive(const TString& lockName, ui64 sessionId) {
    auto desc = DescribeLock(lockName);
    UNIT_ASSERT_VALUES_EQUAL(desc.ExclusiveOwner, sessionId);
}

void TTestContext::VerifyLockShared(const TString& lockName, const THashSet<ui64>& sessionIds) {
    auto desc = DescribeLock(lockName);
    for (ui64 sessionId : sessionIds) {
        UNIT_ASSERT_C(desc.SharedOwners.contains(sessionId), "Session " << sessionId << " is not a shared owner of " << lockName);
    }
    for (ui64 sessionId : desc.SharedOwners) {
        UNIT_ASSERT_C(sessionIds.contains(sessionId), "Session " << sessionId << " is an unexpected shared owner of " << lockName);
    }
}

void TTestContext::VerifyLockWaiters(const TString& lockName, const THashSet<ui64>& sessionIds) {
    auto desc = DescribeLock(lockName, true);
    for (ui64 sessionId : sessionIds) {
        UNIT_ASSERT_C(desc.Waiters.contains(sessionId), "Session " << sessionId << " is not a waiter of " << lockName);
    }
    for (const auto& kv : desc.Waiters) {
        ui64 sessionId = kv.first;
        UNIT_ASSERT_C(sessionIds.contains(sessionId), "Session " << sessionId << " is an unexpected waiter of " << lockName);
    }
}

TTestContext::TSimpleSemaphoreDescription MakeSimpleSemaphoreDescription(const Ydb::Coordination::SemaphoreDescription& desc) {
    TTestContext::TSimpleSemaphoreDescription result;
    result.Limit = desc.limit();
    result.Data = desc.data();
    result.Ephemeral = desc.ephemeral();
    for (const auto& owner : desc.owners()) {
        result.Owners[owner.session_id()] = owner.count();
    }
    for (const auto& waiter : desc.waiters()) {
        result.Waiters[waiter.session_id()] = waiter.count();
    }
    result.WatchAdded = false;
    return result;
}

TTestContext::TSimpleSemaphoreDescription TTestContext::DescribeSemaphore(const TString& name, bool includeWaiters) {
    ui64 cookie = RandomNumber<ui64>();
    TActorId edge = Runtime->AllocateEdgeActor();
    auto request = new TEvKesus::TEvDescribeSemaphore("", name);
    request->Record.SetIncludeOwners(true);
    request->Record.SetIncludeWaiters(includeWaiters);
    SendFromEdge(edge, request, cookie);
    auto event = ExpectEdgeEvent<TEvKesus::TEvDescribeSemaphoreResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetError().GetStatus(), Ydb::StatusIds::SUCCESS);
    const auto& desc = event->Record.GetSemaphoreDescription();
    return MakeSimpleSemaphoreDescription(desc);
}

void TTestContext::VerifySemaphoreNotFound(const TString& name) {
    ui64 cookie = RandomNumber<ui64>();
    TActorId edge = Runtime->AllocateEdgeActor();
    SendFromEdge(edge, new TEvKesus::TEvDescribeSemaphore("", name), cookie);
    auto event = ExpectEdgeEvent<TEvKesus::TEvDescribeSemaphoreResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetError().GetStatus(), Ydb::StatusIds::NOT_FOUND);
}

void TTestContext::VerifySemaphoreOwners(const TString& name, const THashSet<ui64>& sessionIds, bool ephemeral) {
    auto desc = DescribeSemaphore(name);
    UNIT_ASSERT_VALUES_EQUAL(desc.Ephemeral, ephemeral);
    for (ui64 sessionId : sessionIds) {
        UNIT_ASSERT_C(desc.Owners.contains(sessionId), "Session " << sessionId << " is not an owner of " << name);
    }
    for (const auto& kv : desc.Owners) {
        ui64 sessionId = kv.first;
        UNIT_ASSERT_C(sessionIds.contains(sessionId), "Session " << sessionId << " is an unexpected owner of " << name);
    }
}

void TTestContext::VerifySemaphoreWaiters(const TString& name, const THashSet<ui64>& sessionIds, bool ephemeral) {
    auto desc = DescribeSemaphore(name, true);
    UNIT_ASSERT_VALUES_EQUAL(desc.Ephemeral, ephemeral);
    for (ui64 sessionId : sessionIds) {
        UNIT_ASSERT_C(desc.Waiters.contains(sessionId), "Session " << sessionId << " is not a waiter of " << name);
    }
    for (const auto& kv : desc.Waiters) {
        ui64 sessionId = kv.first;
        UNIT_ASSERT_C(sessionIds.contains(sessionId), "Session " << sessionId << " is an unexpected waiter of " << name);
    }
}

void TTestContext::SendSessionDescribeSemaphore(
        ui64 reqId, const TActorId& proxy, ui64 generation, ui64 sessionId,
        const TString& name, bool watchData, bool watchOwners)
{
    auto event = new TEvKesus::TEvDescribeSemaphore("", name);
    event->Record.SetProxyGeneration(generation);
    event->Record.SetSessionId(sessionId);
    event->Record.SetWatchData(watchData);
    event->Record.SetWatchOwners(watchOwners);
    SendFromProxy(proxy, generation, event, reqId);
}

TTestContext::TSimpleSemaphoreDescription TTestContext::ExpectDescribeSemaphoreResult(
        ui64 reqId, const TActorId& proxy, ui64 generation,
        Ydb::StatusIds::StatusCode status)
{
    auto event = ExpectEdgeEvent<TEvKesus::TEvDescribeSemaphoreResult>(proxy, reqId);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetProxyGeneration(), generation);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetError().GetStatus(), status);
    auto result = MakeSimpleSemaphoreDescription(event->Record.GetSemaphoreDescription());
    result.WatchAdded = event->Record.GetWatchAdded();
    return result;
}

TTestContext::TDescribeSemaphoreChanges TTestContext::ExpectDescribeSemaphoreChanged(
        ui64 reqId, const TActorId& proxy, ui64 generation)
{
    auto event = ExpectEdgeEvent<TEvKesus::TEvDescribeSemaphoreChanged>(proxy, reqId);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetProxyGeneration(), generation);
    TDescribeSemaphoreChanges changes;
    changes.DataChanged = event->Record.GetDataChanged();
    changes.OwnersChanged = event->Record.GetOwnersChanged();
    return changes;
}

THolder<TEvKesus::TEvDescribeQuoterResourcesResult> TTestContext::VerifyDescribeQuoterResources(
        const NKikimrKesus::TEvDescribeQuoterResources& req,
        Ydb::StatusIds::StatusCode status)
{
    ui64 cookie = RandomNumber<ui64>();
    auto edge = Runtime->AllocateEdgeActor();
    SendFromEdge(edge, MakeHolder<TEvKesus::TEvDescribeQuoterResources>(req), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvDescribeQuoterResourcesResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
    if (status != Ydb::StatusIds::SUCCESS) {
        UNIT_ASSERT_VALUES_EQUAL(result->Record.ResourcesSize(), 0);
    }
    return result;
}

THolder<TEvKesus::TEvDescribeQuoterResourcesResult> TTestContext::VerifyDescribeQuoterResources(
        const std::vector<ui64>& resourceIds,
        const std::vector<TString>& resourcePaths,
        bool recursive,
        Ydb::StatusIds::StatusCode status)
{
    NKikimrKesus::TEvDescribeQuoterResources req;
    for (ui64 resourceId : resourceIds) {
        req.AddResourceIds(resourceId);
    }
    for (const TString& resourcePath : resourcePaths) {
        req.AddResourcePaths(resourcePath);
    }
    req.SetRecursive(recursive);
    return VerifyDescribeQuoterResources(req, status);
}

NKikimrKesus::TEvDescribeQuoterResourcesResult TTestContext::DescribeQuoterResources(
        const std::vector<ui64>& resourceIds,
        const std::vector<TString>& resourcePaths,
        bool recursive)
{
    NKikimrKesus::TEvDescribeQuoterResourcesResult result = VerifyDescribeQuoterResources(resourceIds, resourcePaths, recursive)->Record;
    std::sort(result.MutableResources()->begin(), result.MutableResources()->end(),
              [](const NKikimrKesus::TStreamingQuoterResource& r1, const NKikimrKesus::TStreamingQuoterResource& r2) {
                  return r1.GetResourcePath() < r2.GetResourcePath();
              });
    return result;
}

ui64 TTestContext::AddQuoterResource(const NKikimrKesus::TStreamingQuoterResource& resource, Ydb::StatusIds::StatusCode status) {
    ui64 cookie = RandomNumber<ui64>();
    auto edge = Runtime->AllocateEdgeActor();
    auto req = MakeHolder<TEvKesus::TEvAddQuoterResource>();
    *req->Record.MutableResource() = resource;
    SendFromEdge(edge, std::move(req), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvAddQuoterResourceResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL_C(result->Record.GetError().GetStatus(), status, "Faied to create new quoter resource \"" << resource.GetResourcePath() << "\"");
    if (status == Ydb::StatusIds::SUCCESS) {
        UNIT_ASSERT(result->Record.GetResourceId());
    }
    return status == Ydb::StatusIds::SUCCESS ? result->Record.GetResourceId() : 0;
}

ui64 TTestContext::AddQuoterResource(const TString& resourcePath, const NKikimrKesus::THierarchicalDRRResourceConfig& config, Ydb::StatusIds::StatusCode status) {
    NKikimrKesus::TStreamingQuoterResource resource;
    resource.SetResourcePath(resourcePath);
    *resource.MutableHierarchicalDRRResourceConfig() = config;
    return AddQuoterResource(resource, status);
}

void TTestContext::UpdateQuoterResource(const NKikimrKesus::TStreamingQuoterResource& resource, Ydb::StatusIds::StatusCode status) {
    ui64 cookie = RandomNumber<ui64>();
    auto edge = Runtime->AllocateEdgeActor();
    auto req = MakeHolder<TEvKesus::TEvUpdateQuoterResource>();
    *req->Record.MutableResource() = resource;
    SendFromEdge(edge, std::move(req), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvUpdateQuoterResourceResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
    if (status == Ydb::StatusIds::SUCCESS) {
        UNIT_ASSERT(result->Record.GetResourceId());
    }
}

void TTestContext::UpdateQuoterResource(const TString& resourcePath, const NKikimrKesus::THierarchicalDRRResourceConfig& config, Ydb::StatusIds::StatusCode status) {
    NKikimrKesus::TStreamingQuoterResource resource;
    resource.SetResourcePath(resourcePath);
    *resource.MutableHierarchicalDRRResourceConfig() = config;
    UpdateQuoterResource(resource, status);
}

void TTestContext::UpdateQuoterResource(ui64 resourceId, const NKikimrKesus::THierarchicalDRRResourceConfig& config, Ydb::StatusIds::StatusCode status) {
    NKikimrKesus::TStreamingQuoterResource resource;
    resource.SetResourceId(resourceId);
    *resource.MutableHierarchicalDRRResourceConfig() = config;
    UpdateQuoterResource(resource, status);
}

void TTestContext::DeleteQuoterResource(const NKikimrKesus::TEvDeleteQuoterResource& req, Ydb::StatusIds::StatusCode status) {
    ui64 cookie = RandomNumber<ui64>();
    auto edge = Runtime->AllocateEdgeActor();
    SendFromEdge(edge, MakeHolder<TEvKesus::TEvDeleteQuoterResource>(req), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvDeleteQuoterResourceResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
}

void TTestContext::DeleteQuoterResource(const TString& resourcePath, Ydb::StatusIds::StatusCode status) {
    NKikimrKesus::TEvDeleteQuoterResource req;
    req.SetResourcePath(resourcePath);
    DeleteQuoterResource(req, status);
}

void TTestContext::DeleteQuoterResource(ui64 resourceId, Ydb::StatusIds::StatusCode status) {
    NKikimrKesus::TEvDeleteQuoterResource req;
    req.SetResourceId(resourceId);
    DeleteQuoterResource(req, status);
}

TTestContext::TResourceConsumingInfo::TResourceConsumingInfo(const TString& path, bool consume, double amount, Ydb::StatusIds::StatusCode status)
    : Path(path)
    , Consume(consume)
    , Amount(amount)
    , ExpectedStatus(status)
{
}

TTestContext::TResourceConsumingInfo::TResourceConsumingInfo(ui64 id, bool consume, double amount, Ydb::StatusIds::StatusCode status)
    : Id(id)
    , Consume(consume)
    , Amount(amount)
    , ExpectedStatus(status)
{
}

NKikimrKesus::TEvSubscribeOnResourcesResult TTestContext::SubscribeOnResources(const TActorId& client, const TActorId& edge, const std::vector<TResourceConsumingInfo>& info) {
    const ui64 cookie = RandomNumber<ui64>();
    auto req = MakeHolder<TEvKesus::TEvSubscribeOnResources>();
    ActorIdToProto(client, req->Record.MutableActorID());
    req->Record.MutableResources()->Reserve(info.size());
    for (const TResourceConsumingInfo& res : info) {
        auto* reqRes = req->Record.AddResources();
        reqRes->SetResourcePath(res.Path);
        Y_ASSERT(!res.Id); // self check
        reqRes->SetStartConsuming(res.Consume);
        reqRes->SetInitialAmount(res.Amount);
    }

    SendFromEdge(edge, std::move(req), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvSubscribeOnResourcesResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.ResultsSize(), info.size());
    for (size_t i = 0; i < info.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(result->Record.GetResults(i).GetError().GetStatus(), info[i].ExpectedStatus);
        const auto status = result->Record.GetResults(i).GetError().GetStatus();
        if (status == Ydb::StatusIds::SUCCESS || status == Ydb::StatusIds::ALREADY_EXISTS) {
            UNIT_ASSERT(result->Record.GetResults(i).GetResourceId() != 0);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(result->Record.GetResults(i).GetResourceId(), 0);
        }
    }
    return result->Record;
}

NKikimrKesus::TEvSubscribeOnResourcesResult TTestContext::SubscribeOnResource(const TActorId& client, const TActorId& edge, const TString& path, bool startConsuming, double amount, Ydb::StatusIds::StatusCode status) {
    return SubscribeOnResources(client, edge, {TResourceConsumingInfo(path, startConsuming, amount, status)});
}

void TTestContext::UpdateConsumptionState(const TActorId& client, const TActorId& edge, const std::vector<TResourceConsumingInfo>& info) {
    const ui64 cookie = RandomNumber<ui64>();
    auto req = MakeHolder<TEvKesus::TEvUpdateConsumptionState>();
    ActorIdToProto(client, req->Record.MutableActorID());
    req->Record.MutableResourcesInfo()->Reserve(info.size());
    for (const TResourceConsumingInfo& res : info) {
        auto* reqRes = req->Record.AddResourcesInfo();
        reqRes->SetResourceId(res.Id);
        Y_ASSERT(res.Path.empty()); // self check
        reqRes->SetConsumeResource(res.Consume);
        reqRes->SetAmount(res.Amount);
    }

    SendFromEdge(edge, std::move(req), cookie);
    ExpectEdgeEvent<TEvKesus::TEvUpdateConsumptionStateAck>(edge, cookie);
}

void TTestContext::UpdateConsumptionState(const TActorId& client, const TActorId& edge, ui64 id, bool consume, double amount, Ydb::StatusIds::StatusCode status) {
    UpdateConsumptionState(client, edge, {TResourceConsumingInfo(id, consume, amount, status)});
}

void TTestContext::AccountResources(const TActorId& client, const TActorId& edge, const std::vector<TResourceAccountInfo>& info) {
    const ui64 cookie = RandomNumber<ui64>();
    auto req = MakeHolder<TEvKesus::TEvAccountResources>();
    ActorIdToProto(client, req->Record.MutableActorID());
    req->Record.MutableResourcesInfo()->Reserve(info.size());
    for (const TResourceAccountInfo& res : info) {
        auto* reqRes = req->Record.AddResourcesInfo();
        reqRes->SetResourceId(res.Id);
        reqRes->SetStartUs(res.Start.MicroSeconds());
        reqRes->SetIntervalUs(res.Interval.MicroSeconds());
        for (double value : res.Amount) {
            reqRes->AddAmount(value);
        }
    }

    SendFromEdge(edge, std::move(req), cookie);
    ExpectEdgeEvent<TEvKesus::TEvAccountResourcesAck>(edge, cookie);
}

void TTestContext::AccountResources(const TActorId& client, const TActorId& edge, ui64 id, TInstant start, TDuration interval, std::vector<double>&& amount) {
    AccountResources(client, edge, {TResourceAccountInfo(id, start, interval, std::move(amount))});
}

NKikimrKesus::TEvGetQuoterResourceCountersResult TTestContext::GetQuoterResourceCounters() {
    const ui64 cookie = RandomNumber<ui64>();
    const auto edge = Runtime->AllocateEdgeActor();
    SendFromEdge(edge, new TEvKesus::TEvGetQuoterResourceCounters(), cookie);

    auto result = ExpectEdgeEvent<TEvKesus::TEvGetQuoterResourceCountersResult>(edge, cookie);
    std::sort(result->Record.MutableResourceCounters()->begin(),
              result->Record.MutableResourceCounters()->end(),
              [](const auto& c1, const auto c2) {
                  return c1.GetResourcePath() < c2.GetResourcePath();
              });
    return result->Record;
}

}
}
