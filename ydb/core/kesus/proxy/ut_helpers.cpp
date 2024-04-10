#include "ut_helpers.h"

namespace NKikimr {
namespace NKesus {

TTestContext::TTestContext()
    : TabletType(TTabletTypes::Kesus)
    , TabletId(MakeTabletID(false, 1))
{}

void TTestContext::Setup(ui32 nodeCount) {
    Y_ABORT_UNLESS(nodeCount >= 2);
    ProxyActors.clear();
    Runtime.Reset(new TTestBasicRuntime(nodeCount));

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
}

void TTestContext::Finalize() {
    Runtime.Reset(nullptr);
    ProxyActors.clear();
}

void TTestContext::SetupLogging() {
    // nothing
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
}

TActorId TTestContext::GetProxy(ui64 proxyId, ui32 nodeIndex) {
    auto* pActor = ProxyActors.FindPtr(proxyId);
    if (!pActor) {
        pActor = &ProxyActors[proxyId];
        *pActor = Runtime->Register(CreateKesusProxyActor(TActorId(), TabletId, ""), nodeIndex);
        Runtime->EnableScheduleForActor(*pActor);
    }
    return *pActor;
}

void TTestContext::SendFromEdge(ui64 proxyId, const TActorId& edge, IEventBase* payload, ui64 cookie) {
    ui32 nodeIndex = edge.NodeId() - Runtime->GetNodeId(0);
    Runtime->Send(
        new IEventHandle(GetProxy(proxyId), edge, payload, 0, cookie),
        nodeIndex,
        true);
}

void TTestContext::ExpectProxyError(const TActorId& edge, ui64 cookie, Ydb::StatusIds::StatusCode status) {
    auto result = ExpectEdgeEvent<TEvKesusProxy::TEvProxyError>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Error.GetStatus(), status);
}

void TTestContext::CreateSemaphore(
    ui64 proxyId, const TString& name, ui64 limit, const TString& data, Ydb::StatusIds::StatusCode status)
{
    ui64 cookie = RandomNumber<ui64>();
    auto edge = Runtime->AllocateEdgeActor(1);
    SendFromEdge(proxyId, edge, new TEvKesus::TEvCreateSemaphore("", name, limit, data), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvCreateSemaphoreResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
}

void TTestContext::DeleteSemaphore(ui64 proxyId, const TString& name, Ydb::StatusIds::StatusCode status) {
    ui64 cookie = RandomNumber<ui64>();
    auto edge = Runtime->AllocateEdgeActor(1);
    SendFromEdge(proxyId, edge, new TEvKesus::TEvDeleteSemaphore("", name), cookie);
    auto result = ExpectEdgeEvent<TEvKesus::TEvDeleteSemaphoreResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
}

ui64 TTestContext::SendAttachSession(
    ui64 proxyId, const TActorId& edge, ui64 sessionId, ui64 timeoutMillis, const TString& description, ui64 seqNo)
{
    ui64 cookie = RandomNumber<ui64>();
    SendFromEdge(proxyId, edge, new TEvKesus::TEvAttachSession("", 0, sessionId, timeoutMillis, description, seqNo), cookie);
    return cookie;
}

ui64 TTestContext::ExpectAttachSessionResult(const TActorId& edge, ui64 cookie, Ydb::StatusIds::StatusCode status) {
    auto result = ExpectEdgeEvent<TEvKesus::TEvAttachSessionResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetError().GetStatus(), status);
    return result->Record.GetSessionId();
}

TTestContext::TSimpleSemaphoreDescription TTestContext::DescribeSemaphore(ui64 proxyId, const TString& name, bool includeWaiters) {
    ui64 cookie = RandomNumber<ui64>();
    TActorId edge = Runtime->AllocateEdgeActor(1);
    auto request = new TEvKesus::TEvDescribeSemaphore("", name);
    request->Record.SetIncludeOwners(true);
    request->Record.SetIncludeWaiters(includeWaiters);
    SendFromEdge(proxyId, edge, request, cookie);
    auto event = ExpectEdgeEvent<TEvKesus::TEvDescribeSemaphoreResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetError().GetStatus(), Ydb::StatusIds::SUCCESS);
    const auto& desc = event->Record.GetSemaphoreDescription();

    TSimpleSemaphoreDescription result;
    result.Limit = desc.limit();
    result.Data = desc.data();
    result.Ephemeral = desc.ephemeral();
    for (const auto& owner : desc.owners()) {
        result.Owners[owner.session_id()] = owner.count();
    }
    for (const auto& waiter : desc.waiters()) {
        result.Waiters[waiter.session_id()] = waiter.count();
    }
    return result;
}

void TTestContext::VerifySemaphoreNotFound(ui64 proxyId, const TString& name) {
    ui64 cookie = RandomNumber<ui64>();
    TActorId edge = Runtime->AllocateEdgeActor(1);
    SendFromEdge(proxyId, edge, new TEvKesus::TEvDescribeSemaphore("", name), cookie);
    auto event = ExpectEdgeEvent<TEvKesus::TEvDescribeSemaphoreResult>(edge, cookie);
    UNIT_ASSERT_VALUES_EQUAL(event->Record.GetError().GetStatus(), Ydb::StatusIds::NOT_FOUND);
}

void TTestContext::VerifySemaphoreParams(ui64 proxyId, const TString& name, ui64 limit, const TString& data, bool ephemeral) {
    auto desc = DescribeSemaphore(proxyId, name);
    UNIT_ASSERT_VALUES_EQUAL(desc.Limit, limit);
    UNIT_ASSERT_VALUES_EQUAL(desc.Data, data);
    UNIT_ASSERT_VALUES_EQUAL(desc.Ephemeral, ephemeral);
}

void TTestContext::VerifySemaphoreOwners(ui64 proxyId, const TString& name, const THashSet<ui64>& sessionIds, bool ephemeral) {
    auto desc = DescribeSemaphore(proxyId, name);
    UNIT_ASSERT_VALUES_EQUAL(desc.Ephemeral, ephemeral);
    for (ui64 sessionId : sessionIds) {
        UNIT_ASSERT_C(desc.Owners.contains(sessionId), "Session " << sessionId << " is not an owner of " << name);
    }
    for (const auto& kv : desc.Owners) {
        ui64 sessionId = kv.first;
        UNIT_ASSERT_C(sessionIds.contains(sessionId), "Session " << sessionId << " is an unexpected owner of " << name);
    }
}

void TTestContext::VerifySemaphoreWaiters(ui64 proxyId, const TString& name, const THashSet<ui64>& sessionIds, bool ephemeral) {
    auto desc = DescribeSemaphore(proxyId, name, true);
    UNIT_ASSERT_VALUES_EQUAL(desc.Ephemeral, ephemeral);
    for (ui64 sessionId : sessionIds) {
        UNIT_ASSERT_C(desc.Waiters.contains(sessionId), "Session " << sessionId << " is not a waiter of " << name);
    }
    for (const auto& kv : desc.Waiters) {
        ui64 sessionId = kv.first;
        UNIT_ASSERT_C(sessionIds.contains(sessionId), "Session " << sessionId << " is an unexpected waiter of " << name);
    }
}

}
}
