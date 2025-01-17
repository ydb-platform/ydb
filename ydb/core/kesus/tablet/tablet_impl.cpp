#include "tablet_impl.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>

#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {
namespace NKesus {

TKesusTablet::TKesusTablet(const TActorId& tablet, TTabletStorageInfo* info)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
{
    TabletCountersPtr.Reset(new TProtobufTabletCounters<
        ESimpleCounters_descriptor,
        ECumulativeCounters_descriptor,
        EPercentileCounters_descriptor,
        ETxTypes_descriptor>());
    TabletCounters = TabletCountersPtr.Get();

    ResetState();
}

TKesusTablet::~TKesusTablet()
{}

void TKesusTablet::ResetState() {
    KesusPath.clear();
    ConfigVersion = 0;
    NextSessionId = 1;
    NextSemaphoreId = 1;
    NextSemaphoreOrderId = 1;
    SelfCheckPeriod = TDuration::Seconds(1);
    SessionGracePeriod = TDuration::Seconds(10);
    ReadConsistencyMode = Ydb::Coordination::CONSISTENCY_MODE_RELAXED;
    AttachConsistencyMode = Ydb::Coordination::CONSISTENCY_MODE_STRICT;
    RateLimiterCountersMode = Ydb::Coordination::RATE_LIMITER_COUNTERS_MODE_AGGREGATED;

    Sessions.clear();
    Semaphores.clear();
    SemaphoresByName.clear();

    SelfCheckCounter = 0;
    StrictMarkerCounter = 0;

    NextQuoterResourceId = 1;
}

void TKesusTablet::ResetCounters() {
    TabletCounters->Simple()[COUNTER_PROXY_COUNT].Set(0);
    TabletCounters->Simple()[COUNTER_SESSION_COUNT].Set(0);
    TabletCounters->Simple()[COUNTER_SESSION_ACTIVE_COUNT].Set(0);
    TabletCounters->Simple()[COUNTER_SEMAPHORE_COUNT].Set(0);
    TabletCounters->Simple()[COUNTER_SEMAPHORE_OWNER_COUNT].Set(0);
    TabletCounters->Simple()[COUNTER_SEMAPHORE_WAITER_COUNT].Set(0);
    TabletCounters->Simple()[COUNTER_QUOTER_RESOURCE_COUNT].Set(0);
}

void TKesusTablet::OnDetach(const TActorContext& ctx) {
    Die(ctx);
}

void TKesusTablet::OnTabletDead(TEvTablet::TEvTabletDead::TPtr& ev, const TActorContext& ctx) {
    Y_UNUSED(ev);
    Die(ctx);
}

void TKesusTablet::OnActivateExecutor(const TActorContext& ctx) {
    LOG_INFO(ctx, NKikimrServices::KESUS_TABLET, "OnActivateExecutor: %" PRIu64, TabletID());

    Executor()->RegisterExternalTabletCounters(TabletCountersPtr.Release());
    Execute(CreateTxInitSchema(), ctx);
}

void TKesusTablet::DefaultSignalTabletActive(const TActorContext& ctx) {
    Y_UNUSED(ctx); // postpone until TxInit initializes everything
}

void TKesusTablet::ClearProxy(TProxyInfo* proxy, const TActorContext& ctx) {
    auto sessionIds = proxy->AttachedSessions; // make a copy
    for (ui64 sessionId : sessionIds) {
        auto* session = Sessions.FindPtr(sessionId);
        Y_ABORT_UNLESS(session);
        Y_ABORT_UNLESS(session->OwnerProxy == proxy);
        Y_ABORT_UNLESS(ScheduleSessionTimeout(session, ctx));
    }
    // All sessions must be detached by now
    Y_ABORT_UNLESS(proxy->AttachedSessions.empty());
}

void TKesusTablet::ForgetProxy(TProxyInfo* proxy) {
    Y_ABORT_UNLESS(proxy->AttachedSessions.empty());
    ui32 nodeId = proxy->ActorID.NodeId();
    if (auto* nodeProxies = ProxiesByNode.FindPtr(nodeId)) {
        nodeProxies->erase(proxy);
        if (nodeProxies->empty()) {
            ProxiesByNode.erase(nodeId);
        }
    }
    Proxies.erase(proxy->ActorID);
    TabletCounters->Simple()[COUNTER_PROXY_COUNT].Add(-1);
}

void TKesusTablet::VerifyKesusPath(const TString& kesusPath) {
    Y_DEBUG_ABORT_UNLESS(SplitPath(kesusPath) == SplitPath(KesusPath),
        "Incoming request has KesusPath=%s (tablet has KesusPath=%s)",
        kesusPath.Quote().data(),
        KesusPath.Quote().data());
}

void TKesusTablet::Handle(TEvents::TEvUndelivered::TPtr& ev) {
    const auto* msg = ev->Get();
    switch (msg->SourceType) {
        case TEvKesus::EvRegisterProxyResult:
            if (auto* proxy = Proxies.FindPtr(ev->Sender)) {
                ClearProxy(proxy, TActivationContext::AsActorContext());
                ForgetProxy(proxy);
            }
            break;
        default:
            break;
    }
}

void TKesusTablet::Handle(TEvInterconnect::TEvNodeConnected::TPtr& ev) {
    Y_UNUSED(ev);
}

void TKesusTablet::Handle(TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
    const auto* msg = ev->Get();
    auto& nodeProxies = ProxiesByNode[msg->NodeId];
    for (auto it = nodeProxies.begin(); it != nodeProxies.end(); /* nothing*/) {
        auto* proxy = *it;
        if (proxy->InterconnectSession && proxy->InterconnectSession != ev->Sender) {
            // Skip proxies from a different session
            ++it;
            continue;
        }
        ClearProxy(proxy, TActivationContext::AsActorContext());
        Proxies.erase(proxy->ActorID);
        TabletCounters->Simple()[COUNTER_PROXY_COUNT].Add(-1);
        TabletCounters->Cumulative()[COUNTER_PROXY_KICKED].Increment(1);
        nodeProxies.erase(it++);
    }
    if (nodeProxies.empty()) {
        ProxiesByNode.erase(msg->NodeId);
    }
}

void TKesusTablet::Handle(TEvents::TEvWakeup::TPtr& ev) {
    switch (ev->Get()->Tag) {
    case QUOTER_TICK_PROCESSING_WAKEUP_TAG:
        QuoterTickProcessingIsScheduled = false;
        return HandleQuoterTick();
    default:
        Y_ABORT_UNLESS(false, "Unknown Wakeup event with tag #%" PRIu64, ev->Get()->Tag);
    }
}

void TKesusTablet::Handle(TEvKesus::TEvDescribeProxies::TPtr& ev) {
    const auto& record = ev->Get()->Record;
    VerifyKesusPath(record.GetKesusPath());

    auto event = MakeHolder<TEvKesus::TEvDescribeProxiesResult>();
    for (const auto& kv : Proxies) {
        const auto* proxy = &kv.second;
        auto* proxyInfo = event->Record.AddProxies();
        ActorIdToProto(proxy->ActorID, proxyInfo->MutableActorID());
        proxyInfo->SetGeneration(proxy->Generation);
        for (ui64 sessionId : proxy->AttachedSessions) {
            proxyInfo->AddAttachedSessions(sessionId);
        }
    }
    Send(ev->Sender, event.Release(), 0, ev->Cookie);
}

void TKesusTablet::Handle(TEvKesus::TEvRegisterProxy::TPtr& ev) {
    Y_ABORT_UNLESS(ev->Sender);
    const auto& record = ev->Get()->Record;
    VerifyKesusPath(record.GetKesusPath());
    TabletCounters->Cumulative()[COUNTER_REQS_PROXY_REGISTER].Increment(1);

    if (record.GetProxyGeneration() <= 0) {
        Send(ev->Sender,
            new TEvKesus::TEvRegisterProxyResult(
                record.GetProxyGeneration(),
                Ydb::StatusIds::BAD_REQUEST,
                "ProxyGeneration must be greater than zero"),
            0, ev->Cookie);
        return;
    }

    auto* proxy = &Proxies[ev->Sender];
    if (record.GetProxyGeneration() <= proxy->Generation) {
        Send(ev->Sender,
            new TEvKesus::TEvRegisterProxyResult(
                record.GetProxyGeneration(),
                Ydb::StatusIds::BAD_REQUEST,
                "ProxyGeneration is expected to always increase"),
            0, ev->Cookie);
        return;
    }
    if (proxy->Generation == 0) {
        TabletCounters->Simple()[COUNTER_PROXY_COUNT].Add(1);
    }
    proxy->ActorID = ev->Sender;
    proxy->Generation = record.GetProxyGeneration();
    proxy->InterconnectSession = ev->InterconnectSession;
    // New proxy is always cleared when it registers
    ClearProxy(proxy, TActivationContext::AsActorContext());
    ProxiesByNode[ev->Sender.NodeId()].insert(proxy);

    // Send result using the same interconnect session the request was received from
    auto result = std::make_unique<IEventHandle>(ev->Sender, SelfId(),
        new TEvKesus::TEvRegisterProxyResult(record.GetProxyGeneration()),
        IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession,
        ev->Cookie);
    if (ev->InterconnectSession) {
        result->Rewrite(TEvInterconnect::EvForward, ev->InterconnectSession);
    }
    TActivationContext::Send(result.release());
}

void TKesusTablet::Handle(TEvKesus::TEvUnregisterProxy::TPtr& ev) {
    Y_ABORT_UNLESS(ev->Sender);
    const auto& record = ev->Get()->Record;
    VerifyKesusPath(record.GetKesusPath());
    TabletCounters->Cumulative()[COUNTER_REQS_PROXY_UNREGISTER].Increment(1);

    auto* proxy = Proxies.FindPtr(ev->Sender);
    if (!proxy || proxy->Generation != record.GetProxyGeneration()) {
        Send(ev->Sender,
            new TEvKesus::TEvUnregisterProxyResult(
                record.GetProxyGeneration(),
                Ydb::StatusIds::BAD_SESSION,
                proxy ? "ProxyGeneration mismatch" : "Proxy is not registered"),
            0, ev->Cookie);
        return;
    }

    ClearProxy(proxy, TActivationContext::AsActorContext());
    ForgetProxy(proxy);
    Send(ev->Sender,
        new TEvKesus::TEvUnregisterProxyResult(record.GetProxyGeneration()),
        0, ev->Cookie);
}

void TKesusTablet::HandleIgnored() {
    // event is ignored
}

STFUNC(TKesusTablet::StateInit) {
    StateInitImpl(ev, SelfId());
}

STFUNC(TKesusTablet::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvents::TEvUndelivered, Handle);
        hFunc(TEvInterconnect::TEvNodeConnected, Handle);
        hFunc(TEvInterconnect::TEvNodeDisconnected, Handle);

        hFunc(TEvKesus::TEvDummyRequest, Handle);

        hFunc(TEvKesus::TEvSetConfig, Handle);
        hFunc(TEvKesus::TEvGetConfig, Handle);

        hFunc(TEvKesus::TEvDescribeProxies, Handle);
        hFunc(TEvKesus::TEvRegisterProxy, Handle);
        hFunc(TEvKesus::TEvUnregisterProxy, Handle);

        hFunc(TEvKesus::TEvAttachSession, Handle);
        hFunc(TEvKesus::TEvDetachSession, Handle);
        hFunc(TEvKesus::TEvDestroySession, Handle);
        hFunc(TEvKesus::TEvDescribeSessions, Handle);
        hFunc(TEvPrivate::TEvSessionTimeout, Handle);

        hFunc(TEvKesus::TEvAcquireSemaphore, Handle);
        hFunc(TEvKesus::TEvCreateSemaphore, Handle);
        hFunc(TEvKesus::TEvDescribeSemaphore, Handle);
        hFunc(TEvKesus::TEvDeleteSemaphore, Handle);
        hFunc(TEvKesus::TEvReleaseSemaphore, Handle);
        hFunc(TEvKesus::TEvUpdateSemaphore, Handle);
        hFunc(TEvPrivate::TEvAcquireSemaphoreTimeout, Handle);

        cFunc(TEvKesus::Deprecated_EvCreateTask, HandleIgnored);
        cFunc(TEvKesus::Deprecated_EvUpdateTask, HandleIgnored);
        cFunc(TEvKesus::Deprecated_EvDeleteTask, HandleIgnored);
        cFunc(TEvKesus::Deprecated_EvClientReady, HandleIgnored);
        cFunc(TEvKesus::Deprecated_EvJobStatus, HandleIgnored);

        hFunc(TEvKesus::TEvDescribeQuoterResources, Handle);
        hFunc(TEvKesus::TEvAddQuoterResource, Handle);
        hFunc(TEvKesus::TEvUpdateQuoterResource, Handle);
        hFunc(TEvKesus::TEvDeleteQuoterResource, Handle);
        hFunc(TEvKesus::TEvSubscribeOnResources, Handle);
        hFunc(TEvKesus::TEvUpdateConsumptionState, Handle);
        hFunc(TEvKesus::TEvAccountResources, Handle);
        hFunc(TEvKesus::TEvReportResources, Handle);
        hFunc(TEvKesus::TEvResourcesAllocatedAck, Handle);
        hFunc(TEvKesus::TEvGetQuoterResourceCounters, Handle);
        hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
        hFunc(TEvents::TEvWakeup, Handle);

        hFunc(TEvPrivate::TEvSelfCheckStart, Handle);
        hFunc(TEvPrivate::TEvSelfCheckTimeout, Handle);

        IgnoreFunc(TEvTabletPipe::TEvServerConnected);
        IgnoreFunc(NKesus::TEvKesus::TEvSyncResourcesAck);

        default:
            if (!HandleDefaultEvents(ev, SelfId())) {
                LOG_WARN(*TActivationContext::ActorSystem(), NKikimrServices::KESUS_TABLET, "Unexpected event 0x%x", ev->GetTypeRewrite());
            }
            break;
    }
}

}
}
