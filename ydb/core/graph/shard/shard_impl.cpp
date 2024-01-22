#include "shard_impl.h"
#include "log.h"
#include <ydb/core/graph/api/events.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/base/tablet_pipe.h>

namespace NKikimr {
namespace NGraph {

TGraphShard::TGraphShard(TTabletStorageInfo* info, const TActorId& tablet)
    : TActor(&TThis::StateWork)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
{

}

TString TGraphShard::GetLogPrefix() const {
    return "SHARD ";
}

void TGraphShard::OnActivateExecutor(const TActorContext&) {
    BLOG_D("OnActivateExecutor");
    ExecuteTxInitSchema();
}

void TGraphShard::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext&) {
    BLOG_D("OnTabletDead");
    PassAway();
}

void TGraphShard::OnDetach(const TActorContext&) {
    BLOG_D("OnDetach");
    PassAway();
}

bool TGraphShard::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext&) {
    if (!Executor() || !Executor()->GetStats().IsActive)
        return false;

    if (!ev)
        return true;

    ExecuteTxMonitoring(std::move(ev));
    return true;
}

void TGraphShard::OnReadyToWork() {
    SignalTabletActive(ActorContext());
}

STFUNC(TGraphShard::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvSubDomain::TEvConfigure, Handle);
        hFunc(TEvTabletPipe::TEvServerConnected, Handle);
        hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
        hFunc(TEvGraph::TEvSendMetrics, Handle);
        hFunc(TEvGraph::TEvGetMetrics, Handle);
    default:
        if (!HandleDefaultEvents(ev, SelfId())) {
            BLOG_W("StateWork unhandled event type: " << ev->GetTypeRewrite() << " event: " << ev->ToString());
        }
        break;
    }
}

void TGraphShard::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev) {
    BLOG_TRACE("Handle TEvTabletPipe::TEvServerConnected(" << ev->Get()->ClientId << ") " << ev->Get()->ServerId);
}

void TGraphShard::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev) {
    BLOG_TRACE("Handle TEvTabletPipe::TEvServerDisconnected(" << ev->Get()->ClientId << ") " << ev->Get()->ServerId);
}

void TGraphShard::Handle(TEvSubDomain::TEvConfigure::TPtr& ev) {
    BLOG_D("Handle TEvSubDomain::TEvConfigure(" << ev->Get()->Record.ShortDebugString() << ")");
    Send(ev->Sender, new TEvSubDomain::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::SUCCESS, TabletID()));
}

void TGraphShard::Handle(TEvGraph::TEvSendMetrics::TPtr& ev) {
    TInstant now = TInstant::Seconds(TActivationContext::Now().Seconds()); // 1 second resolution
    BLOG_TRACE("Handle TEvGraph::TEvSendMetrics from " << ev->Sender << " now is " << now << " md.timestamp is " << MetricsData.Timestamp);
    if (StartTimestamp == TInstant()) {
        StartTimestamp = now;
    }
    if (now != MetricsData.Timestamp) {
        if (MetricsData.Timestamp != TInstant()) {
            BLOG_TRACE("Executing TxStoreMetrics");
            ExecuteTxStoreMetrics(std::move(MetricsData));
        }
        BLOG_TRACE("Updating md.timestamp to " << now);
        MetricsData.Timestamp = now;
        MetricsData.Values.clear();
    }
    if ((now - StartTimestamp) > DURATION_CLEAR_TRIGGER && (now - ClearTimestamp) > DURATION_CLEAR_PERIOD) {
        ClearTimestamp = now;
        BLOG_TRACE("Executing TxClearData");
        ExecuteTxClearData();
    }
    for (const auto& metric : ev->Get()->Record.GetMetrics()) {
        MetricsData.Values[metric.GetName()] += metric.GetValue(); // simple accumulation by name of metric
    }
}

void TGraphShard::Handle(TEvGraph::TEvGetMetrics::TPtr& ev) {
    BLOG_TRACE("Handle TEvGraph::TEvGetMetrics from " << ev->Sender);
    ExecuteTxGetMetrics(ev);
}

IActor* CreateGraphShard(const TActorId& tablet, TTabletStorageInfo* info) {
    return new NGraph::TGraphShard(info, tablet);
}

} // NGraph
} // NKikimr
