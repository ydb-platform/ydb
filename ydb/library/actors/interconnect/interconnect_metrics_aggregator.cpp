#include "interconnect_metrics_aggregator.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/monlib/metrics/metric_sub_registry.h>

namespace NActors::NInterconnectMetricsAggregator {

namespace {

class TInterconnectMetricsAggregatorActor
    : public TActorBootstrapped<TInterconnectMetricsAggregatorActor>
{
    struct TPeerState {
        ui32 Connected = 0;
        i64 ClockSkew = 0;
        ui32 RdmaRetryWatchdogPending = 0;
    };

    struct TLabelState {
        THashMap<TString, TPeerState> Peers;
        NMonitoring::TDynamicCounters::TCounterPtr ConnectedCounter;
        NMonitoring::TDynamicCounters::TCounterPtr ClockSkewCounter;
        NMonitoring::TDynamicCounters::TCounterPtr RdmaRetryWatchdogPendingCounter;
        NMonitoring::IIntGauge* ConnectedGauge = nullptr;
        NMonitoring::IIntGauge* ClockSkewGauge = nullptr;
        NMonitoring::IIntGauge* RdmaRetryWatchdogPendingGauge = nullptr;
    };

public:
    explicit TInterconnectMetricsAggregatorActor(TInterconnectProxyCommon::TPtr common)
        : Common_(std::move(common))
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvRegisterPeer, Handle);
        hFunc(TEvUpdateConnected, Handle);
        hFunc(TEvUpdateClockSkew, Handle);
        hFunc(TEvUpdateRdmaRetryWatchdogPending, Handle);
        hFunc(TEvUnregisterPeer, Handle);
    )

private:
    static ui64 Abs(i64 x) {
        return x >= 0 ? x : ui64(-(x + 1)) + 1;
    }

    TLabelState& GetLabelState(const TString& peerLabel) {
        auto [it, _] = Labels_.try_emplace(peerLabel);
        TLabelState& label = it->second;
        if (!label.ConnectedCounter && !label.ConnectedGauge) {
            if (Common_->Metrics) {
                const auto registry = std::make_shared<NMonitoring::TMetricSubRegistry>(
                    NMonitoring::TLabels{{"peer", peerLabel}}, Common_->Metrics);
                label.ConnectedGauge = registry->IntGauge(
                    NMonitoring::MakeLabels(NMonitoring::TLabels{{"sensor", "interconnect.connected"}}));
                label.ClockSkewGauge = registry->IntGauge(
                    NMonitoring::MakeLabels(NMonitoring::TLabels{{"sensor", "interconnect.clock_skew_microsec"}}));
                label.RdmaRetryWatchdogPendingGauge = registry->IntGauge(NMonitoring::MakeLabels(
                    NMonitoring::TLabels{{"sensor", "interconnect.rdma_retry_watchdog_pending_sessions"}}));
            } else {
                const auto subgroup = Common_->MonCounters->GetSubgroup("peer", peerLabel);
                label.ConnectedCounter = subgroup->GetCounter("Connected");
                label.ClockSkewCounter = subgroup->GetCounter("ClockSkewMicrosec");
                label.RdmaRetryWatchdogPendingCounter = subgroup->GetCounter("RdmaRetryWatchdogPendingSessions");
            }
        }
        return label;
    }

    void Publish(TLabelState& label, ui32 connected, i64 clockSkew, ui32 rdmaRetryWatchdogPending) {
        if (label.ConnectedGauge) {
            label.ConnectedGauge->Set(connected);
            label.ClockSkewGauge->Set(clockSkew);
            label.RdmaRetryWatchdogPendingGauge->Set(rdmaRetryWatchdogPending);
        } else {
            *label.ConnectedCounter = connected;
            *label.ClockSkewCounter = clockSkew;
            *label.RdmaRetryWatchdogPendingCounter = rdmaRetryWatchdogPending;
        }
    }

    void RecalculateAndPublish(TLabelState& label) {
        ui32 connected = 0;
        i64 clockSkew = 0;
        ui64 maxAbsClockSkew = 0;
        ui32 rdmaRetryWatchdogPending = 0;

        for (const auto& [_, peer] : label.Peers) {
            connected += peer.Connected;
            rdmaRetryWatchdogPending += peer.RdmaRetryWatchdogPending;
            const ui64 absClockSkew = Abs(peer.ClockSkew);
            if (absClockSkew > maxAbsClockSkew) {
                maxAbsClockSkew = absClockSkew;
                clockSkew = peer.ClockSkew;
            }
        }

        Publish(label, connected, clockSkew, rdmaRetryWatchdogPending);
    }

    void Handle(TEvRegisterPeer::TPtr& ev) {
        auto& label = GetLabelState(ev->Get()->PeerLabel);
        label.Peers.try_emplace(ev->Get()->PeerName);
        RecalculateAndPublish(label);
    }

    void Handle(TEvUpdateConnected::TPtr& ev) {
        auto& label = GetLabelState(ev->Get()->PeerLabel);
        auto& peer = label.Peers[ev->Get()->PeerName];
        peer.Connected = ev->Get()->Connected;
        RecalculateAndPublish(label);
    }

    void Handle(TEvUpdateClockSkew::TPtr& ev) {
        auto& label = GetLabelState(ev->Get()->PeerLabel);
        auto& peer = label.Peers[ev->Get()->PeerName];
        peer.ClockSkew = ev->Get()->ClockSkew;
        RecalculateAndPublish(label);
    }

    void Handle(TEvUpdateRdmaRetryWatchdogPending::TPtr& ev) {
        auto& label = GetLabelState(ev->Get()->PeerLabel);
        auto& peer = label.Peers[ev->Get()->PeerName];
        peer.RdmaRetryWatchdogPending = ev->Get()->Pending;
        RecalculateAndPublish(label);
    }

    void Handle(TEvUnregisterPeer::TPtr& ev) {
        if (auto it = Labels_.find(ev->Get()->PeerLabel); it != Labels_.end()) {
            auto& label = it->second;
            label.Peers.erase(ev->Get()->PeerName);
            if (label.Peers.empty()) {
                Publish(label, 0, 0, 0);
                Labels_.erase(it);
            } else {
                RecalculateAndPublish(label);
            }
        }
    }

private:
    TInterconnectProxyCommon::TPtr Common_;
    THashMap<TString, TLabelState> Labels_;
};

} // namespace

IActor* CreateInterconnectMetricsAggregatorActor(TIntrusivePtr<TInterconnectProxyCommon> common) {
    return new TInterconnectMetricsAggregatorActor(std::move(common));
}

} // namespace NActors::NInterconnectMetricsAggregator
