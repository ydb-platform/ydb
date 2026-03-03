#include "interconnect_host_metrics_aggregator.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/monlib/metrics/metric_sub_registry.h>

namespace NActors::NInterconnectHostMetrics {

namespace {

class TInterconnectHostMetricsAggregatorActor
    : public TActorBootstrapped<TInterconnectHostMetricsAggregatorActor>
{
    struct TPeerState {
        ui32 Connected = 0;
        i64 ClockSkew = 0;
    };

    struct THostState {
        THashMap<TString, TPeerState> Peers;
        NMonitoring::TDynamicCounters::TCounterPtr ConnectedCounter;
        NMonitoring::TDynamicCounters::TCounterPtr ClockSkewCounter;
        NMonitoring::IIntGauge* ConnectedGauge = nullptr;
        NMonitoring::IIntGauge* ClockSkewGauge = nullptr;
    };

public:
    explicit TInterconnectHostMetricsAggregatorActor(TInterconnectProxyCommon::TPtr common)
        : Common_(std::move(common))
    {}

    void Bootstrap() {
        Become(&TThis::StateFunc);
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvRegisterPeer, Handle);
        hFunc(TEvUpdateConnected, Handle);
        hFunc(TEvUpdateClockSkew, Handle);
        hFunc(TEvUnregisterPeer, Handle);
    )

private:
    static ui64 Abs(i64 x) {
        return x >= 0 ? x : ui64(-(x + 1)) + 1;
    }

    THostState& GetHostState(const TString& peerHost) {
        auto [it, _] = Hosts_.try_emplace(peerHost);
        THostState& host = it->second;
        if (!host.ConnectedCounter && !host.ConnectedGauge) {
            if (Common_->Metrics) {
                const auto registry = std::make_shared<NMonitoring::TMetricSubRegistry>(
                    NMonitoring::TLabels{{"peer", peerHost}}, Common_->Metrics);
                host.ConnectedGauge = registry->IntGauge(
                    NMonitoring::MakeLabels(NMonitoring::TLabels{{"sensor", "interconnect.connected"}}));
                host.ClockSkewGauge = registry->IntGauge(
                    NMonitoring::MakeLabels(NMonitoring::TLabels{{"sensor", "interconnect.clock_skew_microsec"}}));
            } else {
                const auto subgroup = Common_->MonCounters->GetSubgroup("peer", peerHost);
                host.ConnectedCounter = subgroup->GetCounter("Connected");
                host.ClockSkewCounter = subgroup->GetCounter("ClockSkewMicrosec");
            }
        }
        return host;
    }

    void Publish(THostState& host, ui32 connected, i64 clockSkew) {
        if (host.ConnectedGauge) {
            host.ConnectedGauge->Set(connected);
            host.ClockSkewGauge->Set(clockSkew);
        } else {
            *host.ConnectedCounter = connected;
            *host.ClockSkewCounter = clockSkew;
        }
    }

    void RecalculateAndPublish(THostState& host) {
        ui32 connected = 0;
        i64 clockSkew = 0;
        ui64 maxAbsClockSkew = 0;

        for (const auto& [_, peer] : host.Peers) {
            connected += peer.Connected;
            const ui64 absClockSkew = Abs(peer.ClockSkew);
            if (absClockSkew > maxAbsClockSkew) {
                maxAbsClockSkew = absClockSkew;
                clockSkew = peer.ClockSkew;
            }
        }

        Publish(host, connected, clockSkew);
    }

    void Handle(TEvRegisterPeer::TPtr& ev) {
        auto& host = GetHostState(ev->Get()->PeerHost);
        host.Peers.try_emplace(ev->Get()->PeerHumanName);
        RecalculateAndPublish(host);
    }

    void Handle(TEvUpdateConnected::TPtr& ev) {
        auto& host = GetHostState(ev->Get()->PeerHost);
        auto& peer = host.Peers[ev->Get()->PeerHumanName];
        peer.Connected = ev->Get()->Connected;
        RecalculateAndPublish(host);
    }

    void Handle(TEvUpdateClockSkew::TPtr& ev) {
        auto& host = GetHostState(ev->Get()->PeerHost);
        auto& peer = host.Peers[ev->Get()->PeerHumanName];
        peer.ClockSkew = ev->Get()->ClockSkew;
        RecalculateAndPublish(host);
    }

    void Handle(TEvUnregisterPeer::TPtr& ev) {
        if (auto it = Hosts_.find(ev->Get()->PeerHost); it != Hosts_.end()) {
            auto& host = it->second;
            host.Peers.erase(ev->Get()->PeerHumanName);
            if (host.Peers.empty()) {
                Publish(host, 0, 0);
                Hosts_.erase(it);
            } else {
                RecalculateAndPublish(host);
            }
        }
    }

private:
    TInterconnectProxyCommon::TPtr Common_;
    THashMap<TString, THostState> Hosts_;
};

} // namespace

IActor* CreateInterconnectHostMetricsAggregatorActor(TIntrusivePtr<TInterconnectProxyCommon> common) {
    return new TInterconnectHostMetricsAggregatorActor(std::move(common));
}

} // namespace NActors::NInterconnectHostMetrics
