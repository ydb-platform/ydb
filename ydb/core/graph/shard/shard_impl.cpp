#include "shard_impl.h"
#include "log.h"
#include <ydb/core/graph/api/events.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/base/tablet_pipe.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GRAPH

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
    YDB_LOG_DEBUG("OnActivateExecutor",
        {"LogPrefix", GetLogPrefix()});
    ExecuteTxInitSchema();
}

void TGraphShard::OnTabletDead(TEvTablet::TEvTabletDead::TPtr&, const TActorContext&) {
    YDB_LOG_DEBUG("OnTabletDead",
        {"LogPrefix", GetLogPrefix()});
    PassAway();
}

void TGraphShard::OnDetach(const TActorContext&) {
    YDB_LOG_DEBUG("OnDetach",
        {"LogPrefix", GetLogPrefix()});
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
    ApplyConfig(AppData()->GraphConfig);
    SignalTabletActive(ActorContext());
}

void TGraphShard::ApplyConfig(const NKikimrConfig::TGraphConfig& config) {
    YDB_LOG_DEBUG("Updated Config to",
        {"LogPrefix", GetLogPrefix()},
        {"Config", config.ShortDebugString()});
    if (config.HasBackendType()) {
        YDB_LOG_DEBUG("Updated BackendType",
            {"LogPrefix", GetLogPrefix()});
        if (config.GetBackendType() == "Memory") {
            BackendType = EBackendType::Memory;
        }
        if (config.GetBackendType() == "Local") {
            BackendType = EBackendType::Local;
        }
        if (config.GetBackendType() == "External") {
            BackendType = EBackendType::External;
        }
    }
    if (config.HasAggregateCheckPeriodSeconds()) {
        YDB_LOG_DEBUG("Updated AggregateCheckPeriod",
            {"LogPrefix", GetLogPrefix()});
        AggregateCheckPeriod = TDuration::Seconds(config.GetAggregateCheckPeriodSeconds());
    }
    if (config.AggregationSettingsSize() != 0) {
        YDB_LOG_DEBUG("Updated AggregationSettings",
            {"LogPrefix", GetLogPrefix()});
        AggregateSettings.clear();
        for (const auto& protoSettings : config.GetAggregationSettings()) {
            TAggregateSettings& settings = AggregateSettings.emplace_back();
            if (protoSettings.HasPeriodToStartSeconds()) {
                settings.PeriodToStart = TDuration::Seconds(protoSettings.GetPeriodToStartSeconds());
            }
            if (protoSettings.HasSampleSizeSeconds()) {
                settings.SampleSize = TDuration::Seconds(protoSettings.GetSampleSizeSeconds());
            }
            if (protoSettings.HasMinimumStepSeconds()) {
                settings.MinimumStep = TDuration::Seconds(protoSettings.GetMinimumStepSeconds());
            }
        }

        if (AggregateSettings.empty()) {
            YDB_LOG_WARN("Settings are empty - fail-safe settings applied",
                {"LogPrefix", GetLogPrefix()});
            AggregateSettings.emplace_back().PeriodToStart = TDuration::Days(7);
        }
    }
}

void TGraphShard::MergeHistogram(TMetricsData& data, const NKikimrGraph::THistogramMetric& src) {
    std::map<ui64, ui64>& dest(data.HistogramValues[src.GetName()]);
    size_t size(std::min(src.HistogramBoundsSize(), src.HistogramValuesSize()));
    for (size_t n = 0; n < size; ++n) {
        dest[src.GetHistogramBounds(n)] += src.GetHistogramValues(n);
    }
}

void TGraphShard::MergeArithmetic(TMetricsData& data, const NKikimrGraph::TArithmeticMetric& src) {
    auto& dest(data.ArithmeticValues[src.GetName()]);
    switch (src.GetOp()) {
        case NKikimrGraph::TArithmeticMetric::EOP_DIVISION:
            dest.Op = '/';
            dest.ValueA += src.GetValueA();
            dest.ValueB += src.GetValueB();
            break;
        default:
            break;
    }
}

void TGraphShard::MergeMetrics(TMetricsData& data, const NKikimrGraph::TEvSendMetrics& src) {
    for (const auto& metric : src.GetMetrics()) {
        data.Values[metric.GetName()] += metric.GetValue(); // simple accumulation by name of metric
    }
    for (const auto& metric : src.GetHistogramMetrics()) {
        MergeHistogram(data, metric);
    }
    for (const auto& metric : src.GetArithmeticMetrics()) {
        MergeArithmetic(data, metric);
    }
}

void TGraphShard::AggregateMetrics(TMetricsData& data) {
    for (const auto& [name, hist] : data.HistogramValues) {
        AggregateHistogram(data.Values, name, hist);
    }
    data.HistogramValues.clear();
    for (const auto& [name, arithm] : data.ArithmeticValues) {
        switch (arithm.Op) {
            case '/':
                data.Values[name] = arithm.ValueA / arithm.ValueB;
                break;
            default:
                break;
        }
    }
    data.ArithmeticValues.clear();
}

void TGraphShard::AggregateHistogram(std::unordered_map<TString, double>& values, const TString& name, const std::map<ui64, ui64>& histogram) {
    TVector<ui64> histBoundaries;
    TVector<ui64> histValues;
    ui64 total = 0;

    histBoundaries.reserve(histogram.size());
    histValues.reserve(histogram.size());
    for (auto [boundary, value] : histogram) {
        histBoundaries.push_back(boundary);
        histValues.push_back(value);
        total += value;
    }
    // it's hardcoded for now, will be changed to aggregate function later
    values[name + ".p50"] = TBaseBackend::GetTimingForPercentile(.50, histValues, histBoundaries, total);
    values[name + ".p75"] = TBaseBackend::GetTimingForPercentile(.75, histValues, histBoundaries, total);
    values[name + ".p90"] = TBaseBackend::GetTimingForPercentile(.90, histValues, histBoundaries, total);
    values[name + ".p99"] = TBaseBackend::GetTimingForPercentile(.99, histValues, histBoundaries, total);
}

STFUNC(TGraphShard::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvSubDomain::TEvConfigure, Handle);
        hFunc(TEvTabletPipe::TEvServerConnected, Handle);
        hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
        hFunc(TEvGraph::TEvSendMetrics, Handle);
        hFunc(TEvGraph::TEvGetMetrics, Handle);
        hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, Handle);
    default:
        if (!HandleDefaultEvents(ev, SelfId())) {
            YDB_LOG_WARN("StateWork unhandled event",
                {"LogPrefix", GetLogPrefix()},
                {"Type", ev->GetTypeRewrite()},
                {"Event", ev->ToString()});
        }
        break;
    }
}

void TGraphShard::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev) {
    YDB_LOG_TRACE("Handle TEvTabletPipe::TEvServerConnected(",
        {"LogPrefix", GetLogPrefix()},
        {"ClientId", ev->Get()->ClientId},
        {"ServerId", ev->Get()->ServerId});
}

void TGraphShard::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev) {
    YDB_LOG_TRACE("Handle TEvTabletPipe::TEvServerDisconnected(",
        {"LogPrefix", GetLogPrefix()},
        {"ClientId", ev->Get()->ClientId},
        {"ServerId", ev->Get()->ServerId});
}

void TGraphShard::Handle(TEvSubDomain::TEvConfigure::TPtr& ev) {
    YDB_LOG_DEBUG("Handle TEvSubDomain::TEvConfigure(",
        {"LogPrefix", GetLogPrefix()},
        {"ev", ev->Get()->Record.ShortDebugString()});
    Send(ev->Sender, new TEvSubDomain::TEvConfigureStatus(NKikimrTx::TEvSubDomainConfigurationAck::SUCCESS, TabletID()));
}

void TGraphShard::Handle(TEvGraph::TEvSendMetrics::TPtr& ev) {
    YDB_LOG_TRACE("",
        {"LogPrefix", GetLogPrefix()},
        {"ev", ev->Get()->Record.ShortDebugString()});
    if (ev->Get()->Record.HasTime()) { // direct insertion
        TMetricsData data;
        data.Timestamp = TInstant::Seconds(ev->Get()->Record.GetTime());
        MergeMetrics(data, ev->Get()->Record);
        YDB_LOG_TRACE("Executing direct TxStoreMetrics",
            {"LogPrefix", GetLogPrefix()});
        ExecuteTxStoreMetrics(std::move(data));
        return;
    }
    TInstant now = TInstant::Seconds(TActivationContext::Now().Seconds()); // 1 second resolution
    YDB_LOG_TRACE("Handle TEvGraph::TEvSendMetrics from now is md.timestamp is",
        {"LogPrefix", GetLogPrefix()},
        {"Sender", ev->Sender},
        {"Now", now},
        {"Timestamp", MetricsData.Timestamp});
    if (now != MetricsData.Timestamp) {
        if (MetricsData.Timestamp != TInstant()) {
            YDB_LOG_TRACE("Executing TxStoreMetrics",
                {"LogPrefix", GetLogPrefix()});
            ExecuteTxStoreMetrics(std::move(MetricsData));
        }
        YDB_LOG_TRACE("Updating md.timestamp to",
            {"LogPrefix", GetLogPrefix()},
            {"Now", now});
        MetricsData.Timestamp = now;
        MetricsData.Values.clear();
    }
    if ((now - AggregateTimestamp) >= AggregateCheckPeriod) {
        AggregateTimestamp = now;
        for (const auto& settings : AggregateSettings) {
            if (settings.IsItTimeToAggregate(now)) {
                YDB_LOG_TRACE("Executing TxAggregateData for",
                    {"LogPrefix", GetLogPrefix()},
                    {"Settings", settings.ToString()});
                ExecuteTxAggregateData(settings);
            }
        }
    }

    MergeMetrics(MetricsData, ev->Get()->Record);
}

void TGraphShard::Handle(TEvGraph::TEvGetMetrics::TPtr& ev) {
    YDB_LOG_TRACE("Handle TEvGraph::TEvGetMetrics from",
        {"LogPrefix", GetLogPrefix()},
        {"Sender", ev->Sender});
    ExecuteTxGetMetrics(ev);
}

void TGraphShard::Handle(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
    const NKikimrConsole::TConfigNotificationRequest& record = ev->Get()->Record;
    YDB_LOG_DEBUG("Received TEvConsole::TEvConfigNotificationRequest with update of",
        {"LogPrefix", GetLogPrefix()},
        {"Config", record.GetConfig().GetGraphConfig().ShortDebugString()});
    ApplyConfig(record.GetConfig().GetGraphConfig());
    Send(ev->Sender, new NConsole::TEvConsole::TEvConfigNotificationResponse(record), 0, ev->Cookie);
}

IActor* CreateGraphShard(const TActorId& tablet, TTabletStorageInfo* info) {
    return new NGraph::TGraphShard(info, tablet);
}

} // NGraph
} // NKikimr
