#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/graph/protos/graph.pb.h>

namespace NKikimr {
namespace NGraph {

struct TEvGraph {
    enum EEv {
        // requests
        EvSendMetrics = EventSpaceBegin(TKikimrEvents::ES_GRAPH),
        EvGetMetrics,
        EvMetricsResult,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_GRAPH), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_GRAPH)");

    struct TEvSendMetrics : TEventPB<TEvSendMetrics, NKikimrGraph::TEvSendMetrics, EvSendMetrics> {
        TEvSendMetrics() = default;

        TEvSendMetrics(const TString& name, double value) {
            AddMetric(name, value);
        }

        void AddMetric(const TString& name, double value) {
            NKikimrGraph::TMetric* metric = Record.AddMetrics();
            metric->SetName(name);
            metric->SetValue(value);
        }
    };

    struct TEvGetMetrics : TEventPB<TEvGetMetrics, NKikimrGraph::TEvGetMetrics, EvGetMetrics> {
        TEvGetMetrics() = default;

        TEvGetMetrics(const NKikimrGraph::TEvGetMetrics& request)
            : TEventPB<TEvGetMetrics, NKikimrGraph::TEvGetMetrics, EvGetMetrics>(request)
        {}
    };

    struct TEvMetricsResult : TEventPB<TEvMetricsResult, NKikimrGraph::TEvMetricsResult, EvMetricsResult> {
        TEvMetricsResult() = default;

        TEvMetricsResult(NKikimrGraph::TEvMetricsResult&& result)
            : TEventPB<TEvMetricsResult, NKikimrGraph::TEvMetricsResult, EvMetricsResult>(std::move(result))
        {}
    };
};

} // NGraph
} // NKikimr
