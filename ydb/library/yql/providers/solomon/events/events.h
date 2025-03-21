#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/yql/providers/solomon/proto/metrics_queue.pb.h>
#include <ydb/library/yql/providers/solomon/solomon_accessor/client/solomon_accessor_client.h>

namespace NYql::NDq {

struct TEvSolomonProvider {

    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NKikimr::TKikimrEvents::ES_SOLOMON_PROVIDER),

        // lister events
        EvGetNextBatch = EvBegin,
        EvMetricsBatch,
        EvMetricsReadError,

        // read actor events
        EvNewDataBatch,

        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_SOLOMON_PROVIDER), "expect EvEnd < EventSpaceEnd(TEvents::ES_S3_PROVIDER)");

    struct TEvGetNextBatch :
        public NActors::TEventPB<TEvGetNextBatch, NSo::MetricQueue::TEvGetNextBatch, EvGetNextBatch> {
    };

    struct TEvMetricsBatch :
        public NActors::TEventPB<TEvMetricsBatch, NSo::MetricQueue::TEvMetricsBatch, EvMetricsBatch> {

        TEvMetricsBatch() = default;
        TEvMetricsBatch(std::vector<NSo::MetricQueue::TMetricLabels> metrics, bool noMoreMetrics, const NDqProto::TMessageTransportMeta& transportMeta) {
            Record.MutableMetrics()->Assign(
                metrics.begin(), 
                metrics.end());
            Record.SetNoMoreMetrics(noMoreMetrics);
            *Record.MutableTransportMeta() = transportMeta;
        }
    };

    struct TEvMetricsReadError:
        public NActors::TEventPB<TEvMetricsReadError, NSo::MetricQueue::TEvMetricsReadError, EvMetricsReadError> {
        
        TEvMetricsReadError() = default;
        TEvMetricsReadError(const TString& issues, const NDqProto::TMessageTransportMeta& transportMeta) {
            Record.SetIssues(issues);
            *Record.MutableTransportMeta() = transportMeta;
        }
    };

    struct TEvNewDataBatch: public NActors::TEventLocal<TEvNewDataBatch, EvNewDataBatch> {
        ui64 SelectorsCount;
        NSo::ISolomonAccessorClient::TGetDataResult Result;
        TEvNewDataBatch(ui64 selectorsCount, NSo::ISolomonAccessorClient::TGetDataResult result)
            : SelectorsCount(selectorsCount)
            , Result(std::move(result))
        {}
    };
};

} // namespace NYql::NDq
