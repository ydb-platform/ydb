#pragma once

#include <ydb/core/base/events.h>
#include <ydb/library/yql/providers/solomon/proto/metrics_queue.pb.h>
#include <ydb/library/yql/providers/solomon/solomon_accessor/client/solomon_accessor_client.h>

namespace NYql::NDq {

struct TEvSolomonProvider {

    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NKikimr::TKikimrEvents::ES_SOLOMON_PROVIDER),

        // lister events
        EvUpdateConsumersCount = EvBegin,
        EvAck,
        EvGetNextBatch,
        EvMetricsBatch,
        EvMetricsReadError,

        // read actor events
        EvPointsCountBatch,
        EvNewDataBatch,

        EvEnd
    };
    static_assert(EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_SOLOMON_PROVIDER), "expect EvEnd < EventSpaceEnd(NKikimr::TKikimrEvents::ES_SOLOMON_PROVIDER)");

    struct TEvUpdateConsumersCount :
        public NActors::TEventPB<TEvUpdateConsumersCount, NSo::MetricQueue::TEvUpdateConsumersCount, EvUpdateConsumersCount> {
        
        explicit TEvUpdateConsumersCount(ui64 consumersCountDelta = 0) {
            Record.SetConsumersCountDelta(consumersCountDelta);
        }
    };

    struct TEvAck :
        public NActors::TEventPB<TEvAck, NSo::MetricQueue::TEvAck, EvAck> {
        
        TEvAck() = default;
        explicit TEvAck(const NDqProto::TMessageTransportMeta& transportMeta) {
            *Record.MutableTransportMeta() = transportMeta;
        }
    };

    struct TEvGetNextBatch :
        public NActors::TEventPB<TEvGetNextBatch, NSo::MetricQueue::TEvGetNextBatch, EvGetNextBatch> {
    };

    struct TEvMetricsBatch :
        public NActors::TEventPB<TEvMetricsBatch, NSo::MetricQueue::TEvMetricsBatch, EvMetricsBatch> {

        TEvMetricsBatch() = default;
        TEvMetricsBatch(std::vector<NSo::MetricQueue::TMetric> metrics, bool noMoreMetrics, const NDqProto::TMessageTransportMeta& transportMeta) {
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

    struct TEvPointsCountBatch : public NActors::TEventLocal<TEvPointsCountBatch, EvPointsCountBatch> {
        std::vector<NSo::TMetric> Metrics;
        NSo::TGetPointsCountResponse Response;
        TEvPointsCountBatch(std::vector<NSo::TMetric>&& metrics, const NSo::TGetPointsCountResponse& response)
            : Metrics(std::move(metrics))
            , Response(response)
        {}
    };
    
    struct TEvNewDataBatch: public NActors::TEventLocal<TEvNewDataBatch, EvNewDataBatch> {
        NSo::TMetric Metric;
        TInstant From, To;
        NSo::TGetDataResponse Response;
        TEvNewDataBatch(NSo::TMetric metric, TInstant from, TInstant to, const NSo::TGetDataResponse& response)
            : Metric(metric)
            , From(from)
            , To(to)
            , Response(response)
        {}
    };
};

} // namespace NYql::NDq
