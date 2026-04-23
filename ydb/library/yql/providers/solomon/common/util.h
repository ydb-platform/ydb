#pragma once

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>

namespace NYql::NSo {

// ---------------------------------------------------------------------------
// TSolomonReadActorConfig — single source of truth for all tunable parameters
// of the Solomon read pipeline (read_actor, metrics_queue, accessor_client).
// ---------------------------------------------------------------------------

struct TSolomonRetryConfig {
    TDuration MinDelay          = TDuration::MilliSeconds(50);
    TDuration MinLongRetryDelay = TDuration::MilliSeconds(200);
    TDuration MaxDelay          = TDuration::MilliSeconds(1000);
    size_t    MaxRetries        = 10;
    TDuration MaxTime           = TDuration::Seconds(30);
};

struct TSolomonReadActorConfig {
    // ── shared by read_actor + metrics_queue + accessor_client ──────────────
    // Maximum number of concurrent in-flight API requests. Must be >= 1.
    ui64 MaxApiInflight         = 40;
    // Maximum number of metrics returned per listing page. Must be >= 1.
    ui64 MaxListingPageSize     = 20000;
    // Use POST-based API instead of GET for listing requests.
    bool EnablePostApi          = true;

    // ── read_actor only ─────────────────────────────────────────────────────
    // Number of timeseries batched per notification to the compute actor. Must be >= 1.
    ui64 ComputeActorBatchSize  = 100;
    // Maximum total bytes of in-flight data responses before backpressure. Must be >= 1_MB.
    ui64 MaxDataInflightBytes   = 50_MB;
    // Seconds added/subtracted around [from, to] when searching for true data points.
    ui64 TruePointsFindRangeSec = 301;
    // Maximum number of data points fetched in a single GetData gRPC request.
    // Time ranges are split so that each sub-range contains at most this many points.
    // Must be >= 1.
    ui64 MaxPointsPerOneRequest = 10'000;

    // ── metrics_queue only ──────────────────────────────────────────────────
    // Number of metrics sent per batch to a consumer. Must be >= 1.
    ui64 MetricsQueueBatchCountLimit = 100;
    // Number of metrics to prefetch ahead of consumer demand. Must be >= 1.
    ui64 MetricsQueuePrefetchSize    = 1000;
    // Timeout before the MetricsQueue actor self-destructs if no consumer
    // connects after bootstrap (safety net for node-failure during startup).
    TDuration PoisonTimeout          = TDuration::Hours(3);
    // Time window given to all consumers to register before the round-robin
    // distribution phase is released and metrics are sent freely.
    TDuration RoundRobinStageTimeout = TDuration::Seconds(3);

    // ── accessor_client only ─────────────────────────────────────────────────
    // Maximum number of label values returned per labels-listing API call.
    // Configurable to allow reducing the page size for clusters with many labels.
    // Must be >= 1.
    ui64 LabelsListingLimit = 100'000;

    // ── retry policy ────────────────────────────────────────────────────────
    TSolomonRetryConfig RetryConfig;
};

// Parse and validate a TSolomonReadActorConfig from the WITH(...) settings map
// embedded in TDqSolomonSource.Settings. Invalid values are clamped to the
// minimum safe value. Key names are backward-compatible with the existing
// WITH(...) keys:
//   maxApiInflight, maxListingPageSize, enableSolomonClientPostApi,
//   computeActorBatchSize, maxDataInflightBytes, truePointsFindRange,
//   metricsQueueBatchCountLimit, metricsQueuePrefetchSize,
//   retryMinDelayMs, retryMinLongRetryDelayMs, retryMaxDelayMs, retryMaxRetries
TSolomonReadActorConfig ParseSolomonReadActorConfig(
    const google::protobuf::Map<TString, TString>& settings);


struct TSelector {
    TString Op;
    TString Value;

    bool operator==(const TSelector& other) const;
    bool operator<(const TSelector& other) const;
};

using TSelectors = std::map<TString, TSelector>;

void SelectorsToProto(const TSelectors& selectors, NYql::NSo::MetricQueue::TSelectors& proto);
void ProtoToSelectors(const NYql::NSo::MetricQueue::TSelectors& proto, TSelectors& selectors);

struct TMetric {
    TSelectors Selectors;
    TString Type;
};

struct TTimeseries {
    TMetric Metric;
    std::vector<int64_t> Timestamps;
    std::vector<double> Values;
};

struct TLabelValues {
    TString Name;
    bool Absent;
    bool Truncated;
    std::vector<TString> Values;
};

struct TMetricTimeRange {
    TSelectors Selectors;
    TString Program;
    TInstant From;
    TInstant To;

    bool operator<(const TMetricTimeRange& other) const;
};

TMaybe<TString> ParseSelectorValues(const TString& selectors, TSelectors& result);
TMaybe<TString> BuildSelectorValues(const NSo::NProto::TDqSolomonSource& source, const TString& selectors, TSelectors& result);

TMaybe<TString> ParseLabelNames(const TString& labelNames, TVector<TString>& names, TVector<TString>& aliases);
    
NSo::NProto::ESolomonClusterType MapClusterType(TSolomonClusterConfig::ESolomonClusterType clusterType);

NProto::TDqSolomonSource FillSolomonSource(const TSolomonClusterConfig* config, const TString& project);

} // namespace NYql::NSo
