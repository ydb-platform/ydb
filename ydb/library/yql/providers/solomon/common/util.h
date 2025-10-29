#pragma once

#include <util/datetime/base.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/solomon/proto/dq_solomon_shard.pb.h>

namespace NYql::NSo {

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
