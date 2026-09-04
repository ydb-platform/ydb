#pragma once

#include "query_history.h"

#include <ydb/core/sys_view/common/query_metrics_limits.h>

namespace NKikimr {
namespace NSysView {

void Initialize(NKikimrSysView::TQueryMetrics& metrics);

void Aggregate(NKikimrSysView::TQueryMetrics& metrics, const NKikimrSysView::TQueryMetrics& from);

class TQueryInterval {
    std::unordered_map<TQueryHash, TString> Texts; // hash -> text
    std::unordered_map<TQueryHash, NKikimrSysView::TQueryMetrics> Metrics; // hash -> metrics
    std::multimap<ui64, TQueryHash> ByCpu; // cpu sum -> hash
    ui64 TotalCpuTimeUs = 0;
    ui64 CompletedQueries = 0;
    ui64 RejectedQueries = 0;
    ui64 EvictedHashes = 0;

public:
    bool Empty() const;
    void Clear();
    void Swap(TQueryInterval& other);

    void Add(TQueryStatsPtr stats);

    void FillSummary(NKikimrSysView::TEvIntervalQuerySummary::TQuerySet& queries) const;

    ui64 GetTotalCpuTimeUs() const;
    ui64 GetRetainedCpuTimeUs() const;
    ui64 GetCompletedQueries() const;
    ui64 GetRejectedQueries() const;
    ui64 GetEvictedHashes() const;

    void FillMetrics(const NKikimrSysView::TEvGetIntervalMetricsRequest& request,
        NKikimrSysView::TEvGetIntervalMetricsResponse& response) const;
};

} // NSysView
} // NKikimr
