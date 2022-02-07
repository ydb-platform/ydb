#pragma once

#include "query_history.h"

namespace NKikimr {
namespace NSysView {

void Initialize(NKikimrSysView::TQueryMetrics& metrics);

void Aggregate(NKikimrSysView::TQueryMetrics& metrics, const NKikimrSysView::TQueryMetrics& from);

class TQueryInterval {
    std::unordered_map<TQueryHash, TString> Texts; // hash -> text
    std::unordered_map<TQueryHash, NKikimrSysView::TQueryMetrics> Metrics; // hash -> metrics
    std::multimap<ui64, TQueryHash> ByCpu; // cpu sum -> hash

    static constexpr size_t CountLimit = 1024;

public:
    bool Empty() const;
    void Clear();
    void Swap(TQueryInterval& other);

    void Add(TQueryStatsPtr stats);

    void FillSummary(NKikimrSysView::TEvIntervalQuerySummary::TQuerySet& queries) const;

    void FillMetrics(const NKikimrSysView::TEvGetIntervalMetricsRequest& request,
        NKikimrSysView::TEvGetIntervalMetricsResponse& response) const;
};

} // NSysView
} // NKikimr

