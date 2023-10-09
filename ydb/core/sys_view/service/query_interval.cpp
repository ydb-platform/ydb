#include "query_interval.h"

namespace NKikimr {
namespace NSysView {

void Initialize(NKikimrSysView::TQueryMetrics& metrics) {
    metrics.MutableDurationUs()->SetMin(Max<ui64>());
    metrics.MutableCpuTimeUs()->SetMin(Max<ui64>());
    metrics.MutableRequestUnits()->SetMin(Max<ui64>());
    metrics.MutableReadRows()->SetMin(Max<ui64>());
    metrics.MutableReadBytes()->SetMin(Max<ui64>());
    metrics.MutableUpdateRows()->SetMin(Max<ui64>());
    metrics.MutableUpdateBytes()->SetMin(Max<ui64>());
    metrics.MutableDeleteRows()->SetMin(Max<ui64>());
}

static void Aggregate(NKikimrSysView::TQueryMetrics::TMetrics& metrics, ui64 value) {
    metrics.SetSum(metrics.GetSum() + value);
    metrics.SetMin(std::min(metrics.GetMin(), value));
    metrics.SetMax(std::max(metrics.GetMax(), value));
}

static void Aggregate(NKikimrSysView::TQueryMetrics& metrics, TQueryStatsPtr stats) {
    auto& dataStats = stats->GetStats();
    metrics.SetCount(metrics.GetCount() + 1);

    Aggregate(*metrics.MutableDurationUs(), stats->GetDurationMs() * 1000);
    Aggregate(*metrics.MutableCpuTimeUs(), stats->GetTotalCpuTimeUs());
    Aggregate(*metrics.MutableRequestUnits(), stats->GetRequestUnits());

    Aggregate(*metrics.MutableReadRows(), dataStats.GetReadRows());
    Aggregate(*metrics.MutableReadBytes(), dataStats.GetReadBytes());
    Aggregate(*metrics.MutableUpdateRows(), dataStats.GetUpdateRows());
    Aggregate(*metrics.MutableUpdateBytes(), dataStats.GetUpdateBytes());
    Aggregate(*metrics.MutableDeleteRows(), dataStats.GetDeleteRows());
}

static void Aggregate(NKikimrSysView::TQueryMetrics::TMetrics& metrics,
    const NKikimrSysView::TQueryMetrics::TMetrics& from)
{
    metrics.SetSum(metrics.GetSum() + from.GetSum());
    metrics.SetMin(std::min(metrics.GetMin(), from.GetMin()));
    metrics.SetMax(std::max(metrics.GetMax(), from.GetMax()));
}

void Aggregate(NKikimrSysView::TQueryMetrics& metrics,
    const NKikimrSysView::TQueryMetrics& from)
{
    metrics.SetCount(metrics.GetCount() + from.GetCount());

    Aggregate(*metrics.MutableDurationUs(), from.GetDurationUs());
    Aggregate(*metrics.MutableCpuTimeUs(), from.GetCpuTimeUs());
    Aggregate(*metrics.MutableRequestUnits(), from.GetRequestUnits());
    Aggregate(*metrics.MutableReadRows(), from.GetReadRows());
    Aggregate(*metrics.MutableReadBytes(), from.GetReadBytes());
    Aggregate(*metrics.MutableUpdateRows(), from.GetUpdateRows());
    Aggregate(*metrics.MutableUpdateBytes(), from.GetUpdateBytes());
    Aggregate(*metrics.MutableDeleteRows(), from.GetDeleteRows());
}

bool TQueryInterval::Empty() const {
    return Metrics.empty();
}

void TQueryInterval::Clear() {
    Texts.clear();
    Metrics.clear();
    ByCpu.clear();
}

void TQueryInterval::Swap(TQueryInterval& other) {
    Texts.swap(other.Texts);
    Metrics.swap(other.Metrics);
    ByCpu.swap(other.ByCpu);
}

void TQueryInterval::Add(TQueryStatsPtr stats) {
    if (!stats->HasQueryTextHash()) {
        return;
    }
    auto queryHash = stats->GetQueryTextHash();

    if (auto metricsIt = Metrics.find(queryHash); metricsIt != Metrics.end()) {
        auto oldCpu = metricsIt->second.GetCpuTimeUs().GetSum();
        Aggregate(metricsIt->second, stats);

        auto range = ByCpu.equal_range(oldCpu);
        auto it = range.first;
        for (; it != range.second; ++it) {
            if (it->second == queryHash) {
                break;
            }
        }
        Y_ABORT_UNLESS(it != range.second);
        ByCpu.erase(it);

        auto newCpu = metricsIt->second.GetCpuTimeUs().GetSum();
        ByCpu.emplace(newCpu, queryHash);

    } else {
        auto cpu = stats->GetTotalCpuTimeUs();

        if (ByCpu.size() == CountLimit) {
            auto it = ByCpu.begin();
            if (it->first >= cpu) {
                return;
            }
            Texts.erase(it->second);
            Metrics.erase(it->second);
            ByCpu.erase(it);
        }

        NKikimrSysView::TQueryMetrics metrics;
        Initialize(metrics);
        metrics.SetQueryTextHash(queryHash);
        Aggregate(metrics, stats);

        Texts.emplace(queryHash, stats->GetQueryText());
        Metrics.emplace(queryHash, std::move(metrics));
        ByCpu.emplace(cpu, queryHash);
    }
}

void TQueryInterval::FillSummary(NKikimrSysView::TEvIntervalQuerySummary::TQuerySet& queries) const {
    for (auto it = ByCpu.rbegin(); it != ByCpu.rend(); ++it) {
        queries.AddValues(it->first);
        queries.AddHashes(it->second);
    }
}

void TQueryInterval::FillMetrics(const NKikimrSysView::TEvGetIntervalMetricsRequest& request,
    NKikimrSysView::TEvGetIntervalMetricsResponse& response) const
{
    for (auto queryHash : request.GetMetrics()) {
        auto metricsIt = Metrics.find(queryHash);
        if (metricsIt == Metrics.end()) {
            continue;
        }
        response.AddMetrics()->CopyFrom(metricsIt->second);
    }

    for (auto queryHash : request.GetQueryTextsToGet()) {
        auto textsIt = Texts.find(queryHash);
        if (textsIt == Texts.end()) {
            continue;
        }
        auto& queryText = *response.AddQueryTexts();
        queryText.SetHash(queryHash);
        queryText.SetText(textsIt->second);
    }
}

} // NSysView
} // NKikimr

