#include "query_interval.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSysView {
namespace {

TQueryStatsPtr MakeQueryStats(ui64 hash, ui64 cpuTimeUs, const TString& text = {}) {
    auto stats = std::make_shared<NKikimrSysView::TQueryStats>();
    stats->SetQueryTextHash(hash);
    stats->SetQueryText(text);
    stats->SetTotalCpuTimeUs(cpuTimeUs);
    stats->SetDurationMs(cpuTimeUs);
    stats->SetRequestUnits(cpuTimeUs);
    stats->MutableStats()->SetReadRows(cpuTimeUs);
    return stats;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TQueryIntervalTest) {
    Y_UNIT_TEST(AggregatesMetricsAndCoverage) {
        TQueryInterval interval;
        interval.Add(MakeQueryStats(42, 10, "query"));
        interval.Add(MakeQueryStats(42, 20, "query"));

        UNIT_ASSERT_VALUES_EQUAL(interval.GetTotalCpuTimeUs(), 30);
        UNIT_ASSERT_VALUES_EQUAL(interval.GetRetainedCpuTimeUs(), 30);
        UNIT_ASSERT_VALUES_EQUAL(interval.GetCompletedQueries(), 2);
        UNIT_ASSERT_VALUES_EQUAL(interval.GetRejectedQueries(), 0);
        UNIT_ASSERT_VALUES_EQUAL(interval.GetEvictedHashes(), 0);

        NKikimrSysView::TEvIntervalQuerySummary::TQuerySet summary;
        interval.FillSummary(summary);
        UNIT_ASSERT_VALUES_EQUAL(summary.HashesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(summary.GetHashes(0), 42);
        UNIT_ASSERT_VALUES_EQUAL(summary.GetValues(0), 30);

        NKikimrSysView::TEvGetIntervalMetricsRequest request;
        request.AddMetrics(42);
        request.AddQueryTextsToGet(42);
        NKikimrSysView::TEvGetIntervalMetricsResponse response;
        interval.FillMetrics(request, response);

        UNIT_ASSERT_VALUES_EQUAL(response.MetricsSize(), 1);
        const auto& metrics = response.GetMetrics(0);
        UNIT_ASSERT_VALUES_EQUAL(metrics.GetCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(metrics.GetCpuTimeUs().GetSum(), 30);
        UNIT_ASSERT_VALUES_EQUAL(metrics.GetCpuTimeUs().GetMin(), 10);
        UNIT_ASSERT_VALUES_EQUAL(metrics.GetCpuTimeUs().GetMax(), 20);
        UNIT_ASSERT_VALUES_EQUAL(metrics.GetReadRows().GetSum(), 30);

        UNIT_ASSERT_VALUES_EQUAL(response.QueryTextsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.GetQueryTexts(0).GetHash(), 42);
        UNIT_ASSERT_VALUES_EQUAL(response.GetQueryTexts(0).GetText(), "query");
    }

    Y_UNIT_TEST(BoundsCandidatesAndCountsLoss) {
        TQueryInterval interval;
        for (ui64 hash = 1;
            hash <= NQueryMetricsLimits::NodeCandidateCount;
            ++hash)
        {
            interval.Add(MakeQueryStats(hash, 2));
        }

        const ui64 rejectedHash = NQueryMetricsLimits::NodeCandidateCount + 1;
        interval.Add(MakeQueryStats(rejectedHash, 1));
        UNIT_ASSERT_VALUES_EQUAL(
            interval.GetCompletedQueries(), NQueryMetricsLimits::NodeCandidateCount + 1);
        UNIT_ASSERT_VALUES_EQUAL(
            interval.GetTotalCpuTimeUs(), 2 * NQueryMetricsLimits::NodeCandidateCount + 1);
        UNIT_ASSERT_VALUES_EQUAL(
            interval.GetRetainedCpuTimeUs(), 2 * NQueryMetricsLimits::NodeCandidateCount);
        UNIT_ASSERT_VALUES_EQUAL(interval.GetRejectedQueries(), 1);
        UNIT_ASSERT_VALUES_EQUAL(interval.GetEvictedHashes(), 0);

        const ui64 admittedHash = rejectedHash + 1;
        interval.Add(MakeQueryStats(admittedHash, 3));
        UNIT_ASSERT_VALUES_EQUAL(
            interval.GetCompletedQueries(), NQueryMetricsLimits::NodeCandidateCount + 2);
        UNIT_ASSERT_VALUES_EQUAL(
            interval.GetTotalCpuTimeUs(), 2 * NQueryMetricsLimits::NodeCandidateCount + 4);
        UNIT_ASSERT_VALUES_EQUAL(
            interval.GetRetainedCpuTimeUs(), 2 * NQueryMetricsLimits::NodeCandidateCount + 1);
        UNIT_ASSERT_VALUES_EQUAL(interval.GetRejectedQueries(), 1);
        UNIT_ASSERT_VALUES_EQUAL(interval.GetEvictedHashes(), 1);

        NKikimrSysView::TEvIntervalQuerySummary::TQuerySet summary;
        interval.FillSummary(summary);
        UNIT_ASSERT_VALUES_EQUAL(
            summary.HashesSize(), NQueryMetricsLimits::NodeCandidateCount);
        UNIT_ASSERT_VALUES_EQUAL(
            summary.ValuesSize(), NQueryMetricsLimits::NodeCandidateCount);
        UNIT_ASSERT_VALUES_EQUAL(summary.GetHashes(0), admittedHash);
        UNIT_ASSERT_VALUES_EQUAL(summary.GetValues(0), 3);
    }

    Y_UNIT_TEST(FetchesTextsSeparatelyFromMetrics) {
        TQueryInterval interval;
        interval.Add(MakeQueryStats(1, 10, "one"));
        interval.Add(MakeQueryStats(2, 20, "two"));

        NKikimrSysView::TEvGetIntervalMetricsRequest request;
        request.AddMetrics(1);
        request.AddMetrics(2);
        request.AddQueryTextsToGet(2);
        NKikimrSysView::TEvGetIntervalMetricsResponse response;
        interval.FillMetrics(request, response);

        UNIT_ASSERT_VALUES_EQUAL(response.MetricsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(response.QueryTextsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response.GetQueryTexts(0).GetHash(), 2);
        UNIT_ASSERT_VALUES_EQUAL(response.GetQueryTexts(0).GetText(), "two");
    }

    Y_UNIT_TEST(HourIntervalChangesOnlyAfterExactBoundary) {
        const ui64 hourUs = ONE_HOUR_BUCKET_SIZE.MicroSeconds();
        const TInstant boundary = TInstant::MicroSeconds(11 * hourUs);

        UNIT_ASSERT_VALUES_EQUAL(
            EndOfQueryMetricsHourInterval(boundary - TDuration::MicroSeconds(1)),
            boundary);
        UNIT_ASSERT_VALUES_EQUAL(
            EndOfQueryMetricsHourInterval(boundary),
            boundary);
        UNIT_ASSERT_VALUES_EQUAL(
            EndOfQueryMetricsHourInterval(boundary + TDuration::MicroSeconds(1)),
            boundary + ONE_HOUR_BUCKET_SIZE);

        // The interval ending exactly at 11:00 is the last one in the bucket
        // ending at 11:00. Reset switches the accumulator only when the next
        // interval end moves past that boundary.
        UNIT_ASSERT_VALUES_UNEQUAL(
            EndOfQueryMetricsHourInterval(boundary),
            EndOfQueryMetricsHourInterval(boundary + ONE_MINUTE_BUCKET_SIZE));
    }
}

} // namespace NKikimr::NSysView
