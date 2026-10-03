#include "detailed_metrics_descriptor.h"
#include "ydb_metrics_mapper.h"

#include <ydb/core/protos/counters_detailed_datashard.pb.h>

#include <library/cpp/monlib/metrics/histogram_snapshot.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/string/join.h>

using namespace NKikimr;

namespace {

TSourceRef MakeSource(ESourceCounterCategory category, ESourceWrapper wrapper, const TString& name) {
    return TSourceRef{category, wrapper, name};
}

TMetricSpec MakeSpec(const TString& name, TVector<TSourceRef> sources, TVector<ui64> bounds = {}) {
    TMetricSpec spec;
    spec.Name = name;
    spec.Sources = std::move(sources);
    spec.Bounds = std::move(bounds);
    return spec;
}

/**
 * Build a small valid descriptor with every allowed kind of source.
 */
TDetailedMetricsDescriptor MakeValidDescriptor() {
    TDetailedMetricsDescriptor descriptor;

    descriptor.Gauges.push_back(MakeSpec("table.test.gauge_sum", {
        MakeSource(SCC_EXECUTOR, ESourceWrapper::Sum, "ExecGauge"),
        MakeSource(SCC_TABLET, ESourceWrapper::Sum, "AppGauge"),
    }));
    descriptor.Gauges.push_back(MakeSpec("table.test.gauge_max", {
        MakeSource(SCC_EXECUTOR, ESourceWrapper::Max, "ExecMaxGauge"),
    }));
    descriptor.Rates.push_back(MakeSpec("table.test.rate", {
        MakeSource(SCC_TABLET, ESourceWrapper::None, "AppRate1"),
        MakeSource(SCC_TABLET, ESourceWrapper::None, "AppRate2"),
    }));
    descriptor.Histograms.push_back(MakeSpec("table.test.level", {
        MakeSource(SCC_EXECUTOR, ESourceWrapper::Hist, "ExecGauge"),
    }, {1, 2, 3}));
    descriptor.Histograms.push_back(MakeSpec("table.test.plain", {
        MakeSource(SCC_TABLET, ESourceWrapper::None, "AppPercentile"),
    }, {10, 20}));

    return descriptor;
}

TVector<TString> SortedNames(const THashSet<TString>& names) {
    TVector<TString> result(names.begin(), names.end());
    Sort(result);
    return result;
}

TVector<TString> GetNames(const TVector<TMetricSpec>& specs) {
    TVector<TString> names;
    for (const auto& spec : specs) {
        names.push_back(spec.Name);
    }
    return names;
}

/**
 * Format all sources of every metric, e.g. "executor:SUM(DbUniqueRowsTotal)".
 */
TVector<TString> GetSources(const TVector<TMetricSpec>& specs) {
    TVector<TString> result;
    for (const auto& spec : specs) {
        TVector<TString> sources;
        for (const auto& source : spec.Sources) {
            sources.push_back(TString::Join(
                source.Category == SCC_EXECUTOR ? "executor:" : "app:",
                FormatSourceRef(source)));
        }
        result.push_back(JoinSeq(", ", sources));
    }
    return result;
}

const TDetailedMetricsDescriptor& GetDataShardDescriptor() {
    const auto* descriptor = GetDetailedMetricsDescriptor(TTabletTypes::DataShard);
    UNIT_ASSERT(descriptor);
    return *descriptor;
}

/**
 * Finalize a descriptor, which must fail, and return the error.
 */
TString FinalizeInvalid(TDetailedMetricsDescriptor& descriptor) {
    TString error;
    UNIT_ASSERT(!FinalizeDescriptor(descriptor, &error));
    UNIT_ASSERT_VALUES_EQUAL(descriptor.Errors.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(error, descriptor.Errors[0]);
    return error;
}

} // namespace

Y_UNIT_TEST_SUITE(TDetailedMetricsDescriptorTest) {

    Y_UNIT_TEST(DataShardHasTheExpectedMetricCounts) {
        const auto& descriptor = GetDataShardDescriptor();

        UNIT_ASSERT_EQUAL(descriptor.Type, TTabletTypes::DataShard);
        UNIT_ASSERT_VALUES_EQUAL(descriptor.Gauges.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(descriptor.Rates.size(), 13);
        UNIT_ASSERT_VALUES_EQUAL(descriptor.Histograms.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(descriptor.Errors, TVector<TString>());

        // Built once, the same descriptor every time
        UNIT_ASSERT_EQUAL(GetDetailedMetricsDescriptor(TTabletTypes::DataShard), &descriptor);
    }

    Y_UNIT_TEST(DataShardIndexToNameIsTheWireAbi) {
        // The index of a metric is its enum value and its slot on the wire:
        // this table may only grow at the end of each kind
        const auto& descriptor = GetDataShardDescriptor();

        UNIT_ASSERT_VALUES_EQUAL(GetNames(descriptor.Gauges), TVector<TString>({
            "table.datashard.row_count",
            "table.datashard.size_bytes",
        }));
        UNIT_ASSERT_VALUES_EQUAL(GetNames(descriptor.Rates), TVector<TString>({
            "table.datashard.write.rows",
            "table.datashard.write.bytes",
            "table.datashard.read.rows",
            "table.datashard.read.bytes",
            "table.datashard.erase.rows",
            "table.datashard.erase.bytes",
            "table.datashard.bulk_upsert.rows",
            "table.datashard.bulk_upsert.bytes",
            "table.datashard.scan.rows",
            "table.datashard.scan.bytes",
            "table.datashard.cache_hit.bytes",
            "table.datashard.cache_miss.bytes",
            "table.datashard.consumed_cpu_us",
        }));
        UNIT_ASSERT_VALUES_EQUAL(GetNames(descriptor.Histograms), TVector<TString>({
            "table.datashard.used_core_percents",
        }));

        // The enum values are the same slots
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_ROW_COUNT, 0);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_SIZE_BYTES, 1);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_WRITE_ROWS, 0);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_WRITE_BYTES, 1);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_READ_ROWS, 2);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_READ_BYTES, 3);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_ERASE_ROWS, 4);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_ERASE_BYTES, 5);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_BULK_UPSERT_ROWS, 6);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_BULK_UPSERT_BYTES, 7);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_SCAN_ROWS, 8);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_SCAN_BYTES, 9);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_CACHE_HIT_BYTES, 10);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_CACHE_MISS_BYTES, 11);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_CONSUMED_CPU_MICROSECONDS, 12);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_USED_CORE_PERCENTS, 0);
    }

    Y_UNIT_TEST(DataShardSourcesOfEveryMetric) {
        const auto& descriptor = GetDataShardDescriptor();

        UNIT_ASSERT_VALUES_EQUAL(GetSources(descriptor.Gauges), TVector<TString>({
            "executor:SUM(DbUniqueRowsTotal)",
            "executor:SUM(DbUniqueDataBytes)",
        }));
        UNIT_ASSERT_VALUES_EQUAL(GetSources(descriptor.Rates), TVector<TString>({
            "app:DataShard/EngineHostRowUpdates",
            "app:DataShard/EngineHostRowUpdateBytes",
            "app:DataShard/EngineHostRowReads, app:DataShard/EngineHostRangeReadRows",
            "app:DataShard/EngineHostRowReadBytes, app:DataShard/EngineHostRangeReadBytes",
            "app:DataShard/EngineHostRowErases",
            "app:DataShard/EngineHostRowEraseBytes",
            "app:DataShard/UploadRows",
            "app:DataShard/UploadRowsBytes",
            "app:DataShard/ScannedRows",
            "app:DataShard/ScannedBytes",
            "executor:TxCachedBytes",
            "executor:TxReadBytes",
            "executor:ConsumedCPU",
        }));
        UNIT_ASSERT_VALUES_EQUAL(GetSources(descriptor.Histograms), TVector<TString>({
            "executor:HIST(ConsumedCPU)",
        }));

        // The read metrics combine two app sources
        const auto& readRows = descriptor.Rates[NDataShard::COUNTER_DATASHARD_READ_ROWS];
        UNIT_ASSERT_VALUES_EQUAL(readRows.Sources.size(), 2);
        UNIT_ASSERT(readRows.Sources[0] == MakeSource(SCC_TABLET, ESourceWrapper::None, "DataShard/EngineHostRowReads"));
        UNIT_ASSERT(readRows.Sources[1] == MakeSource(SCC_TABLET, ESourceWrapper::None, "DataShard/EngineHostRangeReadRows"));

        const auto& readBytes = descriptor.Rates[NDataShard::COUNTER_DATASHARD_READ_BYTES];
        UNIT_ASSERT_VALUES_EQUAL(readBytes.Sources.size(), 2);
        UNIT_ASSERT(readBytes.Sources[0] == MakeSource(SCC_TABLET, ESourceWrapper::None, "DataShard/EngineHostRowReadBytes"));
        UNIT_ASSERT(readBytes.Sources[1] == MakeSource(SCC_TABLET, ESourceWrapper::None, "DataShard/EngineHostRangeReadBytes"));
    }

    Y_UNIT_TEST(DataShardLeaderOnlyMetrics) {
        const auto& descriptor = GetDataShardDescriptor();

        TVector<TString> leaderOnly;
        for (const auto* specs : {&descriptor.Gauges, &descriptor.Rates, &descriptor.Histograms}) {
            for (const auto& spec : *specs) {
                if (spec.LeaderOnly) {
                    leaderOnly.push_back(spec.Name);
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(leaderOnly, TVector<TString>({
            "table.datashard.row_count",
            "table.datashard.size_bytes",
            "table.datashard.write.rows",
            "table.datashard.write.bytes",
            "table.datashard.erase.rows",
            "table.datashard.erase.bytes",
            "table.datashard.bulk_upsert.rows",
            "table.datashard.bulk_upsert.bytes",
        }));
    }

    Y_UNIT_TEST(DataShardGaugesAreCombinedBySum) {
        const auto& descriptor = GetDataShardDescriptor();

        for (const auto& spec : descriptor.Gauges) {
            UNIT_ASSERT_C(!spec.CombineByMax, spec.Name);
            UNIT_ASSERT_C(!spec.StaticLevel, spec.Name);
            UNIT_ASSERT_C(!spec.Integral, spec.Name);
            UNIT_ASSERT_C(!spec.IsLevel, spec.Name);
            UNIT_ASSERT_C(spec.Bounds.empty(), spec.Name);
        }

        for (const auto& spec : descriptor.Rates) {
            UNIT_ASSERT_C(!spec.CombineByMax, spec.Name);
            UNIT_ASSERT_C(!spec.StaticLevel, spec.Name);
            UNIT_ASSERT_C(!spec.Integral, spec.Name);
            UNIT_ASSERT_C(!spec.IsLevel, spec.Name);
            UNIT_ASSERT_C(spec.Bounds.empty(), spec.Name);
        }
    }

    Y_UNIT_TEST(DataShardUsedCorePercentsIsALevelHistogram) {
        const auto& descriptor = GetDataShardDescriptor();
        const auto& spec = descriptor.Histograms[NDataShard::COUNTER_DATASHARD_USED_CORE_PERCENTS];

        UNIT_ASSERT_VALUES_EQUAL(spec.Name, "table.datashard.used_core_percents");
        UNIT_ASSERT(!spec.LeaderOnly);
        UNIT_ASSERT_VALUES_EQUAL(spec.Sources.size(), 1);
        UNIT_ASSERT(spec.Sources[0] == MakeSource(SCC_EXECUTOR, ESourceWrapper::Hist, "ConsumedCPU"));
        UNIT_ASSERT(spec.StaticLevel);
        UNIT_ASSERT(!spec.CombineByMax);

        // A level because of HIST(x), the enum entry itself is not Integral
        UNIT_ASSERT(spec.IsLevel);
        UNIT_ASSERT(!spec.Integral);

        UNIT_ASSERT_VALUES_EQUAL(spec.Bounds, TVector<ui64>({0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100}));
        UNIT_ASSERT_VALUES_EQUAL(spec.BucketCount(), 12);
    }

    Y_UNIT_TEST(DataShardPartitionNames) {
        const auto& descriptor = GetDataShardDescriptor();

        for (const auto* specs : {&descriptor.Gauges, &descriptor.Rates, &descriptor.Histograms}) {
            for (const auto& spec : *specs) {
                UNIT_ASSERT_VALUES_EQUAL(
                    spec.PartitionName,
                    MakeYdbMetricName(spec.Name, EYdbMetricNameScope::Partition));
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(
            descriptor.Gauges[NDataShard::COUNTER_DATASHARD_ROW_COUNT].PartitionName,
            "table.datashard.partition.row_count");
        UNIT_ASSERT_VALUES_EQUAL(
            descriptor.Rates[NDataShard::COUNTER_DATASHARD_READ_ROWS].PartitionName,
            "table.datashard.partition.read.rows");
    }

    Y_UNIT_TEST(DataShardRawNamesAreTheSourceCounters) {
        // Every source name as written plus x for SUM(x)/MAX(x)/HIST(x);
        // ConsumedCPU is also a plain rate source
        const auto& descriptor = GetDataShardDescriptor();

        UNIT_ASSERT_VALUES_EQUAL(SortedNames(descriptor.RawNames.ExecutorNames), TVector<TString>({
            "ConsumedCPU",
            "DbUniqueDataBytes",
            "DbUniqueRowsTotal",
            "HIST(ConsumedCPU)",
            "SUM(DbUniqueDataBytes)",
            "SUM(DbUniqueRowsTotal)",
            "TxCachedBytes",
            "TxReadBytes",
        }));
        UNIT_ASSERT_VALUES_EQUAL(SortedNames(descriptor.RawNames.AppNames), TVector<TString>({
            "DataShard/EngineHostRangeReadBytes",
            "DataShard/EngineHostRangeReadRows",
            "DataShard/EngineHostRowEraseBytes",
            "DataShard/EngineHostRowErases",
            "DataShard/EngineHostRowReadBytes",
            "DataShard/EngineHostRowReads",
            "DataShard/EngineHostRowUpdateBytes",
            "DataShard/EngineHostRowUpdates",
            "DataShard/ScannedBytes",
            "DataShard/ScannedRows",
            "DataShard/UploadRows",
            "DataShard/UploadRowsBytes",
        }));
    }

    Y_UNIT_TEST(UnsupportedTabletTypeHasNoDescriptor) {
        UNIT_ASSERT(!GetDetailedMetricsDescriptor(TTabletTypes::SchemeShard));
        UNIT_ASSERT(!GetDetailedMetricsDescriptor(TTabletTypes::TypeInvalid));
    }

    Y_UNIT_TEST(ParseSourceRefSpellings) {
        const auto check = [](TStringBuf text, ESourceWrapper wrapper, const TString& name) {
            const auto ref = ParseSourceRef(text, SCC_EXECUTOR);
            UNIT_ASSERT_C(ref, text);
            UNIT_ASSERT_C(*ref == MakeSource(SCC_EXECUTOR, wrapper, name), text);
            UNIT_ASSERT_VALUES_EQUAL(FormatSourceRef(*ref), text);
        };

        check("SUM(DbUniqueRowsTotal)", ESourceWrapper::Sum, "DbUniqueRowsTotal");
        check("MAX(DbUniqueRowsTotal)", ESourceWrapper::Max, "DbUniqueRowsTotal");
        check("HIST(ConsumedCPU)", ESourceWrapper::Hist, "ConsumedCPU");
        check("ConsumedCPU", ESourceWrapper::None, "ConsumedCPU");
        check("DataShard/EngineHostRowReads", ESourceWrapper::None, "DataShard/EngineHostRowReads");

        // A plain name may contain parentheses, the wrapper names are case sensitive
        check("Tx(all)", ESourceWrapper::None, "Tx(all)");
        check("sum(x)", ESourceWrapper::None, "sum(x)");
        check("SUM(Tx(all))", ESourceWrapper::Sum, "Tx(all)");

        // The category is kept as is
        const auto app = ParseSourceRef("SUM(x)", SCC_TABLET);
        UNIT_ASSERT(app);
        UNIT_ASSERT(*app == MakeSource(SCC_TABLET, ESourceWrapper::Sum, "x"));
    }

    Y_UNIT_TEST(ParseSourceRefRejectsMalformedNames) {
        for (const TStringBuf text : {
            TStringBuf(""),
            TStringBuf("SUM("),
            TStringBuf("SUM()"),
            TStringBuf("SUM(x"),
            TStringBuf("MAX(x"),
            TStringBuf("HIST(x"),
            TStringBuf("HIST()"),
            TStringBuf("SUM(MAX(x))"),
            TStringBuf("HIST(SUM(x))"),
            TStringBuf("HIST(a)b)"),
        }) {
            UNIT_ASSERT_C(!ParseSourceRef(text, SCC_EXECUTOR), "'" << text << "'");
        }
    }

    Y_UNIT_TEST(FinalizeValidDescriptor) {
        auto descriptor = MakeValidDescriptor();

        TString error = "not reset";
        UNIT_ASSERT(FinalizeDescriptor(descriptor, &error));
        UNIT_ASSERT_VALUES_EQUAL(error, "");
        UNIT_ASSERT_VALUES_EQUAL(descriptor.Errors, TVector<TString>());

        UNIT_ASSERT_VALUES_EQUAL(descriptor.Gauges[0].PartitionName, "table.test.partition.gauge_sum");
        UNIT_ASSERT(!descriptor.Gauges[0].CombineByMax);
        UNIT_ASSERT(descriptor.Gauges[1].CombineByMax);
        UNIT_ASSERT(!descriptor.Rates[0].CombineByMax);
        UNIT_ASSERT(descriptor.Histograms[0].StaticLevel);
        UNIT_ASSERT(!descriptor.Histograms[1].StaticLevel);
        UNIT_ASSERT(descriptor.Histograms[0].IsLevel);
        UNIT_ASSERT(!descriptor.Histograms[1].IsLevel);
        UNIT_ASSERT_VALUES_EQUAL(descriptor.Histograms[0].BucketCount(), 4);

        UNIT_ASSERT_VALUES_EQUAL(SortedNames(descriptor.RawNames.ExecutorNames), TVector<TString>({
            "ExecGauge",
            "ExecMaxGauge",
            "HIST(ExecGauge)",
            "MAX(ExecMaxGauge)",
            "SUM(ExecGauge)",
        }));
        UNIT_ASSERT_VALUES_EQUAL(SortedNames(descriptor.RawNames.AppNames), TVector<TString>({
            "AppGauge",
            "AppPercentile",
            "AppRate1",
            "AppRate2",
            "SUM(AppGauge)",
        }));

        // A null error is allowed
        auto other = MakeValidDescriptor();
        UNIT_ASSERT(FinalizeDescriptor(other, nullptr));
    }

    Y_UNIT_TEST(FinalizeDerivesIsLevelOfHistograms) {
        auto descriptor = MakeValidDescriptor();

        // An Integral histogram over a plain percentile counter is a level
        descriptor.Histograms[1].Integral = true;

        // The Integral option means nothing for gauges and rates
        descriptor.Gauges[0].Integral = true;
        descriptor.Rates[0].Integral = true;

        UNIT_ASSERT(FinalizeDescriptor(descriptor, nullptr));
        UNIT_ASSERT(descriptor.Histograms[0].IsLevel);
        UNIT_ASSERT(descriptor.Histograms[1].IsLevel);
        UNIT_ASSERT(!descriptor.Histograms[1].StaticLevel);
        UNIT_ASSERT(!descriptor.Gauges[0].IsLevel);
        UNIT_ASSERT(!descriptor.Rates[0].IsLevel);

        // A histogram, which combines HIST(x) with a plain percentile counter,
        // is a level even if it is not Integral: HIST(x) is always a level
        auto mixed = MakeValidDescriptor();
        mixed.Histograms[1].Sources.push_back(MakeSource(SCC_EXECUTOR, ESourceWrapper::Hist, "ExecGauge"));
        UNIT_ASSERT(FinalizeDescriptor(mixed, nullptr));
        UNIT_ASSERT(!mixed.Histograms[1].StaticLevel);
        UNIT_ASSERT(!mixed.Histograms[1].Integral);
        UNIT_ASSERT(mixed.Histograms[1].IsLevel);

        // A plain percentile histogram without the Integral option holds increments
        auto increments = MakeValidDescriptor();
        UNIT_ASSERT(FinalizeDescriptor(increments, nullptr));
        UNIT_ASSERT(!increments.Histograms[1].IsLevel);
    }

    Y_UNIT_TEST(FinalizeRejectsMixedSumAndMaxGauge) {
        auto descriptor = MakeValidDescriptor();
        descriptor.Gauges[0].Sources[1].Wrapper = ESourceWrapper::Max;

        UNIT_ASSERT_STRING_CONTAINS(FinalizeInvalid(descriptor), "mix SUM(x) and MAX(x)");

        // The bad metric is kept without sources, the others are intact
        UNIT_ASSERT_VALUES_EQUAL(descriptor.Gauges.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(descriptor.Gauges[0].Name, "table.test.gauge_sum");
        UNIT_ASSERT_VALUES_EQUAL(descriptor.Gauges[0].PartitionName, "table.test.partition.gauge_sum");
        UNIT_ASSERT(descriptor.Gauges[0].Sources.empty());
        UNIT_ASSERT(!descriptor.Gauges[0].CombineByMax);
        UNIT_ASSERT_VALUES_EQUAL(descriptor.Gauges[1].Sources.size(), 1);
        UNIT_ASSERT(descriptor.Gauges[1].CombineByMax);

        // Its sources are not in the raw allow-list
        UNIT_ASSERT(!descriptor.RawNames.AppNames.contains("AppGauge"));
        UNIT_ASSERT(descriptor.RawNames.ExecutorNames.contains("ExecGauge"));
    }

    Y_UNIT_TEST(FinalizeRejectsMultiTermMaxGauge) {
        auto descriptor = MakeValidDescriptor();
        descriptor.Gauges[1].Sources.push_back(MakeSource(SCC_TABLET, ESourceWrapper::Max, "AppMaxGauge"));

        UNIT_ASSERT_STRING_CONTAINS(FinalizeInvalid(descriptor), "MAX(x) gauge has more than one source");
        UNIT_ASSERT(descriptor.Gauges[1].Sources.empty());
        UNIT_ASSERT(!descriptor.Gauges[1].CombineByMax);
    }

    Y_UNIT_TEST(FinalizeRejectsPlainGaugeSource) {
        auto descriptor = MakeValidDescriptor();
        descriptor.Gauges[0].Sources[0].Wrapper = ESourceWrapper::None;

        UNIT_ASSERT_STRING_CONTAINS(FinalizeInvalid(descriptor), "is not SUM(x) or MAX(x)");
        UNIT_ASSERT(descriptor.Gauges[0].Sources.empty());
    }

    Y_UNIT_TEST(FinalizeRejectsHistGaugeSource) {
        auto descriptor = MakeValidDescriptor();
        descriptor.Gauges[0].Sources[0].Wrapper = ESourceWrapper::Hist;

        UNIT_ASSERT_STRING_CONTAINS(FinalizeInvalid(descriptor), "is not SUM(x) or MAX(x)");
    }

    Y_UNIT_TEST(FinalizeRejectsWrappedRateSource) {
        auto descriptor = MakeValidDescriptor();
        descriptor.Rates[0].Sources[1].Wrapper = ESourceWrapper::Sum;

        UNIT_ASSERT_STRING_CONTAINS(FinalizeInvalid(descriptor), "rate source counter 'SUM(AppRate2)' is not a plain name");
        UNIT_ASSERT(descriptor.Rates[0].Sources.empty());
    }

    Y_UNIT_TEST(FinalizeRejectsHistogramWithoutRanges) {
        auto descriptor = MakeValidDescriptor();
        descriptor.Histograms[1].Bounds.clear();

        UNIT_ASSERT_STRING_CONTAINS(FinalizeInvalid(descriptor), "no bounds");
        UNIT_ASSERT(descriptor.Histograms[1].Sources.empty());
        UNIT_ASSERT_VALUES_EQUAL(descriptor.Histograms[0].Sources.size(), 1);
    }

    Y_UNIT_TEST(FinalizeRejectsTooManyHistogramBounds) {
        auto descriptor = MakeValidDescriptor();
        descriptor.Histograms[1].Bounds.clear();
        for (ui64 bound = 0; bound <= NMonitoring::HISTOGRAM_MAX_BUCKETS_COUNT; ++bound) {
            descriptor.Histograms[1].Bounds.push_back(bound);
        }

        UNIT_ASSERT_STRING_CONTAINS(FinalizeInvalid(descriptor), "bounds, more than");
        UNIT_ASSERT(descriptor.Histograms[1].Sources.empty());

        // The largest bound count, which monlib allows, is fine
        auto largest = MakeValidDescriptor();
        largest.Histograms[1].Bounds.clear();
        for (ui64 bound = 0; bound < NMonitoring::HISTOGRAM_MAX_BUCKETS_COUNT; ++bound) {
            largest.Histograms[1].Bounds.push_back(bound);
        }
        UNIT_ASSERT(FinalizeDescriptor(largest, nullptr));
    }

    Y_UNIT_TEST(FinalizeRejectsUnsortedHistogramBounds) {
        auto descriptor = MakeValidDescriptor();
        descriptor.Histograms[0].Bounds = {1, 3, 3};

        UNIT_ASSERT_STRING_CONTAINS(FinalizeInvalid(descriptor), "not strictly increasing");
    }

    Y_UNIT_TEST(FinalizeRejectsWrappedHistogramSource) {
        auto descriptor = MakeValidDescriptor();
        descriptor.Histograms[0].Sources[0].Wrapper = ESourceWrapper::Max;

        UNIT_ASSERT_STRING_CONTAINS(FinalizeInvalid(descriptor), "is not HIST(x) or a plain name");
    }

    Y_UNIT_TEST(FinalizeRejectsTwoSegmentName) {
        auto descriptor = MakeValidDescriptor();
        descriptor.Rates[0].Name = "table.rate";

        UNIT_ASSERT_STRING_CONTAINS(FinalizeInvalid(descriptor), "fewer than three segments");

        // MakeYdbMetricName() would abort, so the leaf keeps the aggregate name
        UNIT_ASSERT_VALUES_EQUAL(descriptor.Rates[0].PartitionName, "table.rate");
        UNIT_ASSERT(descriptor.Rates[0].Sources.empty());

        auto trailingDot = MakeValidDescriptor();
        trailingDot.Rates[0].Name = "table.rate.";
        UNIT_ASSERT_STRING_CONTAINS(FinalizeInvalid(trailingDot), "fewer than three segments");
    }

    Y_UNIT_TEST(FinalizeRejectsMetricWithoutSources) {
        auto descriptor = MakeValidDescriptor();
        descriptor.Rates[0].Sources.clear();

        UNIT_ASSERT_STRING_CONTAINS(FinalizeInvalid(descriptor), "no valid source counters");

        auto emptyName = MakeValidDescriptor();
        emptyName.Rates[0].Sources[0].Name = "";
        UNIT_ASSERT_STRING_CONTAINS(FinalizeInvalid(emptyName), "empty source counter name");
    }

    Y_UNIT_TEST(FinalizeRejectsDuplicateNames) {
        auto descriptor = MakeValidDescriptor();
        descriptor.Histograms[1].Name = "table.test.rate";

        UNIT_ASSERT_STRING_CONTAINS(FinalizeInvalid(descriptor), "histogram #1 'table.test.rate': the name is not unique");
        UNIT_ASSERT_VALUES_EQUAL(descriptor.Rates[0].Sources.size(), 2);
        UNIT_ASSERT(descriptor.Histograms[1].Sources.empty());
    }

    Y_UNIT_TEST(FinalizeReportsEveryError) {
        auto descriptor = MakeValidDescriptor();
        descriptor.Gauges[0].Sources[0].Wrapper = ESourceWrapper::None;
        descriptor.Histograms[1].Bounds.clear();

        TString error;
        UNIT_ASSERT(!FinalizeDescriptor(descriptor, &error));
        UNIT_ASSERT_VALUES_EQUAL(descriptor.Errors.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(error, JoinSeq("; ", descriptor.Errors));
        UNIT_ASSERT_STRING_CONTAINS(descriptor.Errors[0], "gauge #0 'table.test.gauge_sum'");
        UNIT_ASSERT_STRING_CONTAINS(descriptor.Errors[1], "histogram #1 'table.test.plain'");
    }

} // Y_UNIT_TEST_SUITE(TDetailedMetricsDescriptorTest)
