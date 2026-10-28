#include "detailed_metrics_binding.h"
#include "ut_helpers.h"

#include <ydb/core/protos/counters_datashard.pb.h>
#include <ydb/core/protos/counters_detailed_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters_app.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>
#include <util/string/join.h>

using namespace NKikimr;
using namespace NKikimr::NDetailedMetricsTests;
using NTabletFlatExecutor::TExecutorCounters;

namespace {

// As TDataShard creates them (ydb/core/tx/datashard/datashard.cpp): the app counters,
// then the counters of every transaction type
using TDataShardFullAppCounters = TProtobufTabletCounters<
    NDataShard::ESimpleCounters_descriptor,
    NDataShard::ECumulativeCounters_descriptor,
    NDataShard::EPercentileCounters_descriptor,
    NDataShard::ETxTypes_descriptor
>;

const TDetailedMetricsDescriptor& GetDataShardDescriptor() {
    const auto* descriptor = GetDetailedMetricsDescriptor(TTabletTypes::DataShard);
    UNIT_ASSERT(descriptor);
    return *descriptor;
}

TVector<TString> GetNames(const TVector<TMetricSpec>& specs) {
    TVector<TString> names;
    for (const auto& spec : specs) {
        names.push_back(spec.Name);
    }
    return names;
}

// RANGES_<n>: n - 1 bounds + the implicit +Inf bucket
constexpr TTabletPercentileCounter::TRangeDef RANGES_4[] = {
    {10, "10"},
    {20, "20"},
    {30, "30"},
};

constexpr TTabletPercentileCounter::TRangeDef RANGES_5[] = {
    {10, "10"},
    {20, "20"},
    {30, "30"},
    {40, "40"},
};

TSourceRef ExecutorSource(TStringBuf text) {
    return ParseSourceRef(text, SCC_EXECUTOR);
}

TSourceRef AppSource(TStringBuf text) {
    return ParseSourceRef(text, SCC_TABLET);
}

// 4 buckets, like RANGES_4
TMetricSpec MakeHistogramSpec(const TString& name, TVector<TSourceRef> sources, bool nonDerivative) {
    return TMetricSpec{
        .Name = name,
        .Sources = std::move(sources),
        .Bounds = {1, 2, 3},
        .NonDerivative = nonDerivative,
    };
}

TDetailedMetricsDescriptor MakeTestDescriptor() {
    TDetailedMetricsDescriptor descriptor;
    descriptor.Gauges.push_back(TMetricSpec{.Name = "table.test.sum", .Sources = {
        ExecutorSource("SUM(ExecGauge)"),
        AppSource("SUM(AppGauge)"),
    }});
    descriptor.Gauges.push_back(TMetricSpec{.Name = "table.test.max", .LeaderOnly = true, .Sources = {
        ExecutorSource("MAX(ExecMax)"),
    }});
    descriptor.Rates.push_back(TMetricSpec{.Name = "table.test.rate", .Sources = {
        ExecutorSource("ExecRate"),
        AppSource("AppRate"),
    }});
    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.hist", {
        ExecutorSource("HIST(ExecRate)"),
    }, true /* nonDerivative */));
    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.plain_non_derivative", {
        AppSource("AppIntegral"),
    }, true /* nonDerivative */));
    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.plain_derivative", {
        AppSource("AppDerivative"),
    }, false /* nonDerivative */));
    return descriptor;
}

template <ui32 RangeCount>
TTestCounters MakeTestExecutorCounters(const TTabletPercentileCounter::TRangeDef (&ranges)[RangeCount]) {
    TTestCounters counters({
        .Simple = {"ExecGauge", "ExecMax", "ExecUnused"},
        .Cumulative = {"ExecRate"},
        .Percentile = {"HIST(ExecRate)"},
    });

    // Derivative, as the Executor initializes HIST(x); a HIST(x) metric is non-derivative anyway
    counters.InitPercentile(0, ranges, false /* integral */);
    return counters;
}

template <ui32 RangeCount>
TTestCounters MakeTestAppCounters(const TTabletPercentileCounter::TRangeDef (&ranges)[RangeCount]) {
    TTestCounters counters({
        .Simple = {"AppGauge"},
        .Cumulative = {"AppRate"},
        .Percentile = {"AppIntegral", "AppDerivative"},
    });
    counters.InitPercentile(0, ranges, true /* integral */);
    counters.InitPercentile(1, ranges, false /* integral */);
    return counters;
}

TStringBuf GetOpName(ESourceOp op) {
    switch (op) {
    case ESourceOp::SimpleSum:
        return "SimpleSum";
    case ESourceOp::SimpleMax:
        return "SimpleMax";
    case ESourceOp::CumulativeDelta:
        return "CumulativeDelta";
    case ESourceOp::HistOfSimple:
        return "HistOfSimple";
    case ESourceOp::HistOfCumulative:
        return "HistOfCumulative";
    case ESourceOp::PercentileNonDerivative:
        return "PercentileNonDerivative";
    case ESourceOp::PercentileDerivative:
        return "PercentileDerivative";
    }
    UNIT_FAIL("unexpected source operation");
    return {};
}

// E.g. "rate#2 CumulativeDelta app:51", without SourceBounds
TString FormatTerm(const TBoundTerm& term) {
    static constexpr TStringBuf kindNames[] = {"gauge", "rate", "histogram"};

    TStringBuilder result;
    result << kindNames[static_cast<int>(term.Kind)] << "#" << term.Metric
        << " " << GetOpName(term.Op)
        << " " << (term.Category == SCC_TABLET ? "app" : "executor") << ":" << term.Slot;
    if (term.StateOffset != TBoundTerm::NoSlot) {
        result << " state:" << term.StateOffset;
    }
    if (term.PendingOffset != TBoundTerm::NoSlot) {
        result << " pending:" << term.PendingOffset;
    }
    if (term.LeaderOnly) {
        result << " leader";
    }
    return result;
}

TVector<TString> FormatTerms(const TVector<TBoundTerm>& terms) {
    TVector<TString> result;
    for (const auto& term : terms) {
        result.push_back(FormatTerm(term));
    }
    return result;
}

// Checks that exactly the given problems are reported, as substrings, in order
THolder<TDetailedMetricsBinding> BindWithProblems(
    const TDetailedMetricsDescriptor& descriptor,
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters,
    const TVector<TString>& expectedProblems)
{
    auto binding = BindDetailedMetrics(descriptor, executorCounters, appCounters);
    UNIT_ASSERT_VALUES_EQUAL_C(binding->Problems.size(), expectedProblems.size(), JoinSeq("\n", binding->Problems));
    for (size_t i = 0; i < expectedProblems.size(); ++i) {
        UNIT_ASSERT_STRING_CONTAINS(binding->Problems[i], expectedProblems[i]);
    }
    return binding;
}

TVector<double> MakeSourceBounds(std::initializer_list<double> bounds) {
    TVector<double> result(bounds);
    result.push_back(Max<double>());
    return result;
}

} // namespace

Y_UNIT_TEST_SUITE(TDetailedMetricsBindingTest) {

    Y_UNIT_TEST(DataShardIndexToNameIsTheWireAbi) {
        const auto& descriptor = GetDataShardDescriptor();
        UNIT_ASSERT_EQUAL(descriptor.Type, TTabletTypes::DataShard);

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

        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_ROW_COUNT, 0);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_WRITE_ROWS, 0);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_CONSUMED_CPU_MICROSECONDS, 12);
        UNIT_ASSERT_VALUES_EQUAL((int)NDataShard::COUNTER_DATASHARD_USED_CORE_PERCENTS, 0);
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

    Y_UNIT_TEST(DataShardUsedCorePercentsIsANonDerivativeHistogram) {
        const auto& spec = GetDataShardDescriptor().Histograms[NDataShard::COUNTER_DATASHARD_USED_CORE_PERCENTS];

        UNIT_ASSERT(!spec.LeaderOnly);
        UNIT_ASSERT(spec.Sources == TVector<TSourceRef>({TSourceRef{
            .Category = SCC_EXECUTOR,
            .Wrapper = ESourceWrapper::Hist,
            .Name = "ConsumedCPU",
            .Text = "HIST(ConsumedCPU)",
        }}));
        UNIT_ASSERT(spec.NonDerivative);
        UNIT_ASSERT_VALUES_EQUAL(spec.Bounds, TVector<ui64>({0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100}));
        UNIT_ASSERT_VALUES_EQUAL(spec.BucketCount(), 12);
    }

    Y_UNIT_TEST(UnsupportedTabletTypeHasNoDescriptor) {
        UNIT_ASSERT(!GetDetailedMetricsDescriptor(TTabletTypes::SchemeShard));
        UNIT_ASSERT(!GetDetailedMetricsDescriptor(TTabletTypes::TypeInvalid));
    }

    Y_UNIT_TEST(DataShardCounterNamesAreTheSourcesWithTheirSumMaxBases) {
        const auto& descriptor = GetDataShardDescriptor();

        // Every source of counters_detailed_datashard.proto as written, plus x of SUM(x) and MAX(x), but not of HIST(x).
        // ConsumedCPU is there as the source of the consumed_cpu_us rate.
        UNIT_ASSERT_EQUAL(descriptor.ExecutorCounterNames, THashSet<TString>({
            "SUM(DbUniqueRowsTotal)",
            "DbUniqueRowsTotal",
            "SUM(DbUniqueDataBytes)",
            "DbUniqueDataBytes",
            "TxCachedBytes",
            "TxReadBytes",
            "ConsumedCPU",
            "HIST(ConsumedCPU)",
        }));
        UNIT_ASSERT_EQUAL(descriptor.AppCounterNames, THashSet<TString>({
            "DataShard/EngineHostRowUpdates",
            "DataShard/EngineHostRowUpdateBytes",
            "DataShard/EngineHostRowReads",
            "DataShard/EngineHostRangeReadRows",
            "DataShard/EngineHostRowReadBytes",
            "DataShard/EngineHostRangeReadBytes",
            "DataShard/EngineHostRowErases",
            "DataShard/EngineHostRowEraseBytes",
            "DataShard/UploadRows",
            "DataShard/UploadRowsBytes",
            "DataShard/ScannedRows",
            "DataShard/ScannedBytes",
        }));
    }

    Y_UNIT_TEST(DataShardTemplateLayoutBindsEverySource) {
        const auto& descriptor = GetDataShardDescriptor();
        TExecutorCounters executorCounters;
        const auto appCounters = CreateAppCountersByTabletType(TTabletTypes::DataShard);
        UNIT_ASSERT(appCounters);

        const auto binding = BindWithProblems(descriptor, executorCounters, *appCounters, {});
        UNIT_ASSERT_EQUAL(binding->Descriptor, &descriptor);

        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(binding->Terms), FormatTerms({
            {.Kind = EMetricKind::Gauge, .Metric = 0, .Op = ESourceOp::SimpleSum, .Category = SCC_EXECUTOR,
                .Slot = TExecutorCounters::DB_UNIQUE_ROWS_TOTAL, .StateOffset = 0, .LeaderOnly = true},
            {.Kind = EMetricKind::Gauge, .Metric = 1, .Op = ESourceOp::SimpleSum, .Category = SCC_EXECUTOR,
                .Slot = TExecutorCounters::DB_UNIQUE_DATA_BYTES, .StateOffset = 1, .LeaderOnly = true},
            {.Kind = EMetricKind::Rate, .Metric = 0, .Op = ESourceOp::CumulativeDelta, .Category = SCC_TABLET,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW, .LeaderOnly = true},
            {.Kind = EMetricKind::Rate, .Metric = 1, .Op = ESourceOp::CumulativeDelta, .Category = SCC_TABLET,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW_BYTES, .LeaderOnly = true},
            {.Kind = EMetricKind::Rate, .Metric = 2, .Op = ESourceOp::CumulativeDelta, .Category = SCC_TABLET,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW},
            {.Kind = EMetricKind::Rate, .Metric = 2, .Op = ESourceOp::CumulativeDelta, .Category = SCC_TABLET,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_SELECT_RANGE_ROWS},
            {.Kind = EMetricKind::Rate, .Metric = 3, .Op = ESourceOp::CumulativeDelta, .Category = SCC_TABLET,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW_BYTES},
            {.Kind = EMetricKind::Rate, .Metric = 3, .Op = ESourceOp::CumulativeDelta, .Category = SCC_TABLET,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_SELECT_RANGE_BYTES},
            {.Kind = EMetricKind::Rate, .Metric = 4, .Op = ESourceOp::CumulativeDelta, .Category = SCC_TABLET,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_ERASE_ROW, .LeaderOnly = true},
            {.Kind = EMetricKind::Rate, .Metric = 5, .Op = ESourceOp::CumulativeDelta, .Category = SCC_TABLET,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_ERASE_ROW_BYTES, .LeaderOnly = true},
            {.Kind = EMetricKind::Rate, .Metric = 6, .Op = ESourceOp::CumulativeDelta, .Category = SCC_TABLET,
                .Slot = NDataShard::COUNTER_UPLOAD_ROWS, .LeaderOnly = true},
            {.Kind = EMetricKind::Rate, .Metric = 7, .Op = ESourceOp::CumulativeDelta, .Category = SCC_TABLET,
                .Slot = NDataShard::COUNTER_UPLOAD_ROWS_BYTES, .LeaderOnly = true},
            {.Kind = EMetricKind::Rate, .Metric = 8, .Op = ESourceOp::CumulativeDelta, .Category = SCC_TABLET,
                .Slot = NDataShard::COUNTER_SCANNED_ROWS},
            {.Kind = EMetricKind::Rate, .Metric = 9, .Op = ESourceOp::CumulativeDelta, .Category = SCC_TABLET,
                .Slot = NDataShard::COUNTER_SCANNED_BYTES},
            {.Kind = EMetricKind::Rate, .Metric = 10, .Op = ESourceOp::CumulativeDelta, .Category = SCC_EXECUTOR,
                .Slot = TExecutorCounters::TX_BYTES_CACHED},
            {.Kind = EMetricKind::Rate, .Metric = 11, .Op = ESourceOp::CumulativeDelta, .Category = SCC_EXECUTOR,
                .Slot = TExecutorCounters::TX_BYTES_READ},
            {.Kind = EMetricKind::Rate, .Metric = 12, .Op = ESourceOp::CumulativeDelta, .Category = SCC_EXECUTOR,
                .Slot = TExecutorCounters::CONSUMED_CPU},
            {.Kind = EMetricKind::Histogram, .Metric = 0, .Op = ESourceOp::HistOfCumulative, .Category = SCC_EXECUTOR,
                .Slot = TExecutorCounters::CONSUMED_CPU, .StateOffset = 2},
        }));

        // 2 gauges + 1 HIST(ConsumedCPU) observation per source, no derivative histogram
        UNIT_ASSERT_VALUES_EQUAL(binding->PerSourceStateSize, 3);
        UNIT_ASSERT_VALUES_EQUAL(binding->PendingHistSize, 0);

        UNIT_ASSERT_EQUAL(binding->LayoutSizes, (TDetailedMetricsBinding::TLayoutSizes{
            TExecutorCounters::SIMPLE_COUNTER_SIZE,
            TExecutorCounters::CUMULATIVE_COUNTER_SIZE,
            TExecutorCounters::PERCENTILE_COUNTER_SIZE,
            static_cast<ui32>(NDataShard::ESimpleCounters_descriptor()->value_count()),
            static_cast<ui32>(NDataShard::ECumulativeCounters_descriptor()->value_count()),
            static_cast<ui32>(NDataShard::EPercentileCounters_descriptor()->value_count()),
        }));
    }

    Y_UNIT_TEST(DataShardUsedCorePercentsHasThePublicBucketCount) {
        TExecutorCounters executorCounters;
        const auto appCounters = CreateAppCountersByTabletType(TTabletTypes::DataShard);

        const auto binding = BindWithProblems(GetDataShardDescriptor(), executorCounters, *appCounters, {});
        const auto& term = binding->Terms.back();

        UNIT_ASSERT_EQUAL(term.Op, ESourceOp::HistOfCumulative);
        UNIT_ASSERT_VALUES_EQUAL(
            executorCounters.Percentile()[TExecutorCounters::TX_PERCENTILE_CONSUMED_CPU].GetRangeCount(), 12);
        UNIT_ASSERT_VALUES_EQUAL(term.SourceBounds, MakeSourceBounds({
            0, 100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000,
        }));
    }

    Y_UNIT_TEST(DataShardFullLayoutBindsTheSameSlotsAsTheTemplate) {
        const auto& descriptor = GetDataShardDescriptor();
        TExecutorCounters executorCounters;
        const auto templateCounters = CreateAppCountersByTabletType(TTabletTypes::DataShard);
        TDataShardFullAppCounters fullCounters;

        UNIT_ASSERT_VALUES_EQUAL(TDataShardFullAppCounters::SimpleOpts()->TxOffset, templateCounters->Simple().Size());
        UNIT_ASSERT_VALUES_EQUAL(TDataShardFullAppCounters::CumulativeOpts()->TxOffset, templateCounters->Cumulative().Size());
        UNIT_ASSERT_VALUES_EQUAL(TDataShardFullAppCounters::PercentileOpts()->TxOffset, templateCounters->Percentile().Size());
        UNIT_ASSERT_GT(fullCounters.Cumulative().Size(), templateCounters->Cumulative().Size());

        const auto templateBinding = BindWithProblems(descriptor, executorCounters, *templateCounters, {});
        const auto fullBinding = BindWithProblems(descriptor, executorCounters, fullCounters, {});

        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(fullBinding->Terms), FormatTerms(templateBinding->Terms));
        UNIT_ASSERT_VALUES_EQUAL(fullBinding->Terms.back().SourceBounds, templateBinding->Terms.back().SourceBounds);
        UNIT_ASSERT_VALUES_EQUAL(fullBinding->PerSourceStateSize, templateBinding->PerSourceStateSize);
    }

    Y_UNIT_TEST(SyntheticLayoutBindsEveryOperation) {
        const auto descriptor = MakeTestDescriptor();
        const auto executorCounters = MakeTestExecutorCounters(RANGES_4);
        const auto appCounters = MakeTestAppCounters(RANGES_4);

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {});

        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(binding->Terms), TVector<TString>({
            "gauge#0 SimpleSum executor:0 state:0",
            "gauge#0 SimpleSum app:0 state:1",
            "gauge#1 SimpleMax executor:1 state:2 leader",
            "rate#0 CumulativeDelta executor:0",
            "rate#0 CumulativeDelta app:0",
            "histogram#0 HistOfCumulative executor:0 state:3",
            "histogram#1 PercentileNonDerivative app:0 state:4",
            "histogram#2 PercentileDerivative app:1 pending:0",
        }));

        // The non-derivative percentile takes 4 state slots per source, the derivative one 4 pending buckets
        UNIT_ASSERT_VALUES_EQUAL(binding->PerSourceStateSize, 8);
        UNIT_ASSERT_VALUES_EQUAL(binding->PendingHistSize, 4);
        UNIT_ASSERT_VALUES_EQUAL(binding->Terms[5].SourceBounds, MakeSourceBounds({10, 20, 30}));

        UNIT_ASSERT(!descriptor.Gauges[0].CombineByMax());
        UNIT_ASSERT(descriptor.Gauges[1].CombineByMax());
    }

    Y_UNIT_TEST(DerivativeTermsOfOneMetricShareThePendingBuckets) {
        TDetailedMetricsDescriptor descriptor;
        descriptor.Histograms.push_back(MakeHistogramSpec("table.test.first", {
            AppSource("AppDerivative"),
        }, false /* nonDerivative */));
        descriptor.Histograms.push_back(MakeHistogramSpec("table.test.second", {
            AppSource("AppDerivative"),
            AppSource("AppDerivative2"),
        }, false /* nonDerivative */));

        TTestCounters executorCounters;
        TTestCounters appCounters({.Percentile = {"AppDerivative", "AppDerivative2"}});
        appCounters.InitPercentile(0, RANGES_4, false /* integral */);
        appCounters.InitPercentile(1, RANGES_4, false /* integral */);

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {});

        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(binding->Terms), TVector<TString>({
            "histogram#0 PercentileDerivative app:0 pending:0",
            "histogram#1 PercentileDerivative app:0 pending:4",
            "histogram#1 PercentileDerivative app:1 pending:4",
        }));
        UNIT_ASSERT_VALUES_EQUAL(binding->PerSourceStateSize, 0);
        UNIT_ASSERT_VALUES_EQUAL(binding->PendingHistSize, 8);
    }

    Y_UNIT_TEST(HistogramBaseIsSimpleBeforeCumulative) {
        TDetailedMetricsDescriptor descriptor;
        descriptor.Histograms.push_back(MakeHistogramSpec("table.test.hist", {
            ExecutorSource("HIST(Both)"),
        }, true /* nonDerivative */));

        TTestCounters appCounters;

        TTestCounters both({
            .Simple = {"Other", "Both"},
            .Cumulative = {"Both"},
            .Percentile = {"HIST(Both)"},
        });
        both.InitPercentile(0, RANGES_4, false /* integral */);

        const auto simple = BindWithProblems(descriptor, both.Get(), appCounters.Get(), {});
        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(simple->Terms), TVector<TString>({
            "histogram#0 HistOfSimple executor:1 state:0",
        }));

        TTestCounters cumulativeOnly({
            .Simple = {"Other"},
            .Cumulative = {"Other", "Both"},
            .Percentile = {"HIST(Both)"},
        });
        cumulativeOnly.InitPercentile(0, RANGES_4, false /* integral */);

        const auto cumulative = BindWithProblems(descriptor, cumulativeOnly.Get(), appCounters.Get(), {});
        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(cumulative->Terms), TVector<TString>({
            "histogram#0 HistOfCumulative executor:1 state:0",
        }));
    }

    Y_UNIT_TEST(PlainPercentileKindFollowsItsIntegralFlag) {
        TDetailedMetricsDescriptor descriptor;
        descriptor.Histograms.push_back(MakeHistogramSpec("table.test.non_derivative", {
            AppSource("Percentile"),
        }, true /* nonDerivative */));
        descriptor.Histograms.push_back(MakeHistogramSpec("table.test.derivative", {
            AppSource("Percentile"),
        }, false /* nonDerivative */));

        TTestCounters executorCounters;

        TTestCounters integral({.Percentile = {"Percentile"}});
        integral.InitPercentile(0, RANGES_4, true /* integral */);
        const auto integralBinding = BindWithProblems(descriptor, executorCounters.Get(), integral.Get(), {
            "metric 'table.test.derivative': source counter 'Percentile' is an integral percentile counter",
        });
        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(integralBinding->Terms), TVector<TString>({
            "histogram#0 PercentileNonDerivative app:0 state:0",
        }));

        TTestCounters derivative({.Percentile = {"Percentile"}});
        derivative.InitPercentile(0, RANGES_4, false /* integral */);
        const auto derivativeBinding = BindWithProblems(descriptor, executorCounters.Get(), derivative.Get(), {
            "metric 'table.test.non_derivative': source counter 'Percentile' is a derivative percentile counter",
        });
        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(derivativeBinding->Terms), TVector<TString>({
            "histogram#1 PercentileDerivative app:0 pending:0",
        }));
    }

    Y_UNIT_TEST(ProblemsDropOnlyTheirTerms) {
        const auto descriptor = MakeTestDescriptor();

        {
            TTestCounters executorCounters({.Simple = {"Unrelated"}, .Cumulative = {"Unrelated2"}});
            TTestCounters appCounters;
            const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {
                "metric 'table.test.sum': source counter 'ExecGauge' is missing: no simple counter 'ExecGauge'",
                "metric 'table.test.sum': source counter 'AppGauge' is missing: no simple counter 'AppGauge'",
                "metric 'table.test.max': source counter 'ExecMax' is missing: no simple counter 'ExecMax'",
                "metric 'table.test.rate': source counter 'ExecRate' is missing: no cumulative counter 'ExecRate'",
                "metric 'table.test.rate': source counter 'AppRate' is missing: no cumulative counter 'AppRate'",
                "is missing: no percentile counter 'HIST(ExecRate)'",
                "is missing: no percentile counter 'AppIntegral'",
                "is missing: no percentile counter 'AppDerivative'",
            });
            UNIT_ASSERT(binding->Terms.empty());
            UNIT_ASSERT_VALUES_EQUAL(binding->PerSourceStateSize, 0);
        }

        {
            TTestCounters executorCounters({
                .Simple = {"ExecRate"},
                .Cumulative = {"ExecGauge", "ExecMax"},
                .Percentile = {"HIST(ExecRate)"},
            });
            executorCounters.InitPercentile(0, RANGES_4, false /* integral */);
            TTestCounters appCounters({.Simple = {"AppRate"}, .Cumulative = {"AppGauge"}});
            const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {
                "source counter 'ExecGauge' is missing: no simple counter",
                "source counter 'AppGauge' is missing: no simple counter",
                "source counter 'ExecMax' is missing: no simple counter",
                "source counter 'ExecRate' is missing: no cumulative counter",
                "source counter 'AppRate' is missing: no cumulative counter",
                "no percentile counter 'AppIntegral'",
                "no percentile counter 'AppDerivative'",
            });
            UNIT_ASSERT_VALUES_EQUAL(FormatTerms(binding->Terms), TVector<TString>({
                "histogram#0 HistOfSimple executor:0 state:0",
            }));
        }

        {
            TTestCounters executorCounters({
                .Simple = {"ExecGauge", "ExecMax"},
                .Cumulative = {"ExecRate"},
                .Percentile = {"HIST(ExecRate)"},
            });
            TTestCounters appCounters({
                .Simple = {"AppGauge"},
                .Cumulative = {"AppRate"},
                .Percentile = {"AppIntegral", "AppDerivative"},
            });
            const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {
                "source counter 'ExecRate' has an uninitialized percentile counter (0 buckets)",
                "source counter 'AppIntegral' has an uninitialized percentile counter (0 buckets)",
                "source counter 'AppDerivative' has an uninitialized percentile counter (0 buckets)",
            });
            UNIT_ASSERT_VALUES_EQUAL(binding->Terms.size(), 5);
            UNIT_ASSERT_VALUES_EQUAL(binding->PerSourceStateSize, 3);
            UNIT_ASSERT_VALUES_EQUAL(binding->PendingHistSize, 0);
        }

        // Another bucket count: the state is sized by the public one
        {
            const auto executorCounters = MakeTestExecutorCounters(RANGES_5);
            const auto appCounters = MakeTestAppCounters(RANGES_5);
            const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {
                "source counter 'ExecRate' has 5 buckets, but the histogram has 4 (kept)",
                "source counter 'AppIntegral' has 5 buckets, but the histogram has 4 (kept)",
                "source counter 'AppDerivative' has 5 buckets, but the histogram has 4 (kept)",
            });
            UNIT_ASSERT_VALUES_EQUAL(binding->Terms.size(), 8);
            UNIT_ASSERT_VALUES_EQUAL(binding->PerSourceStateSize, 8);
            UNIT_ASSERT_VALUES_EQUAL(binding->Terms[5].SourceBounds, MakeSourceBounds({10, 20, 30, 40}));
        }

        {
            TDetailedMetricsDescriptor mixed;
            mixed.Gauges.push_back(TMetricSpec{.Name = "table.test.plain_gauge", .Sources = {
                ExecutorSource("ExecGauge"),
            }});
            mixed.Histograms.push_back(MakeHistogramSpec("table.test.hist", {
                ExecutorSource("HIST(ExecRate)"),
                AppSource("AppDerivative"),
            }, true /* nonDerivative */));
            mixed.Histograms.push_back(MakeHistogramSpec("table.test.derivative", {
                AppSource("AppDerivative"),
                AppSource("AppIntegral"),
            }, false /* nonDerivative */));

            const auto executorCounters = MakeTestExecutorCounters(RANGES_4);
            const auto appCounters = MakeTestAppCounters(RANGES_4);
            const auto binding = BindWithProblems(mixed, executorCounters.Get(), appCounters.Get(), {
                "metric 'table.test.plain_gauge': source counter 'ExecGauge' is written in a form the metric kind does not allow",
                "metric 'table.test.hist': source counter 'AppDerivative' is a derivative percentile counter",
                "metric 'table.test.derivative': source counter 'AppIntegral' is an integral percentile counter",
            });
            UNIT_ASSERT_VALUES_EQUAL(FormatTerms(binding->Terms), TVector<TString>({
                "histogram#0 HistOfCumulative executor:0 state:0",
                "histogram#1 PercentileDerivative app:1 pending:0",
            }));
        }
    }

} // Y_UNIT_TEST_SUITE(TDetailedMetricsBindingTest)
