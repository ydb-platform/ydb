#include "detailed_metrics_binding.h"
#include "detailed_metrics_descriptor.h"

#include <ydb/core/protos/counters_datashard.pb.h>
#include <ydb/core/protos/counters_detailed_datashard.pb.h>
#include <ydb/core/tablet/tablet_counters_app.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/ylimits.h>
#include <util/string/builder.h>
#include <util/string/join.h>

using namespace NKikimr;
using NTabletFlatExecutor::TExecutorCounters;

namespace {

////////////////////////////////////////////////////////////////////////////////
// The production layouts of DataShard

/**
 * The full application counters of DataShard, the way TDataShard creates them
 * (ydb/core/tx/datashard/datashard.cpp): the app counters followed by the counters
 * of every transaction type.
 */
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

////////////////////////////////////////////////////////////////////////////////
// Synthetic layouts

constexpr ui32 NO_SLOT = TBoundTerm::NoSlot;

// 3 bounds + the implicit +Inf bucket = 4 buckets
constexpr TTabletPercentileCounter::TRangeDef RANGES_4[] = {
    {10, "10"},
    {20, "20"},
    {30, "30"},
};

// 4 bounds + the implicit +Inf bucket = 5 buckets
constexpr TTabletPercentileCounter::TRangeDef RANGES_5[] = {
    {10, "10"},
    {20, "20"},
    {30, "30"},
    {40, "40"},
};

/**
 * The counter names of one bank, nullptr is an unnamed slot.
 */
struct TTestNames {
    TVector<const char*> Simple;
    TVector<const char*> Cumulative;
    TVector<const char*> Percentile;
};

/**
 * One bank (the Executor or the application counters) of a synthetic layout.
 */
class TTestCounters {
public:
    explicit TTestCounters(TTestNames names = {})
        : Names(std::move(names))
        , Counters(MakeHolder<TTabletCountersBase>(
            Names.Simple.size(),
            Names.Cumulative.size(),
            Names.Percentile.size(),
            Names.Simple.data(),
            Names.Cumulative.data(),
            Names.Percentile.data()))
    {
    }

    template <ui32 RangeCount>
    TTestCounters& InitPercentile(
        ui32 slot,
        const TTabletPercentileCounter::TRangeDef (&ranges)[RangeCount],
        bool integral)
    {
        Counters->Percentile()[slot].Initialize(ranges, integral);
        return *this;
    }

    const TTabletCountersBase& Get() const {
        return *Counters;
    }

private:
    // NOTE: The counters keep pointers into the name vectors, which survive a move
    TTestNames Names;
    THolder<TTabletCountersBase> Counters;
};

TSourceRef ExecutorSource(ESourceWrapper wrapper, const TString& name) {
    return TSourceRef{SCC_EXECUTOR, wrapper, name};
}

TSourceRef AppSource(ESourceWrapper wrapper, const TString& name) {
    return TSourceRef{SCC_TABLET, wrapper, name};
}

TMetricSpec MakeSpec(const TString& name, TVector<TSourceRef> sources) {
    TMetricSpec spec;
    spec.Name = name;
    spec.Sources = std::move(sources);
    return spec;
}

/**
 * A histogram with 4 buckets, which matches RANGES_4.
 */
TMetricSpec MakeHistogramSpec(const TString& name, TVector<TSourceRef> sources, bool integral) {
    TMetricSpec spec = MakeSpec(name, std::move(sources));
    spec.Bounds = {1, 2, 3};
    spec.Integral = integral;
    return spec;
}

TDetailedMetricsDescriptor Finalize(TDetailedMetricsDescriptor descriptor) {
    TString error;
    UNIT_ASSERT_C(FinalizeDescriptor(descriptor, &error), error);
    return descriptor;
}

/**
 * A descriptor with every kind of source.
 */
TDetailedMetricsDescriptor MakeTestDescriptor() {
    TDetailedMetricsDescriptor descriptor;

    descriptor.Gauges.push_back(MakeSpec("table.test.sum", {
        ExecutorSource(ESourceWrapper::Sum, "ExecGauge"),
        AppSource(ESourceWrapper::Sum, "AppGauge"),
    }));
    descriptor.Gauges.push_back(MakeSpec("table.test.max", {
        ExecutorSource(ESourceWrapper::Max, "ExecMax"),
    }));
    descriptor.Gauges.back().LeaderOnly = true;

    descriptor.Rates.push_back(MakeSpec("table.test.rate", {
        ExecutorSource(ESourceWrapper::None, "ExecRate"),
        AppSource(ESourceWrapper::None, "AppRate"),
    }));

    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.level", {
        ExecutorSource(ESourceWrapper::Hist, "ExecRate"),
    }, false /* integral */));
    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.plain_level", {
        AppSource(ESourceWrapper::None, "AppLevel"),
    }, true /* integral */));
    descriptor.Histograms.push_back(MakeHistogramSpec("table.test.plain_increment", {
        AppSource(ESourceWrapper::None, "AppIncrement"),
    }, false /* integral */));

    return Finalize(std::move(descriptor));
}

TTestCounters MakeTestExecutorCounters() {
    TTestCounters counters({
        .Simple = {"ExecGauge", "ExecMax", "ExecUnused"},
        .Cumulative = {"ExecRate"},
        .Percentile = {"HIST(ExecRate)"},
    });

    // The Executor initializes its histogram aggregates as derivative ones,
    // a histogram aggregate is a level anyway
    counters.InitPercentile(0, RANGES_4, false /* integral */);
    return counters;
}

TTestCounters MakeTestAppCounters() {
    TTestCounters counters({
        .Simple = {"AppGauge"},
        .Cumulative = {"AppRate"},
        .Percentile = {"AppLevel", "AppIncrement"},
    });

    counters.InitPercentile(0, RANGES_4, true /* integral */);
    counters.InitPercentile(1, RANGES_4, false /* integral */);
    return counters;
}

////////////////////////////////////////////////////////////////////////////////
// Formatting of the bound terms for readable assertions

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
    case ESourceOp::PercentileLevel:
        return "PercentileLevel";
    case ESourceOp::PercentileIncrement:
        return "PercentileIncrement";
    }

    UNIT_FAIL("unexpected source operation");
    return {};
}

/**
 * Format a bound term (except SourceBounds), e.g. "rate#2.1 CumulativeDelta app:51".
 */
TString FormatTerm(const TBoundTerm& term) {
    TStringBuilder result;
    result << GetMetricKindName(term.Kind) << "#" << term.Metric << "." << term.Source
        << " " << GetOpName(term.Op)
        << " " << (term.Bank == EBank::Executor ? "executor" : "app") << ":" << term.Slot;

    if (term.HistSlot != NO_SLOT) {
        result << " hist:" << term.HistSlot;
    }

    if (term.StateOffset != NO_SLOT) {
        result << " state:" << term.StateOffset;
    }

    if (term.PendingOffset != NO_SLOT) {
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

/**
 * Bind and check that exactly the given problems are reported (as substrings, in order).
 */
THolder<TDetailedMetricsBinding> BindWithProblems(
    const TDetailedMetricsDescriptor& descriptor,
    const TTabletCountersBase& executorCounters,
    const TTabletCountersBase& appCounters,
    const TVector<TString>& expectedProblems)
{
    auto binding = BindDetailedMetrics(descriptor, executorCounters, appCounters);
    UNIT_ASSERT(binding);

    UNIT_ASSERT_VALUES_EQUAL_C(
        binding->Problems.size(),
        expectedProblems.size(),
        JoinSeq("\n", binding->Problems));

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

    Y_UNIT_TEST(DataShardTemplateLayoutBindsEverySource) {
        const auto& descriptor = GetDataShardDescriptor();
        TExecutorCounters executorCounters;
        const auto appCounters = CreateAppCountersByTabletType(TTabletTypes::DataShard);
        UNIT_ASSERT(appCounters);

        const auto binding = BindWithProblems(descriptor, executorCounters, *appCounters, {});
        UNIT_ASSERT_EQUAL(binding->Descriptor, &descriptor);

        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(binding->Terms), FormatTerms({
            {.Kind = EMetricKind::Gauge, .Metric = 0, .Source = 0, .Op = ESourceOp::SimpleSum, .Bank = EBank::Executor,
                .Slot = TExecutorCounters::DB_UNIQUE_ROWS_TOTAL, .StateOffset = 0, .LeaderOnly = true},
            {.Kind = EMetricKind::Gauge, .Metric = 1, .Source = 0, .Op = ESourceOp::SimpleSum, .Bank = EBank::Executor,
                .Slot = TExecutorCounters::DB_UNIQUE_DATA_BYTES, .StateOffset = 1, .LeaderOnly = true},
            {.Kind = EMetricKind::Rate, .Metric = 0, .Source = 0, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::App,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW, .LeaderOnly = true},
            {.Kind = EMetricKind::Rate, .Metric = 1, .Source = 0, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::App,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_UPDATE_ROW_BYTES, .LeaderOnly = true},
            {.Kind = EMetricKind::Rate, .Metric = 2, .Source = 0, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::App,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW},
            {.Kind = EMetricKind::Rate, .Metric = 2, .Source = 1, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::App,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_SELECT_RANGE_ROWS},
            {.Kind = EMetricKind::Rate, .Metric = 3, .Source = 0, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::App,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_SELECT_ROW_BYTES},
            {.Kind = EMetricKind::Rate, .Metric = 3, .Source = 1, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::App,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_SELECT_RANGE_BYTES},
            {.Kind = EMetricKind::Rate, .Metric = 4, .Source = 0, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::App,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_ERASE_ROW, .LeaderOnly = true},
            {.Kind = EMetricKind::Rate, .Metric = 5, .Source = 0, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::App,
                .Slot = NDataShard::COUNTER_ENGINE_HOST_ERASE_ROW_BYTES, .LeaderOnly = true},
            {.Kind = EMetricKind::Rate, .Metric = 6, .Source = 0, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::App,
                .Slot = NDataShard::COUNTER_UPLOAD_ROWS, .LeaderOnly = true},
            {.Kind = EMetricKind::Rate, .Metric = 7, .Source = 0, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::App,
                .Slot = NDataShard::COUNTER_UPLOAD_ROWS_BYTES, .LeaderOnly = true},
            {.Kind = EMetricKind::Rate, .Metric = 8, .Source = 0, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::App,
                .Slot = NDataShard::COUNTER_SCANNED_ROWS},
            {.Kind = EMetricKind::Rate, .Metric = 9, .Source = 0, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::App,
                .Slot = NDataShard::COUNTER_SCANNED_BYTES},
            {.Kind = EMetricKind::Rate, .Metric = 10, .Source = 0, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::Executor,
                .Slot = TExecutorCounters::TX_BYTES_CACHED},
            {.Kind = EMetricKind::Rate, .Metric = 11, .Source = 0, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::Executor,
                .Slot = TExecutorCounters::TX_BYTES_READ},
            {.Kind = EMetricKind::Rate, .Metric = 12, .Source = 0, .Op = ESourceOp::CumulativeDelta, .Bank = EBank::Executor,
                .Slot = TExecutorCounters::CONSUMED_CPU},
            {.Kind = EMetricKind::Histogram, .Metric = 0, .Source = 0, .Op = ESourceOp::HistOfCumulative, .Bank = EBank::Executor,
                .Slot = TExecutorCounters::CONSUMED_CPU, .HistSlot = TExecutorCounters::TX_PERCENTILE_CONSUMED_CPU, .StateOffset = 2},
        }));

        // Two gauge values and one HIST(ConsumedCPU) observation per source,
        // no increment histogram
        UNIT_ASSERT_VALUES_EQUAL(binding->PerSourceStateSize, 3);
        UNIT_ASSERT_VALUES_EQUAL(binding->PendingHistSize, 0);
        UNIT_ASSERT_VALUES_EQUAL(binding->HistIsLevel, TVector<bool>({true}));

        UNIT_ASSERT_EQUAL(binding->LayoutSizes, (TDetailedMetricsBinding::TLayoutSizes{
            TExecutorCounters::SIMPLE_COUNTER_SIZE,
            TExecutorCounters::CUMULATIVE_COUNTER_SIZE,
            TExecutorCounters::PERCENTILE_COUNTER_SIZE,
            static_cast<ui32>(NDataShard::ESimpleCounters_descriptor()->value_count()),
            static_cast<ui32>(NDataShard::ECumulativeCounters_descriptor()->value_count()),
            static_cast<ui32>(NDataShard::EPercentileCounters_descriptor()->value_count()),
        }));

        UNIT_ASSERT(binding->Matches(executorCounters, *appCounters));
        UNIT_ASSERT(binding->LayoutSignature == binding->GetLayoutSignature(executorCounters, *appCounters));

        // Every term names its source
        for (const auto& term : binding->Terms) {
            const auto& spec = descriptor.GetMetrics(term.Kind)[term.Metric];
            UNIT_ASSERT(&binding->GetSource(term) == &spec.Sources[term.Source]);
        }
    }

    Y_UNIT_TEST(DataShardUsedCorePercentsHasThePublicBucketCount) {
        // FLAT_EXECUTOR_CONSUMED_CPU_RANGES (11 bounds + Inf) must match
        // table.datashard.used_core_percents (11 bounds + Inf) bucket by bucket
        const auto& descriptor = GetDataShardDescriptor();
        TExecutorCounters executorCounters;
        const auto appCounters = CreateAppCountersByTabletType(TTabletTypes::DataShard);

        const auto binding = BindWithProblems(descriptor, executorCounters, *appCounters, {});
        const auto& term = binding->Terms.back();
        const auto& spec = descriptor.Histograms[NDataShard::COUNTER_DATASHARD_USED_CORE_PERCENTS];

        UNIT_ASSERT_EQUAL(term.Op, ESourceOp::HistOfCumulative);
        UNIT_ASSERT_VALUES_EQUAL(
            executorCounters.Percentile()[TExecutorCounters::TX_PERCENTILE_CONSUMED_CPU].GetRangeCount(), 12);
        UNIT_ASSERT_VALUES_EQUAL(spec.BucketCount(), 12);
        UNIT_ASSERT_VALUES_EQUAL(term.SourceBounds, MakeSourceBounds({
            0, 100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000,
        }));
    }

    Y_UNIT_TEST(DataShardFullLayoutBindsTheSameSlotsAsTheTemplate) {
        // The tablet reports its app counters followed by the counters of every transaction
        // type (the template has the app counters only), so every bound slot of the template
        // is the very same slot of the full layout
        const auto& descriptor = GetDataShardDescriptor();
        TExecutorCounters executorCounters;
        const auto templateCounters = CreateAppCountersByTabletType(TTabletTypes::DataShard);
        TDataShardFullAppCounters fullCounters;

        UNIT_ASSERT_VALUES_EQUAL(TDataShardFullAppCounters::SimpleOpts()->TxOffset, templateCounters->Simple().Size());
        UNIT_ASSERT_VALUES_EQUAL(TDataShardFullAppCounters::CumulativeOpts()->TxOffset, templateCounters->Cumulative().Size());
        UNIT_ASSERT_VALUES_EQUAL(TDataShardFullAppCounters::PercentileOpts()->TxOffset, templateCounters->Percentile().Size());
        UNIT_ASSERT_GT(fullCounters.Simple().Size(), templateCounters->Simple().Size());
        UNIT_ASSERT_GT(fullCounters.Cumulative().Size(), templateCounters->Cumulative().Size());
        UNIT_ASSERT_GT(fullCounters.Percentile().Size(), templateCounters->Percentile().Size());

        const auto templateBinding = BindWithProblems(descriptor, executorCounters, *templateCounters, {});
        const auto fullBinding = BindWithProblems(descriptor, executorCounters, fullCounters, {});

        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(fullBinding->Terms), FormatTerms(templateBinding->Terms));
        for (size_t i = 0; i < fullBinding->Terms.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(fullBinding->Terms[i].SourceBounds, templateBinding->Terms[i].SourceBounds);
        }

        UNIT_ASSERT_VALUES_EQUAL(fullBinding->PerSourceStateSize, templateBinding->PerSourceStateSize);
        UNIT_ASSERT_VALUES_EQUAL(fullBinding->PendingHistSize, templateBinding->PendingHistSize);
        UNIT_ASSERT_VALUES_EQUAL(fullBinding->HistIsLevel, templateBinding->HistIsLevel);

        // Still two different layouts
        UNIT_ASSERT(fullBinding->Matches(executorCounters, fullCounters));
        UNIT_ASSERT(!fullBinding->Matches(executorCounters, *templateCounters));
        UNIT_ASSERT(!templateBinding->Matches(executorCounters, fullCounters));
        UNIT_ASSERT(!(fullBinding->LayoutSignature == templateBinding->LayoutSignature));
    }

    Y_UNIT_TEST(SyntheticLayoutBindsEveryOperation) {
        const auto descriptor = MakeTestDescriptor();
        const auto executorCounters = MakeTestExecutorCounters();
        const auto appCounters = MakeTestAppCounters();

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {});

        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(binding->Terms), TVector<TString>({
            "gauge#0.0 SimpleSum executor:0 state:0",
            "gauge#0.1 SimpleSum app:0 state:1",
            "gauge#1.0 SimpleMax executor:1 state:2 leader",
            "rate#0.0 CumulativeDelta executor:0",
            "rate#0.1 CumulativeDelta app:0",
            "histogram#0.0 HistOfCumulative executor:0 hist:0 state:3",
            "histogram#1.0 PercentileLevel app:0 state:4",
            "histogram#2.0 PercentileIncrement app:1 pending:0",
        }));

        // A level percentile keeps all its 4 buckets per source,
        // an increment one keeps its 4 pending buckets per metric
        UNIT_ASSERT_VALUES_EQUAL(binding->PerSourceStateSize, 8);
        UNIT_ASSERT_VALUES_EQUAL(binding->PendingHistSize, 4);
        UNIT_ASSERT_VALUES_EQUAL(binding->HistIsLevel, TVector<bool>({true, true, false}));

        UNIT_ASSERT_VALUES_EQUAL(binding->Terms[5].SourceBounds, MakeSourceBounds({10, 20, 30}));
        for (size_t i = 0; i < binding->Terms.size(); ++i) {
            if (i != 5) {
                UNIT_ASSERT_C(binding->Terms[i].SourceBounds.empty(), i);
            }
        }

        UNIT_ASSERT_EQUAL(binding->LayoutSizes, (TDetailedMetricsBinding::TLayoutSizes{3, 1, 1, 1, 1, 2}));
        UNIT_ASSERT(binding->Matches(executorCounters.Get(), appCounters.Get()));
    }

    Y_UNIT_TEST(IncrementTermsOfOneMetricShareThePendingBuckets) {
        TDetailedMetricsDescriptor descriptor;
        descriptor.Histograms.push_back(MakeHistogramSpec("table.test.first", {
            AppSource(ESourceWrapper::None, "AppIncrement"),
        }, false /* integral */));
        descriptor.Histograms.push_back(MakeHistogramSpec("table.test.second", {
            AppSource(ESourceWrapper::None, "AppIncrement"),
            AppSource(ESourceWrapper::None, "AppIncrement2"),
        }, false /* integral */));
        descriptor = Finalize(std::move(descriptor));

        TTestCounters executorCounters;
        TTestCounters appCounters({.Percentile = {"AppIncrement", "AppIncrement2"}});
        appCounters.InitPercentile(0, RANGES_4, false /* integral */);
        appCounters.InitPercentile(1, RANGES_4, false /* integral */);

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {});

        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(binding->Terms), TVector<TString>({
            "histogram#0.0 PercentileIncrement app:0 pending:0",
            "histogram#1.0 PercentileIncrement app:0 pending:4",
            "histogram#1.1 PercentileIncrement app:1 pending:4",
        }));
        UNIT_ASSERT_VALUES_EQUAL(binding->PerSourceStateSize, 0);
        UNIT_ASSERT_VALUES_EQUAL(binding->PendingHistSize, 8);
        UNIT_ASSERT_VALUES_EQUAL(binding->HistIsLevel, TVector<bool>({false, false}));
    }

    Y_UNIT_TEST(HistogramBaseIsSimpleBeforeCumulative) {
        TDetailedMetricsDescriptor descriptor;
        descriptor.Histograms.push_back(MakeHistogramSpec("table.test.level", {
            ExecutorSource(ESourceWrapper::Hist, "Both"),
        }, false /* integral */));
        descriptor = Finalize(std::move(descriptor));

        TTestCounters appCounters;

        // The same order as TAggregatedTabletCounters::Initialize(): the simple counter wins
        TTestCounters both({
            .Simple = {"Other", "Both"},
            .Cumulative = {"Both"},
            .Percentile = {"HIST(Both)"},
        });
        both.InitPercentile(0, RANGES_4, false /* integral */);

        const auto simple = BindWithProblems(descriptor, both.Get(), appCounters.Get(), {});
        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(simple->Terms), TVector<TString>({
            "histogram#0.0 HistOfSimple executor:1 hist:0 state:0",
        }));

        // Only a cumulative counter: one observation is its rate
        TTestCounters cumulativeOnly({
            .Simple = {"Other"},
            .Cumulative = {"Other", "Both"},
            .Percentile = {"HIST(Both)"},
        });
        cumulativeOnly.InitPercentile(0, RANGES_4, false /* integral */);

        const auto cumulative = BindWithProblems(descriptor, cumulativeOnly.Get(), appCounters.Get(), {});
        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(cumulative->Terms), TVector<TString>({
            "histogram#0.0 HistOfCumulative executor:1 hist:0 state:0",
        }));
    }

    Y_UNIT_TEST(PlainPercentileKindFollowsItsIntegralFlag) {
        TDetailedMetricsDescriptor descriptor;
        descriptor.Histograms.push_back(MakeHistogramSpec("table.test.level", {
            AppSource(ESourceWrapper::None, "Percentile"),
        }, true /* integral */));
        descriptor.Histograms.push_back(MakeHistogramSpec("table.test.increment", {
            AppSource(ESourceWrapper::None, "Percentile"),
        }, false /* integral */));
        descriptor = Finalize(std::move(descriptor));

        UNIT_ASSERT(descriptor.Histograms[0].IsLevel);
        UNIT_ASSERT(!descriptor.Histograms[1].IsLevel);

        TTestCounters executorCounters;

        // An integral percentile counter is a level: the level histogram binds it,
        // the increment one rejects it
        TTestCounters integral({.Percentile = {"Percentile"}});
        integral.InitPercentile(0, RANGES_4, true /* integral */);

        const auto level = BindWithProblems(descriptor, executorCounters.Get(), integral.Get(), {
            "histogram 'table.test.increment': source counter 'app:Percentile' is an integral percentile counter (a level), "
            "but the histogram holds increments",
        });
        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(level->Terms), TVector<TString>({
            "histogram#0.0 PercentileLevel app:0 state:0",
        }));
        UNIT_ASSERT_VALUES_EQUAL(level->PerSourceStateSize, 4);
        UNIT_ASSERT_VALUES_EQUAL(level->PendingHistSize, 0);

        // A derivative percentile counter is increments: the other way around
        TTestCounters derivative({.Percentile = {"Percentile"}});
        derivative.InitPercentile(0, RANGES_4, false /* integral */);

        const auto increment = BindWithProblems(descriptor, executorCounters.Get(), derivative.Get(), {
            "histogram 'table.test.level': source counter 'app:Percentile' is a derivative percentile counter (increments), "
            "but the histogram holds a level",
        });
        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(increment->Terms), TVector<TString>({
            "histogram#1.0 PercentileIncrement app:0 pending:0",
        }));
        UNIT_ASSERT_VALUES_EQUAL(increment->PerSourceStateSize, 0);
        UNIT_ASSERT_VALUES_EQUAL(increment->PendingHistSize, 4);

        // The kind of the histogram comes from the descriptor, not from the layout
        UNIT_ASSERT_VALUES_EQUAL(level->HistIsLevel, TVector<bool>({true, false}));
        UNIT_ASSERT_VALUES_EQUAL(increment->HistIsLevel, TVector<bool>({true, false}));
    }

    Y_UNIT_TEST(MixedLevelAndIncrementSourcesAreAProblem) {
        TDetailedMetricsDescriptor descriptor;
        // HIST(x) makes the histogram a level even without the Integral option:
        // the derivative percentile counter does not belong here
        descriptor.Histograms.push_back(MakeHistogramSpec("table.test.hist_level", {
            ExecutorSource(ESourceWrapper::Hist, "ExecRate"),
            AppSource(ESourceWrapper::None, "AppIncrement"),
        }, false /* integral */));
        // An Integral level: the derivative percentile counter does not belong here
        descriptor.Histograms.push_back(MakeHistogramSpec("table.test.level", {
            ExecutorSource(ESourceWrapper::Hist, "ExecRate"),
            AppSource(ESourceWrapper::None, "AppLevel"),
            AppSource(ESourceWrapper::None, "AppIncrement"),
        }, true /* integral */));
        // Increments: the integral percentile counter (a level) does not belong here
        descriptor.Histograms.push_back(MakeHistogramSpec("table.test.increment", {
            AppSource(ESourceWrapper::None, "AppIncrement"),
            AppSource(ESourceWrapper::None, "AppLevel"),
        }, false /* integral */));
        descriptor = Finalize(std::move(descriptor));

        UNIT_ASSERT(!descriptor.Histograms[0].StaticLevel);
        UNIT_ASSERT(descriptor.Histograms[0].IsLevel);
        UNIT_ASSERT(!descriptor.Histograms[1].StaticLevel);
        UNIT_ASSERT(descriptor.Histograms[1].IsLevel);
        UNIT_ASSERT(!descriptor.Histograms[2].IsLevel);

        const auto executorCounters = MakeTestExecutorCounters();
        const auto appCounters = MakeTestAppCounters();

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {
            "histogram 'table.test.hist_level': source counter 'app:AppIncrement' is a derivative percentile counter",
            "histogram 'table.test.level': source counter 'app:AppIncrement' is a derivative percentile counter",
            "histogram 'table.test.increment': source counter 'app:AppLevel' is an integral percentile counter",
        });

        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(binding->Terms), TVector<TString>({
            "histogram#0.0 HistOfCumulative executor:0 hist:0 state:0",
            "histogram#1.0 HistOfCumulative executor:0 hist:0 state:1",
            "histogram#1.1 PercentileLevel app:0 state:2",
            "histogram#2.0 PercentileIncrement app:1 pending:0",
        }));
        UNIT_ASSERT_VALUES_EQUAL(binding->PerSourceStateSize, 6);
        UNIT_ASSERT_VALUES_EQUAL(binding->PendingHistSize, 4);
    }

    Y_UNIT_TEST(MissingSourcesAreDroppedAsProblems) {
        const auto descriptor = MakeTestDescriptor();

        // Nothing but unrelated counters, and an empty application bank
        TTestCounters executorCounters({
            .Simple = {"Unrelated"},
            .Cumulative = {"Unrelated2"},
        });
        TTestCounters appCounters;

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {
            "gauge 'table.test.sum': source counter 'executor:SUM(ExecGauge)' is missing: no simple counter 'ExecGauge'",
            "gauge 'table.test.sum': source counter 'app:SUM(AppGauge)' is missing: no simple counter 'AppGauge'",
            "gauge 'table.test.max': source counter 'executor:MAX(ExecMax)' is missing: no simple counter 'ExecMax'",
            "rate 'table.test.rate': source counter 'executor:ExecRate' is missing: no cumulative counter 'ExecRate'",
            "rate 'table.test.rate': source counter 'app:AppRate' is missing: no cumulative counter 'AppRate'",
            "histogram 'table.test.level': source counter 'executor:HIST(ExecRate)' is missing: no percentile counter 'HIST(ExecRate)'",
            "histogram 'table.test.plain_level': source counter 'app:AppLevel' is missing: no percentile counter 'AppLevel'",
            "histogram 'table.test.plain_increment': source counter 'app:AppIncrement' is missing: no percentile counter 'AppIncrement'",
        });

        UNIT_ASSERT(binding->Terms.empty());
        UNIT_ASSERT_VALUES_EQUAL(binding->PerSourceStateSize, 0);
        UNIT_ASSERT_VALUES_EQUAL(binding->PendingHistSize, 0);

        // The metrics are still there, they just publish zero
        UNIT_ASSERT_VALUES_EQUAL(binding->HistIsLevel, TVector<bool>({true, true, false}));
        UNIT_ASSERT(binding->Matches(executorCounters.Get(), appCounters.Get()));
    }

    Y_UNIT_TEST(PartiallyMissingSourcesKeepTheOthers) {
        const auto descriptor = MakeTestDescriptor();

        // The application counters only lack AppRate (and their gauge)
        const auto executorCounters = MakeTestExecutorCounters();
        TTestCounters appCounters({
            .Percentile = {"AppLevel", "AppIncrement"},
        });
        appCounters.InitPercentile(0, RANGES_4, true /* integral */);
        appCounters.InitPercentile(1, RANGES_4, false /* integral */);

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {
            "source counter 'app:SUM(AppGauge)' is missing",
            "source counter 'app:AppRate' is missing",
        });

        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(binding->Terms), TVector<TString>({
            "gauge#0.0 SimpleSum executor:0 state:0",
            "gauge#1.0 SimpleMax executor:1 state:1 leader",
            "rate#0.0 CumulativeDelta executor:0",
            "histogram#0.0 HistOfCumulative executor:0 hist:0 state:2",
            "histogram#1.0 PercentileLevel app:0 state:3",
            "histogram#2.0 PercentileIncrement app:1 pending:0",
        }));
    }

    Y_UNIT_TEST(UnnamedSlotsAreSkipped) {
        const auto descriptor = MakeTestDescriptor();

        TTestCounters executorCounters({
            .Simple = {nullptr, "ExecGauge", nullptr, "ExecMax"},
            .Cumulative = {nullptr, "ExecRate"},
            .Percentile = {nullptr, "HIST(ExecRate)"},
        });
        executorCounters.InitPercentile(1, RANGES_4, false /* integral */);

        const auto appCounters = MakeTestAppCounters();

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {});

        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(binding->Terms), TVector<TString>({
            "gauge#0.0 SimpleSum executor:1 state:0",
            "gauge#0.1 SimpleSum app:0 state:1",
            "gauge#1.0 SimpleMax executor:3 state:2 leader",
            "rate#0.0 CumulativeDelta executor:1",
            "rate#0.1 CumulativeDelta app:0",
            "histogram#0.0 HistOfCumulative executor:1 hist:1 state:3",
            "histogram#1.0 PercentileLevel app:0 state:4",
            "histogram#2.0 PercentileIncrement app:1 pending:0",
        }));
        UNIT_ASSERT(binding->Matches(executorCounters.Get(), appCounters.Get()));
    }

    Y_UNIT_TEST(UninitializedPercentilesAreProblems) {
        const auto descriptor = MakeTestDescriptor();

        // The percentile counters are named, but never initialized (no buckets)
        TTestCounters executorCounters({
            .Simple = {"ExecGauge", "ExecMax"},
            .Cumulative = {"ExecRate"},
            .Percentile = {"HIST(ExecRate)"},
        });
        TTestCounters appCounters({
            .Simple = {"AppGauge"},
            .Cumulative = {"AppRate"},
            .Percentile = {"AppLevel", "AppIncrement"},
        });

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {
            "histogram 'table.test.level': source counter 'executor:HIST(ExecRate)' has an uninitialized "
            "percentile counter 'HIST(ExecRate)' (0 buckets)",
            "histogram 'table.test.plain_level': source counter 'app:AppLevel' is an uninitialized "
            "percentile counter (0 buckets)",
            "histogram 'table.test.plain_increment': source counter 'app:AppIncrement' is an uninitialized "
            "percentile counter (0 buckets)",
        });

        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(binding->Terms), TVector<TString>({
            "gauge#0.0 SimpleSum executor:0 state:0",
            "gauge#0.1 SimpleSum app:0 state:1",
            "gauge#1.0 SimpleMax executor:1 state:2 leader",
            "rate#0.0 CumulativeDelta executor:0",
            "rate#0.1 CumulativeDelta app:0",
        }));
        UNIT_ASSERT_VALUES_EQUAL(binding->PerSourceStateSize, 3);
        UNIT_ASSERT_VALUES_EQUAL(binding->PendingHistSize, 0);
    }

    Y_UNIT_TEST(GaugeOverCumulativeCounterIsAProblem) {
        const auto descriptor = MakeTestDescriptor();

        // The gauge sources are cumulative counters, the rate sources are simple ones
        TTestCounters executorCounters({
            .Simple = {"ExecRate"},
            .Cumulative = {"ExecGauge", "ExecMax"},
        });
        TTestCounters appCounters({
            .Simple = {"AppRate"},
            .Cumulative = {"AppGauge"},
        });

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {
            "gauge 'table.test.sum': source counter 'executor:SUM(ExecGauge)' needs a simple counter, "
            "but 'ExecGauge' is a cumulative counter",
            "gauge 'table.test.sum': source counter 'app:SUM(AppGauge)' needs a simple counter, "
            "but 'AppGauge' is a cumulative counter",
            "gauge 'table.test.max': source counter 'executor:MAX(ExecMax)' needs a simple counter, "
            "but 'ExecMax' is a cumulative counter",
            "rate 'table.test.rate': source counter 'executor:ExecRate' needs a cumulative counter, "
            "but 'ExecRate' is a simple counter",
            "rate 'table.test.rate': source counter 'app:AppRate' needs a cumulative counter, "
            "but 'AppRate' is a simple counter",
            "no percentile counter 'HIST(ExecRate)'",
            "no percentile counter 'AppLevel'",
            "no percentile counter 'AppIncrement'",
        });

        UNIT_ASSERT(binding->Terms.empty());
    }

    Y_UNIT_TEST(HistogramWithoutBaseCounterIsAProblem) {
        const auto descriptor = MakeTestDescriptor();

        // HIST(ExecRate) is there, but ExecRate is neither a simple nor a cumulative counter
        TTestCounters executorCounters({
            .Simple = {"ExecGauge", "ExecMax"},
            .Percentile = {"HIST(ExecRate)", "ExecRate"},
        });
        executorCounters.InitPercentile(0, RANGES_4, false /* integral */);
        executorCounters.InitPercentile(1, RANGES_4, false /* integral */);

        const auto appCounters = MakeTestAppCounters();

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {
            "rate 'table.test.rate': source counter 'executor:ExecRate' is missing: no cumulative counter 'ExecRate'",
            "histogram 'table.test.level': source counter 'executor:HIST(ExecRate)' has no base counter 'ExecRate' "
            "(neither simple nor cumulative)",
        });

        for (const auto& term : binding->Terms) {
            UNIT_ASSERT_C(term.Op != ESourceOp::HistOfSimple && term.Op != ESourceOp::HistOfCumulative, FormatTerm(term));
        }
    }

    Y_UNIT_TEST(BucketCountMismatchIsAProblemButKeepsTheTerm) {
        const auto descriptor = MakeTestDescriptor();

        // 5 source buckets for the 4 public ones
        TTestCounters executorCounters({
            .Simple = {"ExecGauge", "ExecMax"},
            .Cumulative = {"ExecRate"},
            .Percentile = {"HIST(ExecRate)"},
        });
        executorCounters.InitPercentile(0, RANGES_5, false /* integral */);

        TTestCounters appCounters({
            .Simple = {"AppGauge"},
            .Cumulative = {"AppRate"},
            .Percentile = {"AppLevel", "AppIncrement"},
        });
        appCounters.InitPercentile(0, RANGES_5, true /* integral */);
        appCounters.InitPercentile(1, RANGES_5, false /* integral */);

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {
            "histogram 'table.test.level': source counter 'executor:HIST(ExecRate)' has 5 buckets, "
            "but the histogram has 4 (kept",
            "histogram 'table.test.plain_level': source counter 'app:AppLevel' has 5 buckets, "
            "but the histogram has 4 (kept",
            "histogram 'table.test.plain_increment': source counter 'app:AppIncrement' has 5 buckets, "
            "but the histogram has 4 (kept",
        });

        // Every term is kept, the per-source state and the pending buckets
        // are sized by the public bucket count
        UNIT_ASSERT_VALUES_EQUAL(FormatTerms(binding->Terms), TVector<TString>({
            "gauge#0.0 SimpleSum executor:0 state:0",
            "gauge#0.1 SimpleSum app:0 state:1",
            "gauge#1.0 SimpleMax executor:1 state:2 leader",
            "rate#0.0 CumulativeDelta executor:0",
            "rate#0.1 CumulativeDelta app:0",
            "histogram#0.0 HistOfCumulative executor:0 hist:0 state:3",
            "histogram#1.0 PercentileLevel app:0 state:4",
            "histogram#2.0 PercentileIncrement app:1 pending:0",
        }));
        UNIT_ASSERT_VALUES_EQUAL(binding->PerSourceStateSize, 8);
        UNIT_ASSERT_VALUES_EQUAL(binding->PendingHistSize, 4);

        // The source bounds are those of the source
        UNIT_ASSERT_VALUES_EQUAL(binding->Terms[5].SourceBounds, MakeSourceBounds({10, 20, 30, 40}));
    }

    Y_UNIT_TEST(DescriptorErrorsBindNothingForTheBrokenMetric) {
        auto descriptor = MakeTestDescriptor();

        // A broken metric keeps its place, but has no sources
        descriptor.Rates[0].Sources.front().Wrapper = ESourceWrapper::Sum;
        UNIT_ASSERT(!FinalizeDescriptor(descriptor, nullptr));
        UNIT_ASSERT(descriptor.Rates[0].Sources.empty());

        const auto executorCounters = MakeTestExecutorCounters();
        const auto appCounters = MakeTestAppCounters();

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {});
        for (const auto& term : binding->Terms) {
            UNIT_ASSERT_C(term.Kind != EMetricKind::Rate, FormatTerm(term));
        }
        UNIT_ASSERT_VALUES_EQUAL(binding->Terms.size(), 6);
    }

    Y_UNIT_TEST(MatchesDetectsSizeDrift) {
        const auto descriptor = MakeTestDescriptor();
        const auto executorCounters = MakeTestExecutorCounters();
        const auto appCounters = MakeTestAppCounters();

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {});
        UNIT_ASSERT(binding->Matches(executorCounters.Get(), appCounters.Get()));

        THashSet<TDetailedMetricsLayoutSignature> signatures;
        signatures.insert(binding->LayoutSignature);

        // Every counter array of both banks grows by one unrelated counter at the end:
        // all bound slots keep their names, but the layout is another one
        const auto checkDrift = [&](const TTestCounters& executor, const TTestCounters& app, const TString& what) {
            UNIT_ASSERT_C(!binding->Matches(executor.Get(), app.Get()), what);

            const auto signature = binding->GetLayoutSignature(executor.Get(), app.Get());
            UNIT_ASSERT_C(!(signature == binding->LayoutSignature), what);
            UNIT_ASSERT_C(signatures.insert(signature).second, what);
        };

        {
            TTestCounters executor({
                .Simple = {"ExecGauge", "ExecMax", "ExecUnused", "Extra"},
                .Cumulative = {"ExecRate"},
                .Percentile = {"HIST(ExecRate)"},
            });
            executor.InitPercentile(0, RANGES_4, false /* integral */);
            checkDrift(executor, appCounters, "executor simple");
        }

        {
            TTestCounters executor({
                .Simple = {"ExecGauge", "ExecMax", "ExecUnused"},
                .Cumulative = {"ExecRate", "Extra"},
                .Percentile = {"HIST(ExecRate)"},
            });
            executor.InitPercentile(0, RANGES_4, false /* integral */);
            checkDrift(executor, appCounters, "executor cumulative");
        }

        {
            TTestCounters executor({
                .Simple = {"ExecGauge", "ExecMax", "ExecUnused"},
                .Cumulative = {"ExecRate"},
                .Percentile = {"HIST(ExecRate)", "Extra"},
            });
            executor.InitPercentile(0, RANGES_4, false /* integral */);
            checkDrift(executor, appCounters, "executor percentile");
        }

        {
            TTestCounters app({
                .Simple = {"AppGauge", "Extra"},
                .Cumulative = {"AppRate"},
                .Percentile = {"AppLevel", "AppIncrement"},
            });
            app.InitPercentile(0, RANGES_4, true /* integral */);
            app.InitPercentile(1, RANGES_4, false /* integral */);
            checkDrift(executorCounters, app, "app simple");
        }

        {
            TTestCounters app({
                .Simple = {"AppGauge"},
                .Cumulative = {"AppRate", "Extra"},
                .Percentile = {"AppLevel", "AppIncrement"},
            });
            app.InitPercentile(0, RANGES_4, true /* integral */);
            app.InitPercentile(1, RANGES_4, false /* integral */);
            checkDrift(executorCounters, app, "app cumulative");
        }

        {
            TTestCounters app({
                .Simple = {"AppGauge"},
                .Cumulative = {"AppRate"},
                .Percentile = {"AppLevel", "AppIncrement", "Extra"},
            });
            app.InitPercentile(0, RANGES_4, true /* integral */);
            app.InitPercentile(1, RANGES_4, false /* integral */);
            checkDrift(executorCounters, app, "app percentile");
        }
    }

    Y_UNIT_TEST(MatchesDetectsNameDrift) {
        const auto descriptor = MakeTestDescriptor();
        const auto executorCounters = MakeTestExecutorCounters();
        const auto appCounters = MakeTestAppCounters();

        const auto binding = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {});

        // The very same layout built again: same names, same sizes
        {
            const auto executor = MakeTestExecutorCounters();
            const auto app = MakeTestAppCounters();
            UNIT_ASSERT(binding->Matches(executor.Get(), app.Get()));
            UNIT_ASSERT(binding->GetLayoutSignature(executor.Get(), app.Get()) == binding->LayoutSignature);
        }

        // A renamed slot, which is not bound, does not matter
        {
            TTestCounters executor({
                .Simple = {"ExecGauge", "ExecMax", "Renamed"},
                .Cumulative = {"ExecRate"},
                .Percentile = {"HIST(ExecRate)"},
            });
            executor.InitPercentile(0, RANGES_4, false /* integral */);
            UNIT_ASSERT(binding->Matches(executor.Get(), appCounters.Get()));
            UNIT_ASSERT(binding->GetLayoutSignature(executor.Get(), appCounters.Get()) == binding->LayoutSignature);
        }

        const auto checkDrift = [&](const TTestCounters& executor, const TTestCounters& app, const TString& what) {
            UNIT_ASSERT_C(!binding->Matches(executor.Get(), app.Get()), what);
            UNIT_ASSERT_C(!(binding->GetLayoutSignature(executor.Get(), app.Get()) == binding->LayoutSignature), what);
        };

        // Two bound slots swapped: the same sizes and the same set of names
        {
            TTestCounters executor({
                .Simple = {"ExecMax", "ExecGauge", "ExecUnused"},
                .Cumulative = {"ExecRate"},
                .Percentile = {"HIST(ExecRate)"},
            });
            executor.InitPercentile(0, RANGES_4, false /* integral */);
            checkDrift(executor, appCounters, "swapped gauges");
        }

        // The base counter of HIST(x) renamed
        {
            TTestCounters executor({
                .Simple = {"ExecGauge", "ExecMax", "ExecUnused"},
                .Cumulative = {"ExecRate2"},
                .Percentile = {"HIST(ExecRate)"},
            });
            executor.InitPercentile(0, RANGES_4, false /* integral */);
            checkDrift(executor, appCounters, "renamed base");
        }

        // The percentile counter HIST(x) renamed
        {
            TTestCounters executor({
                .Simple = {"ExecGauge", "ExecMax", "ExecUnused"},
                .Cumulative = {"ExecRate"},
                .Percentile = {"HIST(ExecRate2)"},
            });
            executor.InitPercentile(0, RANGES_4, false /* integral */);
            checkDrift(executor, appCounters, "renamed histogram aggregate");
        }

        // A bound slot without a name
        {
            TTestCounters app({
                .Simple = {"AppGauge"},
                .Cumulative = {nullptr},
                .Percentile = {"AppLevel", "AppIncrement"},
            });
            app.InitPercentile(0, RANGES_4, true /* integral */);
            app.InitPercentile(1, RANGES_4, false /* integral */);
            checkDrift(executorCounters, app, "unnamed rate");
        }

        // A renamed plain percentile counter
        {
            TTestCounters app({
                .Simple = {"AppGauge"},
                .Cumulative = {"AppRate"},
                .Percentile = {"AppLevel", "AppIncrement2"},
            });
            app.InitPercentile(0, RANGES_4, true /* integral */);
            app.InitPercentile(1, RANGES_4, false /* integral */);
            checkDrift(executorCounters, app, "renamed percentile");
        }
    }

    Y_UNIT_TEST(SignatureDependsOnTheBoundNamesOnly) {
        const auto descriptor = MakeTestDescriptor();
        const auto executorCounters = MakeTestExecutorCounters();
        const auto appCounters = MakeTestAppCounters();

        const auto first = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {});
        const auto second = BindWithProblems(descriptor, executorCounters.Get(), appCounters.Get(), {});

        // Deterministic: the same layout gives the same signature and the same hash
        UNIT_ASSERT(first->LayoutSignature == second->LayoutSignature);
        UNIT_ASSERT_VALUES_EQUAL(first->LayoutSignature.Hash(), second->LayoutSignature.Hash());
        UNIT_ASSERT_EQUAL(first->LayoutSignature.Sizes, first->LayoutSizes);

        THashSet<TDetailedMetricsLayoutSignature> signatures;
        UNIT_ASSERT(signatures.insert(first->LayoutSignature).second);
        UNIT_ASSERT(!signatures.insert(second->LayoutSignature).second);

        // The signature of the DataShard templates differs from the synthetic one
        TExecutorCounters dataShardExecutor;
        const auto dataShardApp = CreateAppCountersByTabletType(TTabletTypes::DataShard);
        const auto dataShard = BindWithProblems(GetDataShardDescriptor(), dataShardExecutor, *dataShardApp, {});
        UNIT_ASSERT(signatures.insert(dataShard->LayoutSignature).second);
    }
}
