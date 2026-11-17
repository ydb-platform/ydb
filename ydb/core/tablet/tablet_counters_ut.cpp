#include "tablet_counters.h"
#include "tablet_counters_protobuf.h"
#include "private/aggregated_tablet_counters.h"

#include <ydb/core/protos/counters_bs_controller.pb.h>
#include <ydb/core/protos/counters_cms.pb.h>
#include <ydb/core/protos/counters_columnshard.pb.h>
#include <ydb/core/protos/counters_coordinator.pb.h>
#include <ydb/core/protos/counters_datashard.pb.h>
#include <ydb/core/protos/counters_hive.pb.h>
#include <ydb/core/protos/counters_schemeshard.pb.h>
#include <ydb/core/protos/counters_statistics_aggregator.pb.h>
#include <ydb/core/protos/counters_tx_proxy.pb.h>
#include <ydb/core/protos/sys_view.pb.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/xrange.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(TTabletCountersPercentile) {

    static void CheckValues(const TTabletPercentileCounter& counter, const std::vector<ui64>& gold) {
        UNIT_ASSERT_VALUES_EQUAL(counter.GetRangeCount(), gold.size());

        std::vector<ui64> values;
        values.reserve(gold.size());
        for (auto i: xrange(gold.size())) {
            values.push_back(counter.GetRangeValue(i));
        }

        UNIT_ASSERT_VALUES_EQUAL(values, gold);
    }

    static void CheckRanges(
        const TTabletPercentileCounter& counter,
        const std::vector<TTabletPercentileCounter::TRangeDef>& gold)
    {
        UNIT_ASSERT_VALUES_EQUAL(counter.GetRangeCount(), gold.size());
        for (auto i: xrange(gold.size())) {
            const auto bound = counter.GetRangeBound(i);
            const char* name = counter.GetRangeName(i);
            UNIT_ASSERT_VALUES_EQUAL(bound, gold[i].RangeVal);
            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(name), TStringBuf(gold[i].RangeName));
        }
    }

    Y_UNIT_TEST(SingleBucket) {
        const TTabletPercentileCounter::TRangeDef ranges[1] = {
            {0, "0"},
        };

        std::vector<TTabletPercentileCounter::TRangeDef> goldRanges(ranges, ranges + 1);
        goldRanges.push_back({Max<ui64>(), "inf"});

        TTabletPercentileCounter counter;
        counter.Initialize(ranges, true);

        CheckRanges(counter, goldRanges);

        counter.IncrementFor(0);
        counter.IncrementFor(0);

        counter.IncrementFor(1);
        counter.IncrementFor(2);
        counter.IncrementFor(2);
        counter.IncrementFor(3);
        counter.IncrementFor(4);
        counter.IncrementFor(1000);

        const std::vector<ui64> goldValues = {2, 6};
        CheckValues(counter, goldValues);
    }

    Y_UNIT_TEST(WithoutZero) {
        const TTabletPercentileCounter::TRangeDef ranges[4] = {
            {1, "1"},
            {2, "2"},
            {10, "10"},
            {11, "11"},
        };

        std::vector<TTabletPercentileCounter::TRangeDef> goldRanges(ranges, ranges + 4);
        goldRanges.push_back({Max<ui64>(), "inf"});

        TTabletPercentileCounter counter;
        counter.Initialize(ranges, true);

        CheckRanges(counter, goldRanges);

        counter.IncrementFor(0);
        counter.IncrementFor(0);
        counter.IncrementFor(1);

        counter.IncrementFor(2);
        counter.IncrementFor(2);
        counter.IncrementFor(2);
        counter.IncrementFor(2);
        counter.IncrementFor(2);

        counter.IncrementFor(3);
        counter.IncrementFor(4);
        counter.IncrementFor(4);
        counter.IncrementFor(10);
        counter.IncrementFor(10);
        counter.IncrementFor(10);

        counter.IncrementFor(11);

        counter.IncrementFor(12);
        counter.IncrementFor(13);
        counter.IncrementFor(100);
        counter.IncrementFor(10000);

        const std::vector<ui64> goldValues = {3, 5, 6, 1, 4};
        CheckValues(counter, goldValues);
    }

    Y_UNIT_TEST(StartFromZero) {
        const TTabletPercentileCounter::TRangeDef ranges[5] = {
            {0, "0"},
            {1, "1"},
            {2, "2"},
            {10, "10"},
            {11, "11"},
        };

        std::vector<TTabletPercentileCounter::TRangeDef> goldRanges(ranges, ranges + 5);
        goldRanges.push_back({Max<ui64>(), "inf"});

        TTabletPercentileCounter counter;
        counter.Initialize(ranges, true);

        CheckRanges(counter, goldRanges);

        counter.IncrementFor(0);
        counter.IncrementFor(0);

        counter.IncrementFor(1);

        counter.IncrementFor(2);
        counter.IncrementFor(2);
        counter.IncrementFor(2);
        counter.IncrementFor(2);
        counter.IncrementFor(2);

        counter.IncrementFor(3);
        counter.IncrementFor(4);
        counter.IncrementFor(4);
        counter.IncrementFor(10);
        counter.IncrementFor(10);
        counter.IncrementFor(10);

        counter.IncrementFor(11);

        counter.IncrementFor(13);
        counter.IncrementFor(100);
        counter.IncrementFor(10000);

        const std::vector<ui64> goldValues = {2, 1, 5, 6, 1, 3};
        CheckValues(counter, goldValues);
    }
}

Y_UNIT_TEST_SUITE(TTabletCountersParsedOpts) {
    template <const NProtoBuf::EnumDescriptor* TxTypesDesc()>
    static void CheckDeprecatedTxTypesHaveNoNames() {
        const auto* opts = NAux::GetOpts<ETxTypeSimpleCounters_descriptor, ETxTypeSimpleCounters_descriptor, TxTypesDesc>();
        for (int i = 0; i < TxTypesDesc()->value_count(); ++i) {
            const auto* txType = TxTypesDesc()->value(i);
            const char* name = opts->GetNames()[opts->TxOffset + i * opts->TxCountersSize + COUNTER_TT_INFLY];
            UNIT_ASSERT_VALUES_EQUAL_C(!name, txType->options().deprecated(), txType->full_name());
        }
    }

    Y_UNIT_TEST(DeprecatedTxTypesHaveNoNames) {
        CheckDeprecatedTxTypesHaveNoNames<NBlobStorageController::ETxTypes_descriptor>();
        CheckDeprecatedTxTypesHaveNoNames<NCms::ETxTypes_descriptor>();
        CheckDeprecatedTxTypesHaveNoNames<NColumnShard::ETxTypes_descriptor>();
        CheckDeprecatedTxTypesHaveNoNames<NFlatTxCoordinator::ETxTypes_descriptor>();
        CheckDeprecatedTxTypesHaveNoNames<NDataShard::ETxTypes_descriptor>();
        CheckDeprecatedTxTypesHaveNoNames<NHive::ETxTypes_descriptor>();
        CheckDeprecatedTxTypesHaveNoNames<NSchemeShard::ETxTypes_descriptor>();
        CheckDeprecatedTxTypesHaveNoNames<NStat::ETxTypes_descriptor>();
        CheckDeprecatedTxTypesHaveNoNames<NTxProxy::ETxTypes_descriptor>();
    }

    Y_UNIT_TEST(DeprecatedTxTypeKeepsSysViewLayout) {
        TProtobufTabletCounters<NDataShard::ESimpleCounters_descriptor, NDataShard::ECumulativeCounters_descriptor,
            NDataShard::EPercentileCounters_descriptor, NDataShard::ETxTypes_descriptor> counters;
        counters.TxCumulative(NDataShard::TXTYPE_PROPOSE_DATA_DEPRECATED, COUNTER_TT_RW_COMPLETED).Increment(5);
        counters.TxCumulative(NDataShard::TXTYPE_PROGRESS_START, COUNTER_TT_RW_COMPLETED).Increment(7);

        NPrivate::TAggregatedTabletCounters node(MakeIntrusive<NMonitoring::TDynamicCounters>());
        node.Initialize(&counters);
        node.Apply(1, &counters, TTabletTypes::DataShard, TInstant::Now());
        node.RecalcAll();
        NKikimrSysView::TDbCounters sum, max;
        node.ToProto(sum, max);

        // Expected slots come from the enums alone, so a layout that drops deprecated slots fails here
        const size_t txOffset = NDataShard::ECumulativeCounters_descriptor()->value_count();
        const size_t txCounters = ETxTypeCumulativeCounters_descriptor()->value_count();
        UNIT_ASSERT_VALUES_EQUAL(sum.CumulativeSize(), txOffset + NDataShard::ETxTypes_descriptor()->value_count() * txCounters);
        UNIT_ASSERT_VALUES_EQUAL(sum.GetCumulative(txOffset + NDataShard::TXTYPE_PROPOSE_DATA_DEPRECATED * txCounters + COUNTER_TT_RW_COMPLETED), 0u);
        UNIT_ASSERT_VALUES_EQUAL(sum.GetCumulative(txOffset + NDataShard::TXTYPE_PROGRESS_START * txCounters + COUNTER_TT_RW_COMPLETED), 7u);

        auto processorGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
        NPrivate::TAggregatedTabletCounters processor(processorGroup);
        processor.Initialize(&counters);
        processor.FromProto(sum, max);
        UNIT_ASSERT_VALUES_EQUAL(processorGroup->GetCounter("DataShard/TxProgressStart/RwCompleted", true)->Val(), 7);
    }
}

Y_UNIT_TEST_SUITE(TTabletCountersPublish) {
    Y_UNIT_TEST(UnwrittenCountersAreNotPublished) {
        auto executorGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
        NTabletFlatExecutor::TExecutorCounters executorCounters;
        NPrivate::TAggregatedTabletCounters executor(executorGroup);
        executor.Initialize(&executorCounters);
        UNIT_ASSERT(!executorGroup->FindCounter("SUM(CacheFreshSize)"));
        UNIT_ASSERT(executorGroup->FindCounter("SUM(CacheTotalSharedBody)"));
        UNIT_ASSERT(!executorGroup->FindCounter("TxWastedBytes"));
        UNIT_ASSERT(executorGroup->FindCounter("TxCachedBytes"));

        auto appGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TProtobufTabletCounters<NDataShard::ESimpleCounters_descriptor, NDataShard::ECumulativeCounters_descriptor,
            NDataShard::EPercentileCounters_descriptor, NDataShard::ETxTypes_descriptor> appCounters;
        NPrivate::TAggregatedTabletCounters app(appGroup);
        app.Initialize(&appCounters);
        UNIT_ASSERT(!appGroup->FindCounter("DataShard/TxInit/WrittenBytes"));
        UNIT_ASSERT(appGroup->FindCounter("DataShard/TxInit/RedoWrittenBytes"));
        UNIT_ASSERT(!appGroup->FindCounter("DataShard/ReadColumnsRows"));
        UNIT_ASSERT(appGroup->FindCounter("DataShard/ScannedRows"));
        UNIT_ASSERT(!appGroup->FindHistogram("DataShard/TxProgressExecLatency"));
        UNIT_ASSERT(appGroup->FindHistogram("DataShard/ProposeQueueLatency"));
    }
}

} // NKikimr
