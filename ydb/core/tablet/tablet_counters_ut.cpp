#include "tablet_counters.h"
#include "tablet_counters_aggregator.h"
#include "tablet_counters_protobuf.h"

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
#include <ydb/core/sys_view/common/db_counters.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/tablet_flat/flat_executor_counters.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>

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

namespace {

using TDataShardCounters = TProtobufTabletCounters<NDataShard::ESimpleCounters_descriptor,
    NDataShard::ECumulativeCounters_descriptor, NDataShard::EPercentileCounters_descriptor, NDataShard::ETxTypes_descriptor>;

void ApplyCounters(TTestBasicRuntime& runtime, TTabletTypes::EType tabletType,
    const TTabletCountersBase& executorCounters, const TTabletCountersBase& appCounters)
{
    const TActorId aggregatorId = runtime.Register(CreateTabletCountersAggregator(false));
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
    runtime.DispatchEvents(options, TDuration::Seconds(10));
    runtime.Send(new IEventHandle(aggregatorId, runtime.AllocateEdgeActor(), new TEvTabletCounters::TEvTabletAddCounters(
        new TEvTabletCounters::TInFlightCookie, 1, tabletType, TPathId(1, 1),
        executorCounters.MakeDiffForAggr(TTabletCountersBase()), appCounters.MakeDiffForAggr(TTabletCountersBase()))));
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
        TTestBasicRuntime runtime(1);
        runtime.Initialize(TAppPrepare().Unwrap());
        runtime.GetAppData(0).FeatureFlags.SetEnableDbCounters(true);
        TIntrusivePtr<NSysView::IDbCounters> node;
        auto observer = runtime.AddObserver<NSysView::TEvSysView::TEvRegisterDbCounters>([&](auto& ev) {
            node = ev->Get()->Counters;
        });

        NTabletFlatExecutor::TExecutorCounters executorCounters;
        executorCounters.Cumulative()[NTabletFlatExecutor::TExecutorCounters::GC_DISCOVERED_DEPRECATED].Increment(5);
        executorCounters.Cumulative()[NTabletFlatExecutor::TExecutorCounters::CONSUMED_CPU].Increment(7);
        // Unlike DataShard, Coordinator sends its tx counters to the sys view
        TProtobufTabletCounters<NFlatTxCoordinator::ESimpleCounters_descriptor, NFlatTxCoordinator::ECumulativeCounters_descriptor,
            NFlatTxCoordinator::EPercentileCounters_descriptor, NFlatTxCoordinator::ETxTypes_descriptor> appCounters;
        appCounters.TxCumulative(NFlatTxCoordinator::TXTYPE_RESTART_MEDIATOR_DEPRECATED, COUNTER_TT_RW_COMPLETED).Increment(5);
        appCounters.TxCumulative(NFlatTxCoordinator::TXTYPE_ACQUIRE_READ_STEP, COUNTER_TT_RW_COMPLETED).Increment(7);
        ApplyCounters(runtime, TTabletTypes::Coordinator, executorCounters, appCounters);
        runtime.WaitFor("tablet db counters", [&] { return bool(node); }, TDuration::Seconds(10));
        UNIT_ASSERT(node);
        NSysView::TDbServiceCounters sysView;
        node->ToProto(sysView);

        // Expected slots come from the enums alone, so a layout that drops deprecated slots fails here
        const auto& app = sysView.FindOrAddTabletCounters(TTabletTypes::Coordinator)->GetAppCounters();
        const size_t txOffset = NFlatTxCoordinator::ECumulativeCounters_descriptor()->value_count();
        const size_t txCounters = ETxTypeCumulativeCounters_descriptor()->value_count();
        UNIT_ASSERT_VALUES_EQUAL(app.CumulativeSize(), txOffset + NFlatTxCoordinator::ETxTypes_descriptor()->value_count() * txCounters);
        UNIT_ASSERT_VALUES_EQUAL(app.GetCumulative(txOffset + NFlatTxCoordinator::TXTYPE_RESTART_MEDIATOR_DEPRECATED * txCounters + COUNTER_TT_RW_COMPLETED), 0u);
        UNIT_ASSERT_VALUES_EQUAL(app.GetCumulative(txOffset + NFlatTxCoordinator::TXTYPE_ACQUIRE_READ_STEP * txCounters + COUNTER_TT_RW_COMPLETED), 7u);

        // The processor restores only executor counters of Coordinator; deprecated executor slots precede ConsumedCPU
        auto processorGroup = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto processor = CreateTabletDbCounters(MakeIntrusive<NMonitoring::TDynamicCounters>(), processorGroup,
            MakeHolder<NTabletFlatExecutor::TExecutorCounters>());
        processor->FromProto(sysView);
        UNIT_ASSERT_VALUES_EQUAL(processorGroup->GetSubgroup("type", "Coordinator")->GetSubgroup("category", "executor")
            ->GetCounter("ConsumedCPU", true)->Val(), 7);
    }
}

Y_UNIT_TEST_SUITE(TTabletCountersPublish) {
    Y_UNIT_TEST(UnwrittenCountersAreNotPublished) {
        TTestBasicRuntime runtime(1);
        runtime.Initialize(TAppPrepare().Unwrap());
        ApplyCounters(runtime, TTabletTypes::DataShard, NTabletFlatExecutor::TExecutorCounters(), TDataShardCounters());
        auto typeGroup = runtime.GetAppData(0).Counters->GetSubgroup("counters", "tablets")->GetSubgroup("type", "DataShard");

        auto executorGroup = typeGroup->GetSubgroup("category", "executor");
        UNIT_ASSERT(!executorGroup->FindCounter("SUM(CacheFreshSize)"));
        UNIT_ASSERT(executorGroup->FindCounter("SUM(CacheTotalSharedBody)"));
        UNIT_ASSERT(!executorGroup->FindCounter("TxWastedBytes"));
        UNIT_ASSERT(executorGroup->FindCounter("TxCachedBytes"));

        auto appGroup = typeGroup->GetSubgroup("category", "app");
        UNIT_ASSERT(!appGroup->FindCounter("DataShard/TxInit/WrittenBytes"));
        UNIT_ASSERT(appGroup->FindCounter("DataShard/TxInit/RedoWrittenBytes"));
        UNIT_ASSERT(!appGroup->FindCounter("DataShard/ReadColumnsRows"));
        UNIT_ASSERT(appGroup->FindCounter("DataShard/ScannedRows"));
        UNIT_ASSERT(!appGroup->FindHistogram("DataShard/TxProgressExecLatency"));
        UNIT_ASSERT(appGroup->FindHistogram("DataShard/ProposeQueueLatency"));
    }
}

} // NKikimr
