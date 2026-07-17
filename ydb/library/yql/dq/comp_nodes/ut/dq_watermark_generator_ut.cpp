#include "utils/dq_setup.h"
#include "utils/dq_factories.h"

#include <ydb/library/yql/dq/comp_nodes/dq_watermark_generator.h>
#include <ydb/library/yql/dq/comp_nodes/dq_program_builder.h>
#include <ydb/library/yql/dq/runtime/streaming/dq_watermark_generator_tracker.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_watermark.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/join.h>

namespace NKikimr::NMiniKQL {

namespace {

constexpr auto Cluster = "cluster";
const auto Clusters = JoinSeq(",", {Cluster});

struct TInputItem {
    TString Cluster;
    ui64 PartitionId;
    TInstant Timestamp;
    TInstant WriteTime;
};

using TOutputItem = TMaybe<TInstant>;

class TStreamValue : public TComputationValue<TStreamValue> {
public:
    using TBase = TComputationValue<TStreamValue>;

    TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, std::vector<TInputItem> items)
        : TBase(memInfo)
        , Ctx_(ctx)
        , Items_(std::move(items))
    {
    }

    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
        if (Index_ >= Items_.size()) {
            return NUdf::EFetchStatus::Finish;
        }

        NUdf::TUnboxedValue* itemsPtr = nullptr;
        result = Ctx_.HolderFactory.CreateDirectArrayHolder(4, itemsPtr);
        itemsPtr[0] = MakeString(Items_[Index_].Cluster);
        itemsPtr[1] = NUdf::TUnboxedValuePod(Items_[Index_].PartitionId);
        itemsPtr[2] = NUdf::TUnboxedValuePod(Items_[Index_].Timestamp.MicroSeconds());
        itemsPtr[3] = NUdf::TUnboxedValuePod(Items_[Index_].WriteTime.MicroSeconds());
        ++Index_;
        return NUdf::EFetchStatus::Ok;
    }

private:
    TComputationContext& Ctx_;
    std::vector<TInputItem> Items_;
    size_t Index_ = 0;
};

THolder<IComputationGraph> BuildGraph(TDqSetup<false>& setup, const std::vector<std::string>& partitions) {
    auto& pgmBuilder = setup.GetDqProgramBuilder();
    auto stringType = pgmBuilder.NewDataType(NYql::NProto::String);
    auto ui64Type = pgmBuilder.NewDataType(NYql::NProto::Uint64);
    auto structType = pgmBuilder.NewStructType({
        {"cluster", stringType},
        {"partition_id", ui64Type},
        {"timestamp", ui64Type},
        {"write_time", ui64Type},
    });
    auto sourceNodeType = pgmBuilder.NewStreamType(structType);
    auto sourceNode = TCallableBuilder(pgmBuilder.GetTypeEnvironment(), "ExternalNode", sourceNodeType).Build();

    auto partitionsNodeType = pgmBuilder.NewListType(stringType);
    auto partitionsNode = TCallableBuilder(pgmBuilder.GetTypeEnvironment(), "ExternalNode", partitionsNodeType).Build();

    const auto watermarkExtractor = [&](TRuntimeNode item) {
        return pgmBuilder.Member(item, "timestamp");
    };
    const auto partitionKeyExtractor = [&](TRuntimeNode item) {
        return pgmBuilder.NewStruct({
            {"cluster", pgmBuilder.Member(item, "cluster")},
            {"partition_id", pgmBuilder.Member(item, "partition_id")},
        });
    };
    const auto writeTimeExtractor = [&](TRuntimeNode item) {
        return pgmBuilder.Member(item, "write_time");
    };
    const auto watermarkSettings = std::vector<std::pair<std::string, std::string>>{
        {"WatermarksGranularityUs", "1000000"},
        {"WatermarksLateArrivalDelayUs", "5000000"},
        {"WatermarksIdleTimeoutUs", "100000000"},
    };

    auto pgm = pgmBuilder.DqWatermarkGenerator(
        TRuntimeNode(sourceNode, false),
        watermarkExtractor,
        partitionKeyExtractor,
        writeTimeExtractor,
        watermarkSettings,
        TRuntimeNode(partitionsNode, false)
    );

    auto graph = setup.BuildGraph(pgm, {sourceNode, partitionsNode});

    NUdf::TUnboxedValue* itemsPtr;
    auto partitionsValue = graph->GetHolderFactory().CreateDirectArrayHolder(partitions.size(), itemsPtr);
    for (size_t i = 0; i < partitions.size(); ++i) {
        itemsPtr[i] = NUdf::TUnboxedValuePod::Embedded(partitions[i]);
    }
    graph->GetEntryPoint(1, true)->SetValue(graph->GetContext(), std::move(partitionsValue));

    return graph;
}

void TestImpl(
    const std::vector<TInputItem>& input,
    const std::vector<std::string>& partitions,
    const std::vector<TInputItem>& expectedOutput,
    const std::vector<TOutputItem>& expectedWatermarks
) {
    TWatermark watermark;
    NYql::NDq::TDqWatermarkGeneratorTracker watermarkTracker("Test ");

    TDqSetup<false> setup(GetDqNodeFactory([&](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "DqWatermarkGenerator"sv) {
            return WrapDqWatermarkGenerator(callable, ctx, watermark, &watermarkTracker);
        } else {
            return nullptr;
        }
    }));

    auto graph = BuildGraph(setup, partitions);

    auto stream = graph->GetHolderFactory().Create<TStreamValue>(graph->GetContext(), input);
    graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), std::move(stream));

    auto root = graph->GetValue();

    std::vector<TOutputItem> actualWatermarks;
    for (const auto& source : expectedOutput) {
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, root.Fetch(result));
        const auto clusterValue = result.GetElement(0);
        UNIT_ASSERT_VALUES_EQUAL(TString(clusterValue.AsStringRef()), source.Cluster);
        UNIT_ASSERT_VALUES_EQUAL(result.GetElement(1).Get<ui64>(), source.PartitionId);
        UNIT_ASSERT_VALUES_EQUAL(result.GetElement(2).Get<ui64>(), source.Timestamp.MicroSeconds());
        UNIT_ASSERT_VALUES_EQUAL(result.GetElement(3).Get<ui64>(), source.WriteTime.MicroSeconds());
        actualWatermarks.push_back(watermark.WatermarkIn);
    }

    NUdf::TUnboxedValue result;
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, root.Fetch(result));

    UNIT_ASSERT_VALUES_EQUAL(expectedWatermarks, actualWatermarks);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDqWatermarkGeneratorTest) {
    Y_UNIT_TEST(TestSinglePartitionProgressesOnEachEvent) {
        TestImpl(
            {
                {Cluster, 0, TInstant::Seconds(6), TInstant::Zero()},
                {Cluster, 0, TInstant::Seconds(7), TInstant::Zero()},
                {Cluster, 0, TInstant::Seconds(8), TInstant::Zero()},
            },
            {"0@cluster"},
            {
                {Cluster, 0, TInstant::Seconds(6), TInstant::Zero()},
                {Cluster, 0, TInstant::Seconds(7), TInstant::Zero()},
                {Cluster, 0, TInstant::Seconds(8), TInstant::Zero()},
            },
            {
                TInstant::Seconds(1),
                TInstant::Seconds(2),
                TInstant::Seconds(3),
            }
        );
    }

    Y_UNIT_TEST(TestMissingPartitionBlocksWatermarkUntilSeen) {
        TestImpl(
            {
                {Cluster, 0, TInstant::Seconds(6), TInstant::Zero()},
                {Cluster, 0, TInstant::Seconds(8), TInstant::Zero()},
                {Cluster, 1, TInstant::Seconds(7), TInstant::Zero()},
            },
            {"0@cluster", "1@cluster"},
            {
                {Cluster, 0, TInstant::Seconds(6), TInstant::Zero()},
                {Cluster, 0, TInstant::Seconds(8), TInstant::Zero()},
                {Cluster, 1, TInstant::Seconds(7), TInstant::Zero()},
            },
            {
                Nothing(),
                Nothing(),
                TInstant::Seconds(2),
            }
        );
    }

    Y_UNIT_TEST(TestSinglePartitionDataDoesNotAdvanceIncompleteSet) {
        TestImpl(
            {
                {Cluster, 0, TInstant::Seconds(6), TInstant::Zero()},
                {Cluster, 0, TInstant::Seconds(7), TInstant::Zero()},
                {Cluster, 0, TInstant::Seconds(8), TInstant::Zero()},
            },
            {"0@cluster", "1@cluster"},
            {
                {Cluster, 0, TInstant::Seconds(6), TInstant::Zero()},
                {Cluster, 0, TInstant::Seconds(7), TInstant::Zero()},
                {Cluster, 0, TInstant::Seconds(8), TInstant::Zero()},
            },
            {
                Nothing(),
                Nothing(),
                Nothing(),
            }
        );
    }

    Y_UNIT_TEST(TestEarlyEventIsDroppedAndDoesNotAdvanceWatermark) {
        TestImpl(
            {
                {Cluster, 0, TInstant::Seconds(6), TInstant::Seconds(6)},
                {Cluster, 0, TInstant::Minutes(10), TInstant::Seconds(2)},
                {Cluster, 0, TInstant::Seconds(8), TInstant::Seconds(8)},
            },
            {"0@cluster"},
            {
                {Cluster, 0, TInstant::Seconds(6), TInstant::Seconds(6)},
                {Cluster, 0, TInstant::Seconds(8), TInstant::Seconds(8)},
            },
            {
                TInstant::Seconds(1),
                TInstant::Seconds(3),
            }
        );
    }

    Y_UNIT_TEST(TestLateEventIsForwardedAndDoesNotRegressWatermark) {
        TestImpl(
            {
                {Cluster, 0, TInstant::Seconds(10), TInstant::Seconds(10)},
                {Cluster, 0, TInstant::Seconds(4), TInstant::Seconds(9)},
                {Cluster, 0, TInstant::Seconds(11), TInstant::Seconds(11)},
            },
            {"0@cluster"},
            {
                {Cluster, 0, TInstant::Seconds(10), TInstant::Seconds(10)},
                {Cluster, 0, TInstant::Seconds(4), TInstant::Seconds(9)},
                {Cluster, 0, TInstant::Seconds(11), TInstant::Seconds(11)},
            },
            {
                TInstant::Seconds(5),
                TInstant::Seconds(5),
                TInstant::Seconds(6),
            }
        );
    }

    Y_UNIT_TEST(TestWatermarkIsRoundedToGranularity) {
        TestImpl(
            {
                {Cluster, 0, TInstant::MilliSeconds(6500), TInstant::Zero()},
                {Cluster, 0, TInstant::MilliSeconds(7500), TInstant::Zero()},
            },
            {"0@cluster"},
            {
                {Cluster, 0, TInstant::MilliSeconds(6500), TInstant::Zero()},
                {Cluster, 0, TInstant::MilliSeconds(7500), TInstant::Zero()},
            },
            {
                TInstant::Seconds(1),
                TInstant::Seconds(2),
            }
        );
    }
}

} // namespace NKikimr::NMiniKQL
