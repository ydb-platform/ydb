#include "utils/dq_setup.h"
#include "utils/dq_factories.h"

#include <ydb/library/yql/dq/comp_nodes/dq_watermark_generator.h>
#include <ydb/library/yql/dq/comp_nodes/dq_program_builder.h>
#include <ydb/library/yql/dq/runtime/streaming/dq_source_watermark_tracker.h>
#include <ydb/library/yql/dq/runtime/streaming/partition_key.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_watermark.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

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
        result = Ctx_.HolderFactory.CreateDirectArrayHolder(3, itemsPtr);
        itemsPtr[0] = MakeString(Items_[Index_].Cluster);
        itemsPtr[1] = NUdf::TUnboxedValuePod(Items_[Index_].PartitionId);
        itemsPtr[2] = NUdf::TUnboxedValuePod(Items_[Index_].Timestamp.MicroSeconds());
        ++Index_;
        return NUdf::EFetchStatus::Ok;
    }

private:
    TComputationContext& Ctx_;
    std::vector<TInputItem> Items_;
    size_t Index_ = 0;
};

THolder<IComputationGraph> BuildGraph(TDqSetup<false>& setup, const std::vector<ui64>& partitions) {
    auto& pgmBuilder = setup.GetDqProgramBuilder();
    auto stringType = pgmBuilder.NewDataType(NYql::NProto::String);
    auto ui64Type = pgmBuilder.NewDataType(NYql::NProto::Uint64);
    auto structType = pgmBuilder.NewStructType({
        {"cluster", stringType},
        {"partition_id", ui64Type},
        {"timestamp", ui64Type},
    });
    auto sourceNodeType = pgmBuilder.NewStreamType(structType);
    auto sourceNode = TCallableBuilder(pgmBuilder.GetTypeEnvironment(), "ExternalNode", sourceNodeType).Build();

    auto partitionsNodeType = pgmBuilder.NewListType(ui64Type);
    auto partitionsNode = TCallableBuilder(pgmBuilder.GetTypeEnvironment(), "ExternalNode", partitionsNodeType).Build();

    auto pgm = pgmBuilder.DqWatermarkGenerator(
        TRuntimeNode(sourceNode, false),
        [&](TRuntimeNode item) { return pgmBuilder.Member(item, "timestamp"); },
        [&](TRuntimeNode item) {
            return pgmBuilder.NewStruct({
                {"cluster", pgmBuilder.Member(item, "cluster")},
                {"partition_id", pgmBuilder.Member(item, "partition_id")},
            });
        },
        TConstArrayRef<std::pair<std::string_view, std::string_view>>({
            {"WatermarksGranularityUs", "1000000"},
            {"WatermarksLateArrivalDelayUs", "0"},
            {"WatermarksIdleTimeoutUs", "100000000"},
            {"FederatedClusters", Clusters},
        }),
        TRuntimeNode(partitionsNode, false)
    );

    auto graph = setup.BuildGraph(pgm, {sourceNode, partitionsNode});

    NUdf::TUnboxedValue* itemsPtr;
    auto partitionsValue = graph->GetHolderFactory().CreateDirectArrayHolder(partitions.size(), itemsPtr);
    for (size_t i = 0; i < partitions.size(); ++i) {
        itemsPtr[i] = NUdf::TUnboxedValuePod(partitions[i]);
    }
    graph->GetEntryPoint(1, true)->SetValue(graph->GetContext(), std::move(partitionsValue));

    return graph;
}

void TestImpl(
    const std::vector<TInputItem>& input,
    const std::vector<ui64>& partitions,
    const std::vector<TOutputItem>& expected
) {
    TWatermark watermark;
    NYql::NDq::TDqSourceWatermarkTracker<NYql::NDq::TPartitionKey> watermarkTracker("Test ");

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

    std::vector<TOutputItem> actual;
    for (const auto& source : input) {
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, root.Fetch(result));
        NUdf::AssertUnboxedValueElementEqual(result, std::make_tuple(source.Cluster, source.PartitionId, source.Timestamp.MicroSeconds()));
        actual.push_back(watermark.WatermarkIn);
    }

    NUdf::TUnboxedValue result;
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, root.Fetch(result));

    UNIT_ASSERT_VALUES_EQUAL(expected, actual);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDqWatermarkGeneratorTest) {
    Y_UNIT_TEST(TestSinglePartitionProgressesOnEachEvent) {
        TestImpl(
            {
                {Cluster, 0, TInstant::Seconds(1)},
                {Cluster, 0, TInstant::Seconds(2)},
                {Cluster, 0, TInstant::Seconds(3)},
            },
            {0},
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
                {Cluster, 0, TInstant::Seconds(1)},
                {Cluster, 0, TInstant::Seconds(3)},
                {Cluster, 1, TInstant::Seconds(2)},
            },
            {0, 1},
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
                {Cluster, 0, TInstant::Seconds(1)},
                {Cluster, 0, TInstant::Seconds(2)},
                {Cluster, 0, TInstant::Seconds(3)},
            },
            {0, 1},
            {
                Nothing(),
                Nothing(),
                Nothing(),
            }
        );
    }
}

} // namespace NKikimr::NMiniKQL
