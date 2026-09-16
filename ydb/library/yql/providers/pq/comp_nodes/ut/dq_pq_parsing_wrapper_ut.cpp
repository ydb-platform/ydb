#include <ydb/library/yql/dq/comp_nodes/ut/utils/dq_factories.h>
#include <ydb/library/yql/dq/comp_nodes/ut/utils/dq_setup.h>
#include <ydb/library/yql/providers/pq/comp_nodes/dq_pq_parsing_wrapper.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL {

namespace {

struct TInputItem {
    TString Data;
    ui64 PartitionId = 0;
    TString Cluster;
};

struct TLayout {
    ui32 SourceDataIndex = 0;
    ui32 SourcePartitionIdIndex = 0;
    ui32 SourceClusterIndex = 0;
    ui32 OutputDataIndex = 0;
    ui32 OutputPartitionIdIndex = 0;
    ui32 OutputClusterIndex = 0;
    ui32 OutputWatermarkPartitionIdIndex = 0;
    ui32 OutputWatermarkClusterIndex = 0;
};

class TStreamValue : public TComputationValue<TStreamValue> {
public:
    using TBase = TComputationValue<TStreamValue>;

    TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& ctx, std::vector<TInputItem> items, TLayout layout)
        : TBase(memInfo)
        , Ctx_(ctx)
        , Items_(std::move(items))
        , Layout_(layout)
    {
    }

    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
        if (Index_ >= Items_.size()) {
            return NUdf::EFetchStatus::Finish;
        }

        const auto& item = Items_[Index_++];
        NUdf::TUnboxedValue* itemsPtr = nullptr;
        result = Ctx_.HolderFactory.CreateDirectArrayHolder(3, itemsPtr);
        itemsPtr[Layout_.SourceDataIndex] = MakeString(item.Data);
        itemsPtr[Layout_.SourcePartitionIdIndex] = NUdf::TUnboxedValuePod(item.PartitionId);
        itemsPtr[Layout_.SourceClusterIndex] = MakeString(item.Cluster);
        return NUdf::EFetchStatus::Ok;
    }

private:
    TComputationContext& Ctx_;
    std::vector<TInputItem> Items_;
    const TLayout Layout_;
    size_t Index_ = 0;
};

THolder<IComputationGraph> BuildGraph(TDqSetup<false>& setup, TCallable*& sourceNode, TLayout& layout) {
    auto& pb = setup.GetDqProgramBuilder();

    auto stringType = pb.NewDataType(NUdf::EDataSlot::String);
    auto ui64Type = pb.NewDataType(NUdf::EDataSlot::Uint64);

    auto sourceItemType = pb.NewStructType({
        {"Data", stringType},
        {"__ydb_partition_id", ui64Type},
        {"__ydb_cluster", stringType},
    });
    auto lambdaItemType = pb.NewStructType({
        {"Data", stringType},
        {"__ydb_partition_id", ui64Type},
        {"__ydb_cluster", stringType},
    });
    auto outputItemType = pb.NewStructType({
        {"Data", stringType},
        {"__ydb_partition_id", ui64Type},
        {"__ydb_cluster", stringType},
        {"__ydb_watermark_partition_id", ui64Type},
        {"__ydb_watermark_cluster", stringType},
    });

    const auto sourceStructType = AS_TYPE(TStructType, sourceItemType);
    const auto outputStructType = AS_TYPE(TStructType, outputItemType);
    layout.SourceDataIndex = sourceStructType->GetMemberIndex("Data");
    layout.SourcePartitionIdIndex = sourceStructType->GetMemberIndex("__ydb_partition_id");
    layout.SourceClusterIndex = sourceStructType->GetMemberIndex("__ydb_cluster");
    layout.OutputDataIndex = outputStructType->GetMemberIndex("Data");
    layout.OutputPartitionIdIndex = outputStructType->GetMemberIndex("__ydb_partition_id");
    layout.OutputClusterIndex = outputStructType->GetMemberIndex("__ydb_cluster");
    layout.OutputWatermarkPartitionIdIndex = outputStructType->GetMemberIndex("__ydb_watermark_partition_id");
    layout.OutputWatermarkClusterIndex = outputStructType->GetMemberIndex("__ydb_watermark_cluster");

    auto sourceStreamType = pb.NewStreamType(sourceItemType);
    auto lambdaStreamType = pb.NewStreamType(lambdaItemType);
    auto outputStreamType = pb.NewStreamType(outputItemType);

    sourceNode = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", sourceStreamType).Build();
    auto lambdaArgNode = TCallableBuilder(pb.GetTypeEnvironment(), "ExternalNode", lambdaStreamType).Build();
    auto lambdaArg = TRuntimeNode(lambdaArgNode, false);
    auto lambdaBody = pb.Map(lambdaArg, [](TRuntimeNode item) -> TRuntimeNode {
        return item;
    });

    const TRuntimeNode::TList metadataMapping = {
        pb.NewDataLiteral<NUdf::EDataSlot::String>("__ydb_partition_id"),
        pb.NewDataLiteral<NUdf::EDataSlot::String>("__ydb_watermark_partition_id"),
        pb.NewDataLiteral<NUdf::EDataSlot::String>("__ydb_cluster"),
        pb.NewDataLiteral<NUdf::EDataSlot::String>("__ydb_watermark_cluster"),
    };

    auto callable = TCallableBuilder(pb.GetTypeEnvironment(), "DqPqParsingWrapper", outputStreamType)
        .Add(TRuntimeNode(sourceNode, false))
        .Add(lambdaArg)
        .Add(lambdaBody)
        .Add(pb.NewList(stringType, metadataMapping))
        .Build();

    return setup.BuildGraph(TRuntimeNode(callable, false), {sourceNode});
}

TComputationNodeFactory GetTestNodeFactory() {
    return GetDqNodeFactory([](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "DqPqParsingWrapper"sv) {
            return WrapDqPqParsingWrapper(callable, ctx);
        }
        return nullptr;
    });
}

void SetInput(IComputationGraph& graph, TLayout layout) {
    graph.GetEntryPoint(0, true)->SetValue(
        graph.GetContext(),
        graph.GetHolderFactory().Create<TStreamValue>(
            graph.GetContext(),
            std::vector<TInputItem>{
                {"first", 11, "cluster-a"},
                {"second", 22, "cluster-b"},
            },
            layout));
}

void AssertRow(const NUdf::TUnboxedValue& row, const TLayout& layout, const TInputItem& expected) {
    const auto data = row.GetElement(layout.OutputDataIndex);
    const auto cluster = row.GetElement(layout.OutputClusterIndex);
    const auto watermarkCluster = row.GetElement(layout.OutputWatermarkClusterIndex);
    UNIT_ASSERT_VALUES_EQUAL(TString(data.AsStringRef()), expected.Data);
    UNIT_ASSERT_VALUES_EQUAL(row.GetElement(layout.OutputPartitionIdIndex).Get<ui64>(), expected.PartitionId);
    UNIT_ASSERT_VALUES_EQUAL(TString(cluster.AsStringRef()), expected.Cluster);
    UNIT_ASSERT_VALUES_EQUAL(row.GetElement(layout.OutputWatermarkPartitionIdIndex).Get<ui64>(), expected.PartitionId);
    UNIT_ASSERT_VALUES_EQUAL(TString(watermarkCluster.AsStringRef()), expected.Cluster);
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TDqPqParsingWrapperTest) {
    Y_UNIT_TEST(PassesMetadataThroughLambdaAndAppendsWatermarkMetadata) {
        TDqSetup<false> setup(GetTestNodeFactory());
        TCallable* sourceNode = nullptr;
        TLayout layout;
        auto graph = BuildGraph(setup, sourceNode, layout);
        Y_UNUSED(sourceNode);
        SetInput(*graph, layout);
        auto root = graph->GetValue();

        NUdf::TUnboxedValue result;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, root.Fetch(result));
        AssertRow(result, layout, {"first", 11, "cluster-a"});

        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, root.Fetch(result));
        AssertRow(result, layout, {"second", 22, "cluster-b"});

        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, root.Fetch(result));
    }
}

} // namespace NKikimr::NMiniKQL
