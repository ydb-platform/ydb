#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <arrow/array/builder_primitive.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TTestBlockFlowWrapper: public TStatefulWideFlowComputationNode<TTestBlockFlowWrapper> {
    typedef TStatefulWideFlowComputationNode<TTestBlockFlowWrapper> TBaseComputation;

public:
    TTestBlockFlowWrapper(TComputationMutables& mutables, size_t blockSize, size_t blockCount)
        : TBaseComputation(mutables, nullptr, EValueRepresentation::Any)
        , BlockSize(blockSize)
        , BlockCount(blockCount)
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.HasValue()) {
            state = NUdf::TUnboxedValue::Zero();
        }

        ui64 index = state.Get<ui64>();
        if (index >= BlockCount) {
            return EFetchResult::Finish;
        }

        arrow::UInt64Builder builder(&ctx.ArrowMemoryPool);
        ARROW_OK(builder.Reserve(BlockSize));
        for (size_t i = 0; i < BlockSize; ++i) {
            builder.UnsafeAppend(index * BlockSize + i);
        }

        std::shared_ptr<arrow::ArrayData> block;
        ARROW_OK(builder.FinishInternal(&block));

        *output[0] = ctx.HolderFactory.CreateArrowBlock(std::move(block));
        *output[1] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(index)));
        *output[2] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(BlockSize)));

        state = NUdf::TUnboxedValuePod(++index);
        return EFetchResult::One;
    }

private:
    void RegisterDependencies() const final {
    }

    const size_t BlockSize;
    const size_t BlockCount;
};

IComputationNode* WrapTestBlockFlow(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 0, "Expected no args");
    return new TTestBlockFlowWrapper(ctx.Mutables, 5, 2);
}

TIntrusivePtr<IRandomProvider> CreateRandomProvider() {
    return CreateDeterministicRandomProvider(1);
}

TIntrusivePtr<ITimeProvider> CreateTimeProvider() {
    return CreateDeterministicTimeProvider(10000000);
}

TComputationNodeFactory GetTestFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "TestBlockFlow") {
            return WrapTestBlockFlow(callable, ctx);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

struct TSetup_ {
    TSetup_()
    : Alloc(__LOCATION__)
    {
        FunctionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        RandomProvider = CreateRandomProvider();
        TimeProvider = CreateTimeProvider();

        Env.Reset(new TTypeEnvironment(Alloc));
        PgmBuilder.Reset(new TProgramBuilder(*Env, *FunctionRegistry));
    }

    TAutoPtr<IComputationGraph> BuildGraph(TRuntimeNode pgm, EGraphPerProcess graphPerProcess = EGraphPerProcess::Multi, const std::vector<TNode*>& entryPoints = std::vector<TNode*>()) {
        Explorer.Walk(pgm.GetNode(), *Env);
        TComputationPatternOpts opts(Alloc.Ref(), *Env, GetTestFactory(), FunctionRegistry.Get(),
                                     NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception, "OFF", graphPerProcess);
        Pattern = MakeComputationPattern(Explorer, pgm, entryPoints, opts);
        return Pattern->Clone(opts.ToComputationOptions(*RandomProvider, *TimeProvider));
    }

    TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TIntrusivePtr<ITimeProvider> TimeProvider;

    TScopedAlloc Alloc;
    THolder<TTypeEnvironment> Env;
    THolder<TProgramBuilder> PgmBuilder;

    TExploringNodeVisitor Explorer;
    IComputationPattern::TPtr Pattern;
};

TRuntimeNode MakeFlow(TSetup_& setup) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    TCallableBuilder callableBuilder(*setup.Env, "TestBlockFlow",
                                     pb.NewFlowType(
                                         pb.NewMultiType({
                                             pb.NewBlockType(pb.NewDataType(NUdf::EDataSlot::Uint64), TBlockType::EShape::Many),
                                             pb.NewBlockType(pb.NewDataType(NUdf::EDataSlot::Uint64), TBlockType::EShape::Scalar),
                                             pb.NewBlockType(pb.NewDataType(NUdf::EDataSlot::Uint64), TBlockType::EShape::Scalar),
                                             })));
    return TRuntimeNode(callableBuilder.Build(), false);
}

} // namespace


Y_UNIT_TEST_SUITE(TMiniKQLWideTakeSkipBlocks) {
    Y_UNIT_TEST(TestWideTakeSkipBlocks) {
        TSetup_ setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto flow = MakeFlow(setup);

        const auto part = pb.WideTakeBlocks(pb.WideSkipBlocks(flow, pb.NewDataLiteral<ui64>(3)), pb.NewDataLiteral<ui64>(5));
        const auto plain = pb.WideFromBlocks(part);

        const auto singleValueFlow = pb.NarrowMap(plain, [&](TRuntimeNode::TList items) -> TRuntimeNode {
            // 0,  0;
            // 1,  0;
            // 2,  0;
            // 3,  0; -> 3
            // 4,  0; -> 4
            // 5,  1; -> 6
            // 6,  1; -> 7
            // 7,  1; -> 8
            // 8,  1;
            // 9,  1;
            // 10, 1;
            return pb.Add(items[0], items[1]);
        });

        const auto pgmReturn = pb.ForwardList(singleValueFlow);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 3);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 4);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 6);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 7);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 8);
    }
}

} // namespace NMiniKQL
} // namespace NKikimr


