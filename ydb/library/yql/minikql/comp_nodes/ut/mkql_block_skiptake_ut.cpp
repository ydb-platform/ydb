#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE

#include <arrow/array/builder_primitive.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TTestBlockFlowWrapper: public TStatefulWideFlowCodegeneratorNode<TTestBlockFlowWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TTestBlockFlowWrapper>;

public:
    TTestBlockFlowWrapper(TComputationMutables& mutables, size_t blockSize, size_t blockCount)
        : TBaseComputation(mutables, nullptr, EValueRepresentation::Embedded)
        , BlockSize(blockSize)
        , BlockCount(blockCount)
    {
        mutables.CurValueIndex += 3U;
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        return DoCalculateImpl(state, ctx, *output[0], *output[1], *output[2]);
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto statusType = Type::getInt32Ty(context);

        const auto atTop = &ctx.Func->getEntryBlock().back();

        const auto values0Ptr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), static_cast<const IComputationNode*>(this)->GetIndex() + 1U)}, "values_0_ptr", atTop);
        const auto values1Ptr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), static_cast<const IComputationNode*>(this)->GetIndex() + 2U)}, "values_1_ptr", atTop);
        const auto values2Ptr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), static_cast<const IComputationNode*>(this)->GetIndex() + 3U)}, "values_2_ptr", atTop);

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", atTop);

        const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TTestBlockFlowWrapper::DoCalculateImpl));
        const auto doType = FunctionType::get(statusType, {self->getType(), ptrValueType,  ctx.Ctx->getType(), ptrValueType, ptrValueType, ptrValueType}, false);
        const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(doType), "function", atTop);

        const auto result = CallInst::Create(doType, doFuncPtr, {self, statePtr, ctx.Ctx, values0Ptr, values1Ptr, values2Ptr}, "result", block);

        ICodegeneratorInlineWideNode::TGettersList getters{
            [values0Ptr, valueType](const TCodegenContext&, BasicBlock*& block) { return new LoadInst(valueType, values0Ptr, "value", block); },
            [values1Ptr, valueType](const TCodegenContext&, BasicBlock*& block) { return new LoadInst(valueType, values1Ptr, "value", block); },
            [values2Ptr, valueType](const TCodegenContext&, BasicBlock*& block) { return new LoadInst(valueType, values2Ptr, "value", block); }
        };
        return {result, std::move(getters)};
    }
#endif
private:
    EFetchResult DoCalculateImpl(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue& val1, NUdf::TUnboxedValue& val2, NUdf::TUnboxedValue& val3) const {
        if (!state.HasValue()) {
            state = NUdf::TUnboxedValue::Zero();
        }

        auto index = state.Get<ui64>();
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

        val1 = ctx.HolderFactory.CreateArrowBlock(std::move(block));
        val2 = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(index)));
        val3 = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(BlockSize)));

        state = NUdf::TUnboxedValuePod(++index);
        return EFetchResult::One;
    }

    void RegisterDependencies() const final {
    }

    const size_t BlockSize;
    const size_t BlockCount;
};

IComputationNode* WrapTestBlockFlow(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 0, "Expected no args");
    return new TTestBlockFlowWrapper(ctx.Mutables, 5, 2);
}

TComputationNodeFactory GetNodeFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "TestBlockFlow") {
            return WrapTestBlockFlow(callable, ctx);
        }
        return GetBuiltinFactory()(callable, ctx);
    };

} //namespace

template<bool LLVM>
TRuntimeNode MakeFlow(TSetup<LLVM>& setup) {
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
    Y_UNIT_TEST_LLVM(TestWideSkipBlocks) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto flow = MakeFlow(setup);

        const auto part = pb.WideSkipBlocks(flow, pb.NewDataLiteral<ui64>(7));
        const auto plain = pb.WideFromBlocks(part);

        const auto singleValueFlow = pb.NarrowMap(plain, [&](TRuntimeNode::TList items) -> TRuntimeNode {
            return pb.Add(items[0], items[1]);
        });

        const auto pgmReturn = pb.ForwardList(singleValueFlow);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 8);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 9);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 10);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestWideTakeBlocks) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto flow = MakeFlow(setup);

        const auto part = pb.WideTakeBlocks(flow, pb.NewDataLiteral<ui64>(4));
        const auto plain = pb.WideFromBlocks(part);

        const auto singleValueFlow = pb.NarrowMap(plain, [&](TRuntimeNode::TList items) -> TRuntimeNode {
            return pb.Add(items[0], items[1]);
        });

        const auto pgmReturn = pb.ForwardList(singleValueFlow);

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();

        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 0);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 1);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 2);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 3);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestWideTakeSkipBlocks) {
        TSetup<LLVM> setup(GetNodeFactory());
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

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
}

} // namespace NMiniKQL
} // namespace NKikimr


