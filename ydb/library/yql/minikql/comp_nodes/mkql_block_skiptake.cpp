#include "mkql_block_skiptake.h"

#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TWideSkipBlocksWrapper: public TStatefulWideFlowBlockComputationNode<TWideSkipBlocksWrapper> {
    typedef TStatefulWideFlowBlockComputationNode<TWideSkipBlocksWrapper> TBaseComputation;

public:
    TWideSkipBlocksWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, IComputationNode* count, size_t width)
        : TBaseComputation(mutables, flow, width)
        , Flow(flow)
        , Count(count)
        , Width(width)
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.HasValue()) {
            state = Count->GetValue(ctx);
        }

        Y_VERIFY(output[Width - 1]);
        auto count = state.Get<ui64>();
        ui64 blockSize = 0;
        for (;;) {
            auto result = Flow->FetchValues(ctx, output);
            if (count == 0 || result != EFetchResult::One) {
                return result;
            }

            blockSize = TArrowBlock::From(*output[Width - 1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
            if (blockSize > count) {
                break;
            }
            count -= blockSize;
            state = NUdf::TUnboxedValuePod(count);
        }

        ui64 tailSize = blockSize - count;
        for (size_t i = 0; i < Width - 1; ++i) {
            if (!output[i]) {
                continue;
            }
            auto& datum = TArrowBlock::From(*output[i]).GetDatum();
            if (datum.is_scalar()) {
                continue;
            }

            Y_VERIFY_DEBUG(datum.is_array());
            *output[i] = ctx.HolderFactory.CreateArrowBlock(DeepSlice(datum.array(), count, tailSize));
        }

        *output[Width - 1] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(static_cast<uint64_t>(tailSize)));
        state = NUdf::TUnboxedValuePod::Zero();
        return EFetchResult::One;
    }

private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            DependsOn(flow, Count);
        }
    }

    IComputationWideFlowNode* const Flow;
    IComputationNode* const Count;
    const size_t Width;
};

class TWideTakeBlocksWrapper: public TStatefulWideFlowBlockComputationNode<TWideTakeBlocksWrapper> {
    typedef TStatefulWideFlowBlockComputationNode<TWideTakeBlocksWrapper> TBaseComputation;

public:
    TWideTakeBlocksWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, IComputationNode* count, size_t width)
        : TBaseComputation(mutables, flow, width)
        , Flow(flow)
        , Count(count)
        , Width(width)
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.HasValue()) {
            state = Count->GetValue(ctx);
        }

        auto count = state.Get<ui64>();
        if (!count) {
            return EFetchResult::Finish;
        }

        Y_VERIFY(output[Width - 1]);
        auto result = Flow->FetchValues(ctx, output);
        if (result == EFetchResult::One) {
            ui64 blockSize = TArrowBlock::From(*output[Width - 1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
            if (blockSize > count) {
                for (size_t i = 0; i < Width - 1; ++i) {
                    if (!output[i]) {
                        continue;
                    }
                    auto& datum = TArrowBlock::From(*output[i]).GetDatum();
                    if (datum.is_scalar()) {
                        continue;
                    }

                    Y_VERIFY_DEBUG(datum.is_array());
                    *output[i] = ctx.HolderFactory.CreateArrowBlock(DeepSlice(datum.array(), 0, count));
                }
                *output[Width - 1] = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(static_cast<uint64_t>(count)));
                state = NUdf::TUnboxedValuePod::Zero();
            } else {
                state = NUdf::TUnboxedValuePod(count - blockSize);
            }
        }
        return result;
    }

private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            DependsOn(flow, Count);
        }
    }

    IComputationWideFlowNode* const Flow;
    IComputationNode* const Count;
    const size_t Width;
};

IComputationNode* WrapSkipTake(bool skip, TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto flowWidth = GetWideComponentsCount(flowType);
    MKQL_ENSURE(flowWidth > 0, "Expected at least one column");

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    const auto count = LocateNode(ctx.NodeLocator, callable, 1);
    const auto countType = AS_TYPE(TDataType, callable.GetInput(1).GetStaticType());
    MKQL_ENSURE(countType->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");

    if (skip) {
        return new TWideSkipBlocksWrapper(ctx.Mutables, wideFlow, count, flowWidth);
    }
    return new TWideTakeBlocksWrapper(ctx.Mutables, wideFlow, count, flowWidth);
}

} //namespace

IComputationNode* WrapWideSkipBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    bool skip = true;
    return WrapSkipTake(skip, callable, ctx);
}

IComputationNode* WrapWideTakeBlocks(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    bool skip = false;
    return WrapSkipTake(skip, callable, ctx);
}

}
}
