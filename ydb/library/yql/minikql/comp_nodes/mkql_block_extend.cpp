#include "mkql_block_extend.h"

#include <ydb/library/yql/minikql/computation/mkql_block_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_block_reader.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TExtendBlocks : public TStatefulWideFlowBlockComputationNode<TExtendBlocks> {
public:
    TExtendBlocks(TComputationMutables& mutables, TVector<IComputationWideFlowNode*>&& flows, TVector<TType*>&& types)
        : TStatefulWideFlowBlockComputationNode(mutables, this, types.size())
        , Flows_(std::move(flows))
        , Types_(std::move(types))
        , Width_(Types_.size())
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const
    {
        auto& s = GetState(state, ctx, output);

        size_t yieldCount = 0;
        while (s.LiveInputs) {
            Y_VERIFY_DEBUG(s.InputIndex < s.LiveInputs);
            EFetchResult result = s.Inputs[s.InputIndex]->FetchValues(ctx, output);
            yieldCount = (result == EFetchResult::Yield) ? (yieldCount + 1) : 0;
            if (result == EFetchResult::Finish) {
                std::swap(s.Inputs[s.InputIndex], s.Inputs[--s.LiveInputs]);
                s.NextInput();
                continue;
            }
            if (result == EFetchResult::Yield) {
                s.NextInput();
                if (yieldCount == s.LiveInputs) {
                    return result;
                }
                continue;
            }

            MKQL_ENSURE_S(output[Width_ - 1]);
            const ui64 len = TArrowBlock::From(*output[Width_ - 1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
            for (ui32 i = 0; i + 1 < Width_; ++i) {
                if (s.Builders[i]) {
                    MKQL_ENSURE_S(output[i]);
                    auto datum = TArrowBlock::From(*output[i]).GetDatum();
                    if (datum.is_scalar()) {
                        TBlockItem item = s.Readers[i]->GetScalarItem(*datum.scalar());
                        s.Builders[i]->Add(item, len);
                        *output[i] = ctx.HolderFactory.CreateArrowBlock(s.Builders[i]->Build(false));
                    }
                }
            }
            s.NextInput();
            return result;
        }
        return EFetchResult::Finish;
    }
private:
    struct TState : public TComputationValue<TState> {
        TVector<IComputationWideFlowNode*> Inputs;
        size_t LiveInputs;
        size_t InputIndex = 0;
        TVector<std::unique_ptr<IArrayBuilder>> Builders;
        TVector<std::unique_ptr<IBlockReader>> Readers;

        TState(TMemoryUsageInfo* memInfo, const TVector<IComputationWideFlowNode*>& inputs, ui32 width, NUdf::TUnboxedValue*const* output, const TVector<TType*>& types,
            arrow::MemoryPool& pool, const NUdf::IPgBuilder& pgBuilder)
            : TComputationValue(memInfo)
            , Inputs(inputs)
            , LiveInputs(Inputs.size())
            , Builders(width)
            , Readers(width)
        {
            TTypeInfoHelper helper;
            for (ui32 i = 0; i < width; ++i) {
                if (output[i] && i + 1 != width) {
                    Builders[i] = MakeArrayBuilder(helper, types[i], pool, helper.GetMaxBlockLength(types[i]), &pgBuilder);
                    Readers[i] = MakeBlockReader(helper, types[i]);
                }
            }
        }

        void NextInput() {
            if (++InputIndex >= LiveInputs) {
                InputIndex = 0;
            }
        }
    };

    void RegisterDependencies() const final {
        for (auto& flow : Flows_) {
            FlowDependsOn(flow);
        }
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(Flows_, Width_, output, Types_, ctx.ArrowMemoryPool, ctx.Builder->GetPgBuilder());
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

private:
    const TVector<IComputationWideFlowNode*> Flows_;
    const TVector<TType*> Types_;
    const ui32 Width_;
};

} // namespace

IComputationNode* WrapBlockExtend(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() > 0, "Expected at least 1 arg");

    const auto flowType = AS_TYPE(TFlowType, callable.GetType()->GetReturnType());
    const auto wideComponents = GetWideComponents(flowType);
    const ui32 width = wideComponents.size();
    MKQL_ENSURE(width > 0, "Expected at least one column");

    TVector<TType*> types;
    for (auto& type : wideComponents) {
        types.emplace_back(AS_TYPE(TBlockType, type)->GetItemType());
    }

    TVector<IComputationWideFlowNode*> flows;
    for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
        auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, i));
        MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");
        flows.emplace_back(wideFlow);
    }

    return new TExtendBlocks(ctx.Mutables, std::move(flows), std::move(types));
}


} // namespace NMiniKQL
} // namespace NKikimr
