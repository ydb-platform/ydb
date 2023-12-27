#include "mkql_wide_chain_map.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/utils/cast.h>

namespace NKikimr {
namespace NMiniKQL {

using NYql::EnsureDynamicCast;

namespace {

class TWideChain1MapWrapper : public TStatefulWideFlowCodegeneratorNode<TWideChain1MapWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideChain1MapWrapper>;
public:
     TWideChain1MapWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow,
            TComputationExternalNodePtrVector&& inputs,
            TComputationNodePtrVector&& initItems,
            TComputationExternalNodePtrVector&& outputs,
            TComputationNodePtrVector&& updateItems)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded)
        , Flow(flow)
        , Inputs(std::move(inputs))
        , InitItems(std::move(initItems))
        , Outputs(std::move(outputs))
        , UpdateItems(std::move(updateItems))
        , InputsOnInit(GetPasstroughtMapOneToOne(Inputs, InitItems))
        , InputsOnUpdate(GetPasstroughtMapOneToOne(Inputs, UpdateItems))
        , InitOnInputs(GetPasstroughtMapOneToOne(InitItems, Inputs))
        , UpdateOnInputs(GetPasstroughtMapOneToOne(UpdateItems, Inputs))
        , OutputsOnUpdate(GetPasstroughtMapOneToOne(Outputs, UpdateItems))
        , UpdateOnOutputs(GetPasstroughtMapOneToOne(UpdateItems, Outputs))
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Inputs.size()))
        , TempStateIndex(std::exchange(mutables.CurValueIndex, mutables.CurValueIndex + Outputs.size()))
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (state.IsInvalid()) {
            state = NUdf::TUnboxedValuePod();
            return CalculateFirst(ctx, output);
        }

        return CalculateOther(ctx, output);
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto flagType = Type::getInt1Ty(context);
        const auto flagPtr = new AllocaInst(flagType, 0U, "flag_ptr", &ctx.Func->getEntryBlock().back());

        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto getres = GetNodeValues(Flow, ctx, block);

        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), 0), "special", block);
        BranchInst::Create(done, good, special, block);

        block = good;
        for (auto i = 0U; i < Inputs.size(); ++i)
            if (Inputs[i]->GetDependencesCount() > 0U || !InputsOnInit[i] || !InputsOnUpdate[i])
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Inputs[i])->CreateSetValue(ctx, block, getres.second[i](ctx, block));

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);

        const auto flag = IsInvalid(statePtr, block);
        new StoreInst(flag, flagPtr, block);
        BranchInst::Create(init, next, flag, block);

        block = init;
        for (auto i = 0U; i < Outputs.size(); ++i) {
            if (Outputs[i]->GetDependencesCount() > 0U || OutputsOnUpdate[i]) {
                const auto& map = InitOnInputs[i];
                const auto value = map ? getres.second[*map](ctx, block) : GetNodeValue(InitItems[i], ctx, block);
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Outputs[i])->CreateSetValue(ctx, block, value);
            }
        }

        new StoreInst(GetEmpty(context), statePtr, block);
        BranchInst::Create(done, block);

        block = next;

        std::vector<Value*> outputs(Outputs.size(), nullptr);
        for (auto i = 0U; i < outputs.size(); ++i) {
            if (const auto& dep = OutputsOnUpdate[i]; Outputs[i]->GetDependencesCount() > 0U || (dep && *dep != i)) {
                const auto& map = UpdateOnInputs[i];
                outputs[i] = map ? getres.second[*map](ctx, block) : GetNodeValue(UpdateItems[i], ctx, block);
            }
        }

        for (auto i = 0U; i < outputs.size(); ++i)
            if (const auto out = outputs[i])
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Outputs[i])->CreateSetValue(ctx, block, out);

        BranchInst::Create(done, block);

        block = done;

        ICodegeneratorInlineWideNode::TGettersList result;
        result.reserve(Outputs.size());
        for (auto i = 0U; i < Outputs.size(); ++i) {
            if (const auto& one = InitOnInputs[i], two = UpdateOnInputs[i]; one && two && *one == *two)
                result.emplace_back(getres.second[*two]);
            else if (Outputs[i]->GetDependencesCount() > 0 || OutputsOnUpdate[i])
                result.emplace_back([output = Outputs[i]] (const TCodegenContext& ctx, BasicBlock*& block) { return GetNodeValue(output, ctx, block); });
            else
                result.emplace_back([this, i, source = getres.second, flagPtr, flagType] (const TCodegenContext& ctx, BasicBlock*& block) {
                    auto& context = ctx.Codegen.GetContext();

                    const auto init = BasicBlock::Create(context, "init", ctx.Func);
                    const auto next = BasicBlock::Create(context, "next", ctx.Func);
                    const auto done = BasicBlock::Create(context, "done", ctx.Func);

                    const auto result = PHINode::Create(Type::getInt128Ty(context), 2U, "result", done);

                    const auto flag = new LoadInst(flagType, flagPtr, "flag", block);
                    BranchInst::Create(init, next, flag, block);

                    block = init;
                    if (const auto& map = InitOnInputs[i])
                        result->addIncoming(source[*map](ctx, block), block);
                    else
                        result->addIncoming(GetNodeValue(InitItems[i], ctx, block), block);
                    BranchInst::Create(done, block);

                    block = next;
                    if (const auto& map = UpdateOnInputs[i])
                        result->addIncoming(source[*map](ctx, block), block);
                    else
                        result->addIncoming(GetNodeValue(UpdateItems[i], ctx, block), block);
                    BranchInst::Create(done, block);

                    block = done;
                    return result;
                });
        };
        return {getres.first, std::move(result)};
    }
#endif
private:
    EFetchResult CalculateFirst(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto** fields = ctx.WideFields.data() + WideFieldsIndex;

        for (auto i = 0U; i < Inputs.size(); ++i) {
            if (const auto& map = InputsOnInit[i]; map && !Inputs[i]->GetDependencesCount()) {
                if (const auto& to = UpdateOnOutputs[*map]) {
                    fields[i] = &Outputs[*to]->RefValue(ctx);
                    continue;
                } else if (const auto out = output[*map]) {
                    fields[i] = out;
                    continue;
                }
            } else {
                fields[i] = &Inputs[i]->RefValue(ctx);
                continue;
            }

            fields[i] = nullptr;
        }

        if (const auto result = Flow->FetchValues(ctx, fields); EFetchResult::One != result)
            return result;

        for (auto i = 0U; i < Outputs.size(); ++i) {
            if (Outputs[i]->GetDependencesCount() > 0U || OutputsOnUpdate[i]) {
                if (const auto& map = InitOnInputs[i]; !map || Inputs[*map]->GetDependencesCount() > 0U) {
                    Outputs[i]->SetValue(ctx, InitItems[i]->GetValue(ctx));
                }
            }
        }

        for (auto i = 0U; i < Outputs.size(); ++i) {
            if (const auto out = output[i]) {
                if (Outputs[i]->GetDependencesCount() > 0U || OutputsOnUpdate[i])
                    *out = Outputs[i]->GetValue(ctx);
                else {
                    if (const auto& map = InitOnInputs[i]) {
                        if (const auto from = *map; !Inputs[from]->GetDependencesCount()) {
                            if (const auto first = *InputsOnInit[from]; first != i)
                                *out = *output[first];
                            continue;
                        }
                    }

                    *out = InitItems[i]->GetValue(ctx);
                }
            }
        }

        return EFetchResult::One;
    }

    EFetchResult CalculateOther(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto** fields = ctx.WideFields.data() + WideFieldsIndex;

        for (auto i = 0U; i < Inputs.size(); ++i) {
            if (const auto& map = InputsOnUpdate[i]; map && !Inputs[i]->GetDependencesCount()) {
                if (const auto out = output[*map]) {
                    fields[i] = out;
                    continue;
                }
            } else {
                fields[i] = &Inputs[i]->RefValue(ctx);
                continue;
            }

            fields[i] = nullptr;
        }

        if (const auto result = Flow->FetchValues(ctx, fields); EFetchResult::One != result)
            return result;

        for (auto i = 0U; i < Outputs.size(); ++i) {
            if (Outputs[i]->GetDependencesCount() > 0U || OutputsOnUpdate[i]) {
                if (const auto& map = UpdateOnInputs[i]; !map || Inputs[*map]->GetDependencesCount() > 0U) {
                    ctx.MutableValues[TempStateIndex + i] = UpdateItems[i]->GetValue(ctx);
                }
            }
        }

        for (auto i = 0U; i < Outputs.size(); ++i) {
            if (Outputs[i]->GetDependencesCount() > 0U || OutputsOnUpdate[i]) {
                if (const auto& map = UpdateOnInputs[i]; !map || Inputs[*map]->GetDependencesCount() > 0U) {
                    Outputs[i]->SetValue(ctx, std::move(ctx.MutableValues[TempStateIndex + i]));
                }
            }
        }

        for (auto i = 0U; i < Outputs.size(); ++i) {
            if (const auto out = output[i]) {
                if (Outputs[i]->GetDependencesCount() > 0U || OutputsOnUpdate[i])
                    *out = Outputs[i]->GetValue(ctx);
                else {
                    if (const auto& map = UpdateOnInputs[i]) {
                        if (const auto from = *map; !Inputs[from]->GetDependencesCount()) {
                            if (const auto first = *InputsOnUpdate[from]; first != i)
                                *out = *output[first];
                            continue;
                        }
                    }

                    *out = UpdateItems[i]->GetValue(ctx);
                }
            }
        }

        return EFetchResult::One;
    }

    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            std::for_each(Inputs.cbegin(), Inputs.cend(), std::bind(&TWideChain1MapWrapper::Own, flow, std::placeholders::_1));
            std::for_each(Outputs.cbegin(), Outputs.cend(), std::bind(&TWideChain1MapWrapper::Own, flow, std::placeholders::_1));
            std::for_each(InitItems.cbegin(), InitItems.cend(), std::bind(&TWideChain1MapWrapper::DependsOn, flow, std::placeholders::_1));
            std::for_each(UpdateItems.cbegin(), UpdateItems.cend(), std::bind(&TWideChain1MapWrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationWideFlowNode* const Flow;

    const TComputationExternalNodePtrVector Inputs;
    const TComputationNodePtrVector InitItems;
    const TComputationExternalNodePtrVector Outputs;
    const TComputationNodePtrVector UpdateItems;

    const TPasstroughtMap InputsOnInit, InputsOnUpdate, InitOnInputs, UpdateOnInputs, OutputsOnUpdate, UpdateOnOutputs;

    const ui32 WideFieldsIndex;
    const ui32 TempStateIndex;
};

}

IComputationNode* WrapWideChain1Map(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() > 0U, "Expected argument.");
    const auto inputWidth = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetInput(0U).GetStaticType()));
    const auto outputWidth = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));
    MKQL_ENSURE(callable.GetInputsCount() == inputWidth + outputWidth * 3U + 1U, "Wrong signature.");

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);
    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        TComputationNodePtrVector initOutput(outputWidth, nullptr), updateOutput(outputWidth, nullptr);
        auto index = inputWidth;
        std::generate(initOutput.begin(), initOutput.end(), [&](){ return LocateNode(ctx.NodeLocator, callable, ++index); });

        index += outputWidth;
        std::generate(updateOutput.begin(), updateOutput.end(), [&](){ return LocateNode(ctx.NodeLocator, callable, ++index); });

        TComputationExternalNodePtrVector inputs(inputWidth, nullptr), outputs(outputWidth, nullptr);
        index = 0U;
        std::generate(inputs.begin(), inputs.end(), [&](){ return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

        index += outputWidth;
        std::generate(outputs.begin(), outputs.end(), [&](){ return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

        return new TWideChain1MapWrapper(ctx.Mutables, wide, std::move(inputs), std::move(initOutput), std::move(outputs), std::move(updateOutput));
    }

    THROW yexception() << "Expected wide flow.";
}

}
}
