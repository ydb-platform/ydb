#include "mkql_wide_chain_map.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_custom_list.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/utils/cast.h>

namespace NKikimr::NMiniKQL {

#ifndef MKQL_DISABLE_CODEGEN
using NYql::EnsureDynamicCast;
#endif

namespace {

class TWideChain1MapWrapper: public TStatefulWideFlowCodegeneratorNode<TWideChain1MapWrapper> {
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideChain1MapWrapper>;

public:
    TWideChain1MapWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow,
                          TComputationExternalNodePtrVector&& inputs,
                          TComputationNodePtrVector&& initItems,
                          TComputationExternalNodePtrVector&& outputs,
                          TComputationNodePtrVector&& updateItems)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded)
        , Flow_(flow)
        , Inputs_(std::move(inputs))
        , InitItems_(std::move(initItems))
        , Outputs_(std::move(outputs))
        , UpdateItems_(std::move(updateItems))
        , InputsOnInit_(GetPasstroughtMapOneToOne(Inputs_, InitItems_))
        , InputsOnUpdate_(GetPasstroughtMapOneToOne(Inputs_, UpdateItems_))
        , InitOnInputs_(GetPasstroughtMapOneToOne(InitItems_, Inputs_))
        , UpdateOnInputs_(GetPasstroughtMapOneToOne(UpdateItems_, Inputs_))
        , OutputsOnUpdate_(GetPasstroughtMapOneToOne(Outputs_, UpdateItems_))
        , UpdateOnOutputs_(GetPasstroughtMapOneToOne(UpdateItems_, Outputs_))
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Inputs_.size()))
        , TempStateIndex_(std::exchange(mutables.CurValueIndex, mutables.CurValueIndex + Outputs_.size()))
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            state = NUdf::TUnboxedValuePod();
            return CalculateFirst(ctx, output);
        }

        return CalculateOther(ctx, output);
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto flagType = Type::getInt1Ty(context);
        const auto flagPtr = new AllocaInst(flagType, 0U, "flag_ptr", &ctx.Func->getEntryBlock().back());

        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto getres = GetNodeValues(Flow_, ctx, block);

        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), 0), "special", block);
        BranchInst::Create(done, good, special, block);

        block = good;
        for (auto i = 0U; i < Inputs_.size(); ++i) {
            if (Inputs_[i]->GetDependentsCount() > 0U || !InputsOnInit_[i] || !InputsOnUpdate_[i]) {
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Inputs_[i])->CreateSetValue(ctx, block, getres.second[i](ctx, block));
            }
        }

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);

        const auto flag = IsInvalid(statePtr, block, context);
        new StoreInst(flag, flagPtr, block);
        BranchInst::Create(init, next, flag, block);

        block = init;
        for (auto i = 0U; i < Outputs_.size(); ++i) {
            if (Outputs_[i]->GetDependentsCount() > 0U || OutputsOnUpdate_[i]) {
                const auto& map = InitOnInputs_[i];
                const auto value = map ? getres.second[*map](ctx, block) : GetNodeValue(InitItems_[i], ctx, block);
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Outputs_[i])->CreateSetValue(ctx, block, value);
            }
        }

        new StoreInst(GetEmpty(context), statePtr, block);
        BranchInst::Create(done, block);

        block = next;

        std::vector<Value*> outputs(Outputs_.size(), nullptr);
        for (auto i = 0U; i < outputs.size(); ++i) {
            if (const auto& dep = OutputsOnUpdate_[i]; Outputs_[i]->GetDependentsCount() > 0U || (dep && *dep != i)) {
                const auto& map = UpdateOnInputs_[i];
                outputs[i] = map ? getres.second[*map](ctx, block) : GetNodeValue(UpdateItems_[i], ctx, block);
            }
        }

        for (auto i = 0U; i < outputs.size(); ++i) {
            if (const auto out = outputs[i]) {
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Outputs_[i])->CreateSetValue(ctx, block, out);
            }
        }

        BranchInst::Create(done, block);

        block = done;

        ICodegeneratorInlineWideNode::TGettersList result;
        result.reserve(Outputs_.size());
        for (auto i = 0U; i < Outputs_.size(); ++i) {
            const auto outputNode = Outputs_[i];
            if (const auto &one = InitOnInputs_[i], two = UpdateOnInputs_[i]; one && two && *one == *two) {
                result.emplace_back(getres.second[*two]);
            } else if (Outputs_[i]->GetDependentsCount() > 0 || OutputsOnUpdate_[i]) {
                result.emplace_back([outputNode](const TCodegenContext& ctx, BasicBlock*& block) { return GetNodeValue(outputNode, ctx, block); });
            } else {
                result.emplace_back([this, source = getres.second, flagPtr, flagType, i](const TCodegenContext& ctx, BasicBlock*& block) {
                    auto& context = ctx.Codegen.GetContext();

                    const auto init = BasicBlock::Create(context, "init", ctx.Func);
                    const auto next = BasicBlock::Create(context, "next", ctx.Func);
                    const auto done = BasicBlock::Create(context, "done", ctx.Func);

                    const auto result = PHINode::Create(Type::getInt128Ty(context), 2U, "result", done);

                    const auto flag = new LoadInst(flagType, flagPtr, "flag", block);
                    BranchInst::Create(init, next, flag, block);

                    block = init;
                    if (const auto& map = InitOnInputs_[i]) {
                        result->addIncoming(source[*map](ctx, block), block);
                    } else {
                        result->addIncoming(GetNodeValue(InitItems_[i], ctx, block), block);
                    }
                    BranchInst::Create(done, block);

                    block = next;
                    if (const auto& map = UpdateOnInputs_[i]) {
                        result->addIncoming(source[*map](ctx, block), block);
                    } else {
                        result->addIncoming(GetNodeValue(UpdateItems_[i], ctx, block), block);
                    }
                    BranchInst::Create(done, block);

                    block = done;
                    return result;
                });
            }
        };
        return {getres.first, std::move(result)};
    }
#endif
private:
    EFetchResult CalculateFirst(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        auto** fields = ctx.WideFields.data() + WideFieldsIndex_;

        for (auto i = 0U; i < Inputs_.size(); ++i) {
            if (const auto& map = InputsOnInit_[i]; map && !Inputs_[i]->GetDependentsCount()) {
                if (const auto& to = UpdateOnOutputs_[*map]) {
                    fields[i] = &Outputs_[*to]->RefValue(ctx);
                    continue;
                } else if (const auto out = output[*map]) {
                    fields[i] = out;
                    continue;
                }
            } else {
                fields[i] = &Inputs_[i]->RefValue(ctx);
                continue;
            }

            fields[i] = nullptr;
        }

        if (const auto result = Flow_->FetchValues(ctx, fields); EFetchResult::One != result) {
            return result;
        }

        for (auto i = 0U; i < Outputs_.size(); ++i) {
            if (Outputs_[i]->GetDependentsCount() > 0U || OutputsOnUpdate_[i]) {
                if (const auto& map = InitOnInputs_[i]; !map || Inputs_[*map]->GetDependentsCount() > 0U) {
                    Outputs_[i]->SetValue(ctx, InitItems_[i]->GetValue(ctx));
                }
            }
        }

        for (auto i = 0U; i < Outputs_.size(); ++i) {
            if (const auto out = output[i]) {
                if (Outputs_[i]->GetDependentsCount() > 0U || OutputsOnUpdate_[i]) {
                    *out = Outputs_[i]->GetValue(ctx);
                } else {
                    if (const auto& map = InitOnInputs_[i]) {
                        if (const auto from = *map; !Inputs_[from]->GetDependentsCount()) {
                            if (const auto first = *InputsOnInit_[from]; first != i) {
                                *out = *output[first];
                            }
                            continue;
                        }
                    }

                    *out = InitItems_[i]->GetValue(ctx);
                }
            }
        }

        return EFetchResult::One;
    }

    EFetchResult CalculateOther(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        auto** fields = ctx.WideFields.data() + WideFieldsIndex_;

        for (auto i = 0U; i < Inputs_.size(); ++i) {
            if (const auto& map = InputsOnUpdate_[i]; map && !Inputs_[i]->GetDependentsCount()) {
                if (const auto out = output[*map]) {
                    fields[i] = out;
                    continue;
                }
            } else {
                fields[i] = &Inputs_[i]->RefValue(ctx);
                continue;
            }

            fields[i] = nullptr;
        }

        if (const auto result = Flow_->FetchValues(ctx, fields); EFetchResult::One != result) {
            return result;
        }

        for (auto i = 0U; i < Outputs_.size(); ++i) {
            if (Outputs_[i]->GetDependentsCount() > 0U || OutputsOnUpdate_[i]) {
                if (const auto& map = UpdateOnInputs_[i]; !map || Inputs_[*map]->GetDependentsCount() > 0U) {
                    ctx.MutableValues[TempStateIndex_ + i] = UpdateItems_[i]->GetValue(ctx);
                }
            }
        }

        for (auto i = 0U; i < Outputs_.size(); ++i) {
            if (Outputs_[i]->GetDependentsCount() > 0U || OutputsOnUpdate_[i]) {
                if (const auto& map = UpdateOnInputs_[i]; !map || Inputs_[*map]->GetDependentsCount() > 0U) {
                    Outputs_[i]->SetValue(ctx, std::move(ctx.MutableValues[TempStateIndex_ + i]));
                }
            }
        }

        for (auto i = 0U; i < Outputs_.size(); ++i) {
            if (const auto out = output[i]) {
                if (Outputs_[i]->GetDependentsCount() > 0U || OutputsOnUpdate_[i]) {
                    *out = Outputs_[i]->GetValue(ctx);
                } else {
                    if (const auto& map = UpdateOnInputs_[i]) {
                        if (const auto from = *map; !Inputs_[from]->GetDependentsCount()) {
                            if (const auto first = *InputsOnUpdate_[from]; first != i) {
                                *out = *output[first];
                            }
                            continue;
                        }
                    }

                    *out = UpdateItems_[i]->GetValue(ctx);
                }
            }
        }

        return EFetchResult::One;
    }

    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow_)) {
            std::for_each(Inputs_.cbegin(), Inputs_.cend(), std::bind(&TWideChain1MapWrapper::Own, flow, std::placeholders::_1));
            std::for_each(Outputs_.cbegin(), Outputs_.cend(), std::bind(&TWideChain1MapWrapper::Own, flow, std::placeholders::_1));
            std::for_each(InitItems_.cbegin(), InitItems_.cend(), std::bind(&TWideChain1MapWrapper::DependsOn, flow, std::placeholders::_1));
            std::for_each(UpdateItems_.cbegin(), UpdateItems_.cend(), std::bind(&TWideChain1MapWrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationWideFlowNode* const Flow_;

    const TComputationExternalNodePtrVector Inputs_;
    const TComputationNodePtrVector InitItems_;
    const TComputationExternalNodePtrVector Outputs_;
    const TComputationNodePtrVector UpdateItems_;

    const TPasstroughtMap InputsOnInit_, InputsOnUpdate_, InitOnInputs_, UpdateOnInputs_, OutputsOnUpdate_, UpdateOnOutputs_;

    const ui32 WideFieldsIndex_;
    const ui32 TempStateIndex_;
};

} // namespace

IComputationNode* WrapWideChain1Map(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() > 0U, "Expected argument.");
    const auto inputWidth = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetInput(0U).GetStaticType()));
    const auto outputWidth = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));
    MKQL_ENSURE(callable.GetInputsCount() == inputWidth + outputWidth * 3U + 1U, "Wrong signature.");

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);
    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        TComputationNodePtrVector initOutput(outputWidth, nullptr);
        TComputationNodePtrVector updateOutput(outputWidth, nullptr);
        auto index = inputWidth;
        std::generate(initOutput.begin(), initOutput.end(), [&]() { return LocateNode(ctx.NodeLocator, callable, ++index); });

        index += outputWidth;
        std::generate(updateOutput.begin(), updateOutput.end(), [&]() { return LocateNode(ctx.NodeLocator, callable, ++index); });

        TComputationExternalNodePtrVector inputs(inputWidth, nullptr);
        TComputationExternalNodePtrVector outputs(outputWidth, nullptr);
        index = 0U;
        std::generate(inputs.begin(), inputs.end(), [&]() { return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

        index += outputWidth;
        std::generate(outputs.begin(), outputs.end(), [&]() { return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

        return new TWideChain1MapWrapper(ctx.Mutables, wide, std::move(inputs), std::move(initOutput), std::move(outputs), std::move(updateOutput));
    }

    THROW yexception() << "Expected wide flow.";
}

} // namespace NKikimr::NMiniKQL
