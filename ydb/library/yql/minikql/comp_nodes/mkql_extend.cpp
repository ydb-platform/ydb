#include "mkql_extend.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <util/string/cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TExtendWideFlowWrapper : public TStatefulWideFlowComputationNode<TExtendWideFlowWrapper> {
    typedef TStatefulWideFlowComputationNode<TExtendWideFlowWrapper> TBaseComputation;
public:
    TExtendWideFlowWrapper(TComputationMutables& mutables, TComputationWideFlowNodePtrVector&& flows)
        : TBaseComputation(mutables, this, EValueRepresentation::Any)
        , Flows_(std::move(flows))
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto& s = GetState(state, ctx);

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

            s.NextInput();
            return result;
        }
        return EFetchResult::Finish;
    }

    void RegisterDependencies() const final {
        for (auto& flow : Flows_) {
            FlowDependsOn(flow);
        }
    }

private:
    struct TState : public TComputationValue<TState> {
        TComputationWideFlowNodePtrVector Inputs;
        size_t LiveInputs;
        size_t InputIndex = 0;

        TState(TMemoryUsageInfo* memInfo, const TComputationWideFlowNodePtrVector& inputs)
            : TComputationValue(memInfo)
            , Inputs(inputs)
            , LiveInputs(Inputs.size())
        {
        }

        void NextInput() {
            if (++InputIndex >= LiveInputs) {
                InputIndex = 0;
            }
        }
    };


    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(Flows_);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    const TComputationWideFlowNodePtrVector Flows_;
};

class TExtendFlowWrapper : public TStatefulFlowCodegeneratorNode<TExtendFlowWrapper> {
    typedef TStatefulFlowCodegeneratorNode<TExtendFlowWrapper> TBaseComputation;
public:
     TExtendFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, TComputationNodePtrVector&& flows)
        : TBaseComputation(mutables, this, kind, EValueRepresentation::Embedded), Flows(flows)
    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        for (ui64 index = state.IsInvalid() ? 0ULL : state.Get<ui64>(); index < Flows.size(); ++index) {
            const auto item = Flows[index]->GetValue(ctx);

            if (!item.IsFinish()) {
                state = NUdf::TUnboxedValuePod(index);
                return item;
            }
        }

        return NUdf::TUnboxedValuePod::MakeFinish();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt64Ty(context);

        const auto load = new LoadInst(valueType, statePtr, "load", block);
        const auto state = SelectInst::Create(IsInvalid(load, block), ConstantInt::get(indexType, 0ULL), GetterFor<ui64>(load, context, block), "index", block);

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(valueType, Flows.size() + 1U, "result", done);

        const auto index = PHINode::Create(indexType, 2U, "index", main);
        index->addIncoming(state, block);
        BranchInst::Create(main, block);

        block = main;

        const auto select = SwitchInst::Create(index, done, Flows.size(), block);
        result->addIncoming(GetFinish(context), block);

        for (auto i = 0U; i < Flows.size(); ++i) {
            const auto flow = BasicBlock::Create(context, "flow", ctx.Func);
            select->addCase(ConstantInt::get(indexType, i), flow);

            block = flow;
            const auto item = GetNodeValue(Flows[i], ctx, block);
            result->addIncoming(item, block);
            BranchInst::Create(next, done, IsFinish(item, block), block);
        }

        block = next;
        const auto plus = BinaryOperator::CreateAdd(index, ConstantInt::get(indexType, 1ULL), "plus", block);
        index->addIncoming(plus, block);
        BranchInst::Create(main, block);

        block = done;
        new StoreInst(SetterFor<ui64>(index, context, block), statePtr, block);
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        std::for_each(Flows.cbegin(), Flows.cend(), std::bind(&TExtendFlowWrapper::FlowDependsOn, this, std::placeholders::_1));
    }

    const TComputationNodePtrVector Flows;
};

template <bool IsStream>
class TExtendWrapper : public TMutableCodegeneratorNode<TExtendWrapper<IsStream>> {
    typedef TMutableCodegeneratorNode<TExtendWrapper<IsStream>> TBaseComputation;
public:
    TExtendWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& lists)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Lists(std::move(lists))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        TUnboxedValueVector values;
        values.reserve(Lists.size());
        std::transform(Lists.cbegin(), Lists.cend(), std::back_inserter(values),
            std::bind(&IComputationNode::GetValue, std::placeholders::_1, std::ref(ctx))
        );

        return IsStream ?
            ctx.HolderFactory.ExtendStream(values.data(), values.size()):
            ctx.HolderFactory.ExtendList<false>(values.data(), values.size());
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto sizeType = Type::getInt64Ty(context);
        const auto size = ConstantInt::get(sizeType, Lists.size());

        const auto arrayType = ArrayType::get(valueType, Lists.size());
        const auto array = *this->Stateless || ctx.AlwaysInline ?
            new AllocaInst(arrayType, 0U, "array", &ctx.Func->getEntryBlock().back()):
            new AllocaInst(arrayType, 0U, "array", block);

        for (size_t i = 0U; i < Lists.size(); ++i) {
            const auto ptr = GetElementPtrInst::CreateInBounds(arrayType, array, {ConstantInt::get(sizeType, 0), ConstantInt::get(sizeType, i)}, (TString("ptr_") += ToString(i)).c_str(), block);
            GetNodeValue(ptr, Lists[i], ctx, block);
        }

        const auto factory = ctx.GetFactory();
        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(IsStream ? &THolderFactory::ExtendStream : &THolderFactory::ExtendList<false>));

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(valueType, {factory->getType(), array->getType(), size->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            const auto res = CallInst::Create(funType, funcPtr, {factory, array, size}, "res", block);
            return res;
        } else {
            const auto retPtr = new AllocaInst(valueType, 0U, "ret_ptr", block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {factory->getType(), retPtr->getType(), array->getType(), size->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            CallInst::Create(funType, funcPtr, {factory, retPtr, array, size}, "", block);
            const auto res = new LoadInst(valueType, retPtr, "res", block);
            return res;
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        std::for_each(Lists.cbegin(), Lists.cend(), std::bind(&TExtendWrapper::DependsOn, this, std::placeholders::_1));
    }

    const TComputationNodePtrVector Lists;
};

}

IComputationNode* WrapExtend(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 1, "Expected at least 1 list");
    const auto type = callable.GetType()->GetReturnType();

    TComputationNodePtrVector flows;
    flows.reserve(callable.GetInputsCount());
    for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
        flows.emplace_back(LocateNode(ctx.NodeLocator, callable, i));
    }

    if (type->IsFlow()) {
        if (dynamic_cast<IComputationWideFlowNode*>(flows.front())) {
            TComputationWideFlowNodePtrVector wideFlows;
            wideFlows.reserve(callable.GetInputsCount());
            for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
                wideFlows.emplace_back(dynamic_cast<IComputationWideFlowNode*>(flows[i]));
                MKQL_ENSURE_S(wideFlows.back());
            }
            return new TExtendWideFlowWrapper(ctx.Mutables, std::move(wideFlows));
        }
        return new TExtendFlowWrapper(ctx.Mutables, GetValueRepresentation(AS_TYPE(TFlowType, type)->GetItemType()), std::move(flows));
    } else if (type->IsStream()) {
        return new TExtendWrapper<true>(ctx.Mutables, std::move(flows));
    } else if (type->IsList()) {
        return new TExtendWrapper<false>(ctx.Mutables, std::move(flows));
    }

    THROW yexception() << "Expected either flow, list or stream.";
}

}
}
