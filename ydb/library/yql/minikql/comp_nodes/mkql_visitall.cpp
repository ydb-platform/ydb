#include "mkql_visitall.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <util/string/cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TVisitAllWrapper: public TMutableCodegeneratorNode<TVisitAllWrapper> {
using TBaseComputation = TMutableCodegeneratorNode<TVisitAllWrapper>;
public:
    TVisitAllWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* varNode, TComputationExternalNodePtrVector&& args, TComputationNodePtrVector&& newNodes)
        : TBaseComputation(mutables, kind)
        , VarNode(varNode)
        , Args(std::move(args))
        , NewNodes(std::move(newNodes))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& var = VarNode->GetValue(ctx);
        const auto currentIndex = var.GetVariantIndex();
        if (currentIndex >= Args.size())
            return NUdf::TUnboxedValuePod();
        Args[currentIndex]->SetValue(ctx, var.GetVariantItem());
        return NewNodes[currentIndex]->GetValue(ctx).Release();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto variant = GetNodeValue(VarNode, ctx, block);
        const auto unpack = GetVariantParts(variant, ctx, block);

        const auto result = PHINode::Create(variant->getType(), Args.size() + 1U, "result", done);
        result->addIncoming(ConstantInt::get(variant->getType(), 0ULL), block);

        const auto choise = SwitchInst::Create(unpack.first, done, Args.size(), block);

        for (ui32 i = 0; i < NewNodes.size(); ++i) {
            const auto var = BasicBlock::Create(context, (TString("case_") += ToString(i)).c_str(), ctx.Func);
            choise->addCase(ConstantInt::get(Type::getInt32Ty(context), i), var);
            block = var;

            const auto codegenArg = dynamic_cast<ICodegeneratorExternalNode*>(Args[i]);
            MKQL_ENSURE(codegenArg, "Arg must be codegenerator node.");
            codegenArg->CreateSetValue(ctx, block, unpack.second);
            const auto item = GetNodeValue(NewNodes[i], ctx, block);

            result->addIncoming(item, block);
            BranchInst::Create(done, block);
        }

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(VarNode);
        std::for_each(Args.cbegin(), Args.cend(), std::bind(&TVisitAllWrapper::Own, this, std::placeholders::_1));
        std::for_each(NewNodes.cbegin(), NewNodes.cend(), std::bind(&TVisitAllWrapper::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode *const VarNode;
    const TComputationExternalNodePtrVector Args;
    const TComputationNodePtrVector NewNodes;
};

class TFlowVisitAllWrapper: public TStatefulFlowCodegeneratorNode<TFlowVisitAllWrapper> {
using TBaseComputation = TStatefulFlowCodegeneratorNode<TFlowVisitAllWrapper>;
public:
    TFlowVisitAllWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* varNode, TComputationExternalNodePtrVector&& args, TComputationNodePtrVector&& newNodes)
        : TBaseComputation(mutables, nullptr, kind, EValueRepresentation::Embedded)
        , VarNode(varNode)
        , Args(std::move(args))
        , NewNodes(std::move(newNodes))
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            const auto& var = VarNode->GetValue(ctx);
            const auto index = var.GetVariantIndex();
            state = NUdf::TUnboxedValuePod(index);
            if (index < Args.size()) {
                Args[index]->SetValue(ctx, var.GetVariantItem());
            }
        }

        const auto index = state.Get<ui32>();
        return index < NewNodes.size() ? NewNodes[index]->GetValue(ctx).Release() : NUdf::TUnboxedValuePod::MakeFinish();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto valueType = Type::getInt128Ty(context);
        const auto result = PHINode::Create(valueType, NewNodes.size() + 2U, "result", done);

        BranchInst::Create(init, work, IsInvalid(statePtr, block), block);

        {
            block = init;

            const auto variant = GetNodeValue(VarNode, ctx, block);
            const auto unpack = GetVariantParts(variant, ctx, block);
            const auto index = SetterFor<ui32>(unpack.first, context, block);
            new StoreInst(index, statePtr, block);
            result->addIncoming(GetFinish(context), block);
            const auto choise = SwitchInst::Create(unpack.first, done, Args.size(), block);

            for (ui32 i = 0; i < Args.size(); ++i) {
                const auto var = BasicBlock::Create(context, (TString("init_") += ToString(i)).c_str(), ctx.Func);
                choise->addCase(ConstantInt::get(Type::getInt32Ty(context), i), var);
                block = var;

                const auto codegenArg = dynamic_cast<ICodegeneratorExternalNode*>(Args[i]);
                MKQL_ENSURE(codegenArg, "Arg must be codegenerator node.");
                codegenArg->CreateSetValue(ctx, block, unpack.second);
                BranchInst::Create(work, block);
            }
        }

        {
            block = work;

            const auto state = new LoadInst(valueType, statePtr, "state", block);
            const auto index = GetterFor<ui32>(state, context, block);
            result->addIncoming(GetFinish(context), block);
            const auto choise = SwitchInst::Create(index, done, NewNodes.size(), block);
            for (ui32 i = 0; i < NewNodes.size(); ++i) {
                const auto var = BasicBlock::Create(context, (TString("case_") += ToString(i)).c_str(), ctx.Func);
                choise->addCase(ConstantInt::get(Type::getInt32Ty(context), i), var);
                block = var;

                const auto item = GetNodeValue(NewNodes[i], ctx, block);

                result->addIncoming(item, block);
                BranchInst::Create(done, block);
            }
        }

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOnAll(NewNodes)) {
            DependsOn(flow, VarNode);
            std::for_each(Args.cbegin(), Args.cend(), std::bind(&TFlowVisitAllWrapper::Own, flow, std::placeholders::_1));
        }
        std::for_each(Args.cbegin(), Args.cend(), std::bind(&IComputationNode::AddDependence, VarNode, std::placeholders::_1));
    }

    IComputationNode *const VarNode;
    const TComputationExternalNodePtrVector Args;
    const TComputationNodePtrVector NewNodes;
};

class TWideVisitAllWrapper: public TStatefulWideFlowCodegeneratorNode<TWideVisitAllWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideVisitAllWrapper>;
public:
    TWideVisitAllWrapper(TComputationMutables& mutables, IComputationNode* varNode, TComputationExternalNodePtrVector&& args, TComputationWideFlowNodePtrVector&& newNodes)
        : TBaseComputation(mutables, nullptr, EValueRepresentation::Embedded)
        , VarNode(varNode)
        , Args(std::move(args))
        , NewNodes(std::move(newNodes))
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (state.IsInvalid()) {
            const auto& var = VarNode->GetValue(ctx);
            const auto index = var.GetVariantIndex();
            state = NUdf::TUnboxedValuePod(index);
            if (index < Args.size()) {
                Args[index]->SetValue(ctx, var.GetVariantItem());
            }
        }

        const auto index = state.Get<ui32>();
        return index < NewNodes.size() ? NewNodes[index]->FetchValues(ctx, output) : EFetchResult::Finish;
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto resultType = Type::getInt32Ty(context);
        const auto result = PHINode::Create(resultType, NewNodes.size() + 2U, "result", done);

        BranchInst::Create(init, work, IsInvalid(statePtr, block), block);

        {
            block = init;

            const auto variant = GetNodeValue(VarNode, ctx, block);
            const auto unpack = GetVariantParts(variant, ctx, block);
            const auto index = SetterFor<ui32>(unpack.first, context, block);
            new StoreInst(index, statePtr, block);
            result->addIncoming(ConstantInt::get(resultType, static_cast<i32>(EFetchResult::Finish)), block);
            const auto choise = SwitchInst::Create(unpack.first, done, Args.size(), block);

            for (ui32 i = 0; i < Args.size(); ++i) {
                const auto var = BasicBlock::Create(context, (TString("init_") += ToString(i)).c_str(), ctx.Func);
                choise->addCase(ConstantInt::get(Type::getInt32Ty(context), i), var);
                block = var;

                const auto codegenArg = dynamic_cast<ICodegeneratorExternalNode*>(Args[i]);
                MKQL_ENSURE(codegenArg, "Arg must be codegenerator node.");
                codegenArg->CreateSetValue(ctx, block, unpack.second);
                BranchInst::Create(work, block);
            }
        }

        std::vector<TGettersList> allGetters;
        allGetters.reserve(NewNodes.size());

        {
            block = work;

            const auto state = new LoadInst(Type::getInt128Ty(context), statePtr, "state", block);
            const auto index = GetterFor<ui32>(state, context, block);
            result->addIncoming(ConstantInt::get(resultType, static_cast<i32>(EFetchResult::Finish)), block);
            const auto choise = SwitchInst::Create(index, done, NewNodes.size(), block);
            for (ui32 i = 0; i < NewNodes.size(); ++i) {
                const auto var = BasicBlock::Create(context, (TString("case_") += ToString(i)).c_str(), ctx.Func);
                choise->addCase(ConstantInt::get(Type::getInt32Ty(context), i), var);
                block = var;

                auto get = GetNodeValues(NewNodes[i], ctx, block);
                allGetters.emplace_back(std::move(get.second));

                result->addIncoming(get.first, block);
                BranchInst::Create(done, block);
            }
        }

        TGettersList getters;
        getters.reserve(allGetters.back().size());
        const auto index = static_cast<const IComputationNode*>(this)->GetIndex();
        size_t idx = 0U;
        std::generate_n(std::back_inserter(getters), allGetters.front().size(), [&]() {
            TGettersList slice;
            slice.reserve(allGetters.size());
            std::transform(allGetters.begin(), allGetters.end(), std::back_inserter(slice), [j = idx++](TGettersList& list) { return std::move(list[j]);});
            return [index, slice = std::move(slice)](const TCodegenContext& ctx, BasicBlock*& block) {
                auto& context = ctx.Codegen.GetContext();

                const auto stub = BasicBlock::Create(context, "stub", ctx.Func);
                const auto done = BasicBlock::Create(context, "done", ctx.Func);
                new UnreachableInst(context, stub);

                const auto valueType = Type::getInt128Ty(context);
                const auto res = PHINode::Create(valueType, slice.size(), "res", done);

                const auto statePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), index)}, "state_ptr", block);
                const auto state = new LoadInst(valueType, statePtr, "state", block);
                const auto trunc = GetterFor<ui32>(state, context, block);

                const auto choise = SwitchInst::Create(trunc, stub, slice.size(), block);
                for (auto i = 0U; i < slice.size(); ++i) {
                    const auto var = BasicBlock::Create(context, (TString("case_") += ToString(i)).c_str(), ctx.Func);
                    choise->addCase(ConstantInt::get(Type::getInt32Ty(context), i), var);
                    block = var;

                    const auto get = slice[i](ctx, block);

                    res->addIncoming(get, block);
                    BranchInst::Create(done, block);
                }

                block = done;
                return res;
            };
        });

        block = done;
        return {result, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOnAll(NewNodes)) {
            DependsOn(flow, VarNode);
            std::for_each(Args.cbegin(), Args.cend(), std::bind(&TWideVisitAllWrapper::Own, flow, std::placeholders::_1));
        }
        std::for_each(Args.cbegin(), Args.cend(), std::bind(&IComputationNode::AddDependence, VarNode, std::placeholders::_1));
    }

    IComputationNode *const VarNode;
    const TComputationExternalNodePtrVector Args;
    const TComputationWideFlowNodePtrVector NewNodes;
};

}

IComputationNode* WrapVisitAll(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 3, "Expected at least 3 arguments");
    const auto varType = AS_TYPE(TVariantType, callable.GetInput(0));
    MKQL_ENSURE(callable.GetInputsCount() == varType->GetAlternativesCount() * 2 + 1, "Mismatch handlers count");

    const auto variant = LocateNode(ctx.NodeLocator, callable, 0U);

    TComputationNodePtrVector newNodes;
    newNodes.reserve(varType->GetAlternativesCount());
    for (auto i = 1U; i <= varType->GetAlternativesCount() << 1U; ++i) {
        newNodes.emplace_back(LocateNode(ctx.NodeLocator, callable, ++i));
    }

    TComputationExternalNodePtrVector args;
    args.reserve(varType->GetAlternativesCount());
    for (auto i = 0U; i < varType->GetAlternativesCount() << 1U; ++i) {
        args.emplace_back(LocateExternalNode(ctx.NodeLocator, callable, ++i));
    }

    if (const auto type = callable.GetType()->GetReturnType(); type->IsFlow()) {
        TComputationWideFlowNodePtrVector wideNodes;
        wideNodes.reserve(newNodes.size());
        std::transform(newNodes.cbegin(), newNodes.cend(), std::back_inserter(wideNodes), [](IComputationNode* node){ return dynamic_cast<IComputationWideFlowNode*>(node); });
        wideNodes.erase(std::remove_if(wideNodes.begin(), wideNodes.end(), std::logical_not<IComputationWideFlowNode*>()), wideNodes.cend());
        if (wideNodes.empty())
            return new TFlowVisitAllWrapper(ctx.Mutables, GetValueRepresentation(callable.GetType()->GetReturnType()), variant, std::move(args), std::move(newNodes));
        else if (wideNodes.size() == newNodes.size())
            return new TWideVisitAllWrapper(ctx.Mutables, variant, std::move(args), std::move(wideNodes));
    } else
        return new TVisitAllWrapper(ctx.Mutables, GetValueRepresentation(callable.GetType()->GetReturnType()), variant, std::move(args), std::move(newNodes));

    THROW yexception() << "Wrong signature.";
}

}
}
