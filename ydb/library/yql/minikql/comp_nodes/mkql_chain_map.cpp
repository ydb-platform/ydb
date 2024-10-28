#include "mkql_chain_map.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

struct TComputationNodes {
    IComputationExternalNode* const ItemArg;
    IComputationExternalNode* const StateArg;
    IComputationNode* const NewItem;
    IComputationNode* const NewState;
};

class TFoldMapFlowWrapper : public TStatefulFlowCodegeneratorNode<TFoldMapFlowWrapper> {
    typedef TStatefulFlowCodegeneratorNode<TFoldMapFlowWrapper> TBaseComputation;
public:
     TFoldMapFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow, IComputationNode* initialState, IComputationExternalNode* itemArg, IComputationExternalNode* stateArg, IComputationNode* newItem, IComputationNode* newState)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Embedded),
            Flow(flow), Init(initialState), ComputationNodes({itemArg, stateArg, newItem, newState})

    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            ComputationNodes.StateArg->SetValue(ctx, Init->GetValue(ctx));
            state = NUdf::TUnboxedValuePod(true);
        }

        auto item = Flow->GetValue(ctx);
        if (item.IsSpecial()) {
            return item;
        }

        ComputationNodes.ItemArg->SetValue(ctx, std::move(item));
        const auto value = ComputationNodes.NewItem->GetValue(ctx);
        ComputationNodes.StateArg->SetValue(ctx, ComputationNodes.NewState->GetValue(ctx));
        return value;
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ComputationNodes.ItemArg);
        const auto codegenStateArg = dynamic_cast<ICodegeneratorExternalNode*>(ComputationNodes.StateArg);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenStateArg, "State arg must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);

        const auto state = new LoadInst(valueType, statePtr, "load", block);
        BranchInst::Create(init, work, IsInvalid(state, block), block);

        block = init;
        new StoreInst(GetTrue(context), statePtr, block);
        codegenStateArg->CreateSetValue(ctx, block, GetNodeValue(Init, ctx, block));
        BranchInst::Create(work, block);

        block = work;

        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(valueType, 2U, "result", done);

        const auto item = GetNodeValue(Flow, ctx, block);
        result->addIncoming(item, block);
        BranchInst::Create(done, good, IsSpecial(item, block), block);

        block = good;
        codegenItemArg->CreateSetValue(ctx, block, item);
        const auto value = GetNodeValue(ComputationNodes.NewItem, ctx, block);
        codegenStateArg->CreateSetValue(ctx, block, GetNodeValue(ComputationNodes.NewState, ctx, block));
        result->addIncoming(value, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            DependsOn(flow, Init);
            DependsOn(flow, ComputationNodes.NewItem);
            DependsOn(flow, ComputationNodes.NewState);
            Own(flow, ComputationNodes.ItemArg);
            Own(flow, ComputationNodes.StateArg);
        }
    }

    IComputationNode* const Flow;
    IComputationNode* const Init;
    const TComputationNodes ComputationNodes;
};

template <bool IsStream>
class TBaseChainMapWrapper {
public:
    class TListValue : public TCustomListValue {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& iter, const NUdf::TUnboxedValue& init, const TComputationNodes& computationNodes)
                : TComputationValue<TIterator>(memInfo)
                , CompCtx(compCtx)
                , ComputationNodes(computationNodes)
                , Iter(std::move(iter))
                , Init(init)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                if (!Init.IsInvalid()) {
                    ComputationNodes.StateArg->SetValue(CompCtx, std::move(Init));
                    Init = NUdf::TUnboxedValue::Invalid();
                }

                if (!Iter.Next(ComputationNodes.ItemArg->RefValue(CompCtx))) {
                    return false;
                }

                value = ComputationNodes.NewItem->GetValue(CompCtx);
                ComputationNodes.StateArg->SetValue(CompCtx, ComputationNodes.NewState->GetValue(CompCtx));
                return true;
            }

            TComputationContext& CompCtx;
            const TComputationNodes& ComputationNodes;
            const NUdf::TUnboxedValue Iter;
            NUdf::TUnboxedValue Init;
        };

        TListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& list, NUdf::TUnboxedValue&& init, const TComputationNodes& computationNodes)
            : TCustomListValue(memInfo)
            , CompCtx(compCtx)
            , List(std::move(list))
            , Init(std::move(init))
            , ComputationNodes(computationNodes)
        {}

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            return CompCtx.HolderFactory.Create<TIterator>(CompCtx, List.GetListIterator(), Init, ComputationNodes);
        }

        ui64 GetListLength() const final {
            if (!Length) {
                Length = List.GetListLength();
            }

            return *Length;
        }

        bool HasListItems() const final {
            if (!HasItems) {
                HasItems = List.HasListItems();
            }

            return *HasItems;
        }

        TComputationContext& CompCtx;
        const NUdf::TUnboxedValue List;
        const NUdf::TUnboxedValue Init;
        const TComputationNodes& ComputationNodes;
    };

    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& list, NUdf::TUnboxedValue&& init, const TComputationNodes& computationNodes)
            : TBase(memInfo)
            , CompCtx(compCtx)
            , ComputationNodes(computationNodes)
            , List(std::move(list))
            , Init(std::move(init))
        {}

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& value) final {
            if (!Init.IsInvalid()) {
                ComputationNodes.StateArg->SetValue(CompCtx, std::move(Init));
                Init = NUdf::TUnboxedValuePod::Invalid();
            }

            const auto status = List.Fetch(ComputationNodes.ItemArg->RefValue(CompCtx));
            if (status != NUdf::EFetchStatus::Ok) {
                return status;
            }

            value = ComputationNodes.NewItem->GetValue(CompCtx);
            ComputationNodes.StateArg->SetValue(CompCtx, ComputationNodes.NewState->GetValue(CompCtx));
            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& CompCtx;
        const TComputationNodes& ComputationNodes;
        const NUdf::TUnboxedValue List;
        NUdf::TUnboxedValue Init;
    };

    TBaseChainMapWrapper(IComputationNode* list, IComputationNode* init, IComputationExternalNode* itemArg, IComputationExternalNode* stateArg, IComputationNode* newItem, IComputationNode* newState)
        : List(list), Init(init), ComputationNodes({itemArg, stateArg, newItem, newState})
    {}

#ifndef MKQL_DISABLE_CODEGEN
    Function* GenerateMapper(NYql::NCodegen::ICodegen& codegen, const TString& name) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ComputationNodes.ItemArg);
        const auto codegenStateArg = dynamic_cast<ICodegeneratorExternalNode*>(ComputationNodes.StateArg);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenStateArg, "State arg must be codegenerator node.");

        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto containerType = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ? static_cast<Type*>(PointerType::getUnqual(valueType)) : static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = IsStream ? Type::getInt32Ty(context) : Type::getInt1Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(valueType), PointerType::getUnqual(valueType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto initArg = &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto container = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
            new LoadInst(valueType, containerArg, "load_container", false, block) : static_cast<Value*>(containerArg);

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);

        const auto load = new LoadInst(valueType, initArg, "load", block);
        BranchInst::Create(work, init, IsInvalid(load, block), block);
        block = init;

        codegenStateArg->CreateSetValue(ctx, block, initArg);
        new StoreInst(GetInvalid(context), initArg, block);
        BranchInst::Create(work, block);

        block = work;

        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto itemPtr = codegenItemArg->CreateRefValue(ctx, block);
        const auto status = IsStream ?
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, container, codegen, block, itemPtr):
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(statusType, container, codegen, block, itemPtr);

        const auto icmp = IsStream ?
            CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, status, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), "cond", block):
            CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::getFalse(context), "cond", block);

        BranchInst::Create(done, good, icmp, block);
        block = good;

        SafeUnRefUnboxed(valuePtr, ctx, block);
        GetNodeValue(valuePtr, ComputationNodes.NewItem, ctx, block);

        const auto newState = GetNodeValue(ComputationNodes.NewState, ctx, block);

        codegenStateArg->CreateSetValue(ctx, block, newState);

        BranchInst::Create(done, block);
        block = done;

        ReturnInst::Create(context, status, block);
        return ctx.Func;
    }

    using TChainMapPtr = std::conditional_t<IsStream, TStreamCodegenStatefulValue::TFetchPtr, TCustomListCodegenStatefulValue::TNextPtr>;

    Function* ChainMapFunc = nullptr;

    TChainMapPtr ChainMap = nullptr;
#endif

    IComputationNode* const List;
    IComputationNode* const Init;
    const TComputationNodes ComputationNodes;
};

class TStreamChainMapWrapper : public TCustomValueCodegeneratorNode<TStreamChainMapWrapper>, private TBaseChainMapWrapper<true> {
    typedef TCustomValueCodegeneratorNode<TStreamChainMapWrapper> TBaseComputation;
    typedef TBaseChainMapWrapper<true> TBaseWrapper;
public:
    TStreamChainMapWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationNode* init, IComputationExternalNode* itemArg, IComputationExternalNode* stateArg, IComputationNode* newItem, IComputationNode* newState)
        : TBaseComputation(mutables), TBaseWrapper(list, init, itemArg, stateArg, newItem, newState)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && ChainMap)
            return ctx.HolderFactory.Create<TStreamCodegenStatefulValue>(ChainMap, &ctx, List->GetValue(ctx), Init->GetValue(ctx));
#endif
        return ctx.HolderFactory.Create<TStreamValue>(ctx, List->GetValue(ctx), Init->GetValue(ctx), ComputationNodes);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(List);
        DependsOn(Init);
        DependsOn(ComputationNodes.NewItem);
        DependsOn(ComputationNodes.NewState);
        Own(ComputationNodes.ItemArg);
        Own(ComputationNodes.StateArg);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        ChainMapFunc = GenerateMapper(codegen, TBaseComputation::MakeName("Fetch"));
        codegen.ExportSymbol(ChainMapFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (ChainMapFunc)
            ChainMap = reinterpret_cast<TChainMapPtr>(codegen.GetPointerToFunction(ChainMapFunc));
    }
#endif
};

class TListChainMapWrapper : public TBothWaysCodegeneratorNode<TListChainMapWrapper>, private TBaseChainMapWrapper<false> {
    typedef TBothWaysCodegeneratorNode<TListChainMapWrapper> TBaseComputation;
    typedef TBaseChainMapWrapper<false> TBaseWrapper;
public:
    TListChainMapWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationNode* init, IComputationExternalNode* itemArg, IComputationExternalNode* stateArg, IComputationNode* newItem, IComputationNode* newState)
        : TBaseComputation(mutables), TBaseWrapper(list, init, itemArg, stateArg, newItem, newState)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto init = Init->GetValue(ctx);
        auto list = List->GetValue(ctx);

        if (auto elements = list.GetElements()) {
            auto size = list.GetListLength();

            ComputationNodes.StateArg->SetValue(ctx, std::move(init));

            NUdf::TUnboxedValue* items = nullptr;
            const auto result = ctx.HolderFactory.CreateDirectArrayHolder(size, items);
            while (size--) {
                ComputationNodes.ItemArg->SetValue(ctx, NUdf::TUnboxedValue(*elements++));
                *items++ = ComputationNodes.NewItem->GetValue(ctx);
                ComputationNodes.StateArg->SetValue(ctx, ComputationNodes.NewState->GetValue(ctx));
            }
            return result;
        }

        return ctx.HolderFactory.Create<TListValue>(ctx, std::move(list), std::move(init), ComputationNodes);
    }

#ifndef MKQL_DISABLE_CODEGEN
    NUdf::TUnboxedValuePod MakeLazyList(TComputationContext& ctx, const NUdf::TUnboxedValuePod value, const NUdf::TUnboxedValuePod init) const {
        return ctx.HolderFactory.Create<TCustomListCodegenStatefulValue>(ChainMap, &ctx, value, init);
    }

    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ComputationNodes.ItemArg);
        const auto codegenStateArg = dynamic_cast<ICodegeneratorExternalNode*>(ComputationNodes.StateArg);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenStateArg, "State arg must be codegenerator node.");

        const auto init = GetNodeValue(Init, ctx, block);
        const auto list = GetNodeValue(List, ctx, block);

        const auto lazy = BasicBlock::Create(context, "lazy", ctx.Func);
        const auto hard = BasicBlock::Create(context, "hard", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto map = PHINode::Create(list->getType(), 2U, "map", done);

        const auto elementsType = PointerType::getUnqual(list->getType());
        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(elementsType, list, ctx.Codegen, block);
        const auto fill = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, elements, ConstantPointerNull::get(elementsType), "fill", block);

        BranchInst::Create(hard, lazy, fill, block);

        {
            block = hard;

            const auto size = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListLength>(Type::getInt64Ty(context), list, ctx.Codegen, block);

            codegenStateArg->CreateSetValue(ctx, block, init);

            const auto itemsPtr = *Stateless || ctx.AlwaysInline ?
                new AllocaInst(elementsType, 0U, "items_ptr", &ctx.Func->getEntryBlock().back()):
                new AllocaInst(elementsType, 0U, "items_ptr", block);
            const auto array = GenNewArray(ctx, size, itemsPtr, block);
            const auto items = new LoadInst(elementsType, itemsPtr, "items", block);

            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            const auto stop = BasicBlock::Create(context, "stop", ctx.Func);

            const auto index = PHINode::Create(size->getType(), 2U, "index", loop);
            index->addIncoming(ConstantInt::get(size->getType(), 0), block);

            BranchInst::Create(loop, block);

            block = loop;

            const auto more = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, size, index, "more", block);
            BranchInst::Create(next, stop, more, block);

            block = next;
            const auto src = GetElementPtrInst::CreateInBounds(list->getType(), elements, {index}, "src", block);
            const auto item = new LoadInst(list->getType(), src, "item", block);
            codegenItemArg->CreateSetValue(ctx, block, item);
            const auto dst = GetElementPtrInst::CreateInBounds(list->getType(), items, {index}, "dst", block);
            GetNodeValue(dst, ComputationNodes.NewItem, ctx, block);
            const auto newState = GetNodeValue(ComputationNodes.NewState, ctx, block);
            codegenStateArg->CreateSetValue(ctx, block, newState);
            const auto plus = BinaryOperator::CreateAdd(index, ConstantInt::get(size->getType(), 1), "plus", block);
            index->addIncoming(plus, block);
            BranchInst::Create(loop, block);

            block = stop;
            if (List->IsTemporaryValue()) {
                CleanupBoxed(list, ctx, block);
            }
            map->addIncoming(array, block);
            BranchInst::Create(done, block);
        }

        {
            block = lazy;

            const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TListChainMapWrapper::MakeLazyList));
            const auto ptrType = PointerType::getUnqual(StructType::get(context));
            const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
            if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                const auto funType = FunctionType::get(list->getType() , {self->getType(), ctx.Ctx->getType(), list->getType(), init->getType()}, false);
                const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "function", block);
                const auto value = CallInst::Create(funType, doFuncPtr, {self, ctx.Ctx, list, init}, "value", block);
                map->addIncoming(value, block);
            } else {
                const auto resultPtr = new AllocaInst(list->getType(), 0U, "return", block);
                const auto initPtr = new AllocaInst(init->getType(), 0U, "init", block);
                new StoreInst(list, resultPtr, block);
                new StoreInst(init, initPtr, block);
                const auto funType = FunctionType::get(Type::getVoidTy(context), {self->getType(), resultPtr->getType(), ctx.Ctx->getType(), resultPtr->getType(), initPtr->getType()}, false);
                const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "function", block);
                CallInst::Create(funType, doFuncPtr, {self, resultPtr, ctx.Ctx, resultPtr, initPtr}, "", block);
                const auto value = new LoadInst(list->getType(), resultPtr, "value", block);
                map->addIncoming(value, block);
            }
            BranchInst::Create(done, block);
        }

        block = done;
        return map;
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(List);
        DependsOn(Init);
        DependsOn(ComputationNodes.NewItem);
        DependsOn(ComputationNodes.NewState);
        Own(ComputationNodes.ItemArg);
        Own(ComputationNodes.StateArg);
    }

#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListChainMapWrapper>::GenerateFunctions(codegen);
        ChainMapFunc = GenerateMapper(codegen, TBaseComputation::MakeName("Next"));
        codegen.ExportSymbol(ChainMapFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListChainMapWrapper>::FinalizeFunctions(codegen);
        if (ChainMapFunc)
            ChainMap = reinterpret_cast<TChainMapPtr>(codegen.GetPointerToFunction(ChainMapFunc));
    }
#endif
};

}

IComputationNode* WrapChainMap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 6, "Expected 6 args");
    const auto type = callable.GetType()->GetReturnType();
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    const auto initialState = LocateNode(ctx.NodeLocator, callable, 1);
    const auto newItem = LocateNode(ctx.NodeLocator, callable, 4);
    const auto newState = LocateNode(ctx.NodeLocator, callable, 5);
    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 2);
    const auto stateArg = LocateExternalNode(ctx.NodeLocator, callable, 3);
    if (type->IsFlow()) {
        return new TFoldMapFlowWrapper(ctx.Mutables, GetValueRepresentation(type), flow, initialState, itemArg, stateArg, newItem, newState);
    } else if (type->IsStream()) {
        return new TStreamChainMapWrapper(ctx.Mutables, flow, initialState, itemArg, stateArg, newItem, newState);
    } else if (type->IsList()) {
        return new TListChainMapWrapper(ctx.Mutables, flow, initialState, itemArg, stateArg, newItem, newState);
    }

    THROW yexception() << "Expected flow, list or stream.";
}

}
}
