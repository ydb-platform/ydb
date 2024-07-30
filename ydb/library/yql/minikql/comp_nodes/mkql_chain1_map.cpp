#include "mkql_chain1_map.h"
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
    IComputationNode* const InitItem;
    IComputationNode* const InitState;
    IComputationNode* const UpdateItem;
    IComputationNode* const UpdateState;
};

class TFold1MapFlowWrapper : public TStatefulFlowCodegeneratorNode<TFold1MapFlowWrapper> {
    typedef TStatefulFlowCodegeneratorNode<TFold1MapFlowWrapper> TBaseComputation;
public:
     TFold1MapFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow,
        IComputationExternalNode* itemArg, IComputationExternalNode* stateArg,
        IComputationNode* initItem, IComputationNode* initState,
        IComputationNode* updateItem, IComputationNode* updateState)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Embedded),
            Flow(flow), ComputationNodes({itemArg, stateArg, initItem, initState, updateItem, updateState})

    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        auto item = Flow->GetValue(ctx);
        if (item.IsSpecial()) {
            return item;
        }

        ComputationNodes.ItemArg->SetValue(ctx, std::move(item));

        const bool init = state.IsInvalid();
        const auto value = (init ? ComputationNodes.InitItem : ComputationNodes.UpdateItem)->GetValue(ctx);
        ComputationNodes.StateArg->SetValue(ctx, (init ? ComputationNodes.InitState : ComputationNodes.UpdateState)->GetValue(ctx));

        if (init) {
            state = NUdf::TUnboxedValuePod(true);
        }

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

        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(valueType, 3U, "result", done);

        const auto item = GetNodeValue(Flow, ctx, block);
        result->addIncoming(item, block);
        BranchInst::Create(done, good, IsSpecial(item, block), block);

        block = good;
        codegenItemArg->CreateSetValue(ctx, block, item);

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);

        const auto state = new LoadInst(valueType, statePtr, "load", block);
        BranchInst::Create(init, next, IsInvalid(state, block), block);

        block = init;
        const auto one = GetNodeValue(ComputationNodes.InitItem, ctx, block);
        codegenStateArg->CreateSetValue(ctx, block, GetNodeValue(ComputationNodes.InitState, ctx, block));
        result->addIncoming(one, block);
        new StoreInst(GetTrue(context), statePtr, block);
        BranchInst::Create(done, block);

        block = next;
        const auto two = GetNodeValue(ComputationNodes.UpdateItem, ctx, block);
        codegenStateArg->CreateSetValue(ctx, block, GetNodeValue(ComputationNodes.UpdateState, ctx, block));
        result->addIncoming(two, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            DependsOn(flow, ComputationNodes.InitItem);
            DependsOn(flow, ComputationNodes.InitState);
            DependsOn(flow, ComputationNodes.UpdateItem);
            DependsOn(flow, ComputationNodes.UpdateState);
            Own(flow, ComputationNodes.ItemArg);
            Own(flow, ComputationNodes.StateArg);
        }
    }

    IComputationNode* const Flow;
    const TComputationNodes ComputationNodes;
};

template <bool IsStream>
class TBaseChain1MapWrapper {
public:
    class TListValue : public TCustomListValue {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& iter, const TComputationNodes& computationNodes)
                : TComputationValue<TIterator>(memInfo)
                , CompCtx(compCtx)
                , Iter(std::move(iter))
                , ComputationNodes(computationNodes)
            {}

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                if (!Iter.Next(ComputationNodes.ItemArg->RefValue(CompCtx))) {
                    return false;
                }

                ++Length;

                auto itemNode = Length == 1 ? ComputationNodes.InitItem : ComputationNodes.UpdateItem;
                auto stateNode = Length == 1 ? ComputationNodes.InitState : ComputationNodes.UpdateState;
                value = itemNode->GetValue(CompCtx);
                ComputationNodes.StateArg->SetValue(CompCtx, stateNode->GetValue(CompCtx));
                return true;
            }

            TComputationContext& CompCtx;
            const NUdf::TUnboxedValue Iter;
            const TComputationNodes& ComputationNodes;
            ui64 Length = 0;
        };

        TListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& list, const TComputationNodes& computationNodes)
            : TCustomListValue(memInfo)
            , CompCtx(compCtx)
            , List(std::move(list))
            , ComputationNodes(computationNodes)
        {}

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            return CompCtx.HolderFactory.Create<TIterator>(CompCtx, List.GetListIterator(), ComputationNodes);
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
        const TComputationNodes& ComputationNodes;
    };

    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& list, const TComputationNodes& computationNodes)
            : TBase(memInfo)
            , CompCtx(compCtx)
            , List(std::move(list))
            , ComputationNodes(computationNodes)
        {}

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& value) final {
            const auto status = List.Fetch(ComputationNodes.ItemArg->RefValue(CompCtx));
            if (status != NUdf::EFetchStatus::Ok) {
                return status;
            }

            ++Length;

            auto itemNode = Length == 1 ? ComputationNodes.InitItem : ComputationNodes.UpdateItem;
            auto stateNode = Length == 1 ? ComputationNodes.InitState : ComputationNodes.UpdateState;
            value = itemNode->GetValue(CompCtx);
            ComputationNodes.StateArg->SetValue(CompCtx, stateNode->GetValue(CompCtx));
            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& CompCtx;
        const NUdf::TUnboxedValue List;
        const TComputationNodes& ComputationNodes;
        ui64 Length = 0;
    };

    TBaseChain1MapWrapper(IComputationNode* list, IComputationExternalNode* itemArg, IComputationExternalNode* stateArg,
        IComputationNode* initItem, IComputationNode* initState,
        IComputationNode* updateItem, IComputationNode* updateState)
        : List(list), ComputationNodes({itemArg, stateArg, initItem, initState, updateItem, updateState})
    {}

#ifndef MKQL_DISABLE_CODEGEN
    template<bool IsFirst>
    Function* GenerateMapper(NYql::NCodegen::ICodegen& codegen, const TString& name) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto newItem = IsFirst ? ComputationNodes.InitItem : ComputationNodes.UpdateItem;
        const auto newState = IsFirst ? ComputationNodes.InitState : ComputationNodes.UpdateState;

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
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(valueType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        ctx.Annotator = &annotator;

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto container = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
            new LoadInst(valueType, containerArg, "load_container", false, block) : static_cast<Value*>(containerArg);

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
        GetNodeValue(valuePtr, newItem, ctx, block);

        const auto nextState = GetNodeValue(newState, ctx, block);

        codegenStateArg->CreateSetValue(ctx, block, nextState);

        BranchInst::Create(done, block);
        block = done;

        ReturnInst::Create(context, status, block);
        return ctx.Func;
    }

    using TChainMapPtr = std::conditional_t<IsStream, TStreamCodegenValueOne::TFetchPtr, TListCodegenValueOne::TNextPtr>;

    Function* MapFuncOne = nullptr;
    Function* MapFuncTwo = nullptr;

    TChainMapPtr MapOne = nullptr;
    TChainMapPtr MapTwo = nullptr;
#endif

    IComputationNode* const List;
    const TComputationNodes ComputationNodes;
};

class TStreamChain1MapWrapper : public TCustomValueCodegeneratorNode<TStreamChain1MapWrapper>, private TBaseChain1MapWrapper<true> {
    typedef TCustomValueCodegeneratorNode<TStreamChain1MapWrapper> TBaseComputation;
    typedef TBaseChain1MapWrapper<true> TBaseWrapper;
public:
    TStreamChain1MapWrapper(TComputationMutables& mutables, IComputationNode* list,
        IComputationExternalNode* itemArg, IComputationExternalNode* stateArg,
        IComputationNode* initItem, IComputationNode* initState,
        IComputationNode* updateItem, IComputationNode* updateState
    ) : TBaseComputation(mutables), TBaseWrapper(list, itemArg, stateArg, initItem, initState, updateItem, updateState)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && MapOne && MapTwo)
            return ctx.HolderFactory.Create<TStreamCodegenValueOne>(MapOne, MapTwo, &ctx, List->GetValue(ctx));
#endif
        return ctx.HolderFactory.Create<TStreamValue>(ctx, List->GetValue(ctx), ComputationNodes);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(List);
        DependsOn(ComputationNodes.InitItem);
        DependsOn(ComputationNodes.InitState);
        DependsOn(ComputationNodes.UpdateItem);
        DependsOn(ComputationNodes.UpdateState);
        Own(ComputationNodes.ItemArg);
        Own(ComputationNodes.StateArg);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        MapFuncOne = GenerateMapper<true>(codegen, TBaseComputation::MakeName("Fetch_One"));
        MapFuncTwo = GenerateMapper<false>(codegen, TBaseComputation::MakeName("Fetch_Two"));
        codegen.ExportSymbol(MapFuncOne);
        codegen.ExportSymbol(MapFuncTwo);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (MapFuncOne)
            MapOne = reinterpret_cast<TChainMapPtr>(codegen.GetPointerToFunction(MapFuncOne));
        if (MapFuncTwo)
            MapTwo = reinterpret_cast<TChainMapPtr>(codegen.GetPointerToFunction(MapFuncTwo));
    }
#endif
};

class TListChain1MapWrapper : public TBothWaysCodegeneratorNode<TListChain1MapWrapper>, private TBaseChain1MapWrapper<false> {
    typedef TBothWaysCodegeneratorNode<TListChain1MapWrapper> TBaseComputation;
    typedef TBaseChain1MapWrapper<false> TBaseWrapper;
public:
    TListChain1MapWrapper(TComputationMutables& mutables, IComputationNode* list,
        IComputationExternalNode* itemArg, IComputationExternalNode* stateArg,
        IComputationNode* initItem, IComputationNode* initState,
        IComputationNode* updateItem, IComputationNode* updateState
    ) : TBaseComputation(mutables), TBaseWrapper(list, itemArg, stateArg, initItem, initState, updateItem, updateState)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto list = List->GetValue(ctx);

        if (auto elements = list.GetElements()) {
            auto size = list.GetListLength();

            NUdf::TUnboxedValue* items = nullptr;
            const auto result = ctx.HolderFactory.CreateDirectArrayHolder(size, items);
            if (size) {
                ComputationNodes.ItemArg->SetValue(ctx, NUdf::TUnboxedValue(*elements++));
                *items++ = ComputationNodes.InitItem->GetValue(ctx);
                ComputationNodes.StateArg->SetValue(ctx, ComputationNodes.InitState->GetValue(ctx));
                while (--size) {
                    ComputationNodes.ItemArg->SetValue(ctx, NUdf::TUnboxedValue(*elements++));
                    *items++ = ComputationNodes.UpdateItem->GetValue(ctx);
                    ComputationNodes.StateArg->SetValue(ctx, ComputationNodes.UpdateState->GetValue(ctx));
                }
            }
            return result;
        }

        return ctx.HolderFactory.Create<TListValue>(ctx, std::move(list), ComputationNodes);
    }

#ifndef MKQL_DISABLE_CODEGEN
    NUdf::TUnboxedValuePod MakeLazyList(TComputationContext& ctx, const NUdf::TUnboxedValuePod value) const {
        return ctx.HolderFactory.Create<TListCodegenValueOne>(MapOne, MapTwo, &ctx, value);
    }

    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ComputationNodes.ItemArg);
        const auto codegenStateArg = dynamic_cast<ICodegeneratorExternalNode*>(ComputationNodes.StateArg);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenStateArg, "State arg must be codegenerator node.");

        const auto list = GetNodeValue(List, ctx, block);

        const auto lazy = BasicBlock::Create(context, "lazy", ctx.Func);
        const auto hard = BasicBlock::Create(context, "hard", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto map = PHINode::Create(list->getType(), 3U, "map", done);

        const auto elementsType = PointerType::getUnqual(list->getType());
        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(elementsType, list, ctx.Codegen, block);
        const auto fill = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, elements, ConstantPointerNull::get(elementsType), "fill", block);

        BranchInst::Create(hard, lazy, fill, block);

        {
            block = hard;

            const auto size = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListLength>(Type::getInt64Ty(context), list, ctx.Codegen, block);

            const auto itemsPtr = *Stateless || ctx.AlwaysInline ?
                new AllocaInst(elementsType, 0U, "items_ptr", &ctx.Func->getEntryBlock().back()):
                new AllocaInst(elementsType, 0U, "items_ptr", block);
            const auto array = GenNewArray(ctx, size, itemsPtr, block);
            const auto items = new LoadInst(elementsType, itemsPtr, "items", block);

            const auto init = BasicBlock::Create(context, "init", ctx.Func);
            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            const auto stop = BasicBlock::Create(context, "stop", ctx.Func);

            const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, size, ConstantInt::get(size->getType(), 0), "good", block);
            map->addIncoming(array, block);

            BranchInst::Create(init, done, good, block);

            block = init;
            const auto head = new LoadInst(list->getType(), elements, "head", block);
            codegenItemArg->CreateSetValue(ctx, block, head);
            GetNodeValue(items, ComputationNodes.InitItem, ctx, block);
            const auto state = GetNodeValue(ComputationNodes.InitState, ctx, block);
            codegenStateArg->CreateSetValue(ctx, block, state);

            const auto index = PHINode::Create(size->getType(), 2U, "index", loop);
            index->addIncoming(ConstantInt::get(size->getType(), 1), block);
            BranchInst::Create(loop, block);

            block = loop;

            const auto more = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, size, index, "more", block);
            BranchInst::Create(next, stop, more, block);

            block = next;
            const auto src = GetElementPtrInst::CreateInBounds(list->getType(), elements, {index}, "src", block);
            const auto item = new LoadInst(list->getType(), src, "item", block);
            codegenItemArg->CreateSetValue(ctx, block, item);
            const auto dst = GetElementPtrInst::CreateInBounds(list->getType(), items, {index}, "dst", block);
            GetNodeValue(dst, ComputationNodes.UpdateItem, ctx, block);
            const auto newState = GetNodeValue(ComputationNodes.UpdateState, ctx, block);
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

            const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TListChain1MapWrapper::MakeLazyList));
            const auto ptrType = PointerType::getUnqual(StructType::get(context));
            const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
            if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                const auto funType = FunctionType::get(list->getType() , {self->getType(), ctx.Ctx->getType(), list->getType()}, false);
                const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "function", block);
                const auto value = CallInst::Create(funType, doFuncPtr, {self, ctx.Ctx, list}, "value", block);
                map->addIncoming(value, block);
            } else {
                const auto resultPtr = new AllocaInst(list->getType(), 0U, "return", block);
                new StoreInst(list, resultPtr, block);
                const auto funType = FunctionType::get(Type::getVoidTy(context), {self->getType(), resultPtr->getType(), ctx.Ctx->getType(), resultPtr->getType()}, false);
                const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "function", block);
                CallInst::Create(funType, doFuncPtr, {self, resultPtr, ctx.Ctx, resultPtr}, "", block);
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
        DependsOn(ComputationNodes.InitItem);
        DependsOn(ComputationNodes.InitState);
        DependsOn(ComputationNodes.UpdateItem);
        DependsOn(ComputationNodes.UpdateState);
        Own(ComputationNodes.ItemArg);
        Own(ComputationNodes.StateArg);
    }

#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListChain1MapWrapper>::GenerateFunctions(codegen);
        MapFuncOne = GenerateMapper<true>(codegen, TBaseComputation::MakeName("Next_One"));
        MapFuncTwo = GenerateMapper<false>(codegen, TBaseComputation::MakeName("Next_Two"));
        codegen.ExportSymbol(MapFuncOne);
        codegen.ExportSymbol(MapFuncTwo);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListChain1MapWrapper>::FinalizeFunctions(codegen);
        if (MapFuncOne)
            MapOne = reinterpret_cast<TChainMapPtr>(codegen.GetPointerToFunction(MapFuncOne));
        if (MapFuncTwo)
            MapTwo = reinterpret_cast<TChainMapPtr>(codegen.GetPointerToFunction(MapFuncTwo));
    }
#endif
};

}

IComputationNode* WrapChain1Map(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 7, "Expected 7 args");
    const auto type = callable.GetType()->GetReturnType();
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    const auto initItem = LocateNode(ctx.NodeLocator, callable, 2);
    const auto initState = LocateNode(ctx.NodeLocator, callable, 3);
    const auto updateItem = LocateNode(ctx.NodeLocator, callable, 5);
    const auto updateState = LocateNode(ctx.NodeLocator, callable, 6);
    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1);
    const auto stateArg = LocateExternalNode(ctx.NodeLocator, callable, 4);
    if (type->IsFlow()) {
        return new TFold1MapFlowWrapper(ctx.Mutables, GetValueRepresentation(type), flow, itemArg, stateArg, initItem, initState, updateItem, updateState);
    } else if (type->IsStream()) {
        return new TStreamChain1MapWrapper(ctx.Mutables, flow, itemArg, stateArg, initItem, initState, updateItem, updateState);
    } else if (type->IsList()) {
        return new TListChain1MapWrapper(ctx.Mutables, flow, itemArg, stateArg, initItem, initState, updateItem, updateState);
    }

    THROW yexception() << "Expected flow, list or stream.";
}

}
}
