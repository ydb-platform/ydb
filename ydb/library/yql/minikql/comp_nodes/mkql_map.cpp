#include "mkql_map.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TFlowMapWrapper : public TStatelessFlowCodegeneratorNode<TFlowMapWrapper> {
    typedef TStatelessFlowCodegeneratorNode<TFlowMapWrapper> TBaseComputation;
public:
    TFlowMapWrapper(EValueRepresentation kind, IComputationNode* flow, IComputationExternalNode* item, IComputationNode* newItem)
        : TBaseComputation(flow, kind)
        , Flow(flow)
        , Item(item)
        , NewItem(newItem)
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        if (auto item = Flow->GetValue(ctx); item.IsSpecial()) {
            return item;
        } else {
            Item->SetValue(ctx, std::move(item));
        }
        return NewItem->GetValue(ctx);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto item = GetNodeValue(Flow, ctx, block);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        const auto result = PHINode::Create(item->getType(), 2, "result", pass);
        result->addIncoming(item, block);

        BranchInst::Create(pass, work, IsSpecial(item, block), block);

        block = work;
        codegenItem->CreateSetValue(ctx, block, item);
        const auto out = GetNodeValue(NewItem, ctx, block);
        result->addIncoming(out, block);
        BranchInst::Create(pass, block);

        block = pass;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            Own(flow, Item);
            DependsOn(flow, NewItem);
        }
    }

    IComputationNode* const Flow;
    IComputationExternalNode* const Item;
    IComputationNode* const NewItem;
};

template <bool IsStream>
class TBaseMapWrapper {
protected:
    class TListValue : public TCustomListValue {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& iter, IComputationExternalNode* item, IComputationNode* newItem)
                : TComputationValue<TIterator>(memInfo)
                , CompCtx(compCtx)
                , Iter(std::move(iter))
                , Item(item)
                , NewItem(newItem)
            {}

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                if (!Iter.Next(Item->RefValue(CompCtx))) {
                    return false;
                }

                value = NewItem->GetValue(CompCtx);
                return true;
            }

            TComputationContext& CompCtx;
            const NUdf::TUnboxedValue Iter;
            IComputationExternalNode* const Item;
            IComputationNode* const NewItem;
        };

        TListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& list, IComputationExternalNode* item, IComputationNode* newItem)
            : TCustomListValue(memInfo)
            , CompCtx(compCtx)
            , List(std::move(list))
            , Item(item)
            , NewItem(newItem)
        {}

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            return CompCtx.HolderFactory.Create<TIterator>(CompCtx, List.GetListIterator(), Item, NewItem);
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

        bool HasFastListLength() const final {
            return List.HasFastListLength();
        }

        TComputationContext& CompCtx;
        const NUdf::TUnboxedValue List;
        IComputationExternalNode* const Item;
        IComputationNode* const NewItem;
    };

    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& stream, IComputationExternalNode* item, IComputationNode* newItem)
            : TBase(memInfo)
            , CompCtx(compCtx)
            , Stream(std::move(stream))
            , Item(item)
            , NewItem(newItem)
        {
        }

    private:
        ui32 GetTraverseCount() const final {
            return 1U;
        }

        NUdf::TUnboxedValue GetTraverseItem(ui32) const final {
            return Stream;
        }

        NUdf::TUnboxedValue Save() const final {
            return NUdf::TUnboxedValuePod::Zero();
        }

        void Load(const NUdf::TStringRef&) final {}

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
            const auto status = Stream.Fetch(Item->RefValue(CompCtx));
            if (status != NUdf::EFetchStatus::Ok) {
                return status;
            }

            result = NewItem->GetValue(CompCtx);
            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& CompCtx;
        const NUdf::TUnboxedValue Stream;
        IComputationExternalNode* const Item;
        IComputationNode* const NewItem;
    };

    TBaseMapWrapper(IComputationNode* list, IComputationExternalNode* item, IComputationNode* newItem)
        : List(list), Item(item), NewItem(newItem)
    {}

#ifndef MKQL_DISABLE_CODEGEN
    Function* GenerateMapper(NYql::NCodegen::ICodegen& codegen, const TString& name) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);

        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

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

        const auto itemPtr = codegenItem->CreateRefValue(ctx, block);

        const auto status = IsStream ?
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, container, codegen, block, itemPtr):
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(statusType, container, codegen, block, itemPtr);

        const auto icmp = IsStream ?
            CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, status, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), "cond", block):
            CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::getFalse(context), "cond", block);

        BranchInst::Create(done, good, icmp, block);
        block = good;

        SafeUnRefUnboxed(valuePtr, ctx, block);
        GetNodeValue(valuePtr, NewItem, ctx, block);

        BranchInst::Create(done, block);
        block = done;

        ReturnInst::Create(context, status, block);
        return ctx.Func;
    }

    using TMapPtr = std::conditional_t<IsStream, TStreamCodegenValueStateless::TFetchPtr, TListCodegenValue::TNextPtr>;

    Function* MapFunc = nullptr;

    TMapPtr Map = nullptr;
#endif

    IComputationNode* const List;
    IComputationExternalNode* const Item;
    IComputationNode* const NewItem;
};

class TStreamMapWrapper : public TCustomValueCodegeneratorNode<TStreamMapWrapper>, private TBaseMapWrapper<true> {
    typedef TCustomValueCodegeneratorNode<TStreamMapWrapper> TBaseComputation;
    typedef TBaseMapWrapper<true> TBaseWrapper;
public:
    TStreamMapWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationExternalNode* item, IComputationNode* newItem)
        : TBaseComputation(mutables), TBaseWrapper(list, item, newItem)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Map)
            return ctx.HolderFactory.Create<TStreamCodegenValueStateless>(Map, &ctx, List->GetValue(ctx));
#endif
        return ctx.HolderFactory.Create<TStreamValue>(ctx, List->GetValue(ctx), Item, NewItem);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(List);
        Own(Item);
        DependsOn(NewItem);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        MapFunc = GenerateMapper(codegen, TBaseComputation::MakeName("Fetch"));
        codegen.ExportSymbol(MapFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (MapFunc)
            Map = reinterpret_cast<TMapPtr>(codegen.GetPointerToFunction(MapFunc));
    }
#endif
};

class TListMapWrapper : public TBothWaysCodegeneratorNode<TListMapWrapper>, private TBaseMapWrapper<false> {
    typedef TBothWaysCodegeneratorNode<TListMapWrapper> TBaseComputation;
    typedef TBaseMapWrapper<false> TBaseWrapper;
public:
    TListMapWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationExternalNode* item, IComputationNode* newItem)
        : TBaseComputation(mutables), TBaseWrapper(list, item, newItem)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto list = List->GetValue(ctx);

        if (auto elements = list.GetElements()) {
            auto size = list.GetListLength();
            NUdf::TUnboxedValue* items = nullptr;
            NUdf::TUnboxedValue result = ctx.HolderFactory.CreateDirectArrayHolder(size, items);
            while (size--) {
                Item->SetValue(ctx, NUdf::TUnboxedValue(*elements++));
                *items++ = NewItem->GetValue(ctx);
            }
            return result.Release();
        }

        return ctx.HolderFactory.Create<TListValue>(ctx, std::move(list), Item, NewItem);
    }

#ifndef MKQL_DISABLE_CODEGEN
    NUdf::TUnboxedValuePod MakeLazyList(TComputationContext& ctx, const NUdf::TUnboxedValuePod value) const {
        return ctx.HolderFactory.Create<TListCodegenValue>(Map, &ctx, value);
    }

    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

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
            codegenItem->CreateSetValue(ctx, block, item);
            const auto dst = GetElementPtrInst::CreateInBounds(list->getType(), items, {index}, "dst", block);
            GetNodeValue(dst, NewItem, ctx, block);

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

            const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TListMapWrapper::MakeLazyList));
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
        Own(Item);
        DependsOn(NewItem);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListMapWrapper>::GenerateFunctions(codegen);
        MapFunc = GenerateMapper(codegen, TBaseComputation::MakeName("Next"));
        codegen.ExportSymbol(MapFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListMapWrapper>::FinalizeFunctions(codegen);
        if (MapFunc)
            Map = reinterpret_cast<TMapPtr>(codegen.GetPointerToFunction(MapFunc));
    }
#endif
};

}

IComputationNode* WrapMap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args, got " << callable.GetInputsCount());
    const auto type = callable.GetType()->GetReturnType();
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    const auto newItem = LocateNode(ctx.NodeLocator, callable, 2);
    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1);
    if (type->IsFlow()) {
        return new TFlowMapWrapper(GetValueRepresentation(type), flow, itemArg, newItem);
    } else if (type->IsStream()) {
        return new TStreamMapWrapper(ctx.Mutables, flow, itemArg, newItem);
    } else if (type->IsList()) {
        return new TListMapWrapper(ctx.Mutables, flow, itemArg, newItem);
    }

    THROW yexception() << "Expected flow, list or stream.";
}

}
}
