#include "mkql_multimap.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/utils/cast.h>

#include <util/string/cast.h>

namespace NKikimr {
namespace NMiniKQL {

using NYql::EnsureDynamicCast;

namespace {

class TFlowMultiMapWrapper : public TStatefulFlowCodegeneratorNode<TFlowMultiMapWrapper> {
    typedef TStatefulFlowCodegeneratorNode<TFlowMultiMapWrapper> TBaseComputation;
public:
    TFlowMultiMapWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow, IComputationExternalNode* item, TComputationNodePtrVector&& newItems)
        : TBaseComputation(mutables, flow, kind), Flow(flow), Item(item), NewItems(std::move(newItems))
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish())
            return NUdf::TUnboxedValuePod::MakeFinish();

        const auto pos = state.IsInvalid() ? 0ULL : state.Get<ui64>();
        if (!pos) {
            if (auto item = Flow->GetValue(ctx); item.IsSpecial()) {
                return item.Release();
            } else {
                Item->SetValue(ctx, std::move(item));
            }
        }

        const auto next = pos + 1ULL;
        state = NewItems.size() == next ? NUdf::TUnboxedValuePod::Invalid() : NUdf::TUnboxedValuePod(ui64(next));
        return NewItems[pos]->GetValue(ctx).Release();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);
        const auto state = new LoadInst(valueType, statePtr, "state", block);

        const auto zero = BasicBlock::Create(context, "zero", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);

        const auto result = PHINode::Create(valueType, NewItems.size() + 1U, "result", pass);

        const auto choise = SwitchInst::Create(state, zero, NewItems.size() - 1U, block);

        for (ui32 i = 1U; i < NewItems.size();) {
            const auto part = BasicBlock::Create(context, (TString("part_") += ToString(i)).c_str(), ctx.Func);
            choise->addCase(GetConstant(i, context), part);

            block = part;

            const auto out = GetNodeValue(NewItems[i], ctx, block);
            result->addIncoming(out, block);
            const auto next = ++i;
            new StoreInst(NewItems.size() <= next ? GetInvalid(context) : GetConstant(next, context), statePtr, block);
            BranchInst::Create(pass, block);
        }

        {
            block = zero;

            const auto item = GetNodeValue(Flow, ctx, block);
            result->addIncoming(item, block);

            BranchInst::Create(pass, work, IsSpecial(item, block), block);

            block = work;

            codegenItem->CreateSetValue(ctx, block, item);
            const auto out = GetNodeValue(NewItems.front(), ctx, block);
            result->addIncoming(out, block);
            new StoreInst(GetConstant(1ULL, context), statePtr, block);
            BranchInst::Create(pass, block);
        }

        block = pass;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            Own(flow, Item);
        }
    }

    IComputationNode* const Flow;
    IComputationExternalNode* const Item;
    const TComputationNodePtrVector NewItems;
};

class TListMultiMapWrapper : public TBothWaysCodegeneratorNode<TListMultiMapWrapper> {
private:
    typedef TBothWaysCodegeneratorNode<TListMultiMapWrapper> TBaseComputation;

    class TListValue : public TCustomListValue {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& iter, IComputationExternalNode* item, const TComputationNodePtrVector& newItems)
                : TComputationValue<TIterator>(memInfo)
                , CompCtx(compCtx)
                , Iter(std::move(iter))
                , Item(item)
                , NewItems(newItems)
            {}

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                if (!Position) {
                    if (!Iter.Next(Item->RefValue(CompCtx))) {
                        return false;
                    }
                }

                value = NewItems[Position]->GetValue(CompCtx);
                if (++Position == NewItems.size())
                    Position = 0U;
                return true;
            }

            TComputationContext& CompCtx;
            const NUdf::TUnboxedValue Iter;
            IComputationExternalNode* const Item;
            const TComputationNodePtrVector NewItems;
            size_t Position = 0U;
        };

        TListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& list, IComputationExternalNode* item, const TComputationNodePtrVector& newItems)
            : TCustomListValue(memInfo)
            , CompCtx(compCtx)
            , List(std::move(list))
            , Item(item)
            , NewItems(newItems)
        {}

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            return CompCtx.HolderFactory.Create<TIterator>(CompCtx, List.GetListIterator(), Item, NewItems);
        }

        ui64 GetListLength() const final {
            if (!Length) {
                Length = List.GetListLength() * NewItems.size();
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
        const TComputationNodePtrVector NewItems;
    };

public:
    TListMultiMapWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationExternalNode* item, TComputationNodePtrVector&& newItems)
        : TBaseComputation(mutables), List(list), Item(item), NewItems(std::move(newItems))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto list = List->GetValue(ctx);

        if (auto elements = list.GetElements()) {
            auto size = list.GetListLength();
            NUdf::TUnboxedValue* items = nullptr;
            const auto result = ctx.HolderFactory.CreateDirectArrayHolder(size * NewItems.size(), items);
            while (size--) {
                Item->SetValue(ctx, NUdf::TUnboxedValue(*elements++));
                for (const auto newItem : NewItems)
                    *items++ = newItem->GetValue(ctx);
            }
            return result;
        }

        return ctx.HolderFactory.Create<TListValue>(ctx, std::move(list), Item, NewItems);
    }

#ifndef MKQL_DISABLE_CODEGEN
    using TCodegenValue = TCustomListCodegenStatefulValueT<TCodegenStatefulIterator<ui64>>;

    NUdf::TUnboxedValuePod MakeLazyList(TComputationContext& ctx, const NUdf::TUnboxedValuePod value) const {
        return ctx.HolderFactory.Create<TCodegenValue>(Map, &ctx, value);
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
            const auto full = BinaryOperator::CreateMul(size, ConstantInt::get(size->getType(), NewItems.size()), "full", block);
            const auto array = GenNewArray(ctx, full, itemsPtr, block);
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
            const auto from = BinaryOperator::CreateMul(index, ConstantInt::get(index->getType(), NewItems.size()), "from", block);

            for (ui32 i = 0U; i < NewItems.size(); ++i) {
                const auto pos = BinaryOperator::CreateAdd(from, ConstantInt::get(from->getType(), i), (TString("pos_") += ToString(i)).c_str(), block);
                const auto dst = GetElementPtrInst::CreateInBounds(list->getType(), items, {pos}, (TString("dst_") += ToString(i)).c_str(), block);
                GetNodeValue(dst, NewItems[i], ctx, block);
            }

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

            const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TListMultiMapWrapper::MakeLazyList));
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
        std::for_each(NewItems.cbegin(), NewItems.cend(), std::bind(&TListMultiMapWrapper::DependsOn, this, std::placeholders::_1));
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListMultiMapWrapper>::GenerateFunctions(codegen);
        MapFunc = GenerateMapper(codegen, TBaseComputation::MakeName("Next"));
        codegen.ExportSymbol(MapFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListMultiMapWrapper>::FinalizeFunctions(codegen);
        if (MapFunc)
            Map = reinterpret_cast<TMapPtr>(codegen.GetPointerToFunction(MapFunc));
    }

    Function* GenerateMapper(NYql::NCodegen::ICodegen& codegen, const TString& name) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);

        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto positionType = Type::getInt64Ty(context);
        const auto containerType = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ? static_cast<Type*>(PointerType::getUnqual(valueType)) : static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt1Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(positionType), PointerType::getUnqual(valueType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        ctx.Annotator = &annotator;

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto positionArg = &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto container = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
            new LoadInst(valueType, containerArg, "load_container", false, block) : static_cast<Value*>(containerArg);

        const auto position = new LoadInst(positionArg->getType()->getPointerElementType(), positionArg, "position", false, block);

        const auto zero = BasicBlock::Create(context, "zero", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto choise = SwitchInst::Create(position, zero, NewItems.size() - 1U, block);

        for (ui32 i = 1U; i < NewItems.size();) {
            const auto part = BasicBlock::Create(context, (TString("part_") += ToString(i)).c_str(), ctx.Func);
            choise->addCase(ConstantInt::get(positionType, i), part);

            block = part;

            SafeUnRefUnboxed(valuePtr, ctx, block);
            GetNodeValue(valuePtr, NewItems[i], ctx, block);
            const auto next = ++i;
            new StoreInst(ConstantInt::get(positionType, NewItems.size() <= next ? 0 : next), positionArg, block);
            ReturnInst::Create(context, ConstantInt::getTrue(context), block);
        }

        block = zero;

        const auto itemPtr = codegenItem->CreateRefValue(ctx, block);
        const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(statusType, container, codegen, block, itemPtr);

        BranchInst::Create(good, done, status, block);
        block = good;

        SafeUnRefUnboxed(valuePtr, ctx, block);
        GetNodeValue(valuePtr, NewItems.front(), ctx, block);
        new StoreInst(ConstantInt::get(positionType, 1), positionArg, block);

        BranchInst::Create(done, block);
        block = done;

        ReturnInst::Create(context, status, block);
        return ctx.Func;
    }

    using TMapPtr = TCodegenValue::TNextPtr;

    Function* MapFunc = nullptr;

    TMapPtr Map = nullptr;
#endif

    IComputationNode* const List;
    IComputationExternalNode* const Item;
    const TComputationNodePtrVector NewItems;
};

class TNarrowMultiMapWrapper : public TStatefulFlowCodegeneratorNode<TNarrowMultiMapWrapper> {
using TBaseComputation = TStatefulFlowCodegeneratorNode<TNarrowMultiMapWrapper>;
public:
    TNarrowMultiMapWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, TComputationNodePtrVector&& newItems)
        : TBaseComputation(mutables, flow, kind)
        , Flow(flow)
        , Items(std::move(items))
        , NewItems(std::move(newItems))
        , PasstroughtMap(GetPasstroughtMap(Items, NewItems))
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Items.size()))
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish())
            return NUdf::TUnboxedValuePod::MakeFinish();

        const auto pos = state.IsInvalid() ? 0ULL : state.Get<ui64>();
        if (!pos) {
            auto** fields = ctx.WideFields.data() + WideFieldsIndex;

            for (auto i = 0U; i < Items.size(); ++i)
                if (Items[i]->GetDependencesCount() > 0U || PasstroughtMap[i])
                    fields[i] = &Items[i]->RefValue(ctx);

            switch (Flow->FetchValues(ctx, fields)) {
                case EFetchResult::Finish:
                    return NUdf::TUnboxedValuePod::MakeFinish();
                case EFetchResult::Yield:
                    return NUdf::TUnboxedValuePod::MakeYield();
                default:
                    break;
            }
        }

        const auto next = pos + 1ULL;
        state = NewItems.size() == next ? NUdf::TUnboxedValuePod::Invalid() : NUdf::TUnboxedValuePod(ui64(next));
        return NewItems[pos]->GetValue(ctx).Release();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto state = new LoadInst(valueType, statePtr, "state", block);

        const auto zero = BasicBlock::Create(context, "zero", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);

        const auto result = PHINode::Create(valueType, NewItems.size() + 1U, "result", pass);

        const auto choise = SwitchInst::Create(state, zero, NewItems.size() - 1U, block);

        for (ui32 i = 1U; i < NewItems.size();) {
            const auto part = BasicBlock::Create(context, (TString("part_") += ToString(i)).c_str(), ctx.Func);
            choise->addCase(GetConstant(i, context), part);

            block = part;

            const auto out = GetNodeValue(NewItems[i], ctx, block);
            result->addIncoming(out, block);
            const auto next = ++i;
            new StoreInst(NewItems.size() <= next ? GetInvalid(context) : GetConstant(next, context), statePtr, block);
            BranchInst::Create(pass, block);
        }

        {
            block = zero;

            const auto getres = GetNodeValues(Flow, ctx, block);

            const auto yield = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, getres.first, ConstantInt::get(getres.first->getType(), 0), "yield", block);
            const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, getres.first, ConstantInt::get(getres.first->getType(), 0), "good", block);

            const auto outres = SelectInst::Create(yield, GetYield(context), GetFinish(context), "outres", block);

            result->addIncoming(outres, block);

            BranchInst::Create(work, pass, good, block);

            block = work;

            Value* head = nullptr;
            for (auto i = 0U; i < Items.size(); ++i) {
                if (Items[i]->GetDependencesCount() > 0U || PasstroughtMap[i]) {
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Items[i])->CreateSetValue(ctx, block, NewItems.front() == Items[i] ? (head = getres.second[i](ctx, block)) : getres.second[i](ctx, block));
                }
            }

            const auto out = head ? head : GetNodeValue(NewItems.front(), ctx, block);
            result->addIncoming(out, block);
            new StoreInst(GetConstant(1ULL, context), statePtr, block);
            BranchInst::Create(pass, block);
        }

        block = pass;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            std::for_each(Items.cbegin(), Items.cend(), std::bind(&TNarrowMultiMapWrapper::Own, flow, std::placeholders::_1));
            std::for_each(NewItems.cbegin(), NewItems.cend(), std::bind(&TNarrowMultiMapWrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationWideFlowNode* const Flow;
    const TComputationExternalNodePtrVector Items;
    const TComputationNodePtrVector NewItems;

    const TPasstroughtMap PasstroughtMap;

    const ui32 WideFieldsIndex;
};

}

IComputationNode* WrapMultiMap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() > 2U, "Expected at least three arguments.");

    const auto listType = callable.GetInput(0).GetStaticType();
    const auto type = callable.GetType()->GetReturnType();
    const auto list = LocateNode(ctx.NodeLocator, callable, 0);

    TComputationNodePtrVector newItems;
    newItems.reserve(callable.GetInputsCount() - 2U);
    ui32 index = 1U;
    std::generate_n(std::back_inserter(newItems), callable.GetInputsCount() - 2U, [&](){ return LocateNode(ctx.NodeLocator, callable, ++index); });

    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1U);
    if (listType->IsFlow()) {
        return new TFlowMultiMapWrapper(ctx.Mutables, GetValueRepresentation(type), list, itemArg, std::move(newItems));
    } else if (listType->IsList()) {
        return new TListMultiMapWrapper(ctx.Mutables, list, itemArg, std::move(newItems));
    }

    THROW yexception() << "Expected flow or list.";
}

IComputationNode* WrapNarrowMultiMap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() > 2U, "Expected at least three arguments.");
    auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, callable.GetInput(0U).GetStaticType()));
    const auto width = wideComponents.size();
    MKQL_ENSURE(callable.GetInputsCount() > width + 2U, "Wrong signature.");
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);
    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        TComputationNodePtrVector newItems;
        newItems.reserve(callable.GetInputsCount() - width - 1U);
        ui32 index = width;
        std::generate_n(std::back_inserter(newItems), callable.GetInputsCount() - width - 1U, [&](){ return LocateNode(ctx.NodeLocator, callable, ++index); });

        TComputationExternalNodePtrVector args;
        args.reserve(width);
        index = 0U;
        std::generate_n(std::back_inserter(args), width, [&](){ return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

        return new TNarrowMultiMapWrapper(ctx.Mutables, GetValueRepresentation(callable.GetType()->GetReturnType()), wide, std::move(args), std::move(newItems));
    }

    THROW yexception() << "Expected wide flow.";
}

}
}
