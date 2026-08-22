#include "mkql_multimap.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/utils/cast.h>

#include <util/string/cast.h>

#include <utility>

namespace NKikimr::NMiniKQL {

#ifndef MKQL_DISABLE_CODEGEN
using NYql::EnsureDynamicCast;
#endif

namespace {

class TFlowMultiMapWrapper: public TStatefulFlowCodegeneratorNode<TFlowMultiMapWrapper> {
    using TBaseComputation = TStatefulFlowCodegeneratorNode<TFlowMultiMapWrapper>;

public:
    TFlowMultiMapWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow, IComputationExternalNode* item, TComputationNodePtrVector&& newItems)
        : TBaseComputation(mutables, flow, kind)
        , Flow_(flow)
        , Item_(item)
        , NewItems_(std::move(newItems))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        }

        const auto pos = state.IsInvalid() ? 0ULL : state.Get<ui64>();
        if (!pos) {
            if (auto item = Flow_->GetValue(ctx); item.IsSpecial()) {
                return item.Release();
            } else {
                Item_->SetValue(ctx, std::move(item));
            }
        }

        const auto next = pos + 1ULL;
        state = NewItems_.size() == next ? NUdf::TUnboxedValuePod::Invalid() : NUdf::TUnboxedValuePod(ui64(next));
        return NewItems_[pos]->GetValue(ctx).Release();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item_);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);
        const auto state = new LoadInst(valueType, statePtr, "state", block);

        const auto zero = BasicBlock::Create(context, "zero", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);

        const auto result = PHINode::Create(valueType, NewItems_.size() + 1U, "result", pass);

        const auto choise = SwitchInst::Create(state, zero, NewItems_.size() - 1U, block);

        for (ui32 i = 1U; i < NewItems_.size();) {
            const auto part = BasicBlock::Create(context, (TString("part_") += ToString(i)).c_str(), ctx.Func);
            choise->addCase(GetConstant(i, context), part);

            block = part;

            const auto out = GetNodeValue(NewItems_[i], ctx, block);
            result->addIncoming(out, block);
            const auto next = ++i;
            new StoreInst(NewItems_.size() <= next ? GetInvalid(context) : GetConstant(next, context), statePtr, block);
            BranchInst::Create(pass, block);
        }

        {
            block = zero;

            const auto item = GetNodeValue(Flow_, ctx, block);
            result->addIncoming(item, block);

            BranchInst::Create(pass, work, IsSpecial(item, block, context), block);

            block = work;

            codegenItem->CreateSetValue(ctx, block, item);
            const auto out = GetNodeValue(NewItems_.front(), ctx, block);
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
        if (const auto flow = FlowDependsOn(Flow_)) {
            Own(flow, Item_);
            std::for_each(NewItems_.cbegin(), NewItems_.cend(), std::bind(&TFlowMultiMapWrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationNode* const Flow_;
    IComputationExternalNode* const Item_;
    const TComputationNodePtrVector NewItems_;
};

class TListMultiMapWrapper: public TBothWaysCodegeneratorNode<TListMultiMapWrapper> {
private:
    using TBaseComputation = TBothWaysCodegeneratorNode<TListMultiMapWrapper>;

    class TListValue: public TCustomListValue {
    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& iter, IComputationExternalNode* item, TComputationNodePtrVector newItems)
                : TComputationValue<TIterator>(memInfo)
                , CompCtx_(compCtx)
                , Iter_(std::move(iter))
                , Item_(item)
                , NewItems_(std::move(newItems))
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                if (!Position_) {
                    NYql::NUdf::TUnboxedValue fetchResult;
                    if (!Iter_.Next(fetchResult)) {
                        return false;
                    }
                    Item_->SetValue(CompCtx_, std::move(fetchResult));
                }

                value = NewItems_[Position_]->GetValue(CompCtx_);
                if (++Position_ == NewItems_.size()) {
                    Position_ = 0U;
                }
                return true;
            }

            TComputationContext& CompCtx_;
            const NUdf::TUnboxedValue Iter_;
            IComputationExternalNode* const Item_;
            const TComputationNodePtrVector NewItems_;
            size_t Position_ = 0U;
        };

        TListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& list, IComputationExternalNode* item, TComputationNodePtrVector newItems)
            : TCustomListValue(memInfo)
            , CompCtx_(compCtx)
            , List_(std::move(list))
            , Item_(item)
            , NewItems_(std::move(newItems))
        {
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            return CompCtx_.HolderFactory.Create<TIterator>(CompCtx_, List_.GetListIterator(), Item_, NewItems_);
        }

        ui64 GetListLength() const final {
            if (!Length_) {
                Length_ = List_.GetListLength() * NewItems_.size();
            }

            return *Length_;
        }

        bool HasListItems() const final {
            if (!HasItems_) {
                HasItems_ = List_.HasListItems();
            }

            return *HasItems_;
        }

        bool HasFastListLength() const final {
            return List_.HasFastListLength();
        }

        TComputationContext& CompCtx_;
        const NUdf::TUnboxedValue List_;
        IComputationExternalNode* const Item_;
        const TComputationNodePtrVector NewItems_;
    };

public:
    TListMultiMapWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationExternalNode* item, TComputationNodePtrVector&& newItems)
        : TBaseComputation(mutables)
        , List_(list)
        , Item_(item)
        , NewItems_(std::move(newItems))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto list = List_->GetValue(ctx);

        if (auto elements = list.GetElements()) {
            auto size = list.GetListLength();
            NUdf::TUnboxedValue* items = nullptr;
            const auto result = ctx.HolderFactory.CreateDirectArrayHolder(size * NewItems_.size(), items);
            while (size--) {
                Item_->SetValue(ctx, NUdf::TUnboxedValue(*elements++));
                for (const auto newItem : NewItems_) {
                    *items++ = newItem->GetValue(ctx);
                }
            }
            return result;
        }

        return ctx.HolderFactory.Create<TListValue>(ctx, std::move(list), Item_, NewItems_);
    }

#ifndef MKQL_DISABLE_CODEGEN
    using TCodegenValue = TCustomListCodegenStatefulValueT<TCodegenStatefulIterator<ui64>>;

    NUdf::TUnboxedValuePod MakeLazyList(TComputationContext& ctx, const NUdf::TUnboxedValuePod value) const {
        return ctx.HolderFactory.Create<TCodegenValue>(Map_, &ctx, value);
    }

    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item_);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto list = GetNodeValue(List_, ctx, block);

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
            const auto itemsPtr = *Stateless_ || ctx.AlwaysInline ? new AllocaInst(elementsType, 0U, "items_ptr", &ctx.Func->getEntryBlock().back()) : new AllocaInst(elementsType, 0U, "items_ptr", block);
            const auto full = BinaryOperator::CreateMul(size, ConstantInt::get(size->getType(), NewItems_.size()), "full", block);
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
            const auto from = BinaryOperator::CreateMul(index, ConstantInt::get(index->getType(), NewItems_.size()), "from", block);

            for (ui32 i = 0U; i < NewItems_.size(); ++i) {
                const auto pos = BinaryOperator::CreateAdd(from, ConstantInt::get(from->getType(), i), (TString("pos_") += ToString(i)).c_str(), block);
                const auto dst = GetElementPtrInst::CreateInBounds(list->getType(), items, {pos}, (TString("dst_") += ToString(i)).c_str(), block);
                GetNodeValue(dst, NewItems_[i], ctx, block);
            }

            const auto plus = BinaryOperator::CreateAdd(index, ConstantInt::get(size->getType(), 1), "plus", block);
            index->addIncoming(plus, block);
            BranchInst::Create(loop, block);

            block = stop;
            if (List_->IsTemporaryValue()) {
                CleanupBoxed(list, ctx, block);
            }
            map->addIncoming(array, block);
            BranchInst::Create(done, block);
        }

        {
            block = lazy;

            const auto ptrType = PointerType::getUnqual(StructType::get(context));
            const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
            const auto value = EmitFunctionCall<&TListMultiMapWrapper::MakeLazyList>(list->getType(), {self, ctx.Ctx, list}, ctx, block);
            map->addIncoming(value, block);
            BranchInst::Create(done, block);
        }

        block = done;
        return map;
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(List_);
        Own(Item_);
        std::for_each(NewItems_.cbegin(), NewItems_.cend(), std::bind(&TListMultiMapWrapper::DependsOn, this, std::placeholders::_1));
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListMultiMapWrapper>::GenerateFunctions(codegen);
        MapFunc_ = GenerateMapper(codegen, TBaseComputation::MakeName("Next"));
        codegen.ExportSymbol(MapFunc_);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListMultiMapWrapper>::FinalizeFunctions(codegen);
        if (MapFunc_) {
            Map_ = reinterpret_cast<TMapPtr>(codegen.GetPointerToFunction(MapFunc_));
        }
    }

    Function* GenerateMapper(NYql::NCodegen::ICodegen& codegen, const TString& name) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item_);

        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto valueType = Type::getInt128Ty(context);
        const auto positionType = Type::getInt64Ty(context);
        const auto containerType = static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt1Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(positionType), PointerType::getUnqual(valueType)}, /*isVarArg=*/false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto positionArg = &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto container = static_cast<Value*>(containerArg);

        const auto position = new LoadInst(positionType, positionArg, "position", /*isVolatile=*/false, block);

        const auto zero = BasicBlock::Create(context, "zero", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto choise = SwitchInst::Create(position, zero, NewItems_.size() - 1U, block);

        for (ui32 i = 1U; i < NewItems_.size();) {
            const auto part = BasicBlock::Create(context, (TString("part_") += ToString(i)).c_str(), ctx.Func);
            choise->addCase(ConstantInt::get(positionType, i), part);

            block = part;

            SafeUnRefUnboxedOne(valuePtr, ctx, block);
            GetNodeValue(valuePtr, NewItems_[i], ctx, block);
            const auto next = ++i;
            new StoreInst(ConstantInt::get(positionType, NewItems_.size() <= next ? 0 : next), positionArg, block);
            ReturnInst::Create(context, ConstantInt::getTrue(context), block);
        }

        block = zero;

        const auto [status, itemPtr] = RefValueWithCallResult(codegenItem, ctx, block, [&](Value* itemPtr) {
            return CallBoxedValueNext(container, ctx, block, itemPtr);
        });
        BranchInst::Create(good, done, status, block);
        block = good;

        SafeUnRefUnboxedOne(valuePtr, ctx, block);
        GetNodeValue(valuePtr, NewItems_.front(), ctx, block);
        new StoreInst(ConstantInt::get(positionType, 1), positionArg, block);

        BranchInst::Create(done, block);
        block = done;

        ReturnInst::Create(context, status, block);
        return ctx.Func;
    }

    using TMapPtr = TCodegenValue::TNextPtr;

    Function* MapFunc_ = nullptr;

    TMapPtr Map_ = nullptr;
#endif

    IComputationNode* const List_;
    IComputationExternalNode* const Item_;
    const TComputationNodePtrVector NewItems_;
};

class TNarrowMultiMapWrapper: public TStatefulFlowCodegeneratorNode<TNarrowMultiMapWrapper> {
    using TBaseComputation = TStatefulFlowCodegeneratorNode<TNarrowMultiMapWrapper>;

public:
    TNarrowMultiMapWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, TComputationNodePtrVector&& newItems)
        : TBaseComputation(mutables, flow, kind)
        , Flow_(flow)
        , Items_(std::move(items))
        , NewItems_(std::move(newItems))
        , PasstroughtMap_(GetPasstroughtMap(Items_, NewItems_))
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Items_.size()))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        }

        const auto pos = state.IsInvalid() ? 0ULL : state.Get<ui64>();
        if (!pos) {
            auto** fields = ctx.WideFields.data() + WideFieldsIndex_;

            for (auto i = 0U; i < Items_.size(); ++i) {
                if (Items_[i]->GetDependentsCount() > 0U || PasstroughtMap_[i]) {
                    fields[i] = &Items_[i]->RefValue(ctx);
                }
            }

            switch (Flow_->FetchValues(ctx, fields)) {
                case EFetchResult::Finish:
                    return NUdf::TUnboxedValuePod::MakeFinish();
                case EFetchResult::Yield:
                    return NUdf::TUnboxedValuePod::MakeYield();
                default:
                    break;
            }
        }

        const auto next = pos + 1ULL;
        state = NewItems_.size() == next ? NUdf::TUnboxedValuePod::Invalid() : NUdf::TUnboxedValuePod(ui64(next));
        return NewItems_[pos]->GetValue(ctx).Release();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto state = new LoadInst(valueType, statePtr, "state", block);

        const auto zero = BasicBlock::Create(context, "zero", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);

        const auto result = PHINode::Create(valueType, NewItems_.size() + 1U, "result", pass);

        const auto choise = SwitchInst::Create(state, zero, NewItems_.size() - 1U, block);

        for (ui32 i = 1U; i < NewItems_.size();) {
            const auto part = BasicBlock::Create(context, (TString("part_") += ToString(i)).c_str(), ctx.Func);
            choise->addCase(GetConstant(i, context), part);

            block = part;

            const auto out = GetNodeValue(NewItems_[i], ctx, block);
            result->addIncoming(out, block);
            const auto next = ++i;
            new StoreInst(NewItems_.size() <= next ? GetInvalid(context) : GetConstant(next, context), statePtr, block);
            BranchInst::Create(pass, block);
        }

        {
            block = zero;

            const auto getres = GetNodeValues(Flow_, ctx, block);

            const auto yield = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, getres.first, ConstantInt::get(getres.first->getType(), 0), "yield", block);
            const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, getres.first, ConstantInt::get(getres.first->getType(), 0), "good", block);

            const auto outres = SelectInst::Create(yield, GetYield(context), GetFinish(context), "outres", block);

            result->addIncoming(outres, block);

            BranchInst::Create(work, pass, good, block);

            block = work;

            Value* head = nullptr;
            for (auto i = 0U; i < Items_.size(); ++i) {
                if (Items_[i]->GetDependentsCount() > 0U || PasstroughtMap_[i]) {
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Items_[i])->CreateSetValue(ctx, block, NewItems_.front() == Items_[i] ? (head = getres.second[i](ctx, block)) : getres.second[i](ctx, block));
                }
            }

            const auto out = head ? head : GetNodeValue(NewItems_.front(), ctx, block);
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
        if (const auto flow = FlowDependsOn(Flow_)) {
            std::for_each(Items_.cbegin(), Items_.cend(), std::bind(&TNarrowMultiMapWrapper::Own, flow, std::placeholders::_1));
            std::for_each(NewItems_.cbegin(), NewItems_.cend(), std::bind(&TNarrowMultiMapWrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationWideFlowNode* const Flow_;
    const TComputationExternalNodePtrVector Items_;
    const TComputationNodePtrVector NewItems_;

    const TPasstroughtMap PasstroughtMap_;

    const ui32 WideFieldsIndex_;
};

} // namespace

IComputationNode* WrapMultiMap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() > 2U, "Expected at least three arguments.");

    const auto listType = callable.GetInput(0).GetStaticType();
    const auto type = callable.GetType()->GetReturnType();
    const auto list = LocateNode(ctx.NodeLocator, callable, 0);

    TComputationNodePtrVector newItems;
    newItems.reserve(callable.GetInputsCount() - 2U);
    ui32 index = 1U;
    std::generate_n(std::back_inserter(newItems), callable.GetInputsCount() - 2U, [&]() { return LocateNode(ctx.NodeLocator, callable, ++index); });

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
        std::generate_n(std::back_inserter(newItems), callable.GetInputsCount() - width - 1U, [&]() { return LocateNode(ctx.NodeLocator, callable, ++index); });

        TComputationExternalNodePtrVector args;
        args.reserve(width);
        index = 0U;
        std::generate_n(std::back_inserter(args), width, [&]() { return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

        return new TNarrowMultiMapWrapper(ctx.Mutables, GetValueRepresentation(callable.GetType()->GetReturnType()), wide, std::move(args), std::move(newItems));
    }

    THROW yexception() << "Expected wide flow.";
}

} // namespace NKikimr::NMiniKQL
