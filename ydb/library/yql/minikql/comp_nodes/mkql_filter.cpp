#include "mkql_filter.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TFilterFlowWrapper : public TStatelessFlowCodegeneratorNode<TFilterFlowWrapper> {
    typedef TStatelessFlowCodegeneratorNode<TFilterFlowWrapper> TBaseComputation;
public:
     TFilterFlowWrapper(EValueRepresentation kind, IComputationNode* flow, IComputationExternalNode* item, IComputationNode* predicate)
        : TBaseComputation(flow, kind), Flow(flow), Item(item), Predicate(predicate)
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        do if (auto item = Flow->GetValue(ctx); item.IsSpecial())
                return item;
            else
                Item->SetValue(ctx, std::move(item));
        while (!Predicate->GetValue(ctx).template Get<bool>());
        return Item->GetValue(ctx);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        BranchInst::Create(loop, block);

        block = loop;
        const auto item = GetNodeValue(Flow, ctx, block);
        BranchInst::Create(exit, good, IsSpecial(item, block), block);

        block = good;
        codegenItem->CreateSetValue(ctx, block, item);
        const auto pred = GetNodeValue(Predicate, ctx, block);
        const auto bit = CastInst::Create(Instruction::Trunc, pred, Type::getInt1Ty(context), "bit", block);

        BranchInst::Create(exit, loop, bit, block);

        block = exit;
        return item;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            Own(flow, Item);
            DependsOn(flow, Predicate);
        }
    }

    IComputationNode* const Flow;
    IComputationExternalNode* const Item;
    IComputationNode* const Predicate;
};

class TFilterWithLimitFlowWrapper : public TStatefulFlowCodegeneratorNode<TFilterWithLimitFlowWrapper> {
    typedef TStatefulFlowCodegeneratorNode<TFilterWithLimitFlowWrapper> TBaseComputation;
public:
     TFilterWithLimitFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow, IComputationNode* limit, IComputationExternalNode* item, IComputationNode* predicate)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Embedded), Flow(flow), Limit(limit), Item(item), Predicate(predicate)
    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            state = Limit->GetValue(ctx);
        } else if (!state.Get<ui64>()) {
            return NUdf::TUnboxedValuePod::MakeFinish();
        }

        do if (auto item = Flow->GetValue(ctx); item.IsSpecial())
                return item;
            else
                Item->SetValue(ctx, std::move(item));
        while (!Predicate->GetValue(ctx).template Get<bool>());

        auto todo = state.Get<ui64>();
        state = NUdf::TUnboxedValuePod(--todo);
        return Item->GetValue(ctx);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto test = BasicBlock::Create(context, "test", ctx.Func);
        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto step = BasicBlock::Create(context, "step", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        const auto valueType = Type::getInt128Ty(context);
        const auto result = PHINode::Create(valueType, 3U, "result", exit);

        BranchInst::Create(init, test, IsInvalid(statePtr, block), block);

        block = init;

        GetNodeValue(statePtr, Limit, ctx, block);
        BranchInst::Create(test, block);

        block = test;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto done = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, state, GetFalse(context), "done", block);
        result->addIncoming(GetFinish(context), block);
        BranchInst::Create(exit, loop, done, block);

        block = loop;
        const auto item = GetNodeValue(Flow, ctx, block);
        result->addIncoming(item, block);
        BranchInst::Create(exit, step, IsSpecial(item, block), block);

        block = step;
        codegenItem->CreateSetValue(ctx, block, item);
        const auto pred = GetNodeValue(Predicate, ctx, block);
        const auto bit = CastInst::Create(Instruction::Trunc, pred, Type::getInt1Ty(context), "bit", block);

        BranchInst::Create(good, loop, bit, block);

        block = good;
        const auto decr = BinaryOperator::CreateSub(state, ConstantInt::get(state->getType(), 1ULL), "decr", block);
        new StoreInst(decr, statePtr, block);

        result->addIncoming(item, block);
        BranchInst::Create(exit, block);

        block = exit;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            DependsOn(flow, Limit);
            Own(flow, Item);
            DependsOn(flow, Predicate);
        }
    }

    IComputationNode* const Flow;
    IComputationNode* const Limit;
    IComputationExternalNode* const Item;
    IComputationNode* const Predicate;
};

template <bool IsStream>
class TBaseFilterWrapper {
protected:
    class TListValue : public TCustomListValue {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& iter, IComputationExternalNode* item, IComputationNode* predicate)
                : TComputationValue<TIterator>(memInfo)
                , CompCtx(compCtx)
                , Iter(std::move(iter))
                , Item(item)
                , Predicate(predicate)
            {}

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                while (Iter.Next(Item->RefValue(CompCtx))) {
                    if (Predicate->GetValue(CompCtx).template Get<bool>()) {
                        value = Item->GetValue(CompCtx);
                        return true;
                    }
                }

                return false;
            }

            TComputationContext& CompCtx;
            const NUdf::TUnboxedValue Iter;
            IComputationExternalNode* const Item;
            IComputationNode* const Predicate;
        };

        TListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, const NUdf::TUnboxedValue& list, IComputationExternalNode* item, IComputationNode* predicate)
            : TCustomListValue(memInfo)
            , CompCtx(compCtx)
            , List(list)
            , Item(item)
            , Predicate(predicate)
        {
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            return CompCtx.HolderFactory.Create<TIterator>(CompCtx, List.GetListIterator(), Item, Predicate);
        }

        TComputationContext& CompCtx;
        const NUdf::TUnboxedValue List;
        IComputationExternalNode* const Item;
        IComputationNode* const Predicate;
    };

    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, const NUdf::TUnboxedValue& stream, IComputationExternalNode* item, IComputationNode* predicate)
            : TBase(memInfo)
            , CompCtx(compCtx)
            , Stream(stream)
            , Item(item)
            , Predicate(predicate)
        {
        }

    private:
        ui32 GetTraverseCount() const final {
            return 1;
        }

        NUdf::TUnboxedValue GetTraverseItem(ui32 index) const final {
            Y_UNUSED(index);
            return Stream;
        }

        NUdf::TUnboxedValue Save() const final {
            return NUdf::TUnboxedValue::Zero();
        }

        void Load(const NUdf::TStringRef&) final {}

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
            for (;;) {
                const auto status = Stream.Fetch(Item->RefValue(CompCtx));
                if (NUdf::EFetchStatus::Ok != status) {
                    return status;
                }

                if (Predicate->GetValue(CompCtx).template Get<bool>()) {
                    result = Item->GetValue(CompCtx);
                    return NUdf::EFetchStatus::Ok;
                }
            }

            return NUdf::EFetchStatus::Finish;
        }

        TComputationContext& CompCtx;
        const NUdf::TUnboxedValue Stream;
        IComputationExternalNode* const Item;
        IComputationNode* const Predicate;
    };

    TBaseFilterWrapper(IComputationNode* list, IComputationExternalNode* item, IComputationNode* predicate)
        : List(list), Item(item), Predicate(predicate)
    {}

#ifndef MKQL_DISABLE_CODEGEN
    Function* GenerateFilter(NYql::NCodegen::ICodegen& codegen, const TString& name) const {
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
        ctx.Annotator = &annotator;

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto container = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
            new LoadInst(valueType, containerArg, "load_container", false, block) : static_cast<Value*>(containerArg);

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(loop, block);
        block = loop;

        const auto itemPtr = codegenItem->CreateRefValue(ctx, block);
        const auto status = IsStream ?
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, container, codegen, block, itemPtr):
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(statusType, container, codegen, block, itemPtr);

        const auto icmp = IsStream ?
            CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), "cond", block) : status;

        BranchInst::Create(good, done, icmp, block);
        block = good;

        const auto item = new LoadInst(valueType, itemPtr, "item", block);
        const auto predicate = GetNodeValue(Predicate, ctx, block);

        const auto boolPred = CastInst::Create(Instruction::Trunc, predicate, Type::getInt1Ty(context), "bool", block);

        BranchInst::Create(pass, loop, boolPred, block);
        block = pass;

        SafeUnRefUnboxed(valuePtr, ctx, block);
        new StoreInst(item, valuePtr, block);
        ValueAddRef(Item->GetRepresentation(), valuePtr, ctx, block);
        BranchInst::Create(done, block);

        block = done;
        ReturnInst::Create(context, status, block);
        return ctx.Func;
    }

    using TCodegenValue = std::conditional_t<IsStream, TStreamCodegenValueStateless, TCustomListCodegenValue>;
    using TFilterPtr = std::conditional_t<IsStream, TStreamCodegenValueStateless::TFetchPtr, TCustomListCodegenValue::TNextPtr>;

    Function* FilterFunc = nullptr;

    TFilterPtr Filter = nullptr;
#endif

    IComputationNode* const List;
    IComputationExternalNode* const Item;
    IComputationNode* const Predicate;
};

template <bool IsStream>
class TBaseFilterWithLimitWrapper {
protected:
    class TListValue : public TCustomListValue {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, ui64 limit, NUdf::TUnboxedValue&& iter, IComputationExternalNode* item, IComputationNode* predicate)
                : TComputationValue<TIterator>(memInfo)
                , CompCtx(compCtx)
                , Iter(std::move(iter))
                , Limit(limit)
                , Item(item)
                , Predicate(predicate)
            {}

        private:
            bool Next(NUdf::TUnboxedValue& value) final {
                if (!Limit) {
                    return false;
                }
                while (Iter.Next(Item->RefValue(CompCtx))) {
                    if (Predicate->GetValue(CompCtx).template Get<bool>()) {
                        value = Item->GetValue(CompCtx);
                        --Limit;
                        return true;
                    }
                }

                return false;
            }

            TComputationContext& CompCtx;
            const NUdf::TUnboxedValue Iter;
            ui64 Limit;
            IComputationExternalNode* const Item;
            IComputationNode* const Predicate;
        };

        TListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, const NUdf::TUnboxedValue& list, ui64 limit,  IComputationExternalNode* item, IComputationNode* predicate)
            : TCustomListValue(memInfo)
            , CompCtx(compCtx)
            , List(list)
            , Limit(limit)
            , Item(item)
            , Predicate(predicate)
        {}

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            return CompCtx.HolderFactory.Create<TIterator>(CompCtx, Limit, List.GetListIterator(), Item, Predicate);
        }

        TComputationContext& CompCtx;
        const NUdf::TUnboxedValue List;
        const ui64 Limit;
        IComputationExternalNode* const Item;
        IComputationNode* const Predicate;
    };

    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, const NUdf::TUnboxedValue& stream, ui64 limit, IComputationExternalNode* item, IComputationNode* predicate)
            : TBase(memInfo)
            , CompCtx(compCtx)
            , Stream(stream)
            , Limit(limit)
            , Item(item)
            , Predicate(predicate)
        {
        }

    private:
        ui32 GetTraverseCount() const final {
            return 1;
        }

        NUdf::TUnboxedValue GetTraverseItem(ui32 index) const final {
            Y_UNUSED(index);
            return Stream;
        }

        NUdf::TUnboxedValue Save() const final {
            return NUdf::TUnboxedValue::Zero();
        }

        void Load(const NUdf::TStringRef&) final {}

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
            if (!Limit) {
                return NUdf::EFetchStatus::Finish;
            }

            for (;;) {
                const auto status = Stream.Fetch(Item->RefValue(CompCtx));
                if (NUdf::EFetchStatus::Ok != status) {
                    return status;
                }

                if (Predicate->GetValue(CompCtx).template Get<bool>()) {
                    result = Item->GetValue(CompCtx);
                    --Limit;
                    return NUdf::EFetchStatus::Ok;
                }
            }

            return NUdf::EFetchStatus::Finish;
        }

        TComputationContext& CompCtx;
        const NUdf::TUnboxedValue Stream;
        ui64 Limit;
        IComputationExternalNode* const Item;
        IComputationNode* const Predicate;
    };

    TBaseFilterWithLimitWrapper(IComputationNode* list, IComputationNode* limit, IComputationExternalNode* item, IComputationNode* predicate)
        : List(list), Limit(limit), Item(item), Predicate(predicate)
    {}

#ifndef MKQL_DISABLE_CODEGEN
    Function* GenerateFilter(NYql::NCodegen::ICodegen& codegen, const TString& name) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);

        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto limitType = Type::getInt64Ty(context);
        const auto containerType = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ? static_cast<Type*>(PointerType::getUnqual(valueType)) : static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = IsStream ? Type::getInt32Ty(context) : Type::getInt1Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(limitType), PointerType::getUnqual(valueType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        ctx.Annotator = &annotator;

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto limitArg = &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto nope = BasicBlock::Create(context, "nope", ctx.Func);
        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto limit = new LoadInst(limitType, limitArg, "limit", false, block);
        const auto zero = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, limit, ConstantInt::get(limit->getType(), 0), "zero", block);
        BranchInst::Create(nope, init, zero, block);

        block = nope;
        ReturnInst::Create(context, IsStream ? ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)) : ConstantInt::getFalse(context), block);

        block = init;
        const auto container = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
            new LoadInst(valueType, containerArg, "load_container", false, block) : static_cast<Value*>(containerArg);

        BranchInst::Create(loop, block);
        block = loop;

        const auto itemPtr = codegenItem->CreateRefValue(ctx, block);
        const auto status = IsStream ?
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, container, codegen, block, itemPtr):
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(statusType, container, codegen, block, itemPtr);

        const auto icmp = IsStream ?
            CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), "cond", block) : status;

        BranchInst::Create(good, done, icmp, block);
        block = good;

        const auto item = new LoadInst(valueType, itemPtr, "item", block);
        const auto predicate = GetNodeValue(Predicate, ctx, block);

        const auto boolPred = CastInst::Create(Instruction::Trunc, predicate, Type::getInt1Ty(context), "bool", block);

        BranchInst::Create(pass, loop, boolPred, block);
        block = pass;

        const auto decr = BinaryOperator::CreateSub(limit, ConstantInt::get(limit->getType(), 1ULL), "decr", block);
        new StoreInst(decr, limitArg, block);

        SafeUnRefUnboxed(valuePtr, ctx, block);
        new StoreInst(item, valuePtr, block);
        ValueAddRef(Item->GetRepresentation(), valuePtr, ctx, block);
        BranchInst::Create(done, block);

        block = done;
        ReturnInst::Create(context, status, block);
        return ctx.Func;
    }

    using TCodegenValue = std::conditional_t<IsStream, TStreamCodegenStatefulValueT<ui64>, TCustomListCodegenStatefulValueT<TCodegenStatefulIterator<ui64>>>;
    using TFilterPtr = std::conditional_t<IsStream, TStreamCodegenStatefulValueT<ui64>::TFetchPtr, TCustomListCodegenStatefulValueT<TCodegenStatefulIterator<ui64>>::TNextPtr>;

    Function* FilterFunc = nullptr;

    TFilterPtr Filter = nullptr;
#endif

    IComputationNode* const List;
    IComputationNode* const Limit;
    IComputationExternalNode* const Item;
    IComputationNode* const Predicate;
};

class TStreamFilterWrapper : public TCustomValueCodegeneratorNode<TStreamFilterWrapper>,
    private TBaseFilterWrapper<true> {
    typedef TBaseFilterWrapper<true> TBaseWrapper;
    typedef TCustomValueCodegeneratorNode<TStreamFilterWrapper> TBaseComputation;
public:
    TStreamFilterWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationExternalNode* item, IComputationNode* predicate)
        : TBaseComputation(mutables), TBaseWrapper(list, item, predicate)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Filter)
            return ctx.HolderFactory.Create<typename TBaseWrapper::TCodegenValue>(Filter, &ctx, List->GetValue(ctx));
#endif
        return ctx.HolderFactory.Create<typename TBaseWrapper::TStreamValue>(ctx, List->GetValue(ctx), Item, Predicate);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(List);
        Own(Item);
        DependsOn(Predicate);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        FilterFunc = GenerateFilter(codegen, TBaseComputation::MakeName("Fetch"));
        codegen.ExportSymbol(FilterFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (FilterFunc)
            Filter = reinterpret_cast<typename TBaseWrapper::TFilterPtr>(codegen.GetPointerToFunction(FilterFunc));
    }
#endif
};

class TStreamFilterWithLimitWrapper : public TCustomValueCodegeneratorNode<TStreamFilterWithLimitWrapper>,
    private TBaseFilterWithLimitWrapper<true> {
    typedef TBaseFilterWithLimitWrapper<true> TBaseWrapper;
    typedef TCustomValueCodegeneratorNode<TStreamFilterWithLimitWrapper> TBaseComputation;
public:
    TStreamFilterWithLimitWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationNode* limit, IComputationExternalNode* item, IComputationNode* predicate)
        : TBaseComputation(mutables), TBaseWrapper(list, limit, item, predicate)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Filter)
            return ctx.HolderFactory.Create<typename TBaseWrapper::TCodegenValue>(Filter, &ctx, List->GetValue(ctx), Limit->GetValue(ctx).Get<ui64>());
#endif
        return ctx.HolderFactory.Create<typename TBaseWrapper::TStreamValue>(ctx, List->GetValue(ctx), Limit->GetValue(ctx).Get<ui64>(), Item, Predicate);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(List);
        DependsOn(Limit);
        Own(Item);
        DependsOn(Predicate);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        FilterFunc = GenerateFilter(codegen, TBaseComputation::MakeName("Fetch"));
        codegen.ExportSymbol(FilterFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (FilterFunc)
            Filter = reinterpret_cast<typename TBaseWrapper::TFilterPtr>(codegen.GetPointerToFunction(FilterFunc));
    }
#endif
};

static constexpr size_t UseOnStack = 1ULL << 8ULL;
#ifndef MKQL_DISABLE_CODEGEN
ui64* MyAlloc(const ui64 size) { return TMKQLAllocator<ui64>::allocate(size); }
void MyFree(const ui64 *const ptr, const ui64 size) noexcept { TMKQLAllocator<ui64>::deallocate(ptr, size); }
#endif
class TListFilterWrapper : public TBothWaysCodegeneratorNode<TListFilterWrapper>,
    private TBaseFilterWrapper<false> {
    typedef TBaseFilterWrapper<false> TBaseWrapper;
    typedef TBothWaysCodegeneratorNode<TListFilterWrapper> TBaseComputation;
public:
    TListFilterWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationExternalNode* item, IComputationNode* predicate)
        : TBaseComputation(mutables), TBaseWrapper(list, item, predicate)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto list = List->GetValue(ctx);

        if (auto elements = list.GetElements()) {
            const auto size = list.GetListLength();

            std::array<ui64, UseOnStack> stackBitSet;
            std::unique_ptr<ui64[]> heapBitSet;

            const auto maskSize = (size + 63ULL) >> 6ULL;
            const bool useHeap = maskSize > UseOnStack;

            if (useHeap) {
                heapBitSet = std::make_unique<ui64[]>(maskSize);
            }

            const auto mask = useHeap ? heapBitSet.get() : stackBitSet.data();

            ui64 count = 0ULL;

            for (ui64 i = 0ULL; i < size;) {
                auto& m = mask[i >> 6ULL];
                m = 0ULL;
                for (ui64 bit = 1ULL; bit && i < size; bit <<= 1ULL) {
                    Item->SetValue(ctx, NUdf::TUnboxedValue(elements[i++]));
                    if (Predicate->GetValue(ctx).Get<bool>()) {
                        m |= bit;
                        ++count;
                    }
                }
            }

            if (count == size) {
                return list.Release();
            }

            NUdf::TUnboxedValue* items = nullptr;
            const auto result = ctx.HolderFactory.CreateDirectArrayHolder(count, items);
            for (auto p = mask; count; ++p) {
                auto e = elements;
                elements += 64ULL;
                for (auto bits = *p; bits; bits >>= 1ULL) {
                    if (bits & 1ULL) {
                        *items++ = *e;
                        if (!--count)
                            break;
                    }
                    ++e;
                }
            }
            return result;
        }

        return ctx.HolderFactory.Create<typename TBaseWrapper::TListValue>(ctx, std::move(list), Item, Predicate);
    }

#ifndef MKQL_DISABLE_CODEGEN
    NUdf::TUnboxedValuePod MakeLazyList(TComputationContext& ctx, const NUdf::TUnboxedValuePod value) const {
        return ctx.HolderFactory.Create<typename TBaseWrapper::TCodegenValue>(Filter, &ctx, value);
    }

    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto list = GetNodeValue(List, ctx, block);

        const auto lazy = BasicBlock::Create(context, "lazy", ctx.Func);
        const auto hard = BasicBlock::Create(context, "hard", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto out = PHINode::Create(list->getType(), 3U, "out", done);

        const auto elementsType = PointerType::getUnqual(list->getType());
        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(elementsType, list, ctx.Codegen, block);
        const auto fill = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, elements, ConstantPointerNull::get(elementsType), "fill", block);

        BranchInst::Create(hard, lazy, fill, block);

        {
            block = hard;

            const auto size = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListLength>(Type::getInt64Ty(context), list, ctx.Codegen, block);
            const auto allBits = ConstantInt::get(size->getType(), 63);
            const auto add = BinaryOperator::CreateAdd(size, allBits, "add", block);
            const auto shr = BinaryOperator::CreateLShr(add, ConstantInt::get(add->getType(), 6), "shr", block);

            const auto maskType = Type::getInt64Ty(context);
            const auto zeroMask = ConstantInt::get(maskType, 0);
            const auto plusMask = ConstantInt::get(maskType, 1);

            const auto smsk = BasicBlock::Create(context, "smsk", ctx.Func);
            const auto hmsk = BasicBlock::Create(context, "hmsk", ctx.Func);

            const auto main = BasicBlock::Create(context, "main", ctx.Func);
            const auto bits = PHINode::Create(PointerType::getUnqual(maskType), 2U, "bits", main);

            const auto zeroSize = ConstantInt::get(size->getType(), 0);
            const auto plusSize = ConstantInt::get(size->getType(), 1);

            const auto heap = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, shr, ConstantInt::get(add->getType(), UseOnStack), "heap", block);
            BranchInst::Create(hmsk, smsk, heap, block);

            {
                block = smsk;

                const auto arrayType = ArrayType::get(Type::getInt64Ty(context), UseOnStack);
                const auto array = *Stateless || ctx.AlwaysInline ?
                    new AllocaInst(arrayType, 0U, "array", &ctx.Func->getEntryBlock().back()):
                    new AllocaInst(arrayType, 0U, "array", block);
                const auto ptr = GetElementPtrInst::CreateInBounds(arrayType, array, {zeroSize, zeroSize}, "ptr", block);

                bits->addIncoming(ptr, block);
                BranchInst::Create(main, block);
            }

            {
                block = hmsk;

                const auto fnType = FunctionType::get(bits->getType(), {shr->getType()}, false);
                const auto name = "MyAlloc";
                ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&MyAlloc));
                const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType);
                const auto ptr = CallInst::Create(func, {shr}, "ptr", block);
                bits->addIncoming(ptr, block);
                BranchInst::Create(main, block);
            }

            block = main;

            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto test = BasicBlock::Create(context, "test", ctx.Func);
            const auto save = BasicBlock::Create(context, "save", ctx.Func);
            const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
            const auto make = BasicBlock::Create(context, "make", ctx.Func);
            const auto last = BasicBlock::Create(context, "last", ctx.Func);
            const auto free = BasicBlock::Create(context, "free", ctx.Func);

            const auto output = PHINode::Create(out->getType(), 3U, "output", last);
            const auto index = PHINode::Create(size->getType(), 3U, "index", loop);
            const auto count = PHINode::Create(size->getType(), 3U, "count", loop);
            const auto bitset = PHINode::Create(maskType, 3U, "bitset", loop);

            count->addIncoming(zeroSize, block);
            index->addIncoming(zeroSize, block);
            bitset->addIncoming(zeroMask, block);

            BranchInst::Create(loop, block);

            {
                block = loop;

                const auto plus = BinaryOperator::CreateAdd(index, plusSize, "plus", block);
                const auto more = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULE, plus, size, "more", block);
                BranchInst::Create(test, stop, more, block);

                block = test;

                const auto ptr = GetElementPtrInst::CreateInBounds(list->getType(), elements, {index}, "ptr", block);
                const auto item = new LoadInst(list->getType(), ptr, "item", block);
                codegenItem->CreateSetValue(ctx, block, item);
                const auto predicate = GetNodeValue(Predicate, ctx, block);
                const auto boolPred = CastInst::Create(Instruction::Trunc, predicate, Type::getInt1Ty(context), "bool", block);

                const auto inc = BinaryOperator::CreateAdd(count, plusSize, "inc", block);
                const auto mod = BinaryOperator::CreateAnd(index, allBits, "mod", block);
                const auto bit = BinaryOperator::CreateShl(plusMask, mod, "bit", block);
                const auto set = BinaryOperator::CreateOr(bitset, bit, "set", block);

                const auto newset = SelectInst::Create(boolPred, set, bitset, "newset", block);
                const auto newcount = SelectInst::Create(boolPred, inc, count, "newcount", block);

                const auto next = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, mod, allBits, "next", block);
                const auto last = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGE, plus, size, "last", block);
                const auto step = BinaryOperator::CreateOr(next, last, "step", block);

                index->addIncoming(plus, block);
                count->addIncoming(newcount, block);
                bitset->addIncoming(newset, block);
                BranchInst::Create(save, loop, step, block);

                block = save;
                const auto div = BinaryOperator::CreateLShr(index, ConstantInt::get(index->getType(), 6), "div", block);
                const auto savePtr = GetElementPtrInst::CreateInBounds(maskType, bits, {div}, "save_ptr", block);
                new StoreInst(newset, savePtr, block);

                index->addIncoming(plus, block);
                count->addIncoming(newcount, block);
                bitset->addIncoming(zeroMask, block);
                BranchInst::Create(loop, block);
            }

            block = stop;
            const auto asis = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, size, count, "asis", block);

            output->addIncoming(list, block);
            BranchInst::Create(last, make, asis, block);

            block = make;

            if (List->IsTemporaryValue()) {
                CleanupBoxed(list, ctx, block);
            }

            const auto itemsType = PointerType::getUnqual(list->getType());
            const auto itemsPtr = *Stateless || ctx.AlwaysInline ?
                new AllocaInst(itemsType, 0U, "items_ptr", &ctx.Func->getEntryBlock().back()):
                new AllocaInst(itemsType, 0U, "items_ptr", block);
            const auto array = GenNewArray(ctx, count, itemsPtr, block);
            const auto items = new LoadInst(itemsType, itemsPtr, "items", block);

            const auto move = BasicBlock::Create(context, "move", ctx.Func);
            const auto work = BasicBlock::Create(context, "work", ctx.Func);
            const auto bulk = BasicBlock::Create(context, "bulk", ctx.Func);
            const auto copy = BasicBlock::Create(context, "copy", ctx.Func);

            const auto one = PHINode::Create(count->getType(), 2U, "one", move);
            const auto two = PHINode::Create(count->getType(), 2U, "two", move);
            const auto idx = PHINode::Create(count->getType(), 2U, "idx", move);
            one->addIncoming(zeroSize, block);
            two->addIncoming(zeroSize, block);
            idx->addIncoming(zeroSize, block);

            const auto many = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, zeroSize, count, "many", block);
            output->addIncoming(array, block);
            BranchInst::Create(move, last, many, block);

            block = move;
            const auto mptr = GetElementPtrInst::CreateInBounds(maskType, bits, {idx}, "mptr", block);
            const auto load = new LoadInst(maskType, mptr, "load", block);

            const auto six = BinaryOperator::CreateAdd(one, ConstantInt::get(one->getType(), 64), "six", block);
            const auto inc = BinaryOperator::CreateAdd(idx, plusSize, "inc", block);

            const auto mask = PHINode::Create(load->getType(), 3U, "mask", bulk);
            const auto one0 = PHINode::Create(one->getType(), 3U, "one0", bulk);
            const auto two0 = PHINode::Create(two->getType(), 3U, "two0", bulk);

            mask->addIncoming(load, block);
            one0->addIncoming(one, block);
            two0->addIncoming(two, block);

            BranchInst::Create(bulk, block);

            block = bulk;
            const auto skip = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, zeroMask, mask, "skip", block);

            one->addIncoming(six, block);
            two->addIncoming(two0, block);
            idx->addIncoming(inc, block);
            BranchInst::Create(move, work, skip, block);

            block = work;

            const auto plus = BinaryOperator::CreateAdd(one0, plusSize, "plus", block);
            const auto down = BinaryOperator::CreateLShr(mask, plusMask, "down", block);
            const auto bit = CastInst::Create(Instruction::Trunc, mask, Type::getInt1Ty(context), "bit", block);

            mask->addIncoming(down, block);
            one0->addIncoming(plus, block);
            two0->addIncoming(two0, block);

            BranchInst::Create(copy, bulk, bit, block);

            block = copy;

            const auto src = GetElementPtrInst::CreateInBounds(list->getType(), elements, {one0}, "src", block);
            const auto dst = GetElementPtrInst::CreateInBounds(list->getType(), items, {two0}, "dst", block);

            const auto item = new LoadInst(list->getType(), src, "item", block);
            ValueAddRef(Item->GetRepresentation(), item, ctx, block);
            new StoreInst(item, dst, block);

            const auto next = BinaryOperator::CreateAdd(two0, plusSize, "next", block);
            const auto more = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, next, count, "more", block);

            one0->addIncoming(plus, block);
            two0->addIncoming(next, block);
            mask->addIncoming(down, block);

            output->addIncoming(array, block);

            BranchInst::Create(bulk, last, more, block);
            block = last;

            out->addIncoming(output, block);
            BranchInst::Create(free, done, heap, block);

            {
                block = free;

                const auto fnType = FunctionType::get(Type::getVoidTy(context), {bits->getType(), shr->getType()}, false);
                const auto name = "MyFree";
                ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&MyFree));
                const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType);
                CallInst::Create(func, {bits, shr}, "", block);

                out->addIncoming(output, block);
                BranchInst::Create(done, block);
            }
        }

        {
            block = lazy;

            const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TListFilterWrapper::MakeLazyList));
            const auto ptrType = PointerType::getUnqual(StructType::get(context));
            const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
            if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                const auto funType = FunctionType::get(list->getType() , {self->getType(), ctx.Ctx->getType(), list->getType()}, false);
                const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "function", block);
                const auto value = CallInst::Create(funType, doFuncPtr, {self, ctx.Ctx, list}, "value", block);
                out->addIncoming(value, block);
            } else {
                const auto resultPtr = new AllocaInst(list->getType(), 0U, "return", block);
                new StoreInst(list, resultPtr, block);
                const auto funType = FunctionType::get(Type::getVoidTy(context), {self->getType(), resultPtr->getType(), ctx.Ctx->getType(), resultPtr->getType()}, false);
                const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "function", block);
                CallInst::Create(funType, doFuncPtr, {self, resultPtr, ctx.Ctx, resultPtr}, "", block);
                const auto value = new LoadInst(list->getType(), resultPtr, "value", block);
                out->addIncoming(value, block);
            }
            BranchInst::Create(done, block);
        }

        block = done;
        return out;
    }
#endif

private:
    void RegisterDependencies() const final {
        DependsOn(List);
        Own(Item);
        DependsOn(Predicate);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListFilterWrapper>::GenerateFunctions(codegen);
        FilterFunc = GenerateFilter(codegen, TBaseComputation::MakeName("Next"));
        codegen.ExportSymbol(FilterFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListFilterWrapper>::FinalizeFunctions(codegen);
        if (FilterFunc)
            Filter = reinterpret_cast<typename TBaseWrapper::TFilterPtr>(codegen.GetPointerToFunction(FilterFunc));
    }
#endif
};

class TListFilterWithLimitWrapper : public TBothWaysCodegeneratorNode<TListFilterWithLimitWrapper>,
    private TBaseFilterWithLimitWrapper<false> {
    typedef TBaseFilterWithLimitWrapper<false> TBaseWrapper;
    typedef TBothWaysCodegeneratorNode<TListFilterWithLimitWrapper> TBaseComputation;
public:
    TListFilterWithLimitWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationNode* limit, IComputationExternalNode* item, IComputationNode* predicate)
        : TBaseComputation(mutables), TBaseWrapper(list, limit, item, predicate)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto limit = Limit->GetValue(ctx).Get<ui64>();

        auto list = List->GetValue(ctx);

        if (auto elements = list.GetElements()) {
            const auto size = list.GetListLength();

            std::array<ui64, UseOnStack> stackBitSet;
            std::unique_ptr<ui64[]> heapBitSet;

            const auto maskSize = (size + 63ULL) >> 6ULL;
            const bool useHeap = maskSize > UseOnStack;

            if (useHeap) {
                heapBitSet = std::make_unique<ui64[]>(maskSize);
            }

            const auto mask = useHeap ? heapBitSet.get() : stackBitSet.data();

            ui64 count = 0ULL;

            for (ui64 i = 0ULL; i < size && count < limit;) {
                auto& m = mask[i >> 6ULL];
                m = 0ULL;
                for (ui64 bit = 1ULL; bit && i < size && count < limit; bit <<= 1ULL) {
                    Item->SetValue(ctx, NUdf::TUnboxedValue(elements[i++]));
                    if (Predicate->GetValue(ctx).Get<bool>()) {
                        m |= bit;
                        ++count;
                    }
                }
            }

            if (count == size) {
                return list.Release();
            }

            NUdf::TUnboxedValue* items = nullptr;
            const auto result = ctx.HolderFactory.CreateDirectArrayHolder(count, items);
            for (auto p = mask; count; ++p) {
                auto e = elements;
                elements += 64ULL;
                for (auto bits = *p; bits; bits >>= 1ULL) {
                    if (bits & 1ULL) {
                        *items++ = *e;
                        if (!--count)
                            break;
                    }
                    ++e;
                }
            }
            return result;
        }

        return ctx.HolderFactory.Create<typename TBaseWrapper::TListValue>(ctx, std::move(list), limit, Item, Predicate);
    }

#ifndef MKQL_DISABLE_CODEGEN
    NUdf::TUnboxedValuePod MakeLazyList(TComputationContext& ctx, const NUdf::TUnboxedValuePod value, ui64 limit) const {
        return ctx.HolderFactory.Create<typename TBaseWrapper::TCodegenValue>(Filter, &ctx, value, std::move(limit));
    }

    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto lazy = BasicBlock::Create(context, "lazy", ctx.Func);
        const auto hard = BasicBlock::Create(context, "hard", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto limit = GetterFor<ui64>(GetNodeValue(Limit, ctx, block), context, block);

        const auto list = GetNodeValue(List, ctx, block);
        const auto out = PHINode::Create(list->getType(), 3U, "out", done);

        const auto elementsType = PointerType::getUnqual(list->getType());
        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(elementsType, list, ctx.Codegen, block);
        const auto fill = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, elements, ConstantPointerNull::get(elementsType), "fill", block);

        BranchInst::Create(hard, lazy, fill, block);

        {
            block = hard;

            const auto size = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListLength>(Type::getInt64Ty(context), list, ctx.Codegen, block);
            const auto allBits = ConstantInt::get(size->getType(), 63);
            const auto add = BinaryOperator::CreateAdd(size, allBits, "add", block);
            const auto shr = BinaryOperator::CreateLShr(add, ConstantInt::get(add->getType(), 6), "shr", block);

            const auto maskType = Type::getInt64Ty(context);
            const auto zeroMask = ConstantInt::get(maskType, 0);
            const auto plusMask = ConstantInt::get(maskType, 1);

            const auto smsk = BasicBlock::Create(context, "smsk", ctx.Func);
            const auto hmsk = BasicBlock::Create(context, "hmsk", ctx.Func);

            const auto main = BasicBlock::Create(context, "main", ctx.Func);
            const auto bits = PHINode::Create(PointerType::getUnqual(maskType), 2U, "bits", main);

            const auto zeroSize = ConstantInt::get(size->getType(), 0);
            const auto plusSize = ConstantInt::get(size->getType(), 1);

            const auto heap = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, shr, ConstantInt::get(add->getType(), UseOnStack), "heap", block);
            BranchInst::Create(hmsk, smsk, heap, block);

            {
                block = smsk;

                const auto arrayType = ArrayType::get(Type::getInt64Ty(context), UseOnStack);
                const auto array = *Stateless || ctx.AlwaysInline ?
                    new AllocaInst(arrayType, 0U, "array", &ctx.Func->getEntryBlock().back()):
                    new AllocaInst(arrayType, 0U, "array", block);
                const auto ptr = GetElementPtrInst::CreateInBounds(arrayType, array, {zeroSize, zeroSize}, "ptr", block);

                bits->addIncoming(ptr, block);
                BranchInst::Create(main, block);
            }

            {
                block = hmsk;

                const auto fnType = FunctionType::get(bits->getType(), {shr->getType()}, false);
                const auto name = "MyAlloc";
                ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&MyAlloc));
                const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType);
                const auto ptr = CallInst::Create(func, {shr}, "ptr", block);
                bits->addIncoming(ptr, block);
                BranchInst::Create(main, block);
            }

            block = main;

            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto test = BasicBlock::Create(context, "test", ctx.Func);
            const auto save = BasicBlock::Create(context, "save", ctx.Func);
            const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
            const auto make = BasicBlock::Create(context, "make", ctx.Func);
            const auto last = BasicBlock::Create(context, "last", ctx.Func);
            const auto free = BasicBlock::Create(context, "free", ctx.Func);

            const auto output = PHINode::Create(out->getType(), 3U, "output", last);
            const auto index = PHINode::Create(size->getType(), 3U, "index", loop);
            const auto count = PHINode::Create(size->getType(), 3U, "count", loop);
            const auto bitset = PHINode::Create(maskType, 3U, "bitset", loop);

            count->addIncoming(zeroSize, block);
            index->addIncoming(zeroSize, block);
            bitset->addIncoming(zeroMask, block);

            BranchInst::Create(loop, block);

            {
                block = loop;

                const auto plus = BinaryOperator::CreateAdd(index, plusSize, "plus", block);
                const auto less_index = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULE, plus, size, "less_index", block);
                const auto less_count = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, count, limit, "less_count", block);
                const auto more = BinaryOperator::CreateAnd(less_index, less_count, "more", block);
                BranchInst::Create(test, stop, more, block);

                block = test;

                const auto ptr = GetElementPtrInst::CreateInBounds(list->getType(), elements, {index}, "ptr", block);
                const auto item = new LoadInst(list->getType(), ptr, "item", block);
                codegenItem->CreateSetValue(ctx, block, item);
                const auto predicate = GetNodeValue(Predicate, ctx, block);
                const auto boolPred = CastInst::Create(Instruction::Trunc, predicate, Type::getInt1Ty(context), "bool", block);

                const auto inc = BinaryOperator::CreateAdd(count, plusSize, "inc", block);
                const auto mod = BinaryOperator::CreateAnd(index, allBits, "mod", block);
                const auto bit = BinaryOperator::CreateShl(plusMask, mod, "bit", block);
                const auto set = BinaryOperator::CreateOr(bitset, bit, "set", block);

                const auto newset = SelectInst::Create(boolPred, set, bitset, "newset", block);
                const auto newcount = SelectInst::Create(boolPred, inc, count, "newcount", block);

                const auto next = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, mod, allBits, "next", block);
                const auto last = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGE, plus, size, "last", block);
                const auto full = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGE, newcount, limit, "full", block);
                const auto good = BinaryOperator::CreateOr(full, last, "good", block);
                const auto step = BinaryOperator::CreateOr(next, good, "step", block);

                index->addIncoming(plus, block);
                count->addIncoming(newcount, block);
                bitset->addIncoming(newset, block);
                BranchInst::Create(save, loop, step, block);

                block = save;
                const auto div = BinaryOperator::CreateLShr(index, ConstantInt::get(index->getType(), 6), "div", block);
                const auto savePtr = GetElementPtrInst::CreateInBounds(maskType, bits, {div}, "save_ptr", block);
                new StoreInst(newset, savePtr, block);

                index->addIncoming(plus, block);
                count->addIncoming(newcount, block);
                bitset->addIncoming(zeroMask, block);
                BranchInst::Create(loop, block);
            }

            block = stop;
            const auto asis = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, size, count, "asis", block);

            output->addIncoming(list, block);
            BranchInst::Create(last, make, asis, block);

            block = make;

            if (List->IsTemporaryValue()) {
                CleanupBoxed(list, ctx, block);
            }

            const auto itemsType = PointerType::getUnqual(list->getType());
            const auto itemsPtr = *Stateless || ctx.AlwaysInline ?
                new AllocaInst(itemsType, 0U, "items_ptr", &ctx.Func->getEntryBlock().back()):
                new AllocaInst(itemsType, 0U, "items_ptr", block);
            const auto array = GenNewArray(ctx, count, itemsPtr, block);
            const auto items = new LoadInst(itemsType, itemsPtr, "items", block);

            const auto move = BasicBlock::Create(context, "move", ctx.Func);
            const auto work = BasicBlock::Create(context, "work", ctx.Func);
            const auto bulk = BasicBlock::Create(context, "bulk", ctx.Func);
            const auto copy = BasicBlock::Create(context, "copy", ctx.Func);

            const auto one = PHINode::Create(count->getType(), 2U, "one", move);
            const auto two = PHINode::Create(count->getType(), 2U, "two", move);
            const auto idx = PHINode::Create(count->getType(), 2U, "idx", move);
            one->addIncoming(zeroSize, block);
            two->addIncoming(zeroSize, block);
            idx->addIncoming(zeroSize, block);

            const auto many = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, zeroSize, count, "many", block);
            output->addIncoming(array, block);
            BranchInst::Create(move, last, many, block);

            block = move;
            const auto mptr = GetElementPtrInst::CreateInBounds(maskType, bits, {idx}, "mptr", block);
            const auto load = new LoadInst(maskType, mptr, "load", block);

            const auto six = BinaryOperator::CreateAdd(one, ConstantInt::get(one->getType(), 64), "six", block);
            const auto inc = BinaryOperator::CreateAdd(idx, plusSize, "inc", block);

            const auto mask = PHINode::Create(load->getType(), 3U, "mask", bulk);
            const auto one0 = PHINode::Create(one->getType(), 3U, "one0", bulk);
            const auto two0 = PHINode::Create(two->getType(), 3U, "two0", bulk);

            mask->addIncoming(load, block);
            one0->addIncoming(one, block);
            two0->addIncoming(two, block);

            BranchInst::Create(bulk, block);

            block = bulk;
            const auto skip = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, zeroMask, mask, "skip", block);

            one->addIncoming(six, block);
            two->addIncoming(two0, block);
            idx->addIncoming(inc, block);
            BranchInst::Create(move, work, skip, block);

            block = work;

            const auto plus = BinaryOperator::CreateAdd(one0, plusSize, "plus", block);
            const auto down = BinaryOperator::CreateLShr(mask, plusMask, "down", block);
            const auto bit = CastInst::Create(Instruction::Trunc, mask, Type::getInt1Ty(context), "bit", block);

            mask->addIncoming(down, block);
            one0->addIncoming(plus, block);
            two0->addIncoming(two0, block);

            BranchInst::Create(copy, bulk, bit, block);

            block = copy;

            const auto src = GetElementPtrInst::CreateInBounds(list->getType(), elements, {one0}, "src", block);
            const auto dst = GetElementPtrInst::CreateInBounds(list->getType(), items, {two0}, "dst", block);

            const auto item = new LoadInst(list->getType(), src, "item", block);
            ValueAddRef(Item->GetRepresentation(), item, ctx, block);
            new StoreInst(item, dst, block);

            const auto next = BinaryOperator::CreateAdd(two0, plusSize, "next", block);
            const auto more = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_ULT, next, count, "more", block);

            one0->addIncoming(plus, block);
            two0->addIncoming(next, block);
            mask->addIncoming(down, block);

            output->addIncoming(array, block);

            BranchInst::Create(bulk, last, more, block);
            block = last;

            out->addIncoming(output, block);
            BranchInst::Create(free, done, heap, block);

            {
                block = free;

                const auto fnType = FunctionType::get(Type::getVoidTy(context), {bits->getType(), shr->getType()}, false);
                const auto name = "MyFree";
                ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&MyFree));
                const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType);
                CallInst::Create(func, {bits, shr}, "", block);

                out->addIncoming(output, block);
                BranchInst::Create(done, block);
            }
        }

        {
            block = lazy;

            const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TListFilterWithLimitWrapper::MakeLazyList));
            const auto ptrType = PointerType::getUnqual(StructType::get(context));
            const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
            if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                const auto funType = FunctionType::get(list->getType() , {self->getType(), ctx.Ctx->getType(), list->getType(), limit->getType()}, false);
                const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "function", block);
                const auto value = CallInst::Create(funType, doFuncPtr, {self, ctx.Ctx, list, limit}, "value", block);
                out->addIncoming(value, block);
            } else {
                const auto resultPtr = new AllocaInst(list->getType(), 0U, "return", block);
                new StoreInst(list, resultPtr, block);
                const auto funType = FunctionType::get(Type::getVoidTy(context), {self->getType(), resultPtr->getType(), ctx.Ctx->getType(), resultPtr->getType(), limit->getType()}, false);
                const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "function", block);
                CallInst::Create(funType, doFuncPtr, {self, resultPtr, ctx.Ctx, resultPtr, limit}, "", block);
                const auto value = new LoadInst(list->getType(), resultPtr, "value", block);
                out->addIncoming(value, block);
            }
            BranchInst::Create(done, block);
        }

        block = done;
        return out;
    }
#endif

private:
    void RegisterDependencies() const final {
        DependsOn(List);
        DependsOn(Limit);
        Own(Item);
        DependsOn(Predicate);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListFilterWithLimitWrapper>::GenerateFunctions(codegen);
        FilterFunc = GenerateFilter(codegen, TBaseComputation::MakeName("Next"));
        codegen.ExportSymbol(FilterFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListFilterWithLimitWrapper>::FinalizeFunctions(codegen);
        if (FilterFunc)
            Filter = reinterpret_cast<typename TBaseWrapper::TFilterPtr>(codegen.GetPointerToFunction(FilterFunc));
    }
#endif
};

}

IComputationNode* WrapFilter(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3 || callable.GetInputsCount() == 4, "Expected 3 or 4 args");
    const auto type = callable.GetType()->GetReturnType();
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);

    if (callable.GetInputsCount() == 3) {
        const auto predicate = LocateNode(ctx.NodeLocator, callable, 2);
        const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1);

        if (type->IsFlow()) {
            return new TFilterFlowWrapper(GetValueRepresentation(type), flow, itemArg, predicate);
        } else if (type->IsStream()) {
            return new TStreamFilterWrapper(ctx.Mutables, flow, itemArg, predicate);
        } else if (type->IsList()) {
            return new TListFilterWrapper(ctx.Mutables, flow, itemArg, predicate);
        }
    } else {
        const auto limit = LocateNode(ctx.NodeLocator, callable, 1);
        const auto predicate = LocateNode(ctx.NodeLocator, callable, 3);
        const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 2);

        if (type->IsFlow()) {
            return new TFilterWithLimitFlowWrapper(ctx.Mutables, GetValueRepresentation(type), flow, limit, itemArg, predicate);
        } else if (type->IsStream()) {
            return new TStreamFilterWithLimitWrapper(ctx.Mutables, flow, limit, itemArg, predicate);
        } else if (type->IsList()) {
            return new TListFilterWithLimitWrapper(ctx.Mutables, flow, limit, itemArg, predicate);
        }
    }

    THROW yexception() << "Expected flow, list or stream.";
}

}
}
