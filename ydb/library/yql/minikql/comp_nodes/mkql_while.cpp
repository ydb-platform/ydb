#include "mkql_while.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool SkipOrTake, bool Inclusive>
class TWhileFlowWrapper : public TStatefulFlowCodegeneratorNode<TWhileFlowWrapper<SkipOrTake, Inclusive>> {
using TBaseComputation = TStatefulFlowCodegeneratorNode<TWhileFlowWrapper<SkipOrTake, Inclusive>>;
public:
     TWhileFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow, IComputationExternalNode* item, IComputationNode* predicate)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Embedded), Flow(flow), Item(item), Predicate(predicate)
    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.HasValue() && state.Get<bool>()) {
            return SkipOrTake ? Flow->GetValue(ctx) : NUdf::TUnboxedValue(NUdf::TUnboxedValuePod::MakeFinish());
        }

        if constexpr (SkipOrTake) {
            do if (auto item = Flow->GetValue(ctx); item.IsSpecial())
                return item;
            else
                Item->SetValue(ctx, std::move(item));
            while (Predicate->GetValue(ctx).template Get<bool>());

            state = NUdf::TUnboxedValuePod(true);
            return Inclusive ? Flow->GetValue(ctx) : Item->GetValue(ctx);
        } else {
            if (auto item = Flow->GetValue(ctx); item.IsSpecial())
                return item;
            else
                Item->SetValue(ctx, std::move(item));

            if (Predicate->GetValue(ctx).template Get<bool>()) {
                return Item->GetValue(ctx);
            }

            state = NUdf::TUnboxedValuePod(true);
            return Inclusive ? Item->GetValue(ctx).Release() : NUdf::TUnboxedValuePod::MakeFinish();
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
        const auto skip = BasicBlock::Create(context, "skip", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(valueType, SkipOrTake ? 3U : 4U, "result", done);

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto finished = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, state, GetTrue(context), "finished", block);

        BranchInst::Create(skip, work, finished, block);

        block = work;
        const auto item = GetNodeValue(Flow, ctx, block);
        result->addIncoming(item, block);
        BranchInst::Create(done, good, IsSpecial(item, block), block);

        block = good;
        codegenItem->CreateSetValue(ctx, block, item);
        const auto pred = GetNodeValue(Predicate, ctx, block);
        const auto bit = CastInst::Create(Instruction::Trunc, pred, Type::getInt1Ty(context), "bit", block);

        if constexpr (SkipOrTake) {
            BranchInst::Create(work, stop, bit, block);
        } else {
            result->addIncoming(item, block);
            BranchInst::Create(done, stop, bit, block);
        }

        block = stop;
        new StoreInst(GetTrue(context), statePtr, block);
        const auto last = Inclusive ?
                (SkipOrTake ? GetNodeValue(Flow, ctx, block) : item):
                (SkipOrTake ? item : GetFinish(context));
        result->addIncoming(last, block);
        BranchInst::Create(done, block);

        block = skip;
        const auto res = SkipOrTake ? GetNodeValue(Flow, ctx, block) : GetFinish(context);
        result->addIncoming(res, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            this->Own(flow, Item);
            this->DependsOn(flow, Predicate);
        }
    }

    IComputationNode* const Flow;
    IComputationExternalNode* const Item;
    IComputationNode* const Predicate;
};

template <bool SkipOrTake, bool Inclusive, bool IsStream>
class TBaseWhileWrapper {
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
            bool Next(NUdf::TUnboxedValue& value) override {
                if (FilterWorkFinished) {
                    return SkipOrTake ? Iter.Next(value) : false;
                }

                if constexpr (SkipOrTake) {
                    while (Iter.Next(Item->RefValue(CompCtx))) {
                        if (!Predicate->GetValue(CompCtx).template Get<bool>()) {
                            FilterWorkFinished = true;
                            if constexpr (Inclusive) {
                                return Iter.Next(value);
                            } else {
                                value = Item->GetValue(CompCtx);
                                return true;
                            }
                        }
                    }
                } else {
                    if (Iter.Next(Item->RefValue(CompCtx))) {
                        if (Predicate->GetValue(CompCtx).template Get<bool>()) {
                            value = Item->GetValue(CompCtx);
                            return true;
                        } else {
                            FilterWorkFinished = true;
                            if constexpr (Inclusive) {
                                value = Item->GetValue(CompCtx);
                                return true;
                            }
                        }
                    }
                }

                return false;
            }

            TComputationContext& CompCtx;
            const NUdf::TUnboxedValue Iter;
            IComputationExternalNode* const Item;
            IComputationNode* const Predicate;
            bool FilterWorkFinished = false;
        };

        TListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, const NUdf::TUnboxedValue& list, IComputationExternalNode* item, IComputationNode* predicate)
            : TCustomListValue(memInfo)
            , CompCtx(compCtx)
            , List(list)
            , Item(item)
            , Predicate(predicate)
        {}

    private:
        NUdf::TUnboxedValue GetListIterator() const override {
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
        ui32 GetTraverseCount() const override {
            return 1;
        }

        NUdf::TUnboxedValue GetTraverseItem(ui32 index) const override {
            Y_UNUSED(index);
            return Stream;
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (FilterWorkFinished) {
                return SkipOrTake ? Stream.Fetch(result) : NUdf::EFetchStatus::Finish;
            }

            if constexpr (SkipOrTake) {
                for (;;) {
                    if (const auto status = Stream.Fetch(Item->RefValue(CompCtx)); status != NUdf::EFetchStatus::Ok) {
                        return status;
                    }

                    if (!Predicate->GetValue(CompCtx).template Get<bool>()) {
                        FilterWorkFinished = true;
                        if constexpr (Inclusive) {
                            return Stream.Fetch(result);
                        } else {
                            result = Item->GetValue(CompCtx);
                            return NUdf::EFetchStatus::Ok;
                        }
                    }
                }
            } else {
                switch (const auto status = Stream.Fetch(Item->RefValue(CompCtx))) {
                    case NUdf::EFetchStatus::Yield:
                        return status;

                    case NUdf::EFetchStatus::Ok:
                        if (Predicate->GetValue(CompCtx).template Get<bool>()) {
                            result = Item->GetValue(CompCtx);
                            return NUdf::EFetchStatus::Ok;
                        }
                    case NUdf::EFetchStatus::Finish:
                        break;
                }

                FilterWorkFinished = true;
                if constexpr (Inclusive) {
                    result = Item->GetValue(CompCtx);
                    return NUdf::EFetchStatus::Ok;
                } else {
                    return NUdf::EFetchStatus::Finish;
                }
            }
        }

        TComputationContext& CompCtx;
        const NUdf::TUnboxedValue Stream;
        IComputationExternalNode* const Item;
        IComputationNode* const Predicate;
        bool FilterWorkFinished = false;
    };

#ifndef MKQL_DISABLE_CODEGEN
    class TStreamCodegenWhileValue : public TStreamCodegenStatefulValueT<> {
    public:
        TStreamCodegenWhileValue(TMemoryUsageInfo* memInfo, TFetchPtr fetch, TComputationContext* ctx, NUdf::TUnboxedValue&& stream)
            : TStreamCodegenStatefulValueT(memInfo, fetch, ctx, std::move(stream))
        {}

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
            return State ?
                (SkipOrTake ? Stream.Fetch(result) : NUdf::EFetchStatus::Finish):
                TStreamCodegenStatefulValueT::Fetch(result);
        }
    };

    class TCodegenIteratorWhile : public TCodegenStatefulIterator<> {
    public:
        TCodegenIteratorWhile(TMemoryUsageInfo* memInfo, TNextPtr next, TComputationContext* ctx, NUdf::TUnboxedValue&& iterator, const NUdf::TUnboxedValue& init)
            : TCodegenStatefulIterator(memInfo, next, ctx, std::move(iterator), init)
        {}

    private:
        bool Next(NUdf::TUnboxedValue& value) final {
            return State ?
                (SkipOrTake ? Iterator.Next(value) : false):
                TCodegenStatefulIterator::Next(value);
        }
    };

    using TCustomListCodegenWhileValue = TCustomListCodegenStatefulValueT<TCodegenIteratorWhile>;
#endif

    TBaseWhileWrapper(IComputationNode* list, IComputationExternalNode* item, IComputationNode* predicate)
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
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(valueType), PointerType::getUnqual(valueType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto statePtr = &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto container = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
            new LoadInst(valueType, containerArg, "load_container", false, block) : static_cast<Value*>(containerArg);

        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto loop = SkipOrTake ? BasicBlock::Create(context, "loop", ctx.Func) : nullptr;
        if (loop) {
            BranchInst::Create(loop, block);
            block = loop;
        }

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

        BranchInst::Create(SkipOrTake ? loop : pass, stop, boolPred, block);

        block = stop;
        new StoreInst(GetTrue(context), statePtr, block);

        if constexpr (SkipOrTake) {
            if constexpr (Inclusive) {
                const auto last = IsStream ?
                    CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, container, codegen, block, valuePtr):
                    CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(statusType, container, codegen, block, valuePtr);
                ReturnInst::Create(context, last, block);
            } else {
                BranchInst::Create(pass, block);
            }
        } else {
            if constexpr (Inclusive) {
                BranchInst::Create(pass, block);
            } else {
                ReturnInst::Create(context, IsStream ? ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)) : ConstantInt::getFalse(context), block);
            }
        }

        block = pass;

        SafeUnRefUnboxed(valuePtr, ctx, block);
        new StoreInst(item, valuePtr, block);
        ValueAddRef(Item->GetRepresentation(), valuePtr, ctx, block);
        BranchInst::Create(done, block);

        block = done;
        ReturnInst::Create(context, status, block);
        return ctx.Func;
    }

    using TFilterPtr = std::conditional_t<IsStream, typename TStreamCodegenWhileValue::TFetchPtr, typename TCustomListCodegenWhileValue::TNextPtr>;

    Function* FilterFunc = nullptr;

    TFilterPtr Filter = nullptr;
#endif

    IComputationNode* const List;
    IComputationExternalNode* const Item;
    IComputationNode* const Predicate;
};

template <bool SkipOrTake, bool Inclusive>
class TStreamWhileWrapper : public TCustomValueCodegeneratorNode<TStreamWhileWrapper<SkipOrTake, Inclusive>>,
    private TBaseWhileWrapper<SkipOrTake, Inclusive, true> {
    typedef TBaseWhileWrapper<SkipOrTake, Inclusive, true> TBaseWrapper;
    typedef TCustomValueCodegeneratorNode<TStreamWhileWrapper<SkipOrTake, Inclusive>> TBaseComputation;
public:
    TStreamWhileWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationExternalNode* item, IComputationNode* predicate)
        : TBaseComputation(mutables), TBaseWrapper(list, item, predicate)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && this->Filter)
            return ctx.HolderFactory.Create<typename TBaseWrapper::TStreamCodegenWhileValue>(this->Filter, &ctx, this->List->GetValue(ctx));
#endif
        return ctx.HolderFactory.Create<typename TBaseWrapper::TStreamValue>(ctx, this->List->GetValue(ctx), this->Item, this->Predicate);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(this->List);
        this->Own(this->Item);
        this->DependsOn(this->Predicate);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        this->FilterFunc = this->GenerateFilter(codegen, TBaseComputation::MakeName("Fetch"));
        codegen.ExportSymbol(this->FilterFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (this->FilterFunc)
            this->Filter = reinterpret_cast<typename TBaseWrapper::TFilterPtr>(codegen.GetPointerToFunction(this->FilterFunc));
    }
#endif
};

template <bool SkipOrTake, bool Inclusive>
class TListWhileWrapper : public TBothWaysCodegeneratorNode<TListWhileWrapper<SkipOrTake, Inclusive>>,
    private TBaseWhileWrapper<SkipOrTake, Inclusive, false> {
    typedef TBaseWhileWrapper<SkipOrTake, Inclusive, false> TBaseWrapper;
    typedef TBothWaysCodegeneratorNode<TListWhileWrapper<SkipOrTake, Inclusive>> TBaseComputation;
public:
    TListWhileWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationExternalNode* item, IComputationNode* predicate)
        : TBaseComputation(mutables), TBaseWrapper(list, item, predicate)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto list = this->List->GetValue(ctx);

        if (const auto elements = list.GetElements()) {
            const auto size = list.GetListLength();

            const ui64 init = Inclusive ? 1ULL : 0ULL;
            auto todo = size;

            for (auto e = elements; todo > init; --todo) {
                this->Item->SetValue(ctx, NUdf::TUnboxedValue(*e++));
                if (!this->Predicate->GetValue(ctx).template Get<bool>())
                    break;
            }

            if (init && todo) {
                todo -= init;
            }

            const auto pass = size - todo;
            const auto copy = SkipOrTake ? todo : pass;

            if (copy == size) {
                return list.Release();
            }

            NUdf::TUnboxedValue* items = nullptr;
            const auto result = ctx.HolderFactory.CreateDirectArrayHolder(copy, items);
            const auto from = SkipOrTake ? elements + pass : elements;
            std::copy_n(from, copy, items);
            return result;
        }

        return ctx.HolderFactory.Create<typename TBaseWrapper::TListValue>(ctx, std::move(list), this->Item, this->Predicate);
    }

#ifndef MKQL_DISABLE_CODEGEN
    NUdf::TUnboxedValuePod MakeLazyList(TComputationContext& ctx, const NUdf::TUnboxedValuePod value) const {
        return ctx.HolderFactory.Create<typename TBaseWrapper::TCustomListCodegenWhileValue>(this->Filter, &ctx, value);
    }

    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(this->Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto list = GetNodeValue(this->List, ctx, block);

        const auto lazy = BasicBlock::Create(context, "lazy", ctx.Func);
        const auto hard = BasicBlock::Create(context, "hard", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto out = PHINode::Create(list->getType(), 4U, "out", done);

        const auto elementsType = PointerType::getUnqual(list->getType());
        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(elementsType, list, ctx.Codegen, block);
        const auto fill = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, elements, ConstantPointerNull::get(elementsType), "fill", block);

        BranchInst::Create(hard, lazy, fill, block);

        {
            block = hard;

            const auto size = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListLength>(Type::getInt64Ty(context), list, ctx.Codegen, block);

            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto test = BasicBlock::Create(context, "test", ctx.Func);
            const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
            const auto make = BasicBlock::Create(context, "make", ctx.Func);

            const auto index = PHINode::Create(size->getType(), 2U, "index", loop);
            const auto zero = ConstantInt::get(size->getType(), 0);
            index->addIncoming(zero, block);

            const auto none = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, zero, size, "none", block);

            out->addIncoming(list, block);
            BranchInst::Create(done, loop, none, block);

            block = loop;

            const auto ptr = GetElementPtrInst::CreateInBounds(list->getType(), elements, {index}, "ptr", block);
            const auto plus = BinaryOperator::CreateAdd(index, ConstantInt::get(size->getType(), 1), "plus", block);
            const auto more = CmpInst::Create(Instruction::ICmp, Inclusive ? ICmpInst::ICMP_ULT : ICmpInst::ICMP_ULE, plus, size, "more", block);
            BranchInst::Create(test, stop, more, block);

            block = test;

            const auto item = new LoadInst(list->getType(), ptr, "item", block);
            codegenItem->CreateSetValue(ctx, block, item);
            const auto predicate = GetNodeValue(this->Predicate, ctx, block);
            const auto boolPred = CastInst::Create(Instruction::Trunc, predicate, Type::getInt1Ty(context), "bool", block);
            index->addIncoming(plus, block);
            BranchInst::Create(loop, stop, boolPred, block);

            block = stop;

            const auto pass = Inclusive ? static_cast<Value*>(plus) : static_cast<Value*>(index);
            const auto copy = SkipOrTake ? BinaryOperator::CreateSub(size, pass, "copy", block) : pass;
            const auto asis = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, size, copy, "asis", block);

            out->addIncoming(list, block);
            BranchInst::Create(done, make, asis, block);

            block = make;

            const auto itemsType = PointerType::getUnqual(list->getType());
            const auto itemsPtr = *this->Stateless || ctx.AlwaysInline ?
                new AllocaInst(itemsType, 0U, "items_ptr", &ctx.Func->getEntryBlock().back()):
                new AllocaInst(itemsType, 0U, "items_ptr", block);
            const auto array = GenNewArray(ctx, copy, itemsPtr, block);
            const auto items = new LoadInst(itemsType, itemsPtr, "items", block);
            const auto from = SkipOrTake ? GetElementPtrInst::CreateInBounds(list->getType(), elements, {pass}, "from", block) : elements;

            const auto move = BasicBlock::Create(context, "move", ctx.Func);
            const auto step = BasicBlock::Create(context, "step", ctx.Func);
            const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

            const auto idx = PHINode::Create(copy->getType(), 2U, "idx", move);
            idx->addIncoming(ConstantInt::get(copy->getType(), 0), block);

            BranchInst::Create(move, block);

            block = move;
            const auto finish = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGE, idx, copy, "finish", block);
            BranchInst::Create(exit, step, finish, block);

            block = step;

            const auto src = GetElementPtrInst::CreateInBounds(list->getType(), from, {idx}, "src", block);
            const auto itm = new LoadInst(list->getType(), src, "item", block);
            ValueAddRef(this->Item->GetRepresentation(), itm, ctx, block);
            const auto dst = GetElementPtrInst::CreateInBounds(list->getType(), items, {idx}, "dst", block);
            new StoreInst(itm, dst, block);
            const auto inc = BinaryOperator::CreateAdd(idx, ConstantInt::get(idx->getType(), 1), "inc", block);
            idx->addIncoming(inc, block);
            BranchInst::Create(move, block);

            block = exit;
            if (this->List->IsTemporaryValue()) {
                CleanupBoxed(list, ctx, block);
            }
            out->addIncoming(array, block);
            BranchInst::Create(done, block);
        }

        {
            block = lazy;

            const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TListWhileWrapper::MakeLazyList));
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
        this->DependsOn(this->List);
        this->Own(this->Item);
        this->DependsOn(this->Predicate);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListWhileWrapper<SkipOrTake, Inclusive>>::GenerateFunctions(codegen);
        this->FilterFunc = this->GenerateFilter(codegen, TBaseComputation::MakeName("Next"));
        codegen.ExportSymbol(this->FilterFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListWhileWrapper<SkipOrTake, Inclusive>>::FinalizeFunctions(codegen);
        if (this->FilterFunc)
            this->Filter = reinterpret_cast<typename TBaseWrapper::TFilterPtr>(codegen.GetPointerToFunction(this->FilterFunc));
    }
#endif
};

template <bool SkipOrTake, bool Inclusive>
IComputationNode* WrapFilterWhile(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");
    const auto type = callable.GetType()->GetReturnType();

    const auto predicateType = AS_TYPE(TDataType, callable.GetInput(2));
    MKQL_ENSURE(predicateType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool");

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    const auto predicate = LocateNode(ctx.NodeLocator, callable, 2);
    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1);
    if (type->IsFlow()) {
        return new TWhileFlowWrapper<SkipOrTake, Inclusive>(ctx.Mutables, GetValueRepresentation(type), flow, itemArg, predicate);
    } else if (type->IsStream()) {
        return new TStreamWhileWrapper<SkipOrTake, Inclusive>(ctx.Mutables, flow, itemArg, predicate);
    } else if (type->IsList()) {
        return new TListWhileWrapper<SkipOrTake, Inclusive>(ctx.Mutables, flow, itemArg, predicate);
    }

    THROW yexception() << "Expected flow, list or stream.";
}

}

IComputationNode* WrapTakeWhile(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapFilterWhile<false, false>(callable, ctx);
}

IComputationNode* WrapSkipWhile(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapFilterWhile<true, false>(callable, ctx);
}

IComputationNode* WrapTakeWhileInclusive(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapFilterWhile<false, true>(callable, ctx);
}

IComputationNode* WrapSkipWhileInclusive(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapFilterWhile<true, true>(callable, ctx);
}

}
}
