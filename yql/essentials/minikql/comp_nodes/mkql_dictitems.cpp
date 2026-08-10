#include "mkql_dictitems.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

namespace NKikimr::NMiniKQL {

namespace {

class TDictItemsWrapper: public TCustomValueCodegeneratorNode<TDictItemsWrapper> {
    using TBaseComputation = TCustomValueCodegeneratorNode<TDictItemsWrapper>;

public:
    using TSelf = TDictItemsWrapper;

#ifndef MKQL_DISABLE_CODEGEN
    class TCodegenValue: public TComputationValue<TCodegenValue> {
    public:
        using TNextPtr = TCodegenIterator::TNextPtr;

        TCodegenValue(TMemoryUsageInfo* memInfo, TNextPtr next, TComputationContext* ctx, NUdf::TUnboxedValue&& dict)
            : TComputationValue<TCodegenValue>(memInfo)
            , NextFunc_(next)
            , Ctx_(ctx)
            , Dict_(std::move(dict))
        {
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            return Ctx_->HolderFactory.Create<TCodegenIterator>(NextFunc_, Ctx_, Dict_.GetDictIterator());
        }

        ui64 GetListLength() const final {
            return Dict_.GetDictLength();
        }

        bool HasListItems() const final {
            return Dict_.HasDictItems();
        }

        bool HasFastListLength() const final {
            return true;
        }

        const TNextPtr NextFunc_;
        TComputationContext* const Ctx_;
        const NUdf::TUnboxedValue Dict_;
    };
#endif

    class TValue: public TComputationValue<TValue> {
    public:
        class TIterator: public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& inner,
                      TComputationContext& compCtx, const TSelf* self)
                : TComputationValue<TIterator>(memInfo)
                , Inner_(std::move(inner))
                , CompCtx_(compCtx)
                , Self_(self)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                NUdf::TUnboxedValue key;
                NUdf::TUnboxedValue payload;
                if (!Inner_.NextPair(key, payload)) {
                    return false;
                }

                NUdf::TUnboxedValue* items = nullptr;
                value = Self_->ResPair_.NewArray(CompCtx_, 2, items);
                items[0] = std::move(key);
                items[1] = std::move(payload);
                return true;
            }

            bool Skip() override {
                return Inner_.Skip();
            }

            const NUdf::TUnboxedValue Inner_;
            TComputationContext& CompCtx_;
            const TSelf* const Self_;
        };

        TValue(
            TMemoryUsageInfo* memInfo,
            const NUdf::TUnboxedValue&& dict,
            TComputationContext& compCtx, const TSelf* self)
            : TComputationValue<TValue>(memInfo)
            , Dict_(dict)
            , CompCtx_(compCtx)
            , Self_(self)
        {
        }

    private:
        ui64 GetListLength() const final {
            return Dict_.GetDictLength();
        }

        bool HasListItems() const final {
            return Dict_.HasDictItems();
        }

        bool HasFastListLength() const final {
            return true;
        }

        NUdf::TUnboxedValue GetListIterator() const final {
            return CompCtx_.HolderFactory.Create<TIterator>(Dict_.GetDictIterator(), CompCtx_, Self_);
        }

        const NUdf::TUnboxedValue Dict_;
        TComputationContext& CompCtx_;
        const TSelf* const Self_;
    };

    TDictItemsWrapper(TComputationMutables& mutables, IComputationNode* dict)
        : TBaseComputation(mutables)
        , Dict_(dict)
        , ResPair_(mutables)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Next_) {
            return ctx.HolderFactory.Create<TCodegenValue>(Next_, &ctx, Dict_->GetValue(ctx));
        }
#endif
        return ctx.HolderFactory.Create<TValue>(Dict_->GetValue(ctx), ctx, this);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Dict_);
    }

#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        NextFunc_ = GenerateNext(codegen);
        codegen.ExportSymbol(NextFunc_);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (NextFunc_) {
            Next_ = reinterpret_cast<TNextPtr>(codegen.GetPointerToFunction(NextFunc_));
        }
    }

    Function* GenerateNext(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = TBaseComputation::MakeName("Next");
        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);
        const auto pairType = ArrayType::get(valueType, 2U);
        const auto containerType = static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt1Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(valueType)}, /*isVarArg=*/false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto container = static_cast<Value*>(containerArg);

        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto pairPtr = new AllocaInst(pairType, 0U, "pair_ptr", block);
        new StoreInst(ConstantAggregateZero::get(pairType), pairPtr, block);

        const auto keyPtr = GetElementPtrInst::CreateInBounds(pairType, pairPtr, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 0)}, "key_ptr", block);
        const auto payPtr = GetElementPtrInst::CreateInBounds(pairType, pairPtr, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 1)}, "pay_ptr", block);

        const auto status = CallBoxedValueNextPair(container, ctx, block, keyPtr, payPtr);

        BranchInst::Create(good, done, status, block);
        block = good;

        SafeUnRefUnboxedOne(valuePtr, ctx, block);

        const auto itemsType = PointerType::getUnqual(pairType);
        const auto itemsPtr = new AllocaInst(itemsType, 0U, "items_ptr", block);
        const auto output = ResPair_.GenNewArray(2U, itemsPtr, ctx, block);
        AddRefBoxed(output, ctx, block);
        const auto items = new LoadInst(itemsType, itemsPtr, "items", block);
        const auto pair = new LoadInst(pairType, pairPtr, "pair", block);
        new StoreInst(pair, items, block);
        new StoreInst(output, valuePtr, block);
        BranchInst::Create(done, block);

        block = done;
        ReturnInst::Create(context, status, block);
        return ctx.Func;
    }

    using TNextPtr = typename TCodegenIterator::TNextPtr;

    Function* NextFunc_ = nullptr;

    TNextPtr Next_ = nullptr;
#endif

    IComputationNode* const Dict_;
    const TContainerCacheOnContext ResPair_;
};

template <bool KeysOrPayloads>
class TDictHalfsWrapper: public TMutableComputationNode<TDictHalfsWrapper<KeysOrPayloads>> {
    using TBaseComputation = TMutableComputationNode<TDictHalfsWrapper<KeysOrPayloads>>;

public:
    using TSelf = TDictHalfsWrapper<KeysOrPayloads>;

    class TValue: public TComputationValue<TValue> {
    public:
        TValue(
            TMemoryUsageInfo* memInfo,
            const NUdf::TUnboxedValue&& dict,
            TComputationContext&, const TSelf*)
            : TComputationValue<TValue>(memInfo)
            , Dict_(dict)
        {
        }

    private:
        ui64 GetListLength() const final {
            return Dict_.GetDictLength();
        }

        bool HasListItems() const final {
            return Dict_.HasDictItems();
        }

        bool HasFastListLength() const final {
            return true;
        }

        NUdf::TUnboxedValue GetListIterator() const final {
            return KeysOrPayloads ? Dict_.GetKeysIterator() : Dict_.GetPayloadsIterator();
        }

        const NUdf::TUnboxedValue Dict_;
    };

    TDictHalfsWrapper(TComputationMutables& mutables, IComputationNode* dict)
        : TBaseComputation(mutables)
        , Dict_(dict)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TValue>(Dict_->GetValue(ctx), ctx, this);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Dict_);
    }

    IComputationNode* const Dict_;
};

} // namespace

IComputationNode* WrapDictItems(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected one arg");
    const auto node = LocateNode(ctx.NodeLocator, callable, 0);
    return new TDictItemsWrapper(ctx.Mutables, node);
}

IComputationNode* WrapDictKeys(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected one arg");
    const auto node = LocateNode(ctx.NodeLocator, callable, 0);
    return new TDictHalfsWrapper<true>(ctx.Mutables, node);
}

IComputationNode* WrapDictPayloads(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected one arg");
    const auto node = LocateNode(ctx.NodeLocator, callable, 0);
    return new TDictHalfsWrapper<false>(ctx.Mutables, node);
}

} // namespace NKikimr::NMiniKQL
