#include "mkql_dictitems.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TDictItemsWrapper : public TCustomValueCodegeneratorNode<TDictItemsWrapper> {
    typedef TCustomValueCodegeneratorNode<TDictItemsWrapper> TBaseComputation;
public:
    using TSelf = TDictItemsWrapper;

#ifndef MKQL_DISABLE_CODEGEN
    class TCodegenValue : public TComputationValue<TCodegenValue> {
    public:
        using TNextPtr = TCodegenIterator::TNextPtr;

        TCodegenValue(TMemoryUsageInfo* memInfo, TNextPtr next, TComputationContext* ctx, NUdf::TUnboxedValue&& dict)
            : TComputationValue<TCodegenValue>(memInfo)
            , NextFunc(next)
            , Ctx(ctx)
            , Dict(std::move(dict))
        {}

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            return Ctx->HolderFactory.Create<TCodegenIterator>(NextFunc, Ctx, Dict.GetDictIterator());
        }

        ui64 GetListLength() const final {
            return Dict.GetDictLength();
        }

        bool HasListItems() const final {
            return Dict.HasDictItems();
        }

        bool HasFastListLength() const final {
            return true;
        }

        const TNextPtr NextFunc;
        TComputationContext* const Ctx;
        const NUdf::TUnboxedValue Dict;
    };
#endif

    class TValue : public TComputationValue<TValue> {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& inner,
                TComputationContext& compCtx, const TSelf* self)
                : TComputationValue<TIterator>(memInfo)
                , Inner(std::move(inner))
                , CompCtx(compCtx)
                , Self(self)
            {
            }

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                NUdf::TUnboxedValue key, payload;
                if (!Inner.NextPair(key, payload))
                    return false;

                NUdf::TUnboxedValue* items = nullptr;
                value = Self->ResPair.NewArray(CompCtx, 2, items);
                items[0] = std::move(key);
                items[1] = std::move(payload);
                return true;
            }

            bool Skip() override {
                return Inner.Skip();
            }

            const NUdf::TUnboxedValue Inner;
            TComputationContext& CompCtx;
            const TSelf* const Self;
        };

        TValue(
            TMemoryUsageInfo* memInfo,
            const NUdf::TUnboxedValue&& dict,
            TComputationContext& compCtx, const TSelf* self)
            : TComputationValue<TValue>(memInfo)
            , Dict(std::move(dict))
            , CompCtx(compCtx)
            , Self(self)
        {
        }

    private:
        ui64 GetListLength() const final {
            return Dict.GetDictLength();
        }

        bool HasListItems() const final {
            return Dict.HasDictItems();
        }

        bool HasFastListLength() const final {
            return true;
        }

        NUdf::TUnboxedValue GetListIterator() const final {
            return CompCtx.HolderFactory.Create<TIterator>(Dict.GetDictIterator(), CompCtx, Self);
        }

        const NUdf::TUnboxedValue Dict;
        TComputationContext& CompCtx;
        const TSelf* const Self;
    };

    TDictItemsWrapper(TComputationMutables& mutables, IComputationNode* dict)
        : TBaseComputation(mutables)
        , Dict(dict)
        , ResPair(mutables)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Next)
            return ctx.HolderFactory.Create<TCodegenValue>(Next, &ctx, Dict->GetValue(ctx));
#endif
        return ctx.HolderFactory.Create<TValue>(Dict->GetValue(ctx), ctx, this);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Dict);
    }

#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        NextFunc = GenerateNext(codegen);
        codegen.ExportSymbol(NextFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (NextFunc)
            Next = reinterpret_cast<TNextPtr>(codegen.GetPointerToFunction(NextFunc));
    }

    Function* GenerateNext(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = TBaseComputation::MakeName("Next");
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);
        const auto pairType = ArrayType::get(valueType, 2U);
        const auto containerType = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ? static_cast<Type*>(PointerType::getUnqual(valueType)) : static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt1Ty(context);
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

        const auto pairPtr = new AllocaInst(pairType, 0U, "pair_ptr", block);
        new StoreInst(ConstantAggregateZero::get(pairType), pairPtr, block);

        const auto keyPtr = GetElementPtrInst::CreateInBounds(pairType, pairPtr, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 0)}, "key_ptr", block);
        const auto payPtr = GetElementPtrInst::CreateInBounds(pairType, pairPtr, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 1)}, "pay_ptr", block);

        const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::NextPair>(statusType, container, codegen, block, keyPtr, payPtr);

        BranchInst::Create(good, done, status, block);
        block = good;

        SafeUnRefUnboxed(valuePtr, ctx, block);

        const auto itemsType = PointerType::getUnqual(pairType);
        const auto itemsPtr = new AllocaInst(itemsType, 0U, "items_ptr", block);
        const auto output = ResPair.GenNewArray(2U, itemsPtr, ctx, block);
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

    Function* NextFunc = nullptr;

    TNextPtr Next = nullptr;
#endif

    IComputationNode* const Dict;
    const TContainerCacheOnContext ResPair;
};

template <bool KeysOrPayloads>
class TDictHalfsWrapper : public TMutableComputationNode<TDictHalfsWrapper<KeysOrPayloads>> {
    typedef TMutableComputationNode<TDictHalfsWrapper<KeysOrPayloads>> TBaseComputation;
public:
    using TSelf = TDictHalfsWrapper<KeysOrPayloads>;

    class TValue : public TComputationValue<TValue> {
    public:
        TValue(
            TMemoryUsageInfo* memInfo,
            const NUdf::TUnboxedValue&& dict,
            TComputationContext&, const TSelf*)
            : TComputationValue<TValue>(memInfo)
            , Dict(std::move(dict))
        {}

    private:
        ui64 GetListLength() const final {
            return Dict.GetDictLength();
        }

        bool HasListItems() const final {
            return Dict.HasDictItems();
        }

        bool HasFastListLength() const final {
            return true;
        }

        NUdf::TUnboxedValue GetListIterator() const final {
            return KeysOrPayloads ? Dict.GetKeysIterator() : Dict.GetPayloadsIterator();
        }

        const NUdf::TUnboxedValue Dict;
    };

    TDictHalfsWrapper(TComputationMutables& mutables, IComputationNode* dict)
        : TBaseComputation(mutables), Dict(dict)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TValue>(Dict->GetValue(ctx), ctx, this);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Dict);
    }

    IComputationNode* const Dict;
};

}

IComputationNode* WrapDictItems(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1 ||  callable.GetInputsCount() == 2, "Expected one or two args");
    const auto node = LocateNode(ctx.NodeLocator, callable, 0);

    if (1U == callable.GetInputsCount()) {
        return new TDictItemsWrapper(ctx.Mutables, node);
    }

    const auto mode = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui32>();
    switch (static_cast<EDictItems>(mode)) {
    case EDictItems::Both:
        return new TDictItemsWrapper(ctx.Mutables, node);
    case EDictItems::Keys:
        return new TDictHalfsWrapper<true>(ctx.Mutables, node);
    case EDictItems::Payloads:
        return new TDictHalfsWrapper<false>(ctx.Mutables, node);
    default:
        Y_ABORT("Unknown mode: %" PRIu32, mode);
    }
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

}
}
