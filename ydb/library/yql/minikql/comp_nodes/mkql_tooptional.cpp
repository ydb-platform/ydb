#include "mkql_tooptional.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool IsOptional>
class THeadWrapper : public TMutableCodegeneratorPtrNode<THeadWrapper<IsOptional>> {
    typedef TMutableCodegeneratorPtrNode<THeadWrapper<IsOptional>> TBaseComputation;
public:
    THeadWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* list)
        : TBaseComputation(mutables, kind), List(list)
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        const auto& value = List->GetValue(ctx);
        if (const auto ptr = value.GetElements()) {
            if (value.GetListLength() > 0ULL) {
                return NUdf::TUnboxedValuePod(*ptr).MakeOptionalIf<IsOptional>();
            }
        } else if (const auto iter = value.GetListIterator()) {
            NUdf::TUnboxedValue result;
            if (iter.Next(result)) {
                return result.Release().MakeOptionalIf<IsOptional>();
            }
        }

        return NUdf::TUnboxedValue();
    }

#ifndef MKQL_DISABLE_CODEGEN
    void DoGenerateGetValue(const TCodegenContext& ctx, Value* result, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);
        const auto ptrType = PointerType::getUnqual(valueType);

        const auto list = GetNodeValue(List, ctx, block);

        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(ptrType, list, ctx.Codegen, block);

        const auto null = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, elements, ConstantPointerNull::get(ptrType), "null", block);

        const auto fast = BasicBlock::Create(context, "fast", ctx.Func);
        const auto slow = BasicBlock::Create(context, "slow", ctx.Func);
        const auto many = BasicBlock::Create(context, "many", ctx.Func);
        const auto none = BasicBlock::Create(context, "none", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto good = IsOptional ? BasicBlock::Create(context, "good", ctx.Func) : done;

        BranchInst::Create(slow, fast, null, block);
        {
            block = fast;
            const auto size = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListLength>(Type::getInt64Ty(context), list, ctx.Codegen, block);
            const auto test = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, size, ConstantInt::get(size->getType(), 0ULL), "test", block);
            BranchInst::Create(many, none, test, block);

            block = many;
            const auto item = new LoadInst(valueType, elements, "item", block);
            ValueAddRef(this->GetRepresentation(), item, ctx, block);
            new StoreInst(IsOptional ? MakeOptional(context, item, block) : item, result, block);
            BranchInst::Create(done, block);
        }

        {
            block = slow;
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(result, list, ctx.Codegen, block);

            const auto iter = new LoadInst(valueType, result, "iter", block);
            new StoreInst(ConstantInt::get(valueType, 0ULL), result, block);

            const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(Type::getInt1Ty(context), iter, ctx.Codegen, block, result);
            UnRefBoxed(iter, ctx, block);

            BranchInst::Create(good, none, status, block);

            if constexpr (IsOptional) {
                block = good;

                const auto item = new LoadInst(valueType, result, "item", block);
                new StoreInst(MakeOptional(context, item, block), result, block);
                BranchInst::Create(done, block);
            }
        }

        block = none;
        new StoreInst(ConstantInt::get(valueType, 0ULL), result, block);
        BranchInst::Create(done, block);

        block = done;
        if (List->IsTemporaryValue())
            CleanupBoxed(list, ctx, block);
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(List);
    }

    IComputationNode* const List;
};

template <bool IsOptional>
class TLastWrapper : public TMutableCodegeneratorPtrNode<TLastWrapper<IsOptional>> {
    typedef TMutableCodegeneratorPtrNode<TLastWrapper<IsOptional>> TBaseComputation;
public:
    TLastWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* list)
        : TBaseComputation(mutables, kind), List(list)
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        const auto& value = List->GetValue(ctx);
        if (const auto ptr = value.GetElements()) {
            if (const auto size = value.GetListLength()) {
                return NUdf::TUnboxedValuePod(ptr[size - 1U]).MakeOptionalIf<IsOptional>();
            }
        } else if (const auto iter = value.GetListIterator()) {
            NUdf::TUnboxedValue result;
            if (iter.Next(result)) {
                while (iter.Next(result)) continue;
                return result.Release().MakeOptionalIf<IsOptional>();
            }
        }

        return NUdf::TUnboxedValue();
    }

#ifndef MKQL_DISABLE_CODEGEN
    void DoGenerateGetValue(const TCodegenContext& ctx, Value* result, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);
        const auto ptrType = PointerType::getUnqual(valueType);

        const auto list = GetNodeValue(List, ctx, block);

        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(ptrType, list, ctx.Codegen, block);

        const auto null = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, elements, ConstantPointerNull::get(ptrType), "null", block);

        const auto fast = BasicBlock::Create(context, "fast", ctx.Func);
        const auto slow = BasicBlock::Create(context, "slow", ctx.Func);
        const auto nope = BasicBlock::Create(context, "nope", ctx.Func);
        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto many = BasicBlock::Create(context, "many", ctx.Func);
        const auto none = BasicBlock::Create(context, "none", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(slow, fast, null, block);
        {
            block = fast;
            const auto size = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListLength>(Type::getInt64Ty(context), list, ctx.Codegen, block);
            const auto test = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, size, ConstantInt::get(size->getType(), 0ULL), "test", block);
            BranchInst::Create(many, none, test, block);

            block = many;
            const auto index = BinaryOperator::CreateSub(size, ConstantInt::get(size->getType(), 1), "index", block);
            const auto last = GetElementPtrInst::CreateInBounds(valueType, elements, {index}, "last", block);
            const auto item = new LoadInst(valueType, last, "item", block);
            ValueAddRef(this->GetRepresentation(), item, ctx, block);
            new StoreInst(IsOptional ? MakeOptional(context, item, block) : item, result, block);
            BranchInst::Create(done, block);
        }

        {
            block = slow;
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(result, list, ctx.Codegen, block);

            const auto iter = new LoadInst(valueType, result, "iter", block);
            new StoreInst(ConstantInt::get(valueType, 0ULL), result, block);

            const auto first = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(Type::getInt1Ty(context), iter, ctx.Codegen, block, result);
            BranchInst::Create(loop, nope, first, block);

            block = nope;

            UnRefBoxed(iter, ctx, block);
            BranchInst::Create(none, block);

            block = loop;

            const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(Type::getInt1Ty(context), iter, ctx.Codegen, block, result);
            BranchInst::Create(loop, good, status, block);

            block = good;

            UnRefBoxed(iter, ctx, block);

            if constexpr (IsOptional) {
                const auto item = new LoadInst(valueType, result, "item", block);
                new StoreInst(MakeOptional(context, item, block), result, block);
            }

            BranchInst::Create(done, block);
        }

        block = none;
        new StoreInst(ConstantInt::get(valueType, 0ULL), result, block);
        BranchInst::Create(done, block);

        block = done;
        if (List->IsTemporaryValue())
            CleanupBoxed(list, ctx, block);
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(List);
    }

    IComputationNode* const List;
};

}

IComputationNode* WrapHead(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args");
    if (AS_TYPE(TOptionalType, callable.GetType()->GetReturnType())->IsOptional()) {
        return new THeadWrapper<true>(ctx.Mutables, GetValueRepresentation(callable.GetType()->GetReturnType()), LocateNode(ctx.NodeLocator, callable, 0));
    } else {
        return new THeadWrapper<false>(ctx.Mutables, GetValueRepresentation(callable.GetType()->GetReturnType()), LocateNode(ctx.NodeLocator, callable, 0));
    }
}

IComputationNode* WrapLast(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args");
    if (AS_TYPE(TOptionalType, callable.GetType()->GetReturnType())->IsOptional()) {
        return new TLastWrapper<true>(ctx.Mutables, GetValueRepresentation(callable.GetType()->GetReturnType()), LocateNode(ctx.NodeLocator, callable, 0));
    } else {
        return new TLastWrapper<false>(ctx.Mutables, GetValueRepresentation(callable.GetType()->GetReturnType()), LocateNode(ctx.NodeLocator, callable, 0));
    }
}

}
}
