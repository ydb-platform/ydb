#include "mkql_heap.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

#include <ydb/library/yql/utils/sort.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {
using TComparator = std::function<bool(const NUdf::TUnboxedValuePod l, const NUdf::TUnboxedValuePod r)>;
using TAlgorithm = void(*)(NUdf::TUnboxedValuePod*, NUdf::TUnboxedValuePod*, TComparator);
using TArgsPlace = std::array<NUdf::TUnboxedValuePod, 2U>;
using TComparePtr = bool (*)(TComputationContext& ctx, const NUdf::TUnboxedValuePod l, const NUdf::TUnboxedValuePod r);

class THeapWrapper : public TMutableCodegeneratorNode<THeapWrapper>
#ifndef MKQL_DISABLE_CODEGEN
    , public ICodegeneratorRootNode
#endif
{
    typedef TMutableCodegeneratorNode<THeapWrapper> TBaseComputation;
public:
    THeapWrapper(TAlgorithm algorithm, TComputationMutables& mutables, IComputationNode* list, IComputationExternalNode* left, IComputationExternalNode* right, IComputationNode* compare)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Algorithm(algorithm)
        , List(list)
        , Left(left)
        , Right(right)
        , Compare(compare)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto list = List->GetValue(ctx);

        const auto size = list.GetListLength();

        if (size < 2U)
            return list.Release();

        NUdf::TUnboxedValue *items = nullptr;
        const auto next = ctx.HolderFactory.CloneArray(list.Release(), items);

        NUdf::TUnboxedValuePod *const begin = items, *const end = items + size;

        Do(ctx, begin, end);

        return next;
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);

        const auto fact = ctx.GetFactory();

        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::CloneArray));// TODO: Generate code instead of call CloneArray.

        const auto list = GetNodeValue(List, ctx, block);

        const auto size = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListLength>(Type::getInt64Ty(context), list, ctx.Codegen, block);

        const auto test = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, size, ConstantInt::get(size->getType(), 1ULL), "test", block);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(valueType, 2U, "result", done);
        result->addIncoming(list, block);

        BranchInst::Create(work, done, test, block);

        block = work;

        const auto itemsType = PointerType::getUnqual(valueType);
        const auto itemsPtr = *Stateless || ctx.AlwaysInline ?
            new AllocaInst(itemsType, 0U, "items_ptr", &ctx.Func->getEntryBlock().back()):
            new AllocaInst(itemsType, 0U, "items_ptr", block);

        const auto idxType = Type::getInt32Ty(context);

        Value* array = nullptr;
        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(valueType, {fact->getType(), list->getType(), itemsPtr->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            array = CallInst::Create(funType, funcPtr, {fact, list, itemsPtr}, "array", block);
        } else {
            const auto arrayPtr = new AllocaInst(valueType, 0U, "array_ptr", block);
            new StoreInst(list, arrayPtr, block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {fact->getType(), arrayPtr->getType(), arrayPtr->getType(), itemsPtr->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            CallInst::Create(funType, funcPtr, {fact, arrayPtr, arrayPtr, itemsPtr}, "", block);
            array = new LoadInst(valueType, arrayPtr, "array", block);
        }

        result->addIncoming(array, block);

        const auto algo = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THeapWrapper::Do));
        const auto self = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(this));

        const auto items = new LoadInst(itemsType, itemsPtr, "items", block);
        const auto zero = ConstantInt::get(idxType, 0);
        const auto begin = GetElementPtrInst::CreateInBounds(valueType, items, {zero}, "begin", block);
        const auto end = GetElementPtrInst::CreateInBounds(valueType, items, {size}, "end", block);

        const auto selfPtr = CastInst::Create(Instruction::IntToPtr, self, PointerType::getUnqual(StructType::get(context)), "comp", block);
        const auto doType = FunctionType::get(Type::getVoidTy(context), {selfPtr->getType(), ctx.Ctx->getType(), begin->getType(), end->getType()}, false);
        const auto doPtr = CastInst::Create(Instruction::IntToPtr, algo, PointerType::getUnqual(doType), "do", block);

        CallInst::Create(doType, doPtr, {selfPtr, ctx.Ctx, begin, end}, "", block);

        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
private:
    void Do(TComputationContext& ctx, NUdf::TUnboxedValuePod* begin, NUdf::TUnboxedValuePod* end) const {
        if (ctx.ExecuteLLVM && Comparator) {
            return Algorithm(begin, end, std::bind(Comparator, std::ref(ctx), std::placeholders::_1, std::placeholders::_2));
        }

        TArgsPlace args;
        Left->SetGetter([&](TComputationContext&) { return args.front(); });
        Right->SetGetter([&](TComputationContext&) { return args.back(); });
        Algorithm(begin, end, std::bind(&THeapWrapper::Comp, this, std::ref(args), std::ref(ctx), std::placeholders::_1, std::placeholders::_2));
    }

    bool Comp(TArgsPlace& args, TComputationContext& ctx, const NUdf::TUnboxedValuePod l, const NUdf::TUnboxedValuePod r) const {
        args = {{l, r}};
        Left->InvalidateValue(ctx);
        Right->InvalidateValue(ctx);
        return Compare->GetValue(ctx).Get<bool>();
    }

    void RegisterDependencies() const final {
        this->DependsOn(List);
        this->Own(Left);
        this->Own(Right);
        this->DependsOn(Compare);
    }

    const TAlgorithm Algorithm;

    IComputationNode* const List;
    IComputationExternalNode* const Left;
    IComputationExternalNode* const Right;
    IComputationNode* const Compare;

    TComparePtr Comparator = nullptr;

#ifndef MKQL_DISABLE_CODEGEN
    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::compare_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (CompareFunc) {
            Comparator = reinterpret_cast<TComparePtr>(codegen.GetPointerToFunction(CompareFunc));
        }
    }

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        CompareFunc = GenerateCompareFunction(codegen, MakeName(), Left, Right, Compare);
        codegen.ExportSymbol(CompareFunc);
    }

    Function* CompareFunc = nullptr;
#endif
};

IComputationNode* WrapHeap(TAlgorithm algorithm, TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");

    const auto list = LocateNode(ctx.NodeLocator, callable, 0);
    const auto compare = LocateNode(ctx.NodeLocator, callable, 3);
    const auto left = LocateExternalNode(ctx.NodeLocator, callable, 1);
    const auto right = LocateExternalNode(ctx.NodeLocator, callable, 2);

    return new THeapWrapper(algorithm, ctx.Mutables, list, left, right, compare);
}

using TNthAlgorithm = void(*)(NUdf::TUnboxedValuePod*, NUdf::TUnboxedValuePod*,  NUdf::TUnboxedValuePod*, TComparator);

class TNthWrapper : public TMutableCodegeneratorNode<TNthWrapper>
#ifndef MKQL_DISABLE_CODEGEN
    , public ICodegeneratorRootNode
#endif
{
    typedef TMutableCodegeneratorNode<TNthWrapper> TBaseComputation;
public:
    TNthWrapper(TNthAlgorithm algorithm, TComputationMutables& mutables, IComputationNode* list, IComputationNode* middle, IComputationExternalNode* left, IComputationExternalNode* right, IComputationNode* compare)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Algorithm(algorithm)
        , List(list)
        , Middle(middle)
        , Left(left)
        , Right(right)
        , Compare(compare)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto list = List->GetValue(ctx);

        auto middle = Middle->GetValue(ctx).Get<ui64>();
        const auto size = list.GetListLength();

        middle = std::min(middle, size);

        if (middle == 0U || size < 2U)
            return list.Release();

        NUdf::TUnboxedValue *items = nullptr;
        const auto next = ctx.HolderFactory.CloneArray(list.Release(), items);

        NUdf::TUnboxedValuePod *const begin = items, *const mid = items + middle, *const end = items + size;

        Do(ctx, begin, mid, end);

        return next;
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);

        const auto fact = ctx.GetFactory();

        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::CloneArray));// TODO: Generate code instead of call CloneArray.

        const auto list = GetNodeValue(List, ctx, block);
        const auto midv = GetNodeValue(Middle, ctx, block);
        const auto middle = GetterFor<ui64>(midv, context, block);

        const auto size = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListLength>(Type::getInt64Ty(context), list, ctx.Codegen, block);

        const auto greater = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, middle, size, "greater", block);

        const auto min = SelectInst::Create(greater, size, middle, "min", block);

        const auto one = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, min, ConstantInt::get(size->getType(), 0ULL), "one", block);
        const auto two = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, size, ConstantInt::get(size->getType(), 1ULL), "two", block);
        const auto test = BinaryOperator::CreateAnd(one, two, "and", block);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(valueType, 2U, "result", done);
        result->addIncoming(list, block);

        BranchInst::Create(work, done, test, block);

        block = work;

        const auto itemsType = PointerType::getUnqual(valueType);
        const auto itemsPtr = *Stateless || ctx.AlwaysInline ?
            new AllocaInst(itemsType, 0U, "items_ptr", &ctx.Func->getEntryBlock().back()):
            new AllocaInst(itemsType, 0U, "items_ptr", block);

        const auto idxType = Type::getInt32Ty(context);

        Value* array = nullptr;
        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(valueType, {fact->getType(), list->getType(), itemsPtr->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            array = CallInst::Create(funType, funcPtr, {fact, list, itemsPtr}, "array", block);
        } else {
            const auto arrayPtr = new AllocaInst(valueType, 0U, "array_ptr", block);
            new StoreInst(list, arrayPtr, block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {fact->getType(), arrayPtr->getType(), arrayPtr->getType(), itemsPtr->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            CallInst::Create(funType, funcPtr, {fact, arrayPtr, arrayPtr, itemsPtr}, "", block);
            array = new LoadInst(valueType, arrayPtr, "array", block);
        }

        result->addIncoming(array, block);

        const auto algo = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TNthWrapper::Do));
        const auto self = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(this));

        const auto items = new LoadInst(itemsType, itemsPtr, "items", block);
        const auto zero = ConstantInt::get(idxType, 0);
        const auto begin = GetElementPtrInst::CreateInBounds(valueType, items, {zero}, "begin", block);
        const auto mid = GetElementPtrInst::CreateInBounds(valueType, items, {min}, "middle", block);
        const auto end = GetElementPtrInst::CreateInBounds(valueType, items, {size}, "end", block);

        const auto selfPtr = CastInst::Create(Instruction::IntToPtr, self, PointerType::getUnqual(StructType::get(context)), "comp", block);
        const auto doType = FunctionType::get(Type::getVoidTy(context), {selfPtr->getType(), ctx.Ctx->getType(), begin->getType(), mid->getType(), end->getType()}, false);
        const auto doPtr = CastInst::Create(Instruction::IntToPtr, algo, PointerType::getUnqual(doType), "do", block);

        CallInst::Create(doType, doPtr, {selfPtr, ctx.Ctx, begin, mid, end}, "", block);

        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
private:
    void Do(TComputationContext& ctx, NUdf::TUnboxedValuePod* begin, NUdf::TUnboxedValuePod* nth,  NUdf::TUnboxedValuePod* end) const {
        if (ctx.ExecuteLLVM && Comparator) {
            return Algorithm(begin, nth, end, std::bind(Comparator, std::ref(ctx), std::placeholders::_1, std::placeholders::_2));
        }

        TArgsPlace args;
        Left->SetGetter([&](TComputationContext&) { return args.front(); });
        Right->SetGetter([&](TComputationContext&) { return args.back(); });
        Algorithm(begin, nth, end, std::bind(&TNthWrapper::Comp, this, std::ref(args), std::ref(ctx), std::placeholders::_1, std::placeholders::_2));
    }

    bool Comp(TArgsPlace& args, TComputationContext& ctx, const NUdf::TUnboxedValuePod l, const NUdf::TUnboxedValuePod r) const {
        args = {{l, r}};
        Left->InvalidateValue(ctx);
        Right->InvalidateValue(ctx);
        return Compare->GetValue(ctx).Get<bool>();
    }

    void RegisterDependencies() const final {
        this->DependsOn(List);
        this->DependsOn(Middle);
        this->Own(Left);
        this->Own(Right);
        this->DependsOn(Compare);
    }

    const TNthAlgorithm Algorithm;

    IComputationNode* const List;
    IComputationNode* const Middle;
    IComputationExternalNode* const Left;
    IComputationExternalNode* const Right;
    IComputationNode* const Compare;

    TComparePtr Comparator = nullptr;

#ifndef MKQL_DISABLE_CODEGEN
    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::compare_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (CompareFunc) {
            Comparator = reinterpret_cast<TComparePtr>(codegen.GetPointerToFunction(CompareFunc));
        }
    }

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        CompareFunc = GenerateCompareFunction(codegen, MakeName(), Left, Right, Compare);
        codegen.ExportSymbol(CompareFunc);
    }

    Function* CompareFunc = nullptr;
#endif
};

IComputationNode* WrapNth(TNthAlgorithm algorithm, TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");

    const auto list = LocateNode(ctx.NodeLocator, callable, 0);
    const auto middle = LocateNode(ctx.NodeLocator, callable, 1);
    const auto compare = LocateNode(ctx.NodeLocator, callable, 4);
    const auto left = LocateExternalNode(ctx.NodeLocator, callable, 2);
    const auto right = LocateExternalNode(ctx.NodeLocator, callable, 3);

    return new TNthWrapper(algorithm, ctx.Mutables, list, middle, left, right, compare);
}

}

IComputationNode* WrapMakeHeap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapHeap(&std::make_heap<NUdf::TUnboxedValuePod*, TComparator>, callable, ctx);
}

IComputationNode* WrapPushHeap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapHeap(&std::push_heap<NUdf::TUnboxedValuePod*, TComparator>, callable, ctx);
}

IComputationNode* WrapPopHeap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapHeap(&std::pop_heap<NUdf::TUnboxedValuePod*, TComparator>, callable, ctx);
}

IComputationNode* WrapSortHeap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapHeap(&std::sort_heap<NUdf::TUnboxedValuePod*, TComparator>, callable, ctx);
}

IComputationNode* WrapStableSort(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapHeap(&std::stable_sort<NUdf::TUnboxedValuePod*, TComparator>, callable, ctx);
}

IComputationNode* WrapNthElement(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapNth(&NYql::FastNthElement<NUdf::TUnboxedValuePod*, TComparator>, callable, ctx);
}

IComputationNode* WrapPartialSort(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapNth(&NYql::FastPartialSort<NUdf::TUnboxedValuePod*, TComparator>, callable, ctx);
}
}
}
