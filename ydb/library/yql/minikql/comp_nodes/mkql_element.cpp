#include "mkql_element.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool IsOptional>
class TElementsWrapper : public TMutableCodegeneratorNode<TElementsWrapper<IsOptional>> {
    typedef TMutableCodegeneratorNode<TElementsWrapper<IsOptional>> TBaseComputation;
public:
    TElementsWrapper(TComputationMutables& mutables, IComputationNode* array)
        : TBaseComputation(mutables, EValueRepresentation::Embedded), Array(array)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        const auto& array = Array->GetValue(compCtx);
        if constexpr (IsOptional) {
            return array ? NUdf::TUnboxedValuePod(reinterpret_cast<ui64>(array.GetElements())) : NUdf::TUnboxedValuePod();
        } else {
            return NUdf::TUnboxedValuePod(reinterpret_cast<ui64>(array.GetElements()));
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto array = GetNodeValue(Array, ctx, block);
        const auto elementsType = PointerType::getUnqual(array->getType());

        if constexpr (IsOptional) {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);

            const auto result = PHINode::Create(array->getType(), 2U, "result", done);
            result->addIncoming(ConstantInt::get(array->getType(), 0ULL), block);
            BranchInst::Create(done, good, IsEmpty(array, block), block);

            block = good;
            const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(elementsType, array, ctx.Codegen, block);
            const auto cast = CastInst::Create(Instruction::PtrToInt, elements, Type::getInt64Ty(context), "cast", block);
            const auto wide = SetterFor<ui64>(cast, context, block);
            result->addIncoming(wide, block);
            BranchInst::Create(done, block);

            block = done;
            return result;
        } else {
            const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(elementsType, array, ctx.Codegen, block);
            const auto cast = CastInst::Create(Instruction::PtrToInt, elements, Type::getInt64Ty(context), "cast", block);
            return SetterFor<ui64>(cast, context, block);
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Array);
    }

    IComputationNode* const Array;
};

template <bool IsOptional>
class TElementWrapper : public TMutableCodegeneratorPtrNode<TElementWrapper<IsOptional>> {
    typedef TMutableCodegeneratorPtrNode<TElementWrapper<IsOptional>> TBaseComputation;
public:
    TElementWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* cache, IComputationNode* array, ui32 index)
        : TBaseComputation(mutables, kind), Cache(cache), Array(array), Index(index)
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        if (Cache->GetDependencesCount() > 1U) {
            const auto cache = Cache->GetValue(ctx);
            if (IsOptional && !cache) {
                return NUdf::TUnboxedValue();
            }
            if (const auto elements = cache.Get<ui64>()) {
                return reinterpret_cast<const NUdf::TUnboxedValuePod*>(elements)[Index];
            }
        }

        const auto& array = Array->GetValue(ctx);
        if constexpr (IsOptional) {
            return array ? array.GetElement(Index) : NUdf::TUnboxedValue();
        } else {
            return array.GetElement(Index);
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    void DoGenerateGetElement(const TCodegenContext& ctx, Value* pointer, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto array = GetNodeValue(Array, ctx, block);
        const auto index = ConstantInt::get(Type::getInt32Ty(context), Index);
        if constexpr (IsOptional) {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto zero = BasicBlock::Create(context, "zero", ctx.Func);
            const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

            BranchInst::Create(zero, good, IsEmpty(array, block), block);

            block = zero;
            new StoreInst(ConstantInt::get(array->getType(), 0ULL), pointer, block);
            BranchInst::Create(exit, block);

            block = good;
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElement>(pointer, array, ctx.Codegen, block, index);
            if (Array->IsTemporaryValue())
                CleanupBoxed(array, ctx, block);
            BranchInst::Create(exit, block);

            block = exit;
        } else {
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElement>(pointer, array, ctx.Codegen, block, index);
            if (Array->IsTemporaryValue())
                CleanupBoxed(array, ctx, block);
        }
    }

    void DoGenerateGetValue(const TCodegenContext& ctx, Value* pointer, BasicBlock*& block) const {
        if (Cache->GetDependencesCount() <= 1U) {
            return DoGenerateGetElement(ctx, pointer, block);
        }

        auto& context = ctx.Codegen.GetContext();
        const auto cache = GetNodeValue(Cache, ctx, block);

        const auto fast = BasicBlock::Create(context, "fast", ctx.Func);
        const auto slow = BasicBlock::Create(context, "slow", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        if constexpr (IsOptional) {
            const auto zero = ConstantInt::get(cache->getType(), 0ULL);
            const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, cache, zero, "check", block);

            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto none = BasicBlock::Create(context, "none", ctx.Func);

            BranchInst::Create(none, good, check, block);

            block = none;
            new StoreInst(zero, pointer, block);
            BranchInst::Create(done, block);

            block = good;
        }

        const auto trunc = CastInst::Create(Instruction::Trunc, cache, Type::getInt64Ty(context), "trunc", block);
        const auto type = PointerType::getUnqual(cache->getType());
        const auto elements = CastInst::Create(Instruction::IntToPtr, trunc, type, "elements", block);
        const auto fill = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, elements, ConstantPointerNull::get(type), "fill", block);
        BranchInst::Create(fast, slow, fill, block);

        block = fast;
        const auto index = ConstantInt::get(Type::getInt32Ty(context), this->Index);
        const auto ptr = GetElementPtrInst::CreateInBounds(cache->getType(), elements, {index}, "ptr", block);
        const auto item = new LoadInst(cache->getType(), ptr, "item", block);
        ValueAddRef(this->GetRepresentation(), item, ctx, block);
        new StoreInst(item, pointer, block);
        BranchInst::Create(done, block);

        block = slow;
        DoGenerateGetElement(ctx, pointer, block);
        BranchInst::Create(done, block);

        block = done;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Array);
        this->DependsOn(Cache);
    }

    IComputationNode *const Cache;
    IComputationNode *const Array;
    const ui32 Index;
};

IComputationNode* WrapElements(IComputationNode* array, const TComputationNodeFactoryContext& ctx, bool isOptional) {
    if (isOptional) {
        return new TElementsWrapper<true>(ctx.Mutables, array);
    } else {
        return new TElementsWrapper<false>(ctx.Mutables, array);
    }
}

}

IComputationNode* WrapNth(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2U, "Expected two args.");
    const auto input = callable.GetInput(0U);
    bool isOptional;
    const auto tupleType = AS_TYPE(TTupleType, UnpackOptional(input.GetStaticType(), isOptional));
    const auto indexData = AS_VALUE(TDataLiteral, callable.GetInput(1U));
    const auto index = indexData->AsValue().Get<ui32>();
    MKQL_ENSURE(index < tupleType->GetElementsCount(), "Bad tuple index");

    const auto tuple = LocateNode(ctx.NodeLocator, callable, 0);
    const auto ins = ctx.ElementsCache.emplace(tuple, nullptr);
    if (ins.second) {
        ctx.NodePushBack(ins.first->second = WrapElements(tuple, ctx, isOptional));
    }

    if (isOptional) {
        return new TElementWrapper<true>(ctx.Mutables, GetValueRepresentation(tupleType->GetElementType(index)), ins.first->second, tuple, index);
    } else {
        return new TElementWrapper<false>(ctx.Mutables, GetValueRepresentation(tupleType->GetElementType(index)), ins.first->second, tuple, index);
    }
}

IComputationNode* WrapMember(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2U, "Expected two args.");
    const auto input = callable.GetInput(0U);
    bool isOptional;
    const auto structType = AS_TYPE(TStructType, UnpackOptional(input.GetStaticType(), isOptional));
    const auto indexData = AS_VALUE(TDataLiteral, callable.GetInput(1U));
    const auto index = indexData->AsValue().Get<ui32>();
    MKQL_ENSURE(index < structType->GetMembersCount(), "Bad member index");

    const auto structObj = LocateNode(ctx.NodeLocator, callable, 0U);
    const auto ins = ctx.ElementsCache.emplace(structObj, nullptr);
    if (ins.second) {
        ctx.NodePushBack(ins.first->second = WrapElements(structObj, ctx, isOptional));
    }
    if (isOptional) {
        return new TElementWrapper<true>(ctx.Mutables, GetValueRepresentation(structType->GetMemberType(index)), ins.first->second, structObj,  index);
    } else {
        return new TElementWrapper<false>(ctx.Mutables, GetValueRepresentation(structType->GetMemberType(index)), ins.first->second, structObj, index);
    }
}

}
}
