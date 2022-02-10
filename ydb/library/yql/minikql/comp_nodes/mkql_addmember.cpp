#include "mkql_addmember.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TAddMemberWrapper : public TMutableCodegeneratorFallbackNode<TAddMemberWrapper> {
    typedef TMutableCodegeneratorFallbackNode<TAddMemberWrapper> TBaseComputation;
public:
    TAddMemberWrapper(TComputationMutables& mutables, IComputationNode* structObj, IComputationNode* member, ui32 index,
        std::vector<EValueRepresentation>&& representations)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , StructObj(structObj)
        , Member(member)
        , Index(index)
        , Representations(std::move(representations))
        , Cache(mutables)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& baseStruct = StructObj->GetValue(ctx);

        NUdf::TUnboxedValue* itemsPtr = nullptr;
        const auto result = Cache.NewArray(ctx, Representations.size() + 1U, itemsPtr);
        if (const auto ptr = baseStruct.GetElements()) {
            for (ui32 i = 0; i < Index; ++i) {
                *itemsPtr++ = ptr[i];
            }

            *itemsPtr++ = Member->GetValue(ctx);

            for (ui32 i = Index; i < Representations.size(); ++i) {
                *itemsPtr++ = ptr[i];
            }
        } else {
            for (ui32 i = 0; i < Index; ++i) {
                *itemsPtr++ = baseStruct.GetElement(i);
            }

            *itemsPtr++ = Member->GetValue(ctx);

            for (ui32 i = Index; i < Representations.size(); ++i) {
                *itemsPtr++ = baseStruct.GetElement(i);
            }
        }

        return result;
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        if (Representations.size() > CodegenArraysFallbackLimit)
            return TBaseComputation::DoGenerateGetValue(ctx, block);

        auto& context = ctx.Codegen->GetContext();

        const auto newSize = Representations.size() + 1U;

        const auto valType = Type::getInt128Ty(context);
        const auto ptrType = PointerType::getUnqual(valType);
        const auto idxType = Type::getInt32Ty(context);
        const auto type = ArrayType::get(valType, newSize);
        const auto itms = *Stateless || ctx.AlwaysInline ?
            new AllocaInst(PointerType::getUnqual(type), 0U, "itms", &ctx.Func->getEntryBlock().back()):
            new AllocaInst(PointerType::getUnqual(type), 0U, "itms", block);
        const auto result = Cache.GenNewArray(newSize, itms, ctx, block);
        const auto itemsPtr = new LoadInst(itms, "items", block);

        const auto array = GetNodeValue(StructObj, ctx, block);
        const auto zero = ConstantInt::get(idxType, 0);

        const auto itemPtr = GetElementPtrInst::CreateInBounds(itemsPtr, {zero, ConstantInt::get(idxType, Index)}, "item", block);
        GetNodeValue(itemPtr, Member, ctx, block);

        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(ptrType, array, ctx.Codegen, block);

        const auto null = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, elements, ConstantPointerNull::get(ptrType), "null", block);

        const auto fast = BasicBlock::Create(context, "fast", ctx.Func);
        const auto slow = BasicBlock::Create(context, "slow", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(slow, fast, null, block);
        {
            block = fast;
            for (ui32 i = 0; i < Index; ++i) {
                const auto index = ConstantInt::get(idxType, i);
                const auto srcPtr = GetElementPtrInst::CreateInBounds(elements, {index}, "src", block);
                const auto dstPtr = GetElementPtrInst::CreateInBounds(itemsPtr, {zero, index}, "dst", block);
                const auto item = new LoadInst(srcPtr, "item", block);
                new StoreInst(item, dstPtr, block);
                ValueAddRef(Representations[i], dstPtr, ctx, block);
            }

            for (ui32 i = Index + 1U; i < newSize; ++i) {
                const auto oldIndex = ConstantInt::get(idxType, --i);
                const auto newIndex = ConstantInt::get(idxType, ++i);
                const auto srcPtr = GetElementPtrInst::CreateInBounds(elements, {oldIndex}, "src", block);
                const auto dstPtr = GetElementPtrInst::CreateInBounds(itemsPtr, {zero, newIndex}, "dst", block);
                const auto item = new LoadInst(srcPtr, "item", block);
                new StoreInst(item, dstPtr, block);
                ValueAddRef(Representations[i - 1U], dstPtr, ctx, block);
            }
            BranchInst::Create(done, block);
        }
        {
            block = slow;
            for (ui32 i = 0; i < Index; ++i) {
                const auto index = ConstantInt::get(idxType, i);
                const auto itemPtr = GetElementPtrInst::CreateInBounds(itemsPtr, {zero, index}, "item", block);
                CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElement>(itemPtr, array, ctx.Codegen, block, index);
            }

            for (ui32 i = Index + 1U; i < newSize; ++i) {
                const auto oldIndex = ConstantInt::get(idxType, --i);
                const auto newIndex = ConstantInt::get(idxType, ++i);
                const auto itemPtr = GetElementPtrInst::CreateInBounds(itemsPtr, {zero, newIndex}, "item", block);
                CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElement>(itemPtr, array, ctx.Codegen, block, oldIndex);
            }
            BranchInst::Create(done, block);
        }
        block = done;
        if (StructObj->IsTemporaryValue())
            CleanupBoxed(array, ctx, block);
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(StructObj);
        DependsOn(Member);
    }

    IComputationNode* const StructObj;
    IComputationNode* const Member;
    const ui32 Index;
    const std::vector<EValueRepresentation> Representations;

    const TContainerCacheOnContext Cache;
};

}

IComputationNode* WrapAddMember(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");

    const auto structType = AS_TYPE(TStructType, callable.GetInput(0));
    const auto indexData = AS_VALUE(TDataLiteral, callable.GetInput(2));

    const ui32 index = indexData->AsValue().Get<ui32>();
    MKQL_ENSURE(index <= structType->GetMembersCount(), "Bad member index");

    std::vector<EValueRepresentation> representations;
    representations.reserve(structType->GetMembersCount());
    for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
        representations.emplace_back(GetValueRepresentation(structType->GetMemberType(i)));
    }

    const auto structObj = LocateNode(ctx.NodeLocator, callable, 0);
    const auto member = LocateNode(ctx.NodeLocator, callable, 1);
    return new TAddMemberWrapper(ctx.Mutables, structObj, member, index, std::move(representations));
}

}
}
