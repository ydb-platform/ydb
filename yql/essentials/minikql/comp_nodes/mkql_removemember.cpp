#include "mkql_removemember.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

class TRemoveMemberWrapper: public TMutableCodegeneratorFallbackNode<TRemoveMemberWrapper> {
    using TBaseComputation = TMutableCodegeneratorFallbackNode<TRemoveMemberWrapper>;

public:
    TRemoveMemberWrapper(TComputationMutables& mutables, IComputationNode* structObj, ui32 index, std::vector<EValueRepresentation>&& representations)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , StructObj_(structObj)
        , Index_(index)
        , Representations_(std::move(representations))
        , Cache_(mutables)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& baseStruct = StructObj_->GetValue(ctx);

        NUdf::TUnboxedValue* itemsPtr = nullptr;
        const auto result = Cache_.NewArray(ctx, Representations_.size() - 1U, itemsPtr);
        if (Representations_.size() > 1) {
            Y_ABORT_UNLESS(itemsPtr);
            if (const auto ptr = baseStruct.GetElements()) {
                for (ui32 i = 0; i < Index_; ++i) {
                    *itemsPtr++ = ptr[i];
                }

                for (ui32 i = Index_ + 1; i < Representations_.size(); ++i) {
                    *itemsPtr++ = ptr[i];
                }
            } else {
                for (ui32 i = 0; i < Index_; ++i) {
                    *itemsPtr++ = baseStruct.GetElement(i);
                }

                for (ui32 i = Index_ + 1; i < Representations_.size(); ++i) {
                    *itemsPtr++ = baseStruct.GetElement(i);
                }
            }
        }

        return result;
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        if (Representations_.size() > CodegenArraysFallbackLimit) {
            return TBaseComputation::DoGenerateGetValue(ctx, block);
        }

        auto& context = ctx.Codegen.GetContext();

        const auto newSize = Representations_.size() - 1U;

        const auto valType = Type::getInt128Ty(context);
        const auto ptrType = PointerType::getUnqual(valType);
        const auto idxType = Type::getInt32Ty(context);
        const auto type = ArrayType::get(valType, newSize);
        const auto itmsType = PointerType::getUnqual(type);
        const auto itms = *Stateless_ || ctx.AlwaysInline ? new AllocaInst(itmsType, 0U, "itms", &ctx.Func->getEntryBlock().back()) : new AllocaInst(itmsType, 0U, "itms", block);
        const auto result = Cache_.GenNewArray(newSize, itms, ctx, block);
        const auto itemsPtr = new LoadInst(itmsType, itms, "items", block);

        const auto array = GetNodeValue(StructObj_, ctx, block);

        const auto zero = ConstantInt::get(idxType, 0);

        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(ptrType, array, ctx.Codegen, block);

        const auto null = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, elements, ConstantPointerNull::get(ptrType), "null", block);

        const auto fast = BasicBlock::Create(context, "fast", ctx.Func);
        const auto slow = BasicBlock::Create(context, "slow", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(slow, fast, null, block);
        {
            block = fast;
            for (ui32 i = 0; i < Index_; ++i) {
                const auto index = ConstantInt::get(idxType, i);
                const auto srcPtr = GetElementPtrInst::CreateInBounds(valType, elements, {index}, "src", block);
                const auto dstPtr = GetElementPtrInst::CreateInBounds(type, itemsPtr, {zero, index}, "dst", block);
                const auto item = new LoadInst(valType, srcPtr, "item", block);
                new StoreInst(item, dstPtr, block);
                ValueAddRef(Representations_[i], dstPtr, ctx, block);
            }

            for (ui32 i = Index_ + 1U; i < Representations_.size(); ++i) {
                const auto oldIndex = ConstantInt::get(idxType, i);
                const auto newIndex = ConstantInt::get(idxType, i - 1U);
                const auto srcPtr = GetElementPtrInst::CreateInBounds(valType, elements, {oldIndex}, "src", block);
                const auto dstPtr = GetElementPtrInst::CreateInBounds(type, itemsPtr, {zero, newIndex}, "dst", block);
                const auto item = new LoadInst(valType, srcPtr, "item", block);
                new StoreInst(item, dstPtr, block);
                ValueAddRef(Representations_[i], dstPtr, ctx, block);
            }
            BranchInst::Create(done, block);
        }
        {
            block = slow;
            for (ui32 i = 0; i < Index_; ++i) {
                const auto index = ConstantInt::get(idxType, i);
                const auto itemPtr = GetElementPtrInst::CreateInBounds(type, itemsPtr, {zero, index}, "item", block);
                CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElement>(itemPtr, array, ctx.Codegen, block, index);
            }

            for (ui32 i = Index_ + 1U; i < Representations_.size(); ++i) {
                const auto oldIndex = ConstantInt::get(idxType, i);
                const auto newIndex = ConstantInt::get(idxType, i - 1U);
                const auto itemPtr = GetElementPtrInst::CreateInBounds(type, itemsPtr, {zero, newIndex}, "item", block);
                CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElement>(itemPtr, array, ctx.Codegen, block, oldIndex);
            }
            BranchInst::Create(done, block);
        }
        block = done;
        if (StructObj_->IsTemporaryValue()) {
            CleanupBoxed(array, ctx, block);
        }
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(StructObj_);
    }

    IComputationNode* const StructObj_;
    const ui32 Index_;
    const std::vector<EValueRepresentation> Representations_;

    const TContainerCacheOnContext Cache_;
};

} // namespace

IComputationNode* WrapRemoveMember(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto structType = AS_TYPE(TStructType, callable.GetInput(0));
    const auto indexData = AS_VALUE(TDataLiteral, callable.GetInput(1));

    const ui32 index = indexData->AsValue().Get<ui32>();
    MKQL_ENSURE(index < structType->GetMembersCount(), "Bad member index");

    std::vector<EValueRepresentation> representations;
    representations.reserve(structType->GetMembersCount());
    for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
        representations.emplace_back(GetValueRepresentation(structType->GetMemberType(i)));
    }

    const auto structObj = LocateNode(ctx.NodeLocator, callable, 0);
    return new TRemoveMemberWrapper(ctx.Mutables, structObj, index, std::move(representations));
}

} // namespace NKikimr::NMiniKQL
