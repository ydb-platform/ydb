#pragma once

namespace NKikimr {
namespace NMiniKQL {

class TMaybeFetchResult final {
    ui64 Raw;

    explicit TMaybeFetchResult(ui64 raw) : Raw(raw) {}

public:
    /* implicit */ TMaybeFetchResult(EFetchResult res) : TMaybeFetchResult(static_cast<ui64>(static_cast<ui32>(res))) {}


    [[nodiscard]] bool Empty() const {
        return Raw >> ui64(32);
    }

    [[nodiscard]] EFetchResult Get() const {
        Y_ABORT_IF(Empty());
        return static_cast<EFetchResult>(static_cast<ui32>(Raw));
    }

    [[nodiscard]] ui64 RawU64() const {
        return Raw;
    }

    static TMaybeFetchResult None() {
        return TMaybeFetchResult(ui64(1) << ui64(32));
    }

#ifndef MKQL_DISABLE_CODEGEN
    static Type* LLVMType(LLVMContext& context) {
        return Type::getInt64Ty(context);
    }

    static Value* LLVMFromFetchResult(Value *fetchRes, const Twine& name, BasicBlock* block) {
        return new ZExtInst(fetchRes, LLVMType(fetchRes->getContext()), name, block);
    }

    Value* LLVMConst(LLVMContext& context) const {
        return ConstantInt::get(LLVMType(context), RawU64());
    }
#endif
};

#ifndef MKQL_DISABLE_CODEGEN
using TResultCodegenerator = std::function<ICodegeneratorInlineWideNode::TGenerateResult(const TCodegenContext&, BasicBlock*&)>;
#endif

class TSimpleWideFlowCodegeneratorNodeLLVMBase {
public:
    struct TMethPtrTable {
        uintptr_t ThisPtr;
        uintptr_t InitStateMethPtr;
        uintptr_t PrepareInputMethPtr;
        uintptr_t DoProcessMethPtr;
    };

    TSimpleWideFlowCodegeneratorNodeLLVMBase(IComputationWideFlowNode* source, ui32 inWidth, ui32 outWidth, TMethPtrTable ptrTable)
        : SourceFlow(source), InWidth(inWidth), OutWidth(outWidth)
        , PtrTable(ptrTable) {}

protected:
#ifndef MKQL_DISABLE_CODEGEN
    virtual ICodegeneratorInlineWideNode::TGenerateResult DispatchGenFetchProcess(Value* statePtrVal, const TCodegenContext& ctx, const TResultCodegenerator& fetchGenerator, BasicBlock*& block) const = 0;

    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValuesBase(const NKikimr::NMiniKQL::TCodegenContext &ctx, llvm::Value *statePtrVal, llvm::BasicBlock *&genToBlock) const;
#endif

    IComputationWideFlowNode* const SourceFlow;
    const ui32 InWidth, OutWidth;
    const TMethPtrTable PtrTable;
};

template<typename TDerived, EValueRepresentation StateKind = EValueRepresentation::Embedded>
class TSimpleStatefulWideFlowCodegeneratorNode
        : public TStatefulWideFlowCodegeneratorNode<TSimpleStatefulWideFlowCodegeneratorNode<TDerived, StateKind>>
        , public TSimpleWideFlowCodegeneratorNodeLLVMBase {
    using TBase = TStatefulWideFlowCodegeneratorNode<TSimpleStatefulWideFlowCodegeneratorNode>;
    using TLLVMBase = TSimpleWideFlowCodegeneratorNodeLLVMBase;

protected:
    TSimpleStatefulWideFlowCodegeneratorNode(TComputationMutables& mutables, IComputationWideFlowNode* source, ui32 inWidth, ui32 outWidth)
        : TBase(mutables, source, StateKind)
        , TLLVMBase(source, inWidth, outWidth, {
            .ThisPtr = reinterpret_cast<uintptr_t>(this),
            .InitStateMethPtr = GetMethodPtr(&TDerived::InitState),
            .PrepareInputMethPtr = GetMethodPtr(&TDerived::PrepareInput),
            .DoProcessMethPtr = GetMethodPtr(&TDerived::DoProcess)
        }) {}
    
#ifndef MKQL_DISABLE_CODEGEN
    virtual ICodegeneratorInlineWideNode::TGenerateResult GenFetchProcess(Value* statePtrVal, const TCodegenContext& ctx, const TResultCodegenerator& fetchGenerator, BasicBlock*& block) const {
        Y_UNUSED(statePtrVal);
        Y_UNUSED(ctx);
        Y_UNUSED(fetchGenerator);
        Y_UNUSED(block);
        return {nullptr, {}};
    }

    virtual ICodegeneratorInlineWideNode::TGenerateResult DispatchGenFetchProcess(Value* statePtrVal, const TCodegenContext& ctx, const TResultCodegenerator& fetchGenerator, BasicBlock*& block) const override final {
        return GenFetchProcess(statePtrVal, ctx, fetchGenerator, block);
    }
#endif

#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtrVal, BasicBlock*& genToBlock) const final {
        return TSimpleStatefulWideFlowCodegeneratorNodeLLVMBase::DoGenGetValues(ctx, statePtrVal, genToBlock);
    }
#endif

public:
    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (state.IsInvalid()) {
            state = NUdf::TUnboxedValuePod();
            static_cast<const TDerived*>(this)->InitState(state, ctx);
        }
        NUdf::TUnboxedValue *const stub = nullptr;
        if (!output && !OutWidth) {
            output = &stub;
        }
        auto result = TMaybeFetchResult::None();
        while (result.Empty()) {
            NUdf::TUnboxedValue*const* input = static_cast<const TDerived*>(this)->PrepareInput(state, ctx, output);
            TMaybeFetchResult fetchResult = input ? SourceFlow->FetchValues(ctx, input) : TMaybeFetchResult::None();
            result = static_cast<const TDerived*>(this)->DoProcess(state, ctx, fetchResult, output);
        }
        return result.Get();
    }

#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const NKikimr::NMiniKQL::TCodegenContext &ctx, llvm::Value *statePtrVal, llvm::BasicBlock *&genToBlock) const {
        return DoGenGetValuesBase(ctx, statePtrVal, genToBlock);
    }
#endif
};

template<typename TDerived>
class TSimpleStatelessWideFlowCodegeneratorNode
        : public TStatelessWideFlowCodegeneratorNode<TSimpleStatelessWideFlowCodegeneratorNode<TDerived>> 
        , public TSimpleWideFlowCodegeneratorNodeLLVMBase {
    using TBase = TStatelessWideFlowCodegeneratorNode<TSimpleStatelessWideFlowCodegeneratorNode<TDerived>>;
    using TLLVMBase = TSimpleWideFlowCodegeneratorNodeLLVMBase;

protected:
    TSimpleStatelessWideFlowCodegeneratorNode(IComputationWideFlowNode* source, ui32 inWidth, ui32 outWidth)
        : TBase (source)
        , TLLVMBase(source, inWidth, outWidth, {
            .ThisPtr = reinterpret_cast<uintptr_t>(this),
            .InitStateMethPtr = 0,
            .PrepareInputMethPtr = GetMethodPtr(&TDerived::PrepareInput),
            .DoProcessMethPtr = GetMethodPtr(&TDerived::DoProcess)
        }) {}

#ifndef MKQL_DISABLE_CODEGEN
    virtual ICodegeneratorInlineWideNode::TGenerateResult GenFetchProcess(const TCodegenContext& ctx, const TResultCodegenerator& fetchGenerator, BasicBlock*& block) const {
        Y_UNUSED(ctx);
        Y_UNUSED(fetchGenerator);
        Y_UNUSED(block);
        return {nullptr, {}};
    }

    virtual ICodegeneratorInlineWideNode::TGenerateResult DispatchGenFetchProcess(Value* statePtrVal, const TCodegenContext& ctx, const TResultCodegenerator& fetchGenerator, BasicBlock*& block) const override final {
        Y_UNUSED(statePtrVal);
        return GenFetchProcess(ctx, fetchGenerator, block);
    }
#endif

public:
    EFetchResult DoCalculate(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
         NUdf::TUnboxedValue *const stub = nullptr;
        if (!output && !OutWidth) {
            output = &stub;
        }
        auto result = TMaybeFetchResult::None();
        while (result.Empty()) {
            NUdf::TUnboxedValue*const* input = static_cast<const TDerived*>(this)->PrepareInput(ctx, output);
            TMaybeFetchResult fetchResult = input ? SourceFlow->FetchValues(ctx, input) : TMaybeFetchResult::None();
            result = static_cast<const TDerived*>(this)->DoProcess(ctx, fetchResult, output);
        }
        return result.Get();
    }

#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const NKikimr::NMiniKQL::TCodegenContext &ctx, llvm::BasicBlock *&genToBlock) const {
        return DoGenGetValuesBase(ctx, nullptr, genToBlock);
    }
#endif
};

}
}