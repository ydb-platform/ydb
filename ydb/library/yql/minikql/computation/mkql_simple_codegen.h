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

class TSimpleStatefulWideFlowCodegeneratorNodeLLVMBase {
public:
    struct TMethPtrTable {
        uintptr_t ThisPtr;
        uintptr_t InitStateMethPtr;
        uintptr_t PrepareInputMethPtr;
        uintptr_t DoProcessMethPtr;
    };

    TSimpleStatefulWideFlowCodegeneratorNodeLLVMBase(IComputationWideFlowNode* source, ui32 inWidth, ui32 outWidth, TMethPtrTable ptrTable)
        : SourceFlow(source), InWidth(inWidth), OutWidth(outWidth)
        , PtrTable(ptrTable) {}

#ifndef MKQL_DISABLE_CODEGEN
    virtual ICodegeneratorInlineWideNode::TGenerateResult GenFetchProcess(Value* statePtrVal, const TCodegenContext& ctx, const TResultCodegenerator& fetchGenerator, BasicBlock*& block) const {
        Y_UNUSED(statePtrVal);
        Y_UNUSED(ctx);
        Y_UNUSED(fetchGenerator);
        Y_UNUSED(block);
        return {nullptr, {}};
    }

    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtrVal, BasicBlock*& genToBlock) const;
#endif

protected:
    IComputationWideFlowNode* const SourceFlow;
    const ui32 InWidth, OutWidth;
    const TMethPtrTable PtrTable;
};

template<typename TDerived, typename TState, EValueRepresentation StateKind = EValueRepresentation::Embedded>
class TSimpleStatefulWideFlowCodegeneratorNode
        : public TStatefulWideFlowCodegeneratorNode<TSimpleStatefulWideFlowCodegeneratorNode<TDerived, TState, StateKind>>
        , public TSimpleStatefulWideFlowCodegeneratorNodeLLVMBase {
    using TBase = TStatefulWideFlowCodegeneratorNode<TSimpleStatefulWideFlowCodegeneratorNode>;
    using TLLVMBase = TSimpleStatefulWideFlowCodegeneratorNodeLLVMBase;

protected:
    TSimpleStatefulWideFlowCodegeneratorNode(TComputationMutables& mutables, IComputationWideFlowNode* source, ui32 inWidth, ui32 outWidth)
            : TBase(mutables, source, StateKind)
            , TLLVMBase(source, inWidth, outWidth, {
                .ThisPtr = reinterpret_cast<uintptr_t>(this),
                .InitStateMethPtr = GetMethodPtr(&TDerived::InitState),
                .PrepareInputMethPtr = GetMethodPtr(&TDerived::PrepareInput),
                .DoProcessMethPtr = GetMethodPtr(&TDerived::DoProcess)
            }) {}

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
};

}
}