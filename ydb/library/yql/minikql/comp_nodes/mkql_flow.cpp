#include "mkql_flow.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool IsStream>
class TToFlowWrapper : public TFlowSourceCodegeneratorNode<TToFlowWrapper<IsStream>> {
    typedef TFlowSourceCodegeneratorNode<TToFlowWrapper<IsStream>> TBaseComputation;
public:
    TToFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind,IComputationNode* stream)
        : TBaseComputation(mutables, kind, EValueRepresentation::Any), Stream(stream)
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& stream, TComputationContext& ctx) const {
        if (stream.IsInvalid()) {
            stream = IsStream ? Stream->GetValue(ctx) : Stream->GetValue(ctx).GetListIterator();
        }

        NUdf::TUnboxedValue next;
        if constexpr (IsStream) {
            switch (const auto state = stream.Fetch(next)) {
                case NUdf::EFetchStatus::Ok: return next.Release();
                case NUdf::EFetchStatus::Finish: return NUdf::TUnboxedValuePod::MakeFinish();
                case NUdf::EFetchStatus::Yield: return NUdf::TUnboxedValuePod::MakeYield();
            }
        } else {
            return stream.Next(next) ? next.Release() : NUdf::TUnboxedValuePod::MakeFinish();
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        const auto load = new LoadInst(valueType, statePtr, "load", block);
        const auto state = PHINode::Create(load->getType(), 2U, "state", main);
        state->addIncoming(load, block);

        BranchInst::Create(init, main, IsInvalid(load, block), block);

        block = init;

        if constexpr (IsStream) {
            GetNodeValue(statePtr, Stream, ctx, block);
        } else {
            const auto list = GetNodeValue(Stream, ctx, block);
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(statePtr, list, ctx.Codegen, block);
            if (Stream->IsTemporaryValue())
                CleanupBoxed(list, ctx, block);
        }

        const auto save = new LoadInst(valueType, statePtr, "save", block);
        state->addIncoming(save, block);
        BranchInst::Create(main, block);

        block = main;

        const auto valuePtr = new AllocaInst(valueType, 0U, "value_ptr", &ctx.Func->getEntryBlock().back());
        new StoreInst(ConstantInt::get(valueType, 0), valuePtr, block);

        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(valueType, 2U, "result", done);

        if constexpr (IsStream) {
            const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(Type::getInt32Ty(context), state, ctx.Codegen, block, valuePtr);
            const auto ok = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), "ok", block);

            const auto none = BasicBlock::Create(context, "none", ctx.Func);
            BranchInst::Create(good, none, ok, block);

            block = none;

            const auto yield = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Yield)), "yield", block);
            const auto special = SelectInst::Create(yield, GetYield(context), GetFinish(context), "special", block);
            result->addIncoming(special, block);
            BranchInst::Create(done, block);
        } else {
            const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(Type::getInt1Ty(context), state, ctx.Codegen, block, valuePtr);
            result->addIncoming(GetFinish(context), block);
            BranchInst::Create(good, done, status, block);
        }

        block = good;
        const auto value = new LoadInst(valueType, valuePtr, "value", block);
        ValueRelease(static_cast<const IComputationNode*>(this)->GetRepresentation(), value, ctx, block);
        result->addIncoming(value, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream);
    }

    IComputationNode* const Stream;
};

template <bool IsItemOptional = true>
class TOptToFlowWrapper : public TFlowSourceCodegeneratorNode<TOptToFlowWrapper<IsItemOptional>> {
    typedef TFlowSourceCodegeneratorNode<TOptToFlowWrapper<IsItemOptional>> TBaseComputation;
public:
    TOptToFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* optional)
        : TBaseComputation(mutables, kind, EValueRepresentation::Embedded), Optional(optional)
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        }

        state = NUdf::TUnboxedValue::MakeFinish();
        if (auto value = Optional->GetValue(ctx)) {
            return value.Release().GetOptionalValueIf<IsItemOptional>();
        }

        return state;
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto load = new LoadInst(valueType, statePtr, "load", block);
        const auto result = PHINode::Create(valueType, 2U, "state", done);

        result->addIncoming(load, block);
        BranchInst::Create(done, main, IsFinish(load, block), block);

        block = main;

        const auto finish = GetFinish(context);
        new StoreInst(finish, statePtr, block);

        const auto optional = GetNodeValue(Optional, ctx, block);
        const auto value = IsItemOptional ? GetOptionalValue(context, optional, block) : optional;
        const auto output = SelectInst::Create(IsEmpty(optional, block), finish, value, "output", block);

        result->addIncoming(output, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Optional);
    }

    IComputationNode* const Optional;
};

class TFromFlowWrapper : public TCustomValueCodegeneratorNode<TFromFlowWrapper> {
    typedef TCustomValueCodegeneratorNode<TFromFlowWrapper> TBaseComputation;
public:

    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, IComputationNode* flow)
            : TBase(memInfo), CompCtx(compCtx), Flow(flow)
        {}

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            result = Flow->GetValue(CompCtx);
            if (result.IsFinish())
                return NUdf::EFetchStatus::Finish;
            if (result.IsYield())
                return NUdf::EFetchStatus::Yield;
            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& CompCtx;
        IComputationNode* const Flow;
    };

    class TStreamCodegenValue : public TComputationValue<TStreamCodegenValue> {
    public:
        using TBase = TComputationValue<TStreamCodegenValue>;
        using TFetchPtr = NUdf::EFetchStatus (*)(TComputationContext*, NUdf::TUnboxedValuePod&);

        TStreamCodegenValue(TMemoryUsageInfo* memInfo, TFetchPtr fetch, TComputationContext* ctx)
            : TBase(memInfo), FetchFunc(fetch), Ctx(ctx)
        {}

    protected:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            return FetchFunc(Ctx, result);
        }

        const TFetchPtr FetchFunc;
        TComputationContext* const Ctx;
    };

    TFromFlowWrapper(TComputationMutables& mutables, IComputationNode* flow)
        : TBaseComputation(mutables), Flow(flow)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Fetch)
            return ctx.HolderFactory.Create<TStreamCodegenValue>(Fetch, &ctx);
#endif
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Flow);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Flow);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        FetchFunc = GenerateFetcher(codegen);
        codegen.ExportSymbol(FetchFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (FetchFunc)
            Fetch = reinterpret_cast<TStreamCodegenValue::TFetchPtr>(codegen.GetPointerToFunction(FetchFunc));
    }

    Function* GenerateFetcher(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = TBaseComputation::MakeName("Fetch");
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), PointerType::getUnqual(valueType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        SafeUnRefUnboxed(valuePtr, ctx, block);
        GetNodeValue(valuePtr, Flow, ctx, block);

        const auto value = new LoadInst(valueType, valuePtr, "value", block);

        const auto second = SelectInst::Create(IsYield(value, block), ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), "second", block);
        const auto first = SelectInst::Create(IsFinish(value, block), ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), second, "second", block);

        ReturnInst::Create(context, first, block);
        return ctx.Func;
    }

    Function* FetchFunc = nullptr;

    TStreamCodegenValue::TFetchPtr Fetch = nullptr;
#endif
    IComputationNode* const Flow;
};

class TToWideFlowWrapper : public TWideFlowSourceCodegeneratorNode<TToWideFlowWrapper> {
using TBaseComputation = TWideFlowSourceCodegeneratorNode<TToWideFlowWrapper>;
public:
    TToWideFlowWrapper(TComputationMutables& mutables, IComputationNode* stream, ui32 width)
        : TBaseComputation(mutables, EValueRepresentation::Any)
        , Stream(stream)
        , Width(width)
        , TempStateIndex(std::exchange(mutables.CurValueIndex, mutables.CurValueIndex + Width))
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.HasValue()) {
            state = Stream->GetValue(ctx);
        }

        switch (const auto status = state.WideFetch(ctx.MutableValues.get() + TempStateIndex, Width)) {
        case NUdf::EFetchStatus::Finish:
            return EFetchResult::Finish;
        case NUdf::EFetchStatus::Yield:
            return EFetchResult::Yield;
        case NUdf::EFetchStatus::Ok:
            break;
        }

        for (auto i = 0U; i < Width; ++i) {
            if (const auto out = output[i]) {
                *out = std::move(ctx.MutableValues[TempStateIndex + i]);
            }
        }
        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);
        const auto values = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(indexType, TempStateIndex)}, "values", &ctx.Func->getEntryBlock().back());

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        const auto load = new LoadInst(valueType, statePtr, "load", block);
        const auto state = PHINode::Create(load->getType(), 2U, "state", main);
        state->addIncoming(load, block);

        BranchInst::Create(init, main, IsInvalid(load, block), block);

        block = init;

        GetNodeValue(statePtr, Stream, ctx, block);

        const auto save = new LoadInst(valueType, statePtr, "save", block);
        state->addIncoming(save, block);
        BranchInst::Create(main, block);

        block = main;

        const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::WideFetch>(indexType, state, ctx.Codegen, block, values, ConstantInt::get(indexType, Width));

        const auto ok = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(indexType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), "ok", block);
        const auto yield = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(indexType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), "yield", block);
        const auto special = SelectInst::Create(yield, ConstantInt::get(indexType, static_cast<i32>(EFetchResult::Yield)), ConstantInt::get(indexType, static_cast<i32>(EFetchResult::Finish)), "special", block);
        const auto complete = SelectInst::Create(ok, ConstantInt::get(indexType, static_cast<i32>(EFetchResult::One)), special, "complete", block);

        TGettersList getters(Width);
        for (auto i = 0U; i < getters.size(); ++i) {
            getters[i] = [idx = TempStateIndex + i, valueType, indexType](const TCodegenContext& ctx, BasicBlock*& block) {
                const auto valuePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(indexType, idx)}, (TString("ptr_") += ToString(idx)).c_str(), block);
                return new LoadInst(valueType, valuePtr, (TString("val_") += ToString(idx)).c_str(), block);
            };
        };
        return {complete, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream);
    }

    IComputationNode* const Stream;
    const ui32 Width;
    const ui32 TempStateIndex;
};

class TFromWideFlowWrapper : public TCustomValueCodegeneratorNode<TFromWideFlowWrapper> {
using TBaseComputation = TCustomValueCodegeneratorNode<TFromWideFlowWrapper>;
public:
    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, IComputationWideFlowNode* wideFlow, ui32 width, ui32 stubsIndex)
            : TBase(memInfo)
            , CompCtx(compCtx)
            , WideFlow(wideFlow)
            , Width(width)
            , StubsIndex(stubsIndex)
            , ClientBuffer(nullptr)
        {}
    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) final {
            if (width != Width)
                Throw(width, Width);

            const auto valuePtrs = CompCtx.WideFields.data() + StubsIndex;
            if (result != ClientBuffer) {
                for (ui32 i = 0; i < width; ++i) {
                    valuePtrs[i] = result + i;
                }
                ClientBuffer = result;
            }

            switch (const auto status = WideFlow->FetchValues(CompCtx, valuePtrs)) {
            case EFetchResult::Finish:
                return NUdf::EFetchStatus::Finish;
            case EFetchResult::Yield:
                return NUdf::EFetchStatus::Yield;
            case EFetchResult::One:
                return NUdf::EFetchStatus::Ok;
            }
        }

        TComputationContext& CompCtx;
        IComputationWideFlowNode* const WideFlow;
        const ui32 Width;
        const ui32 StubsIndex;
        const NUdf::TUnboxedValue* ClientBuffer;
    };

    class TStreamCodegenValue : public TComputationValue<TStreamCodegenValue> {
    public:
        using TBase = TComputationValue<TStreamCodegenValue>;
        using TWideFetchPtr = NUdf::EFetchStatus (*)(TComputationContext*, NUdf::TUnboxedValuePod*, ui32);

        TStreamCodegenValue(TMemoryUsageInfo* memInfo, TWideFetchPtr fetch, TComputationContext* ctx)
            : TBase(memInfo), WideFetchFunc(fetch), Ctx(ctx)
        {}
    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) final {
            return WideFetchFunc(Ctx, result, width);
        }

        const TWideFetchPtr WideFetchFunc;
        TComputationContext* const Ctx;
    };

    TFromWideFlowWrapper(TComputationMutables& mutables, IComputationWideFlowNode* wideFlow, std::vector<EValueRepresentation>&& representations)
        : TBaseComputation(mutables)
        , WideFlow(wideFlow)
        , Representations(std::move(representations))
        , StubsIndex(mutables.IncrementWideFieldsIndex(Representations.size()))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && WideFetch)
            return ctx.HolderFactory.Create<TStreamCodegenValue>(WideFetch, &ctx);
#endif
        return ctx.HolderFactory.Create<TStreamValue>(ctx, WideFlow, Representations.size(), StubsIndex);
    }
private:
    void RegisterDependencies() const final {
        this->DependsOn(WideFlow);
    }

    [[noreturn]] static void Throw(ui32 requested, ui32 expected) {
        TStringBuilder res;
        res << "Requested " << requested << " fields, but expected " << expected;
        UdfTerminate(res.data());
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        WideFetchFunc = GenerateFetcher(codegen);
        codegen.ExportSymbol(WideFetchFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (WideFetchFunc)
            WideFetch = reinterpret_cast<TStreamCodegenValue::TWideFetchPtr>(codegen.GetPointerToFunction(WideFetchFunc));
    }

    Function* GenerateFetcher(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = TBaseComputation::MakeName("WideFetch");
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt32Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), PointerType::getUnqual(valueType), indexType}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto valuesPtr = &*++args;
        const auto width = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto fail = BasicBlock::Create(context, "fail", ctx.Func);

        auto block = main;

        const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, width, ConstantInt::get(width->getType(), Representations.size()), "check", block);

        BranchInst::Create(work, fail, check, block);

        block = work;

        std::vector<Value*> pointers(Representations.size());
        for (auto i = 0U; i < pointers.size(); ++i) {
            pointers[i] = GetElementPtrInst::CreateInBounds(valueType, valuesPtr, {ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), block);
            SafeUnRefUnboxed(pointers[i], ctx, block);
        }

        const auto getres = GetNodeValues(WideFlow, ctx, block);

        const auto yield = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, getres.first, ConstantInt::get(indexType, static_cast<i32>(EFetchResult::Yield)), "yield", block);
        const auto special = SelectInst::Create(yield, ConstantInt::get(indexType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), ConstantInt::get(indexType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), "special", block);

        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(statusType, 2U, "result", done);
        result->addIncoming(special, block);

        const auto row = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, getres.first, ConstantInt::get(indexType, static_cast<i32>(EFetchResult::One)), "row", block);
        BranchInst::Create(good, done, row, block);

        block = good;

        for (auto i = 0U; i < pointers.size(); ++i) {
            auto value = getres.second[i](ctx, block);
            ValueAddRef(Representations[i], value, ctx, block);
            new StoreInst(value, pointers[i], block);
        }

        result->addIncoming(ConstantInt::get(indexType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), block);
        BranchInst::Create(done, block);

        block = done;
        ReturnInst::Create(context, result, block);

        block = fail;

        const auto throwFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TFromWideFlowWrapper::Throw));
        const auto throwFuncType = FunctionType::get(Type::getVoidTy(context), { indexType, indexType }, false);
        const auto throwFuncPtr = CastInst::Create(Instruction::IntToPtr, throwFunc, PointerType::getUnqual(throwFuncType), "thrower", block);
        CallInst::Create(throwFuncType, throwFuncPtr, { width, ConstantInt::get(width->getType(), Representations.size()) }, "", block)->setTailCall();
        new UnreachableInst(context, block);

        return ctx.Func;
    }

    Function* WideFetchFunc = nullptr;

    TStreamCodegenValue::TWideFetchPtr WideFetch = nullptr;
#endif
    IComputationWideFlowNode* const WideFlow;
    const std::vector<EValueRepresentation> Representations;
    const ui32 StubsIndex;
};

} // namespace

IComputationNode* WrapToFlow(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());
    const auto type = callable.GetInput(0).GetStaticType();
    const auto outType = AS_TYPE(TFlowType, callable.GetType()->GetReturnType())->GetItemType();
    const auto kind = GetValueRepresentation(outType);
    if (type->IsStream()) {
        if (const auto streamType = AS_TYPE(TStreamType, type); streamType->GetItemType()->IsMulti()) {
            const auto multiType = AS_TYPE(TMultiType, streamType->GetItemType());
            return new TToWideFlowWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), multiType->GetElementsCount());
        }
        return new TToFlowWrapper<true>(ctx.Mutables, kind, LocateNode(ctx.NodeLocator, callable, 0));
    } else if (type->IsList()) {
        return new TToFlowWrapper<false>(ctx.Mutables, kind, LocateNode(ctx.NodeLocator, callable, 0));
    } else if (type->IsOptional()) {
        if (outType->IsOptional()) {
            return new TOptToFlowWrapper<true>(ctx.Mutables, kind, LocateNode(ctx.NodeLocator, callable, 0));
        } else {
            return new TOptToFlowWrapper<false>(ctx.Mutables, kind, LocateNode(ctx.NodeLocator, callable, 0));
        }
    }

    THROW yexception() << "Expected optional, list or stream.";
}

IComputationNode* WrapFromFlow(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args, got " << callable.GetInputsCount());
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        const auto multiType = AS_TYPE(TMultiType, AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType())->GetItemType());
        std::vector<EValueRepresentation> outputRepresentations(multiType->GetElementsCount());
        for (auto i = 0U; i < outputRepresentations.size(); ++i)
            outputRepresentations[i] = GetValueRepresentation(multiType->GetElementType(i));
        return new TFromWideFlowWrapper(ctx.Mutables, wide, std::move(outputRepresentations));
    }
    return new TFromFlowWrapper(ctx.Mutables, flow);
}

}
}
