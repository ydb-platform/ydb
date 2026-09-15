#include "mkql_flow.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

template <bool IsStream>
class TToFlowWrapper: public TFlowSourceCodegeneratorNode<TToFlowWrapper<IsStream>> {
    using TBaseComputation = TFlowSourceCodegeneratorNode<TToFlowWrapper<IsStream>>;

public:
    TToFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* stream, TComputationNodePtrVector&& dependencies)
        : TBaseComputation(mutables, kind, EValueRepresentation::Any)
        , Stream_(stream)
        , Dependencies_(std::move(dependencies))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& stream, TComputationContext& ctx) const {
        if (stream.IsInvalid()) {
            stream = IsStream ? Stream_->GetValue(ctx) : Stream_->GetValue(ctx).GetListIterator();
        }

        NUdf::TUnboxedValue next;
        if constexpr (IsStream) {
            switch (/* const auto state = */ stream.Fetch(next)) {
                case NUdf::EFetchStatus::Ok:
                    return next.Release();
                case NUdf::EFetchStatus::Finish:
                    return NUdf::TUnboxedValuePod::MakeFinish();
                case NUdf::EFetchStatus::Yield:
                    return NUdf::TUnboxedValuePod::MakeYield();
            }
        } else {
            return stream.Next(next) ? next.Release() : NUdf::TUnboxedValuePod::MakeFinish();
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        const auto load = new LoadInst(valueType, statePtr, "load", block);
        const auto state = PHINode::Create(load->getType(), 2U, "state", main);
        state->addIncoming(load, block);

        BranchInst::Create(init, main, IsInvalid(load, block, context), block);

        block = init;

        if constexpr (IsStream) {
            GetNodeValue(statePtr, Stream_, ctx, block);
        } else {
            const auto list = GetNodeValue(Stream_, ctx, block);
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(statePtr, list, ctx.Codegen, block);
            if (Stream_->IsTemporaryValue()) {
                CleanupBoxed(list, ctx, block);
            }
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
            const auto status = CallBoxedValueFetch(state, ctx, block, valuePtr);
            const auto ok = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), "ok", block);

            const auto none = BasicBlock::Create(context, "none", ctx.Func);
            BranchInst::Create(good, none, ok, block);

            block = none;

            const auto yield = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Yield)), "yield", block);
            const auto special = SelectInst::Create(yield, GetYield(context), GetFinish(context), "special", block);
            result->addIncoming(special, block);
            BranchInst::Create(done, block);
        } else {
            const auto status = CallBoxedValueNext(state, ctx, block, valuePtr);
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
        this->DependsOn(Stream_);
        std::for_each(Dependencies_.cbegin(), Dependencies_.cend(), std::bind(&TToFlowWrapper::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode* const Stream_;
    TComputationNodePtrVector Dependencies_;
};

template <bool IsItemOptional = true>
class TOptToFlowWrapper: public TFlowSourceCodegeneratorNode<TOptToFlowWrapper<IsItemOptional>> {
    using TBaseComputation = TFlowSourceCodegeneratorNode<TOptToFlowWrapper<IsItemOptional>>;

public:
    TOptToFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* optional, TComputationNodePtrVector&& dependencies)
        : TBaseComputation(mutables, kind, EValueRepresentation::Embedded)
        , Optional_(optional)
        , Dependencies_(std::move(dependencies))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        }

        state = NUdf::TUnboxedValue::MakeFinish();
        if (auto value = Optional_->GetValue(ctx)) {
            return value.Release().GetOptionalValueIf<IsItemOptional>();
        }

        return state;
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto load = new LoadInst(valueType, statePtr, "load", block);
        const auto result = PHINode::Create(valueType, 2U, "state", done);

        result->addIncoming(load, block);
        BranchInst::Create(done, main, IsFinish(load, block, context), block);

        block = main;

        const auto finish = GetFinish(context);
        new StoreInst(finish, statePtr, block);

        const auto optional = GetNodeValue(Optional_, ctx, block);
        const auto value = IsItemOptional ? GetOptionalValue(context, optional, block) : optional;
        const auto output = SelectInst::Create(IsEmpty(optional, block, context), finish, value, "output", block);

        result->addIncoming(output, block);
        BranchInst::Create(done, block);

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Optional_);
        std::for_each(Dependencies_.cbegin(), Dependencies_.cend(), std::bind(&TOptToFlowWrapper::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode* const Optional_;
    TComputationNodePtrVector Dependencies_;
};

class TFromFlowWrapper: public TCustomValueCodegeneratorNode<TFromFlowWrapper> {
    using TBaseComputation = TCustomValueCodegeneratorNode<TFromFlowWrapper>;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, IComputationNode* flow)
            : TBase(memInfo)
            , CompCtx_(compCtx)
            , Flow_(flow)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            NYql::NUdf::TUnboxedValue fetchResult;
            fetchResult = Flow_->GetValue(CompCtx_);
            if (fetchResult.IsFinish()) {
                return NUdf::EFetchStatus::Finish;
            }
            if (fetchResult.IsYield()) {
                return NUdf::EFetchStatus::Yield;
            }
            result = std::move(fetchResult);
            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& CompCtx_;
        IComputationNode* const Flow_;
    };

    class TStreamCodegenValue: public TComputationValue<TStreamCodegenValue> {
    public:
        using TBase = TComputationValue<TStreamCodegenValue>;
        using TFetchPtr = NUdf::EFetchStatus (*)(TComputationContext*, NUdf::TUnboxedValuePod&);

        TStreamCodegenValue(TMemoryUsageInfo* memInfo, TFetchPtr fetch, TComputationContext* ctx)
            : TBase(memInfo)
            , FetchFunc_(fetch)
            , Ctx_(ctx)
        {
        }

    protected:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override Y_NO_SANITIZE("undefined") {
            NUdf::TUnboxedValue fetchResult;
            if (const auto status = FetchFunc_(Ctx_, fetchResult); NUdf::EFetchStatus::Ok != status) {
                return status;
            }
            result = std::move(fetchResult);
            return NUdf::EFetchStatus::Ok;
        }

        const TFetchPtr FetchFunc_;
        TComputationContext* const Ctx_;
    };

    TFromFlowWrapper(TComputationMutables& mutables, IComputationNode* flow)
        : TBaseComputation(mutables)
        , Flow_(flow)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Fetch_) {
            return ctx.HolderFactory.Create<TStreamCodegenValue>(Fetch_, &ctx);
        }
#endif
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Flow_);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Flow_);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        FetchFunc_ = GenerateFetcher(codegen);
        codegen.ExportSymbol(FetchFunc_);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (FetchFunc_) {
            Fetch_ = reinterpret_cast<TStreamCodegenValue::TFetchPtr>(codegen.GetPointerToFunction(FetchFunc_));
        }
    }

    Function* GenerateFetcher(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = TBaseComputation::MakeName("Fetch");
        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto valueType = Type::getInt128Ty(context);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), PointerType::getUnqual(valueType)}, /*isVarArg=*/false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        SafeUnRefUnboxedOne(valuePtr, ctx, block);
        GetNodeValue(valuePtr, Flow_, ctx, block);

        const auto value = new LoadInst(valueType, valuePtr, "value", block);

        const auto second = SelectInst::Create(IsYield(value, block, context), ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), "second", block);
        const auto first = SelectInst::Create(IsFinish(value, block, context), ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), second, "second", block);

        ReturnInst::Create(context, first, block);
        return ctx.Func;
    }

    Function* FetchFunc_ = nullptr;

    TStreamCodegenValue::TFetchPtr Fetch_ = nullptr;
#endif
    IComputationNode* const Flow_;
};

class TToWideFlowWrapper: public TWideFlowSourceCodegeneratorNode<TToWideFlowWrapper> {
    using TBaseComputation = TWideFlowSourceCodegeneratorNode<TToWideFlowWrapper>;

public:
    TToWideFlowWrapper(TComputationMutables& mutables, IComputationNode* stream, ui32 width, TComputationNodePtrVector&& dependencies)
        : TBaseComputation(mutables, EValueRepresentation::Any)
        , Stream_(stream)
        , Dependencies_(std::move(dependencies))
        , Width_(width)
        , TempStateIndex_(std::exchange(mutables.CurValueIndex, mutables.CurValueIndex + Width_))
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            state = Stream_->GetValue(ctx);
        }

        switch (/* const auto status = */ state.WideFetch(ctx.MutableValues.get() + TempStateIndex_, Width_)) {
            case NUdf::EFetchStatus::Finish:
                return EFetchResult::Finish;
            case NUdf::EFetchStatus::Yield:
                return EFetchResult::Yield;
            case NUdf::EFetchStatus::Ok:
                break;
        }

        for (auto i = 0U; i < Width_; ++i) {
            if (const auto out = output[i]) {
                *out = std::move(ctx.MutableValues[TempStateIndex_ + i]);
            }
        }
        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);
        const auto values = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(indexType, TempStateIndex_)}, "values", &ctx.Func->getEntryBlock().back());

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        const auto load = new LoadInst(valueType, statePtr, "load", block);
        const auto state = PHINode::Create(load->getType(), 2U, "state", main);
        state->addIncoming(load, block);

        BranchInst::Create(init, main, IsInvalid(load, block, context), block);

        block = init;

        GetNodeValue(statePtr, Stream_, ctx, block);

        const auto save = new LoadInst(valueType, statePtr, "save", block);
        state->addIncoming(save, block);
        BranchInst::Create(main, block);

        block = main;

        const auto status = CallBoxedValueWideFetch(state, ctx, block, values, Width_);

        const auto ok = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(indexType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), "ok", block);
        const auto yield = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(indexType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), "yield", block);
        const auto special = SelectInst::Create(yield, ConstantInt::get(indexType, static_cast<i32>(EFetchResult::Yield)), ConstantInt::get(indexType, static_cast<i32>(EFetchResult::Finish)), "special", block);
        const auto complete = SelectInst::Create(ok, ConstantInt::get(indexType, static_cast<i32>(EFetchResult::One)), special, "complete", block);

        TGettersList getters(Width_);
        for (auto i = 0U; i < getters.size(); ++i) {
            getters[i] = [idx = TempStateIndex_ + i, valueType, indexType](const TCodegenContext& ctx, BasicBlock*& block) {
                const auto valuePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(indexType, idx)}, (TString("ptr_") += ToString(idx)).c_str(), block);
                return new LoadInst(valueType, valuePtr, (TString("val_") += ToString(idx)).c_str(), block);
            };
        };
        return {complete, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
        std::for_each(Dependencies_.cbegin(), Dependencies_.cend(), std::bind(&TToWideFlowWrapper::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode* const Stream_;
    TComputationNodePtrVector Dependencies_;
    const ui32 Width_;
    const ui32 TempStateIndex_;
};

class TFromWideFlowWrapper: public TCustomValueCodegeneratorNode<TFromWideFlowWrapper> {
    using TBaseComputation = TCustomValueCodegeneratorNode<TFromWideFlowWrapper>;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, IComputationWideFlowNode* wideFlow, ui32 width, ui32 stubsIndex)
            : TBase(memInfo)
            , CompCtx_(compCtx)
            , WideFlow_(wideFlow)
            , Width_(width)
            , StubsIndex_(stubsIndex)
            , ClientBuffer_(nullptr)
        {
        }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) final {
            if (width != Width_) {
                Throw(width, Width_);
            }

            const auto valuePtrs = CompCtx_.WideFields.data() + StubsIndex_;
            if (result != ClientBuffer_) {
                for (ui32 i = 0; i < width; ++i) {
                    valuePtrs[i] = result + i;
                }
                ClientBuffer_ = result;
            }

            switch (/* const auto status = */ WideFlow_->FetchValues(CompCtx_, valuePtrs)) {
                case EFetchResult::Finish:
                    return NUdf::EFetchStatus::Finish;
                case EFetchResult::Yield:
                    return NUdf::EFetchStatus::Yield;
                case EFetchResult::One:
                    return NUdf::EFetchStatus::Ok;
            }
        }

        TComputationContext& CompCtx_;
        IComputationWideFlowNode* const WideFlow_;
        const ui32 Width_;
        const ui32 StubsIndex_;
        const NUdf::TUnboxedValue* ClientBuffer_;
    };

    class TStreamCodegenValue: public TComputationValue<TStreamCodegenValue> {
    public:
        using TBase = TComputationValue<TStreamCodegenValue>;
        using TWideFetchPtr = NUdf::EFetchStatus (*)(TComputationContext*, NUdf::TUnboxedValuePod*, ui32);

        TStreamCodegenValue(TMemoryUsageInfo* memInfo, TWideFetchPtr fetch, TComputationContext* ctx)
            : TBase(memInfo)
            , WideFetchFunc_(fetch)
            , Ctx_(ctx)
        {
        }

    private:
        NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* result, ui32 width) final {
            return WideFetchFunc_(Ctx_, result, width);
        }

        const TWideFetchPtr WideFetchFunc_;
        TComputationContext* const Ctx_;
    };

    TFromWideFlowWrapper(TComputationMutables& mutables, IComputationWideFlowNode* wideFlow, std::vector<EValueRepresentation>&& representations)
        : TBaseComputation(mutables)
        , WideFlow_(wideFlow)
        , Representations_(std::move(representations))
        , StubsIndex_(mutables.IncrementWideFieldsIndex(Representations_.size()))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && WideFetch_) {
            return ctx.HolderFactory.Create<TStreamCodegenValue>(WideFetch_, &ctx);
        }
#endif
        return ctx.HolderFactory.Create<TStreamValue>(ctx, WideFlow_, Representations_.size(), StubsIndex_);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(WideFlow_);
    }

    [[noreturn]] static void Throw(ui32 requested, ui32 expected) {
        TStringBuilder res;
        res << "Requested " << requested << " fields, but expected " << expected;
        UdfTerminate(res.data());
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        WideFetchFunc_ = GenerateFetcher(codegen);
        codegen.ExportSymbol(WideFetchFunc_);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (WideFetchFunc_) {
            WideFetch_ = reinterpret_cast<TStreamCodegenValue::TWideFetchPtr>(codegen.GetPointerToFunction(WideFetchFunc_));
        }
    }

    Function* GenerateFetcher(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = TBaseComputation::MakeName("WideFetch");
        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto valueType = Type::getInt128Ty(context);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt32Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), PointerType::getUnqual(valueType), indexType}, /*isVarArg=*/false);

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

        const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, width, ConstantInt::get(width->getType(), Representations_.size()), "check", block);

        BranchInst::Create(work, fail, check, block);

        block = work;

        std::vector<Value*> pointers(Representations_.size());
        for (auto i = 0U; i < pointers.size(); ++i) {
            pointers[i] = GetElementPtrInst::CreateInBounds(valueType, valuesPtr, {ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), block);
            SafeUnRefUnboxedOne(pointers[i], ctx, block);
        }

        const auto getres = GetNodeValues(WideFlow_, ctx, block);

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
            ValueAddRef(Representations_[i], value, ctx, block);
            new StoreInst(value, pointers[i], block);
        }

        result->addIncoming(ConstantInt::get(indexType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), block);
        BranchInst::Create(done, block);

        block = done;
        ReturnInst::Create(context, result, block);

        block = fail;

        EmitFunctionCall<&TFromWideFlowWrapper::Throw>(Type::getVoidTy(context), {width, ConstantInt::get(width->getType(), Representations_.size())}, ctx, block);
        new UnreachableInst(context, block);

        return ctx.Func;
    }

    Function* WideFetchFunc_ = nullptr;

    TStreamCodegenValue::TWideFetchPtr WideFetch_ = nullptr;
#endif
    IComputationWideFlowNode* const WideFlow_;
    const std::vector<EValueRepresentation> Representations_;
    const ui32 StubsIndex_;
};

} // namespace

IComputationNode* WrapToFlow(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 1, "Expected at least 1 arg");
    const auto type = callable.GetInput(0).GetStaticType();
    const auto outType = AS_TYPE(TFlowType, callable.GetType()->GetReturnType())->GetItemType();
    const auto kind = GetValueRepresentation(outType);

    const auto input = LocateNode(ctx.NodeLocator, callable, 0);
    TComputationNodePtrVector dependencies;
    dependencies.reserve(callable.GetInputsCount() - 1);
    for (ui32 i = 1; i < callable.GetInputsCount(); i++) {
        dependencies.emplace_back(LocateNode(ctx.NodeLocator, callable, i));
    }

    if (type->IsStream()) {
        if (const auto streamType = AS_TYPE(TStreamType, type); streamType->GetItemType()->IsMulti()) {
            const auto multiType = AS_TYPE(TMultiType, streamType->GetItemType());
            return new TToWideFlowWrapper(ctx.Mutables, input, multiType->GetElementsCount(), std::move(dependencies));
        }
        return new TToFlowWrapper<true>(ctx.Mutables, kind, input, std::move(dependencies));
    } else if (type->IsList()) {
        return new TToFlowWrapper<false>(ctx.Mutables, kind, input, std::move(dependencies));
    } else if (type->IsOptional()) {
        if (outType->IsOptional()) {
            return new TOptToFlowWrapper<true>(ctx.Mutables, kind, input, std::move(dependencies));
        } else {
            return new TOptToFlowWrapper<false>(ctx.Mutables, kind, input, std::move(dependencies));
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
        for (auto i = 0U; i < outputRepresentations.size(); ++i) {
            outputRepresentations[i] = GetValueRepresentation(multiType->GetElementType(i));
        }
        return new TFromWideFlowWrapper(ctx.Mutables, wide, std::move(outputRepresentations));
    }
    return new TFromFlowWrapper(ctx.Mutables, flow);
}

} // namespace NKikimr::NMiniKQL
