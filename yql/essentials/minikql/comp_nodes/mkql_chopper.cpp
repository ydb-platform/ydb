#include "mkql_chopper.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <utility>

namespace NKikimr::NMiniKQL {

namespace {

class TChopperFlowWrapper: public TStatefulFlowCodegeneratorNode<TChopperFlowWrapper> {
    using TBaseComputation = TStatefulFlowCodegeneratorNode<TChopperFlowWrapper>;

public:
    enum class EState: ui64 {
        Work,
        Chop,
        Next,
        Skip
    };

    TChopperFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow, IComputationExternalNode* itemArg, IComputationNode* key, IComputationExternalNode* keyArg, IComputationNode* chop, IComputationExternalNode* input, IComputationNode* output)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Any)
        , Flow_(flow)
        , ItemArg_(itemArg)
        , Key_(key)
        , KeyArg_(keyArg)
        , Chop_(chop)
        , Input_(input)
        , Output_(output)
    {
        Input_->SetGetter(std::bind(&TChopperFlowWrapper::Getter, this, std::bind(&TChopperFlowWrapper::RefState, this, std::placeholders::_1), std::placeholders::_1));

#ifndef MKQL_DISABLE_CODEGEN
        const auto codegenInput = dynamic_cast<ICodegeneratorExternalNode*>(Input_);
        MKQL_ENSURE(codegenInput, "Input arg must be codegenerator node.");
        codegenInput->SetValueGetterBuilder([this](const TCodegenContext& ctx) {
            return GenerateHandler(ctx.Codegen);
        });
#endif
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            if (auto item = Flow_->GetValue(ctx); item.IsSpecial()) {
                return item.Release();
            } else {
                state = NUdf::TUnboxedValuePod(ui64(EState::Next));
                ItemArg_->SetValue(ctx, std::move(item));
                KeyArg_->SetValue(ctx, Key_->GetValue(ctx));
            }
        } else if (EState::Skip == EState(state.Get<ui64>())) {
            do {
                if (auto next = Flow_->GetValue(ctx); next.IsSpecial()) {
                    return next.Release();
                } else {
                    ItemArg_->SetValue(ctx, std::move(next));
                }
            } while (!Chop_->GetValue(ctx).Get<bool>());

            KeyArg_->SetValue(ctx, Key_->GetValue(ctx));
            state = NUdf::TUnboxedValuePod(ui64(EState::Next));
        }

        while (true) {
            auto output = Output_->GetValue(ctx);
            if (output.IsFinish()) {
                Input_->InvalidateValue(ctx);
                switch (EState(state.Get<ui64>())) {
                    case EState::Work:
                    case EState::Next:
                        do {
                            if (auto next = Flow_->GetValue(ctx); next.IsSpecial()) {
                                if (next.IsYield()) {
                                    state = NUdf::TUnboxedValuePod(ui64(EState::Skip));
                                }
                                return next.Release();
                            } else {
                                ItemArg_->SetValue(ctx, std::move(next));
                            }
                        } while (!Chop_->GetValue(ctx).Get<bool>());
                    case EState::Chop:
                        KeyArg_->SetValue(ctx, Key_->GetValue(ctx));
                        state = NUdf::TUnboxedValuePod(ui64(EState::Next));
                    default:
                        continue;
                }
            }
            return output.Release();
        }
    }

    NUdf::TUnboxedValuePod Getter(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (EState::Next == EState(state.Get<ui64>())) {
            state = NUdf::TUnboxedValuePod(ui64(EState::Work));
            return ItemArg_->GetValue(ctx).Release();
        }

        auto item = Flow_->GetValue(ctx);
        if (!item.IsSpecial()) {
            ItemArg_->SetValue(ctx, NUdf::TUnboxedValue(item));

            if (Chop_->GetValue(ctx).Get<bool>()) {
                state = NUdf::TUnboxedValuePod(ui64(EState::Chop));
                return NUdf::TUnboxedValuePod::MakeFinish();
            }
        }
        return item.Release();
    }
#ifndef MKQL_DISABLE_CODEGEN
private:
    Function* GenerateHandler(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        TStringStream out;
        out << this->DebugString() << "::Handler_(" << static_cast<const void*>(this) << ").";
        const auto& name = out.Str();
        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ItemArg_);
        const auto codegenKeyArg = dynamic_cast<ICodegeneratorExternalNode*>(KeyArg_);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenKeyArg, "Key arg must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);
        const auto funcType = FunctionType::get(valueType, {PointerType::getUnqual(GetCompContextType(context))}, /*isVarArg=*/false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        ctx.Ctx = &*ctx.Func->arg_begin();
        ctx.Ctx->addAttr(Attribute::NonNull);

        auto block = main;

        const auto load = BasicBlock::Create(context, "load", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);

        const auto statePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), static_cast<const IComputationNode*>(this)->GetIndex())}, "state_ptr", block);
        const auto entry = new LoadInst(valueType, statePtr, "entry", block);
        const auto next = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, entry, GetConstant(ui64(EState::Next), context), "next", block);

        BranchInst::Create(load, work, next, block);

        {
            block = load;
            new StoreInst(GetConstant(ui64(EState::Work), context), statePtr, block);
            const auto item = GetNodeValue(ItemArg_, ctx, block);
            ReturnInst::Create(context, item, block);
        }

        {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto step = BasicBlock::Create(context, "step", ctx.Func);
            const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

            block = work;

            const auto item = GetNodeValue(Flow_, ctx, block);

            BranchInst::Create(exit, good, IsSpecial(item, block, context), block);

            block = good;

            codegenItemArg->CreateSetValue(ctx, block, item);

            const auto chop = GetNodeValue(Chop_, ctx, block);
            const auto cast = CastInst::Create(Instruction::Trunc, chop, Type::getInt1Ty(context), "bool", block);
            BranchInst::Create(step, exit, cast, block);

            block = step;

            new StoreInst(GetConstant(ui64(EState::Chop), context), statePtr, block);
            ReturnInst::Create(context, GetFinish(context), block);

            block = exit;
            ReturnInst::Create(context, item, block);
        }

        return ctx.Func;
    }

public:
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ItemArg_);
        const auto codegenKeyArg = dynamic_cast<ICodegeneratorExternalNode*>(KeyArg_);
        const auto codegenInput = dynamic_cast<ICodegeneratorExternalNode*>(Input_);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenKeyArg, "Key arg must be codegenerator node.");
        MKQL_ENSURE(codegenInput, "Input arg must be codegenerator node.");

        auto& context = ctx.Codegen.GetContext();

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);

        const auto valueType = Type::getInt128Ty(context);
        const auto result = PHINode::Create(valueType, 5U, "result", exit);

        const auto first = new LoadInst(valueType, statePtr, "first", block);
        const auto enter = SwitchInst::Create(first, loop, 2U, block);
        enter->addCase(GetInvalid(context), init);
        enter->addCase(GetConstant(ui64(EState::Skip), context), pass);

        {
            const auto next = BasicBlock::Create(context, "next", ctx.Func);

            block = init;

            const auto item = GetNodeValue(Flow_, ctx, block);
            result->addIncoming(item, block);
            BranchInst::Create(exit, next, IsSpecial(item, block, context), block);

            block = next;

            new StoreInst(GetConstant(ui64(EState::Next), context), statePtr, block);
            codegenItemArg->CreateSetValue(ctx, block, item);
            const auto key = GetNodeValue(Key_, ctx, block);
            codegenKeyArg->CreateSetValue(ctx, block, key);

            BranchInst::Create(loop, block);
        }

        {
            const auto part = BasicBlock::Create(context, "part", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto step = BasicBlock::Create(context, "step", ctx.Func);
            const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

            block = loop;

            const auto item = GetNodeValue(Output_, ctx, block);
            const auto state = new LoadInst(valueType, statePtr, "state", block);

            result->addIncoming(item, block);
            BranchInst::Create(part, exit, IsFinish(item, block, context), block);

            block = part;

            codegenInput->CreateInvalidate(ctx, block);

            result->addIncoming(GetFinish(context), block);

            const auto choise = SwitchInst::Create(state, exit, 3U, block);
            choise->addCase(GetConstant(ui64(EState::Next), context), pass);
            choise->addCase(GetConstant(ui64(EState::Work), context), pass);
            choise->addCase(GetConstant(ui64(EState::Chop), context), step);

            block = pass;

            const auto next = GetNodeValue(Flow_, ctx, block);

            result->addIncoming(next, block);

            const auto way = SwitchInst::Create(next, good, 2U, block);
            way->addCase(GetFinish(context), exit);
            way->addCase(GetYield(context), skip);

            block = good;

            codegenItemArg->CreateSetValue(ctx, block, next);

            const auto chop = GetNodeValue(Chop_, ctx, block);
            const auto cast = CastInst::Create(Instruction::Trunc, chop, Type::getInt1Ty(context), "bool", block);

            BranchInst::Create(step, pass, cast, block);

            block = step;

            new StoreInst(GetConstant(ui64(EState::Next), context), statePtr, block);
            const auto key = GetNodeValue(Key_, ctx, block);
            codegenKeyArg->CreateSetValue(ctx, block, key);

            BranchInst::Create(loop, block);

            block = skip;
            new StoreInst(GetConstant(ui64(EState::Skip), context), statePtr, block);
            result->addIncoming(next, block);
            BranchInst::Create(exit, block);
        }

        block = exit;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow_)) {
            Own(flow, ItemArg_);
            DependsOn(flow, Key_);
            Own(flow, KeyArg_);
            DependsOn(flow, Chop_);

            Own(flow, Input_);
            DependsOn(flow, Output_);
        }
    }

    IComputationNode* const Flow_;

    IComputationExternalNode* const ItemArg_;
    IComputationNode* const Key_;
    IComputationExternalNode* const KeyArg_;
    IComputationNode* const Chop_;

    IComputationExternalNode* const Input_;
    IComputationNode* const Output_;
};

class TChopperWrapper: public TCustomValueCodegeneratorNode<TChopperWrapper> {
    using TBaseComputation = TCustomValueCodegeneratorNode<TChopperWrapper>;

private:
    enum class EState: ui8 {
        Init,
        Work,
        Chop,
        Next,
        Skip,
    };
    using TStatePtr = std::shared_ptr<EState>;

    class TSubStream: public TComputationValue<TSubStream> {
    public:
        using TBase = TComputationValue<TSubStream>;

        TSubStream(TMemoryUsageInfo* memInfo, TStatePtr state, NUdf::TUnboxedValue stream, IComputationExternalNode* itemArg, IComputationNode* chop, TComputationContext& ctx)
            : TBase(memInfo)
            , State_(std::move(state))
            , Stream_(std::move(stream))
            , ItemArg_(itemArg)
            , Chop_(chop)
            , Ctx_(ctx)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            auto& state = *State_;
            if (EState::Next == state) {
                state = EState::Work;
                result = ItemArg_->GetValue(Ctx_);
                return NUdf::EFetchStatus::Ok;
            }

            if (EState::Chop == state) {
                return NUdf::EFetchStatus::Finish;
            }

            while (true) {
                NYql::NUdf::TUnboxedValue fetchResult;
                switch (const auto status = Stream_.Fetch(fetchResult)) {
                    case NUdf::EFetchStatus::Ok: {
                        ItemArg_->SetValue(Ctx_, NUdf::TUnboxedValue(fetchResult));

                        if (Chop_->GetValue(Ctx_).Get<bool>()) {
                            state = EState::Chop;
                            return NUdf::EFetchStatus::Finish;
                        }

                        result = std::move(fetchResult);
                        return status;
                    }

                    case NUdf::EFetchStatus::Finish:
                    case NUdf::EFetchStatus::Yield:
                        return status;
                }
            }
        }

        const TStatePtr State_;
        const NUdf::TUnboxedValue Stream_;

        IComputationExternalNode* const ItemArg_;
        IComputationNode* const Chop_;

        TComputationContext& Ctx_;
    };

    class TMainStream: public TComputationValue<TMainStream> {
    public:
        TMainStream(TMemoryUsageInfo* memInfo, TStatePtr&& state, NUdf::TUnboxedValue&& stream, const IComputationExternalNode* itemArg, const IComputationNode* key, const IComputationExternalNode* keyArg, const IComputationNode* chop, const IComputationExternalNode* input, const IComputationNode* output, TComputationContext& ctx)
            : TComputationValue(memInfo)
            , State_(std::move(state))
            , ItemArg_(itemArg)
            , Key_(key)
            , Chop_(chop)
            , KeyArg_(keyArg)
            , Input_(input)
            , Output_(output)
            , InputStream_(std::move(stream))
            , Ctx_(ctx)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            while (true) {
                if (Stream_) {
                    if (const auto status = Stream_.Fetch(result); NUdf::EFetchStatus::Finish != status) {
                        return status;
                    }

                    Stream_ = NUdf::TUnboxedValuePod();
                    Input_->InvalidateValue(Ctx_);
                }
                NYql::NUdf::TUnboxedValue fetchResult;
                switch (auto& state = *State_) {
                    case EState::Init:
                        if (const auto status = InputStream_.Fetch(fetchResult); NUdf::EFetchStatus::Ok != status) {
                            return status;
                        }
                        ItemArg_->SetValue(Ctx_, std::move(fetchResult));
                        state = EState::Next;
                        KeyArg_->SetValue(Ctx_, Key_->GetValue(Ctx_));
                        break;
                    case EState::Work:
                    case EState::Next:
                    case EState::Skip:
                        do {
                            switch (const auto status = InputStream_.Fetch(fetchResult)) {
                                case NUdf::EFetchStatus::Ok:
                                    ItemArg_->SetValue(Ctx_, std::move(fetchResult));
                                    break;
                                case NUdf::EFetchStatus::Yield:
                                    state = EState::Skip;
                                case NUdf::EFetchStatus::Finish:
                                    return status;
                            }
                        } while (!Chop_->GetValue(Ctx_).Get<bool>());
                        [[fallthrough]];
                    case EState::Chop:
                        state = EState::Next;
                        KeyArg_->SetValue(Ctx_, Key_->GetValue(Ctx_));
                        break;
                }
                Stream_ = Output_->GetValue(Ctx_);
            }
        }

        const TStatePtr State_;
        const IComputationExternalNode* const ItemArg_;
        const IComputationNode* Key_;
        const IComputationNode* Chop_;
        const IComputationExternalNode* KeyArg_;
        const IComputationExternalNode* Input_;
        const IComputationNode* Output_;
        const NUdf::TUnboxedValue InputStream_;
        NUdf::TUnboxedValue Stream_;
        TComputationContext& Ctx_;
    };
#ifndef MKQL_DISABLE_CODEGEN
    class TCodegenInput: public TComputationValue<TCodegenInput> {
    public:
        using TBase = TComputationValue<TCodegenInput>;

        using TFetchPtr = NUdf::EFetchStatus (*)(TComputationContext*, NUdf::TUnboxedValuePod, EState&, NUdf::TUnboxedValuePod&);

        TCodegenInput(TMemoryUsageInfo* memInfo, TFetchPtr fetch, NUdf::TUnboxedValue stream, TComputationContext* ctx, TStatePtr init)
            : TBase(memInfo)
            , FetchFunc_(fetch)
            , Stream_(std::move(stream))
            , Ctx_(ctx)
            , State_(std::move(init))
        {
        }

    protected:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            return FetchFunc_(Ctx_, static_cast<const NUdf::TUnboxedValuePod&>(Stream_), *State_, result);
        }

        const TFetchPtr FetchFunc_;
        const NUdf::TUnboxedValue Stream_;
        TComputationContext* const Ctx_;
        const TStatePtr State_;
    };

    class TCodegenOutput: public TComputationValue<TCodegenOutput> {
    public:
        using TBase = TComputationValue<TCodegenOutput>;

        using TFetchPtr = NUdf::EFetchStatus (*)(TComputationContext*, NUdf::TUnboxedValuePod&, EState&, NUdf::TUnboxedValuePod, NUdf::TUnboxedValuePod&);

        TCodegenOutput(TMemoryUsageInfo* memInfo, TFetchPtr fetch, TComputationContext* ctx, TStatePtr&& init, NUdf::TUnboxedValue&& input)
            : TBase(memInfo)
            , FetchFunc_(fetch)
            , Ctx_(ctx)
            , State_(std::move(init))
            , InputStream_(std::move(input))
        {
        }

    protected:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            return FetchFunc_(Ctx_, Stream_, *State_, InputStream_, result);
        }

        const TFetchPtr FetchFunc_;
        TComputationContext* const Ctx_;
        const TStatePtr State_;
        const NUdf::TUnboxedValue InputStream_;
        NUdf::TUnboxedValue Stream_;
    };
#endif
public:
    TChopperWrapper(TComputationMutables& mutables, IComputationNode* stream, IComputationExternalNode* itemArg, IComputationNode* key, IComputationExternalNode* keyArg, IComputationNode* chop, IComputationExternalNode* input, IComputationNode* output)
        : TBaseComputation(mutables)
        , Stream_(stream)
        , ItemArg_(itemArg)
        , Key_(key)
        , KeyArg_(keyArg)
        , Chop_(chop)
        , Input_(input)
        , Output_(output)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto sharedState = std::allocate_shared<EState, TMKQLAllocator<EState>>(TMKQLAllocator<EState>(), EState::Init);
        auto stream = Stream_->GetValue(ctx);
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && InputPtr_) {
            Input_->SetValue(ctx, ctx.HolderFactory.Create<TCodegenInput>(InputPtr_, stream, &ctx, sharedState));
        } else {
#endif
            Input_->SetValue(ctx, ctx.HolderFactory.Create<TSubStream>(sharedState, stream, ItemArg_, Chop_, ctx));
#ifndef MKQL_DISABLE_CODEGEN
        }
#endif

#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && OutputPtr_) {
            return ctx.HolderFactory.Create<TCodegenOutput>(OutputPtr_, &ctx, std::move(sharedState), std::move(stream));
        }
#endif
        return ctx.HolderFactory.Create<TMainStream>(std::move(sharedState), std::move(stream), ItemArg_, Key_, KeyArg_, Chop_, Input_, Output_, ctx);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Stream_);

        Own(ItemArg_);
        DependsOn(Key_);
        Own(KeyArg_);
        DependsOn(Chop_);

        Own(Input_);
        DependsOn(Output_);
    }

#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        InputFunc_ = GenerateInput(codegen);
        OutputFunc_ = GenerateOutput(codegen);
        codegen.ExportSymbol(InputFunc_);
        codegen.ExportSymbol(OutputFunc_);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (InputFunc_) {
            InputPtr_ = reinterpret_cast<TInputPtr>(codegen.GetPointerToFunction(InputFunc_));
        }
        if (OutputFunc_) {
            OutputPtr_ = reinterpret_cast<TOutputPtr>(codegen.GetPointerToFunction(OutputFunc_));
        }
    }

    Function* GenerateInput(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = MakeName("Input");
        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ItemArg_);
        const auto codegenKeyArg = dynamic_cast<ICodegeneratorExternalNode*>(KeyArg_);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenKeyArg, "Key arg must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);
        const auto containerType = static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto stateType = Type::getInt8Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(stateType), PointerType::getUnqual(valueType)}, /*isVarArg=*/false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto stateArg = &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto load = BasicBlock::Create(context, "load", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto returnFinish = BasicBlock::Create(context, "return_finish", ctx.Func);

        auto block = main;

        const auto container = static_cast<Value*>(containerArg);

        const auto first = new LoadInst(stateType, stateArg, "first", block);

        const auto dispatch = SwitchInst::Create(first, work, 2U, block);
        dispatch->addCase(ConstantInt::get(stateType, ui8(EState::Next)), load);
        dispatch->addCase(ConstantInt::get(stateType, ui8(EState::Chop)), returnFinish);

        {
            block = returnFinish;
            ReturnInst::Create(context, ConstantInt::get(statusType, ui32(NUdf::EFetchStatus::Finish)), block);
        }

        {
            block = load;

            new StoreInst(ConstantInt::get(stateType, ui8(EState::Work)), stateArg, block);
            SafeUnRefUnboxedOne(valuePtr, ctx, block);
            GetNodeValue(valuePtr, ItemArg_, ctx, block);
            ReturnInst::Create(context, ConstantInt::get(statusType, ui32(NUdf::EFetchStatus::Ok)), block);
        }

        {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto step = BasicBlock::Create(context, "step", ctx.Func);
            const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
            const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

            block = work;

            const auto [status, itemPtr] = RefValueWithCallResult(codegenItemArg, ctx, block, [&](Value* itemPtr) {
                return CallBoxedValueFetch(container, ctx, block, itemPtr);
            });
            const auto none = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, status, ConstantInt::get(statusType, ui32(NUdf::EFetchStatus::Ok)), "none", block);

            BranchInst::Create(exit, good, none, block);

            block = good;

            const auto chop = GetNodeValue(Chop_, ctx, block);
            const auto cast = CastInst::Create(Instruction::Trunc, chop, Type::getInt1Ty(context), "bool", block);
            BranchInst::Create(step, pass, cast, block);

            block = step;

            new StoreInst(ConstantInt::get(stateType, ui8(EState::Chop)), stateArg, block);
            ReturnInst::Create(context, ConstantInt::get(statusType, ui32(NUdf::EFetchStatus::Finish)), block);

            block = pass;

            SafeUnRefUnboxedOne(valuePtr, ctx, block);
            GetNodeValue(valuePtr, ItemArg_, ctx, block);
            BranchInst::Create(exit, block);

            block = exit;
            ReturnInst::Create(context, status, block);
        }

        return ctx.Func;
    }

    Function* GenerateOutput(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = MakeName("Output");
        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto codegenInput = dynamic_cast<ICodegeneratorExternalNode*>(Input_);
        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ItemArg_);
        const auto codegenKeyArg = dynamic_cast<ICodegeneratorExternalNode*>(KeyArg_);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenKeyArg, "Key arg must be codegenerator node.");
        MKQL_ENSURE(codegenInput, "Input arg must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);
        const auto containerType = static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto stateType = Type::getInt8Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), PointerType::getUnqual(valueType), PointerType::getUnqual(stateType), containerType, PointerType::getUnqual(valueType)}, /*isVarArg=*/false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto streamArg = &*++args;
        const auto stateArg = &*++args;
        const auto inputArg = &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        const auto skip = BasicBlock::Create(context, "skip", ctx.Func);
        const auto pull = BasicBlock::Create(context, "pull", ctx.Func);
        const auto init = BasicBlock::Create(context, "init", ctx.Func);

        auto block = main;

        const auto input = static_cast<Value*>(inputArg);

        BranchInst::Create(loop, block);

        block = loop;

        const auto stream = new LoadInst(valueType, streamArg, "stream", block);
        BranchInst::Create(next, work, IsEmpty(stream, block, context), block);

        {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto step = BasicBlock::Create(context, "step", ctx.Func);

            block = work;

            const auto status = CallBoxedValueFetch(stream, ctx, block, valuePtr);
            const auto icmp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, status, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Finish)), "cond", block);

            BranchInst::Create(good, step, icmp, block);

            block = good;

            ReturnInst::Create(context, status, block);

            block = step;

            UnRefBoxed(stream, ctx, block);
            new StoreInst(ConstantInt::get(stream->getType(), 0), streamArg, block);
            codegenInput->CreateInvalidate(ctx, block);
            BranchInst::Create(next, block);
        }

        block = next;

        const auto state = new LoadInst(stateType, stateArg, "state", block);
        const auto choise = SwitchInst::Create(state, skip, 2U, block);
        choise->addCase(ConstantInt::get(stateType, ui8(EState::Init)), init);
        choise->addCase(ConstantInt::get(stateType, ui8(EState::Chop)), pass);

        {
            const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

            block = init;

            const auto [status, itemPtr] = RefValueWithCallResult(codegenItemArg, ctx, block, [&](Value* itemPtr) {
                return CallBoxedValueFetch(input, ctx, block, itemPtr);
            });
            const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, status, ConstantInt::get(statusType, ui32(NUdf::EFetchStatus::Ok)), "special", block);

            BranchInst::Create(exit, pass, special, block);

            block = exit;

            ReturnInst::Create(context, status, block);
        }

        {
            const auto test = BasicBlock::Create(context, "test", ctx.Func);
            const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);

            block = skip;

            const auto [status, itemPtr] = RefValueWithCallResult(codegenItemArg, ctx, block, [&](Value* itemPtr) {
                return CallBoxedValueFetch(input, ctx, block, itemPtr);
            });
            const auto way = SwitchInst::Create(status, test, 2U, block);
            way->addCase(ConstantInt::get(statusType, ui32(NUdf::EFetchStatus::Yield)), exit);
            way->addCase(ConstantInt::get(statusType, ui32(NUdf::EFetchStatus::Finish)), done);

            block = exit;

            new StoreInst(ConstantInt::get(stateType, ui8(EState::Skip)), stateArg, block);
            BranchInst::Create(done, block);

            block = done;
            ReturnInst::Create(context, status, block);

            block = test;

            const auto chop = GetNodeValue(Chop_, ctx, block);
            const auto cast = CastInst::Create(Instruction::Trunc, chop, Type::getInt1Ty(context), "bool", block);
            BranchInst::Create(pass, skip, cast, block);
        }

        block = pass;

        new StoreInst(ConstantInt::get(stateType, ui8(EState::Next)), stateArg, block);
        const auto key = GetNodeValue(Key_, ctx, block);
        codegenKeyArg->CreateSetValue(ctx, block, key);
        BranchInst::Create(pull, block);

        block = pull;

        GetNodeValue(streamArg, Output_, ctx, block);
        BranchInst::Create(loop, block);

        return ctx.Func;
    }

    using TInputPtr = typename TCodegenInput::TFetchPtr;
    using TOutputPtr = typename TCodegenOutput::TFetchPtr;

    Function* InputFunc_ = nullptr;
    Function* OutputFunc_ = nullptr;

    TInputPtr InputPtr_ = nullptr;
    TOutputPtr OutputPtr_ = nullptr;
#endif
    IComputationNode* const Stream_;

    IComputationExternalNode* const ItemArg_;
    IComputationNode* const Key_;
    IComputationExternalNode* const KeyArg_;
    IComputationNode* const Chop_;

    IComputationExternalNode* const Input_;
    IComputationNode* const Output_;
};

} // namespace

IComputationNode* WrapChopper(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 7U, "Expected seven args.");
    const auto type = callable.GetType()->GetReturnType();

    const auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto keyResult = LocateNode(ctx.NodeLocator, callable, 2);
    const auto switchResult = LocateNode(ctx.NodeLocator, callable, 4);
    const auto output = LocateNode(ctx.NodeLocator, callable, 6);

    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1);
    const auto keyArg = LocateExternalNode(ctx.NodeLocator, callable, 3);
    const auto input = LocateExternalNode(ctx.NodeLocator, callable, 5);

    if (type->IsFlow()) {
        const auto kind = GetValueRepresentation(AS_TYPE(TFlowType, type)->GetItemType());
        return new TChopperFlowWrapper(ctx.Mutables, kind, stream, itemArg, keyResult, keyArg, switchResult, input, output);
    } else if (type->IsStream()) {
        return new TChopperWrapper(ctx.Mutables, stream, itemArg, keyResult, keyArg, switchResult, input, output);
    }

    THROW yexception() << "Expected flow or stream.";
}

} // namespace NKikimr::NMiniKQL
