#include "mkql_chopper.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TChopperFlowWrapper : public TStatefulFlowCodegeneratorNode<TChopperFlowWrapper> {
    typedef TStatefulFlowCodegeneratorNode<TChopperFlowWrapper> TBaseComputation;
public:
    enum class EState : ui64 {
        Work,
        Chop,
        Next,
        Skip
    };

    TChopperFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow,  IComputationExternalNode* itemArg, IComputationNode* key, IComputationExternalNode* keyArg, IComputationNode* chop, IComputationExternalNode* input, IComputationNode* output)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Any)
        , Flow(flow)
        , ItemArg(itemArg)
        , Key(key)
        , KeyArg(keyArg)
        , Chop(chop)
        , Input(input)
        , Output(output)
    {
        Input->SetGetter(std::bind(&TChopperFlowWrapper::Getter, this, std::bind(&TChopperFlowWrapper::RefState, this, std::placeholders::_1), std::placeholders::_1));

#ifndef MKQL_DISABLE_CODEGEN
        const auto codegenInput = dynamic_cast<ICodegeneratorExternalNode*>(Input);
        MKQL_ENSURE(codegenInput, "Input arg must be codegenerator node.");
        codegenInput->SetValueGetterBuilder([this](const TCodegenContext& ctx) {
            return GenerateHandler(ctx.Codegen);
        });
#endif
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            if (auto item = Flow->GetValue(ctx); item.IsSpecial()) {
                return item.Release();
            } else {
                state = NUdf::TUnboxedValuePod(ui64(EState::Next));
                ItemArg->SetValue(ctx, std::move(item));
                KeyArg->SetValue(ctx, Key->GetValue(ctx));
            }
        } else if (EState::Skip == EState(state.Get<ui64>())) {
            do {
                if (auto next = Flow->GetValue(ctx); next.IsSpecial())
                    return next.Release();
                else
                    ItemArg->SetValue(ctx, std::move(next));
            } while (!Chop->GetValue(ctx).Get<bool>());

            KeyArg->SetValue(ctx, Key->GetValue(ctx));
            state = NUdf::TUnboxedValuePod(ui64(EState::Next));
        }

        while (true) {
            auto output = Output->GetValue(ctx);
            if (output.IsFinish()) {
                Input->InvalidateValue(ctx);
                switch (EState(state.Get<ui64>())) {
                    case EState::Work:
                    case EState::Next:
                        do {
                            if (auto next = Flow->GetValue(ctx); next.IsSpecial()) {
                                if (next.IsYield()) {
                                    state = NUdf::TUnboxedValuePod(ui64(EState::Skip));
                                }
                                return next.Release();
                            } else {
                                ItemArg->SetValue(ctx, std::move(next));
                            }
                        } while (!Chop->GetValue(ctx).Get<bool>());
                    case EState::Chop:
                        KeyArg->SetValue(ctx, Key->GetValue(ctx));
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
            return ItemArg->GetValue(ctx).Release();
        }

        auto item = Flow->GetValue(ctx);
        if (!item.IsSpecial()) {
            ItemArg->SetValue(ctx, NUdf::TUnboxedValue(item));

            if (Chop->GetValue(ctx).Get<bool>()) {
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
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ItemArg);
        const auto codegenKeyArg = dynamic_cast<ICodegeneratorExternalNode*>(KeyArg);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenKeyArg, "Key arg must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);
        const auto funcType = FunctionType::get(valueType, {PointerType::getUnqual(GetCompContextType(context))}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        ctx.Annotator = &annotator;

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
            const auto item = GetNodeValue(ItemArg, ctx, block);
            ReturnInst::Create(context, item, block);
        }

        {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto step = BasicBlock::Create(context, "step", ctx.Func);
            const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

            block = work;

            const auto item = GetNodeValue(Flow, ctx, block);

            BranchInst::Create(exit, good, IsSpecial(item, block), block);

            block = good;

            codegenItemArg->CreateSetValue(ctx, block, item);

            const auto chop = GetNodeValue(Chop, ctx, block);
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
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ItemArg);
        const auto codegenKeyArg = dynamic_cast<ICodegeneratorExternalNode*>(KeyArg);
        const auto codegenInput = dynamic_cast<ICodegeneratorExternalNode*>(Input);

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

            const auto item = GetNodeValue(Flow, ctx, block);
            result->addIncoming(item, block);
            BranchInst::Create(exit, next, IsSpecial(item, block), block);

            block = next;

            new StoreInst(GetConstant(ui64(EState::Next), context), statePtr, block);
            codegenItemArg->CreateSetValue(ctx, block, item);
            const auto key = GetNodeValue(Key, ctx, block);
            codegenKeyArg->CreateSetValue(ctx, block, key);

            BranchInst::Create(loop, block);
        }

        {
            const auto part = BasicBlock::Create(context, "part", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto step = BasicBlock::Create(context, "step", ctx.Func);
            const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

            block = loop;

            const auto item = GetNodeValue(Output, ctx, block);
            const auto state = new LoadInst(valueType, statePtr, "state", block);

            result->addIncoming(item, block);
            BranchInst::Create(part, exit, IsFinish(item, block), block);

            block = part;

            codegenInput->CreateInvalidate(ctx, block);

            result->addIncoming(GetFinish(context), block);

            const auto choise = SwitchInst::Create(state, exit, 3U, block);
            choise->addCase(GetConstant(ui64(EState::Next), context), pass);
            choise->addCase(GetConstant(ui64(EState::Work), context), pass);
            choise->addCase(GetConstant(ui64(EState::Chop), context), step);

            block = pass;

            const auto next = GetNodeValue(Flow, ctx, block);

            result->addIncoming(next, block);

            const auto way = SwitchInst::Create(next, good, 2U, block);
            way->addCase(GetFinish(context), exit);
            way->addCase(GetYield(context), skip);

            block = good;

            codegenItemArg->CreateSetValue(ctx, block, next);

            const auto chop = GetNodeValue(Chop, ctx, block);
            const auto cast = CastInst::Create(Instruction::Trunc, chop, Type::getInt1Ty(context), "bool", block);

            BranchInst::Create(step, pass, cast, block);

            block = step;

            new StoreInst(GetConstant(ui64(EState::Next), context), statePtr, block);
            const auto key = GetNodeValue(Key, ctx, block);
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
        if (const auto flow = FlowDependsOn(Flow)) {
            Own(flow, ItemArg);
            DependsOn(flow, Key);
            Own(flow, KeyArg);
            DependsOn(flow, Chop);

            Own(flow, Input);
            DependsOn(flow, Output);
        }
    }

    IComputationNode *const Flow;

    IComputationExternalNode *const ItemArg;
    IComputationNode *const Key;
    IComputationExternalNode *const KeyArg;
    IComputationNode *const Chop;

    IComputationExternalNode *const Input;
    IComputationNode *const Output;
};

class TChopperWrapper : public TCustomValueCodegeneratorNode<TChopperWrapper> {
    typedef TCustomValueCodegeneratorNode<TChopperWrapper> TBaseComputation;
private:
    enum class EState : ui8 {
        Init,
        Work,
        Chop,
        Next,
        Skip,
    };
    using TStatePtr = std::shared_ptr<EState>;

    class TSubStream : public TComputationValue<TSubStream> {
    public:
        using TBase = TComputationValue<TSubStream>;

        TSubStream(TMemoryUsageInfo* memInfo, const TStatePtr& state, const NUdf::TUnboxedValue& stream, IComputationExternalNode* itemArg, IComputationNode* chop, TComputationContext& ctx)
            : TBase(memInfo), State(state), Stream(stream)
            , ItemArg(itemArg)
            , Chop(chop)
            , Ctx(ctx)
        {}
    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            auto& state = *State;
            if (EState::Next == state) {
                state = EState::Work;
                result = ItemArg->GetValue(Ctx);
                return NUdf::EFetchStatus::Ok;
            }

            while (true) {
                switch (const auto status = Stream.Fetch(result)) {
                    case NUdf::EFetchStatus::Ok: {
                        ItemArg->SetValue(Ctx, NUdf::TUnboxedValue(result));

                        if (Chop->GetValue(Ctx).Get<bool>()) {
                            state = EState::Chop;
                            return NUdf::EFetchStatus::Finish;
                        }

                        return status;
                    }

                    case NUdf::EFetchStatus::Finish:
                    case NUdf::EFetchStatus::Yield:
                        return status;
                }
            }
        }

        const TStatePtr State;
        const NUdf::TUnboxedValue Stream;

        IComputationExternalNode *const ItemArg;
        IComputationNode *const Chop;

        TComputationContext& Ctx;
    };

    class TMainStream : public TComputationValue<TMainStream> {
    public:
        TMainStream(TMemoryUsageInfo* memInfo, TStatePtr&& state, NUdf::TUnboxedValue&& stream, const IComputationExternalNode *itemArg, const IComputationNode *key, const IComputationExternalNode *keyArg, const IComputationNode *chop, const IComputationExternalNode *input, const IComputationNode *output, TComputationContext& ctx)
            : TComputationValue(memInfo), State(std::move(state)), ItemArg(itemArg), Key(key), Chop(chop), KeyArg(keyArg), Input(input), Output(output), InputStream(std::move(stream)), Ctx(ctx)
        {}
    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            while (true) {
                if (Stream) {
                    if (const auto status = Stream.Fetch(result); NUdf::EFetchStatus::Finish != status) {
                        return status;
                    }

                    Stream = NUdf::TUnboxedValuePod();
                    Input->InvalidateValue(Ctx);
                }

                switch (auto& state = *State) {
                    case EState::Init:
                        if (const auto status = InputStream.Fetch(ItemArg->RefValue(Ctx)); NUdf::EFetchStatus::Ok != status) {
                            return status;
                        }
                        state = EState::Next;
                        KeyArg->SetValue(Ctx, Key->GetValue(Ctx));
                        break;
                    case EState::Work:
                    case EState::Next:
                    case EState::Skip:
                        do switch (const auto status = InputStream.Fetch(ItemArg->RefValue(Ctx))) {
                            case NUdf::EFetchStatus::Ok:
                                break;
                            case NUdf::EFetchStatus::Yield:
                                state = EState::Skip;
                            case NUdf::EFetchStatus::Finish:
                                return status;
                        } while (!Chop->GetValue(Ctx).Get<bool>());
                        [[fallthrough]];
                    case EState::Chop:
                        state = EState::Next;
                        KeyArg->SetValue(Ctx, Key->GetValue(Ctx));
                        break;
                }
                Stream = Output->GetValue(Ctx);
            }
        }

        const TStatePtr State;
        const IComputationExternalNode *const ItemArg;
        const IComputationNode* Key;
        const IComputationNode* Chop;
        const IComputationExternalNode* KeyArg;
        const IComputationExternalNode* Input;
        const IComputationNode* Output;
        const NUdf::TUnboxedValue InputStream;
        NUdf::TUnboxedValue Stream;
        TComputationContext& Ctx;
    };
#ifndef MKQL_DISABLE_CODEGEN
    class TCodegenInput : public TComputationValue<TCodegenInput> {
    public:
        using TBase = TComputationValue<TCodegenInput>;

        using TFetchPtr = NUdf::EFetchStatus (*)(TComputationContext*, NUdf::TUnboxedValuePod, EState&, NUdf::TUnboxedValuePod&);

        TCodegenInput(TMemoryUsageInfo* memInfo, TFetchPtr fetch, const NUdf::TUnboxedValue& stream, TComputationContext* ctx, const TStatePtr& init)
            : TBase(memInfo)
            , FetchFunc(fetch)
            , Stream(stream)
            , Ctx(ctx)
            , State(init)
        {}

    protected:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            return FetchFunc(Ctx, static_cast<const NUdf::TUnboxedValuePod&>(Stream), *State, result);
        }

        const TFetchPtr FetchFunc;
        const NUdf::TUnboxedValue Stream;
        TComputationContext* const Ctx;
        const TStatePtr State;
    };

    class TCodegenOutput : public TComputationValue<TCodegenOutput> {
    public:
        using TBase = TComputationValue<TCodegenOutput>;

        using TFetchPtr = NUdf::EFetchStatus (*)(TComputationContext*, NUdf::TUnboxedValuePod&, EState&, NUdf::TUnboxedValuePod, NUdf::TUnboxedValuePod&);

        TCodegenOutput(TMemoryUsageInfo* memInfo, TFetchPtr fetch, TComputationContext* ctx, TStatePtr&& init, NUdf::TUnboxedValue&& input)
            : TBase(memInfo)
            , FetchFunc(fetch)
            , Ctx(ctx)
            , State(std::move(init))
            , InputStream(std::move(input))
        {}

    protected:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            return FetchFunc(Ctx, Stream, *State, InputStream, result);
        }

        const TFetchPtr FetchFunc;
        TComputationContext* const Ctx;
        const TStatePtr State;
        const NUdf::TUnboxedValue InputStream;
        NUdf::TUnboxedValue Stream;
    };
#endif
public:
    TChopperWrapper(TComputationMutables& mutables, IComputationNode* stream,  IComputationExternalNode* itemArg, IComputationNode* key, IComputationExternalNode* keyArg, IComputationNode* chop, IComputationExternalNode* input, IComputationNode* output)
        : TBaseComputation(mutables)
        , Stream(stream)
        , ItemArg(itemArg)
        , Key(key)
        , KeyArg(keyArg)
        , Chop(chop)
        , Input(input)
        , Output(output)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto sharedState = std::allocate_shared<EState, TMKQLAllocator<EState>>(TMKQLAllocator<EState>(), EState::Init);
        auto stream = Stream->GetValue(ctx);
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && InputPtr)
            Input->SetValue(ctx, ctx.HolderFactory.Create<TCodegenInput>(InputPtr, stream, &ctx, sharedState));
        else
#endif
            Input->SetValue(ctx, ctx.HolderFactory.Create<TSubStream>(sharedState, stream, ItemArg, Chop, ctx));

#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && OutputPtr)
            return ctx.HolderFactory.Create<TCodegenOutput>(OutputPtr, &ctx, std::move(sharedState), std::move(stream));
#endif
        return ctx.HolderFactory.Create<TMainStream>(std::move(sharedState), std::move(stream), ItemArg, Key, KeyArg, Chop, Input, Output, ctx);
    }
private:
    void RegisterDependencies() const final {
        DependsOn(Stream);

        Own(ItemArg);
        DependsOn(Key);
        Own(KeyArg);
        DependsOn(Chop);

        Own(Input);
        DependsOn(Output);
    }

#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        InputFunc = GenerateInput(codegen);
        OutputFunc = GenerateOutput(codegen);
        codegen.ExportSymbol(InputFunc);
        codegen.ExportSymbol(OutputFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (InputFunc)
            InputPtr = reinterpret_cast<TInputPtr>(codegen.GetPointerToFunction(InputFunc));
        if (OutputFunc)
            OutputPtr = reinterpret_cast<TOutputPtr>(codegen.GetPointerToFunction(OutputFunc));
    }

    Function* GenerateInput(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = MakeName("Input");
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ItemArg);
        const auto codegenKeyArg = dynamic_cast<ICodegeneratorExternalNode*>(KeyArg);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenKeyArg, "Key arg must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);
        const auto containerType = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ? static_cast<Type*>(PointerType::getUnqual(valueType)) : static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto stateType = Type::getInt8Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(stateType), PointerType::getUnqual(valueType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        ctx.Annotator = &annotator;

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto stateArg = &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto load = BasicBlock::Create(context, "load", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);

        auto block = main;

        const auto container = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
            new LoadInst(valueType, containerArg, "load_container", false, block) : static_cast<Value*>(containerArg);

        const auto first = new LoadInst(stateType, stateArg, "first", block);
        const auto reload = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, first, ConstantInt::get(stateType, ui8(EState::Next)), "reload", block);

        BranchInst::Create(load, work, reload, block);

        {
            block = load;

            new StoreInst(ConstantInt::get(stateType, ui8(EState::Work)), stateArg, block);
            SafeUnRefUnboxed(valuePtr, ctx, block);
            GetNodeValue(valuePtr, ItemArg, ctx, block);
            ReturnInst::Create(context, ConstantInt::get(statusType, ui32(NUdf::EFetchStatus::Ok)), block);
        }

        {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto step = BasicBlock::Create(context, "step", ctx.Func);
            const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
            const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

            block = work;

            const auto itemPtr = codegenItemArg->CreateRefValue(ctx, block);
            const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, container, codegen, block, itemPtr);
            const auto none = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, status, ConstantInt::get(statusType, ui32(NUdf::EFetchStatus::Ok)), "none", block);

            BranchInst::Create(exit, good, none, block);

            block = good;

            const auto chop = GetNodeValue(Chop, ctx, block);
            const auto cast = CastInst::Create(Instruction::Trunc, chop, Type::getInt1Ty(context), "bool", block);
            BranchInst::Create(step, pass, cast, block);

            block = step;

            new StoreInst(ConstantInt::get(stateType, ui8(EState::Chop)), stateArg, block);
            ReturnInst::Create(context, ConstantInt::get(statusType, ui32(NUdf::EFetchStatus::Finish)), block);

            block = pass;

            SafeUnRefUnboxed(valuePtr, ctx, block);
            GetNodeValue(valuePtr, ItemArg, ctx, block);
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
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto codegenInput = dynamic_cast<ICodegeneratorExternalNode*>(Input);
        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(ItemArg);
        const auto codegenKeyArg = dynamic_cast<ICodegeneratorExternalNode*>(KeyArg);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenKeyArg, "Key arg must be codegenerator node.");
        MKQL_ENSURE(codegenInput, "Input arg must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);
        const auto containerType = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ? static_cast<Type*>(PointerType::getUnqual(valueType)) : static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto stateType = Type::getInt8Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), PointerType::getUnqual(valueType), PointerType::getUnqual(stateType), containerType, PointerType::getUnqual(valueType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        ctx.Annotator = &annotator;

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

        const auto input = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
            new LoadInst(valueType, inputArg, "load_input", false, block) : static_cast<Value*>(inputArg);

        BranchInst::Create(loop, block);

        block = loop;

        const auto stream = new LoadInst(valueType, streamArg, "stream", block);
        BranchInst::Create(next, work, IsEmpty(stream, block), block);

        {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto step = BasicBlock::Create(context, "step", ctx.Func);

            block = work;

            const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, stream, codegen, block, valuePtr);
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

            const auto itemPtr = codegenItemArg->CreateRefValue(ctx, block);
            const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, input, codegen, block, itemPtr);
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

            const auto itemPtr = codegenItemArg->CreateRefValue(ctx, block);
            const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, input, codegen, block, itemPtr);

            const auto way = SwitchInst::Create(status, test, 2U, block);
            way->addCase(ConstantInt::get(statusType, ui32(NUdf::EFetchStatus::Yield)), exit);
            way->addCase(ConstantInt::get(statusType, ui32(NUdf::EFetchStatus::Finish)), done);

            block = exit;

            new StoreInst(ConstantInt::get(stateType, ui8(EState::Skip)), stateArg, block);
            BranchInst::Create(done, block);

            block = done;
            ReturnInst::Create(context, status, block);

            block = test;

            const auto chop = GetNodeValue(Chop, ctx, block);
            const auto cast = CastInst::Create(Instruction::Trunc, chop, Type::getInt1Ty(context), "bool", block);
            BranchInst::Create(pass, skip, cast, block);
        }

        block = pass;

        new StoreInst(ConstantInt::get(stateType, ui8(EState::Next)), stateArg, block);
        const auto key = GetNodeValue(Key, ctx, block);
        codegenKeyArg->CreateSetValue(ctx, block, key);
        BranchInst::Create(pull, block);

        block = pull;

        GetNodeValue(streamArg, Output, ctx, block);
        BranchInst::Create(loop, block);

        return ctx.Func;
    }

    using TInputPtr = typename TCodegenInput::TFetchPtr;
    using TOutputPtr = typename TCodegenOutput::TFetchPtr;

    Function* InputFunc = nullptr;
    Function* OutputFunc = nullptr;

    TInputPtr InputPtr = nullptr;
    TOutputPtr OutputPtr = nullptr;
#endif
    IComputationNode *const Stream;

    IComputationExternalNode *const ItemArg;
    IComputationNode *const Key;
    IComputationExternalNode *const KeyArg;
    IComputationNode *const Chop;

    IComputationExternalNode *const Input;
    IComputationNode *const Output;
};

}

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

}
}
