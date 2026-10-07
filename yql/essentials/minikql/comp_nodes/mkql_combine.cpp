#include "mkql_combine.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_llvm_base.h>                // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_stats_registry.h>
#include <yql/essentials/minikql/defs.h>

namespace NKikimr::NMiniKQL {

TStatKey Combine_FlushesCount("Combine_FlushesCount", /*deriv=*/true);
TStatKey Combine_MaxRowsCount("Combine_MaxRowsCount", /*deriv=*/false);

namespace {

using TEqualsPtr = bool (*)(NUdf::TUnboxedValuePod, NUdf::TUnboxedValuePod);
using THashPtr = NYql::NUdf::THashType (*)(NUdf::TUnboxedValuePod);

using TEqualsFunc = std::function<bool(NUdf::TUnboxedValuePod, NUdf::TUnboxedValuePod)>;
using THashFunc = std::function<NYql::NUdf::THashType(NUdf::TUnboxedValuePod)>;

using TDependsOn = std::function<void(IComputationNode*)>;
using TOwn = std::function<void(IComputationExternalNode*)>;

struct TCombineCoreNodes {
    IComputationExternalNode* ItemNode;
    IComputationExternalNode* KeyNode;
    IComputationExternalNode* StateNode;

    IComputationNode* KeyResultNode;
    IComputationNode* InitResultNode;
    IComputationNode* UpdateResultNode;
    IComputationNode* FinishResultNode;

    NUdf::TUnboxedValuePod ExtractKey(TComputationContext& compCtx, NUdf::TUnboxedValue value) const {
        ItemNode->SetValue(compCtx, std::move(value));
        auto key = KeyResultNode->GetValue(compCtx);
        const auto result = static_cast<const NUdf::TUnboxedValuePod&>(key);
        KeyNode->SetValue(compCtx, std::move(key));
        return result;
    }

    void ProcessItem(TComputationContext& compCtx, NUdf::TUnboxedValuePod& state) const {
        if (auto& st = static_cast<NUdf::TUnboxedValue&>(state); state.IsInvalid()) {
            st = InitResultNode->GetValue(compCtx);
        } else {
            StateNode->SetValue(compCtx, std::move(st));
            st = UpdateResultNode->GetValue(compCtx);
        }
    }

    NUdf::TUnboxedValuePod FinishItem(TComputationContext& compCtx, NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& state) const {
        KeyNode->SetValue(compCtx, std::move(key));
        StateNode->SetValue(compCtx, std::move(state));
        return FinishResultNode->GetValue(compCtx).Release();
    }

    void RegisterDependencies(const TDependsOn& dependsOn, const TOwn& own) const {
        own(ItemNode);
        own(KeyNode);
        own(StateNode);

        dependsOn(KeyResultNode);
        dependsOn(InitResultNode);
        dependsOn(UpdateResultNode);
        dependsOn(FinishResultNode);
    }
};

class TState: public TComputationValue<TState> {
    using TBase = TComputationValue<TState>;
    using TStateMap = std::unordered_map<
        NUdf::TUnboxedValuePod, NUdf::TUnboxedValuePod,
        THashFunc, TEqualsFunc,
        TMKQLAllocator<std::pair<const NUdf::TUnboxedValuePod, NUdf::TUnboxedValuePod>>>;

public:
    TState(TMemoryUsageInfo* memInfo, const THashFunc& hash, const TEqualsFunc& equal)
        : TBase(memInfo)
        , States_(0, hash, equal)
    {
        States_.max_load_factor(1.2F);
    }

    NUdf::TUnboxedValuePod& At(const NUdf::TUnboxedValuePod key) {
        const auto ins = States_.emplace(key, NUdf::TUnboxedValuePod::Invalid());
        if (ins.second) {
            key.Ref();
        }
        return ins.first->second;
    }

    bool IsEmpty() const {
        if (!States_.empty()) {
            return false;
        }

        CleanupCurrentContext();
        return true;
    }

    void PushStat(IStatsRegistry* stats) const {
        if (!States_.empty()) {
            MKQL_SET_MAX_STAT(stats, Combine_MaxRowsCount, static_cast<i64>(States_.size()));
            MKQL_INC_STAT(stats, Combine_FlushesCount);
        }
    }

    bool Extract(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& state) {
        if (States_.empty()) {
            return false;
        }

        const auto& node = States_.extract(States_.cbegin());
        static_cast<NUdf::TUnboxedValuePod&>(key) = node.key();
        static_cast<NUdf::TUnboxedValuePod&>(state) = node.mapped();
        return true;
    }

    NUdf::EFetchStatus InputStatus = NUdf::EFetchStatus::Ok;

private:
    TStateMap States_;
};

#ifndef MKQL_DISABLE_CODEGEN
class TLLVMFieldsStructureState: public TLLVMFieldsStructure<TComputationValue<TState>> {
private:
    using TBase = TLLVMFieldsStructure<TComputationValue<TState>>;
    llvm::PointerType* StructPtrType_;
    llvm::IntegerType* StatusType_;

protected:
    using TBase::GetContext;

public:
    std::vector<llvm::Type*> GetFieldsArray() {
        std::vector<llvm::Type*> result = TBase::GetFields();
        result.emplace_back(StatusType_);    // status
        result.emplace_back(StructPtrType_); // map

        return result;
    }

    llvm::Constant* GetStatus() {
        return ConstantInt::get(Type::getInt32Ty(GetContext()), TBase::GetFieldsCount() + 0);
    }

    llvm::Constant* GetMap() {
        return ConstantInt::get(Type::getInt32Ty(GetContext()), TBase::GetFieldsCount() + 1);
    }

    explicit TLLVMFieldsStructureState(llvm::LLVMContext& context)
        : TBase(context)
        , StructPtrType_(PointerType::getUnqual(StructType::get(context)))
        , StatusType_(Type::getInt32Ty(context))
    {
    }
};
#endif

template <typename TDerived, bool IsPairState>
class TCombineFlowCodegeneratorBase;

template <typename TDerived>
class TCombineFlowCodegeneratorBase<TDerived, false>
    : public TStatefulFlowCodegeneratorNode<TDerived> {
    using TBase = TStatefulFlowCodegeneratorNode<TDerived>;

protected:
    TCombineFlowCodegeneratorBase(TComputationMutables& mutables, IComputationNode* flow, EValueRepresentation kind)
        : TBase(mutables, flow, kind, EValueRepresentation::Any)
    {
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        return static_cast<const TDerived*>(this)->GenerateGetValue(ctx, statePtr, block);
    }
#endif
};

template <typename TDerived>
class TCombineFlowCodegeneratorBase<TDerived, true>
    : public TPairStateFlowCodegeneratorNode<TDerived> {
    using TBase = TPairStateFlowCodegeneratorNode<TDerived>;

protected:
    TCombineFlowCodegeneratorBase(TComputationMutables& mutables, IComputationNode* flow, EValueRepresentation kind)
        : TBase(mutables, flow, kind, EValueRepresentation::Any)
    {
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, Value* currentPtr, BasicBlock*& block) const override {
        return static_cast<const TDerived*>(this)->GenerateGetValue(ctx, statePtr, currentPtr, block);
    }
#endif
};

template <bool IsMultiRowState, bool StateContainerOpt, bool TrackRss>
class TCombineCoreFlowWrapper: public TCombineFlowCodegeneratorBase<TCombineCoreFlowWrapper<IsMultiRowState, StateContainerOpt, TrackRss>, IsMultiRowState>
#ifndef MKQL_DISABLE_CODEGEN
    ,
                               public ICodegeneratorRootNode
#endif
{
    using TBaseComputation = TCombineFlowCodegeneratorBase<TCombineCoreFlowWrapper<IsMultiRowState, StateContainerOpt, TrackRss>, IsMultiRowState>;

public:
    TCombineCoreFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow, const TCombineCoreNodes& nodes, TKeyTypes&& keyTypes, bool isTuple, ui64 memLimit)
        : TBaseComputation(mutables, flow, kind)
        , Flow_(flow)
        , Nodes_(nodes)
        , KeyTypes_(std::move(keyTypes))
        , IsTuple_(isTuple)
        , MemLimit_(memLimit)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);
        }

        while (const auto ptr = static_cast<TState*>(state.AsBoxed().Get())) {
            if (ptr->IsEmpty()) {
                switch (ptr->InputStatus) {
                    case NUdf::EFetchStatus::Ok:
                        break;
                    case NUdf::EFetchStatus::Finish:
                        return NUdf::TUnboxedValuePod::MakeFinish();
                    case NUdf::EFetchStatus::Yield:
                        ptr->InputStatus = NUdf::EFetchStatus::Ok;
                        return NUdf::TUnboxedValuePod::MakeYield();
                }

                const auto initUsage = MemLimit_ ? ctx.HolderFactory.GetMemoryUsed() : 0ULL;

                do {
                    auto item = Flow_->GetValue(ctx);
                    if (item.IsYield()) {
                        ptr->InputStatus = NUdf::EFetchStatus::Yield;
                        break;
                    } else if (item.IsFinish()) {
                        ptr->InputStatus = NUdf::EFetchStatus::Finish;
                        break;
                    }
                    const auto key = Nodes_.ExtractKey(ctx, item);
                    Nodes_.ProcessItem(ctx, ptr->At(key));
                } while (!ctx.template CheckAdjustedMemLimit<TrackRss>(MemLimit_, initUsage));

                ptr->PushStat(ctx.Stats);
            }

            if (NUdf::TUnboxedValue key, state; ptr->Extract(key, state)) {
                if (const auto out = Nodes_.FinishItem(ctx, key, state)) {
                    return out.template GetOptionalValueIf < !IsMultiRowState && StateContainerOpt > ();
                }
            }
        }
        MKQL_ENSURE(false, "Unreachable");
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, NUdf::TUnboxedValue& current, TComputationContext& ctx) const {
        while (true) {
            if (current.HasValue()) {
                if constexpr (StateContainerOpt) {
                    NUdf::TUnboxedValue result;
                    switch (current.Fetch(result)) {
                        case NUdf::EFetchStatus::Ok:
                            return result.Release();
                        case NUdf::EFetchStatus::Yield:
                            return NUdf::TUnboxedValuePod::MakeYield();
                        case NUdf::EFetchStatus::Finish:
                            break;
                    }
                } else if (NUdf::TUnboxedValue result; current.Next(result)) {
                    return result.Release();
                }
                current.Clear();
            }

            if (NUdf::TUnboxedValue output = DoCalculate(state, ctx); output.IsSpecial()) {
                return output.Release();
            } else {
                current = StateContainerOpt ? std::move(output) : output.GetListIterator();
            }
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* GenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(Nodes_.ItemNode);
        const auto codegenKeyArg = dynamic_cast<ICodegeneratorExternalNode*>(Nodes_.KeyNode);
        const auto codegenStateArg = dynamic_cast<ICodegeneratorExternalNode*>(Nodes_.StateNode);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenKeyArg, "Key arg must be codegenerator node.");
        MKQL_ENSURE(codegenStateArg, "State arg must be codegenerator node.");

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto statusType = Type::getInt32Ty(context);

        TLLVMFieldsStructureState fieldsStruct(context);
        const auto stateType = StructType::get(context, fieldsStruct.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto onePtr = new AllocaInst(valueType, 0U, "one_ptr", &ctx.Func->getEntryBlock().back());
        const auto twoPtr = new AllocaInst(valueType, 0U, "two_ptr", &ctx.Func->getEntryBlock().back());
        new StoreInst(ConstantInt::get(valueType, 0), onePtr, block);
        new StoreInst(ConstantInt::get(valueType, 0), twoPtr, block);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);

        BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TCombineCoreFlowWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        BranchInst::Create(more, block);

        block = more;

        const auto over = BasicBlock::Create(context, "over", ctx.Func);
        const auto result = PHINode::Create(valueType, 3U, "result", over);

        const auto empty = EmitFunctionCall<&TState::IsEmpty>(Type::getInt1Ty(context), {stateArg}, ctx, block);

        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto full = BasicBlock::Create(context, "full", ctx.Func);

        BranchInst::Create(next, full, empty, block);

        {
            block = next;

            const auto rest = BasicBlock::Create(context, "rest", ctx.Func);
            const auto pull = BasicBlock::Create(context, "pull", ctx.Func);
            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);

            const auto statusPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {fieldsStruct.This(), fieldsStruct.GetStatus()}, "last", block);

            const auto last = new LoadInst(statusType, statusPtr, "last", block);

            result->addIncoming(GetFinish(context), block);
            const auto choise = SwitchInst::Create(last, pull, 2U, block);
            choise->addCase(ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), rest);
            choise->addCase(ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), over);

            block = rest;
            new StoreInst(ConstantInt::get(last->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), statusPtr, block);
            result->addIncoming(GetYield(context), block);
            BranchInst::Create(over, block);

            block = pull;

            const auto used = GetMemoryUsed(MemLimit_, ctx, block);

            BranchInst::Create(loop, block);

            block = loop;

            const auto item = GetNodeValue(Flow_, ctx, block);

            const auto finsh = IsFinish(item, block, context);
            const auto yield = IsYield(item, block, context);
            const auto special = BinaryOperator::CreateOr(finsh, yield, "special", block);

            const auto fin = SelectInst::Create(finsh, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), "fin", block);
            const auto save = SelectInst::Create(yield, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), fin, "save", block);
            new StoreInst(save, statusPtr, block);

            BranchInst::Create(done, good, special, block);

            block = good;

            codegenItemArg->CreateSetValue(ctx, block, item);
            const auto key = GetNodeValue(Nodes_.KeyResultNode, ctx, block);
            codegenKeyArg->CreateSetValue(ctx, block, key);

            const auto place = EmitFunctionCall<&TState::At>(ptrValueType, {stateArg, key}, ctx, block);

            const auto init = BasicBlock::Create(context, "init", ctx.Func);
            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            const auto test = BasicBlock::Create(context, "test", ctx.Func);

            BranchInst::Create(init, next, IsInvalid(place, block, context), block);

            block = init;
            GetNodeValue(place, Nodes_.InitResultNode, ctx, block);
            BranchInst::Create(test, block);

            block = next;
            codegenStateArg->CreateSetValue(ctx, block, place);
            GetNodeValue(place, Nodes_.UpdateResultNode, ctx, block);
            BranchInst::Create(test, block);

            block = test;

            const auto check = CheckAdjustedMemLimit<TrackRss>(MemLimit_, used, ctx, block);
            BranchInst::Create(done, loop, check, block);

            block = done;

            EmitFunctionCall<&TState::PushStat>(Type::getVoidTy(context), {stateArg, ctx.GetStat()}, ctx, block);

            BranchInst::Create(full, block);
        }

        {
            block = full;

            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            const auto has = EmitFunctionCall<&TState::Extract>(Type::getInt1Ty(context), {stateArg, onePtr, twoPtr}, ctx, block);

            BranchInst::Create(good, more, has, block);

            block = good;

            codegenKeyArg->CreateSetValue(ctx, block, onePtr);
            codegenStateArg->CreateSetValue(ctx, block, twoPtr);

            const auto value = GetNodeValue(Nodes_.FinishResultNode, ctx, block);
            if constexpr (IsMultiRowState) {
                result->addIncoming(value, block);
                BranchInst::Create(over, block);
            } else if constexpr (StateContainerOpt) {
                const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

                BranchInst::Create(more, exit, IsEmpty(value, block, context), block);
                block = exit;

                const auto strip = GetOptionalValue(context, value, block);
                result->addIncoming(strip, block);
                BranchInst::Create(over, block);
            } else {
                result->addIncoming(value, block);
                BranchInst::Create(more, over, IsEmpty(value, block, context), block);
            }
        }

        block = over;
        return result;
    }

    Value* GenerateGetValue(const TCodegenContext& ctx, Value* statePtr, Value* currentPtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto statusType = Type::getInt32Ty(context);
        const auto valueType = Type::getInt128Ty(context);
        const auto valuePtr = new AllocaInst(valueType, 0U, "value_ptr", &ctx.Func->getEntryBlock().back());
        new StoreInst(ConstantInt::get(valueType, 0), valuePtr, block);

        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto pull = BasicBlock::Create(context, "pull", ctx.Func);
        const auto skip = BasicBlock::Create(context, "skip", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        const auto result = PHINode::Create(valueType, StateContainerOpt ? 3U : 2U, "result", over);

        BranchInst::Create(more, block);

        block = more;

        const auto current = new LoadInst(valueType, currentPtr, "current", block);
        BranchInst::Create(pull, skip, HasValue(current, block, context), block);

        {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            block = pull;

            if constexpr (StateContainerOpt) {
                const auto status = CallBoxedValueFetch(current, ctx, block, valuePtr);

                result->addIncoming(GetYield(context), block);
                const auto choise = SwitchInst::Create(status, good, 2U, block);
                choise->addCase(ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), over);
                choise->addCase(ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), next);
            } else {
                const auto status = CallBoxedValueNext(current, ctx, block, valuePtr);
                BranchInst::Create(good, next, status, block);
            }

            block = good;
            const auto value = new LoadInst(valueType, valuePtr, "value", block);
            ValueRelease(static_cast<const IComputationNode*>(this)->GetRepresentation(), value, ctx, block);
            result->addIncoming(value, block);
            BranchInst::Create(over, block);

            block = next;
            UnRefBoxed(current, ctx, block);
            new StoreInst(ConstantInt::get(current->getType(), 0), currentPtr, block);
            BranchInst::Create(skip, block);
        }

        {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            block = skip;

            const auto list = GenerateGetValue(ctx, statePtr, block);
            result->addIncoming(list, block);
            BranchInst::Create(over, good, IsSpecial(list, block, context), block);

            block = good;
            if constexpr (StateContainerOpt) {
                new StoreInst(list, currentPtr, block);
                AddRefBoxed(list, ctx, block);
            } else {
                CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(currentPtr, list, ctx.Codegen, block);
                CleanupBoxed(list, ctx, block);
            }
            BranchInst::Create(more, block);
        }

        block = over;
        return result;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
#ifdef MKQL_DISABLE_CODEGEN
        state = ctx.HolderFactory.Create<TState>(TValueHasher(KeyTypes_, IsTuple_, /*hash=*/nullptr), TValueEqual(KeyTypes_, IsTuple_, /*equate=*/nullptr));
#else
        state = ctx.HolderFactory.Create<TState>(
            ctx.ExecuteLLVM && Hash_ ? THashFunc(std::ptr_fun(Hash_)) : THashFunc(TValueHasher(KeyTypes_, IsTuple_, /*hash=*/nullptr)),
            ctx.ExecuteLLVM && Equals_ ? TEqualsFunc(std::ptr_fun(Equals_)) : TEqualsFunc(TValueEqual(KeyTypes_, IsTuple_, /*equate=*/nullptr)));
#endif
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            Nodes_.RegisterDependencies(
                [this, flow](IComputationNode* node) { this->DependsOn(flow, node); },
                [this, flow](IComputationExternalNode* node) { this->Own(flow, node); });
        }
    }

    IComputationNode* const Flow_;
    const TCombineCoreNodes Nodes_;
    const TKeyTypes KeyTypes_;
    const bool IsTuple_;
    const ui64 MemLimit_;
#ifndef MKQL_DISABLE_CODEGEN
    TEqualsPtr Equals_ = nullptr;
    THashPtr Hash_ = nullptr;

    Function* EqualsFunc_ = nullptr;
    Function* HashFunc_ = nullptr;

    template <bool EqualsOrHash>
    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::" << (EqualsOrHash ? "Equals" : "Hash") << "_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (EqualsFunc_) {
            Equals_ = reinterpret_cast<TEqualsPtr>(codegen.GetPointerToFunction(EqualsFunc_));
        }
        if (HashFunc_) {
            Hash_ = reinterpret_cast<THashPtr>(codegen.GetPointerToFunction(HashFunc_));
        }
    }

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        codegen.ExportSymbol(HashFunc_ = GenerateHashFunction(codegen, MakeName<false>(), IsTuple_, KeyTypes_));
        codegen.ExportSymbol(EqualsFunc_ = GenerateEqualsFunction(codegen, MakeName<true>(), IsTuple_, KeyTypes_));
    }
#endif
};

template <bool IsMultiRowState, bool StateContainerOpt, bool TrackRss>
class TCombineCoreWrapper: public TCustomValueCodegeneratorNode<TCombineCoreWrapper<IsMultiRowState, StateContainerOpt, TrackRss>> {
    using TBaseComputation = TCustomValueCodegeneratorNode<TCombineCoreWrapper<IsMultiRowState, StateContainerOpt, TrackRss>>;
#ifndef MKQL_DISABLE_CODEGEN
    using TCodegenValue = std::conditional_t<IsMultiRowState, TStreamCodegenSelfStatePlusValue<TState>, TStreamCodegenSelfStateValue<TState>>;
#endif
public:
    class TStreamValue: public TState {
    public:
        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream, const TCombineCoreNodes& nodes, ui64 memLimit, TComputationContext& compCtx, const THashFunc& hash, const TEqualsFunc& equal)
            : TState(memInfo, hash, equal)
            , Stream_(std::move(stream))
            , Nodes_(nodes)
            , MemLimit_(memLimit)
            , CompCtx_(compCtx)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            for (;;) {
                if (IsMultiRowState && Iterator_) {
                    if constexpr (StateContainerOpt) {
                        const auto status = Iterator_.Fetch(result);
                        if (status != NUdf::EFetchStatus::Finish) {
                            return status;
                        }

                        Iterator_.Clear();
                    } else if (Iterator_.Next(result)) {
                        return NUdf::EFetchStatus::Ok;
                    }
                    Iterator_.Clear();
                }

                if (IsEmpty()) {
                    switch (InputStatus) {
                        case NUdf::EFetchStatus::Ok:
                            break;
                        case NUdf::EFetchStatus::Finish:
                            return NUdf::EFetchStatus::Finish;
                        case NUdf::EFetchStatus::Yield:
                            InputStatus = NUdf::EFetchStatus::Ok;
                            return NUdf::EFetchStatus::Yield;
                    }

                    const auto initUsage = MemLimit_ ? CompCtx_.HolderFactory.GetMemoryUsed() : 0ULL;

                    do {
                        NUdf::TUnboxedValue item;
                        InputStatus = Stream_.Fetch(item);
                        if (NUdf::EFetchStatus::Ok != InputStatus) {
                            break;
                        }

                        const auto key = Nodes_.ExtractKey(CompCtx_, item);
                        Nodes_.ProcessItem(CompCtx_, At(key));
                    } while (!CompCtx_.template CheckAdjustedMemLimit<TrackRss>(MemLimit_, initUsage));

                    PushStat(CompCtx_.Stats);
                }

                if (NUdf::TUnboxedValue key, state; Extract(key, state)) {
                    NUdf::TUnboxedValue finishItem = Nodes_.FinishItem(CompCtx_, key, state);

                    if constexpr (IsMultiRowState) {
                        Iterator_ = StateContainerOpt ? std::move(finishItem) : finishItem.GetListIterator();
                    } else {
                        result = finishItem.Release().GetOptionalValueIf<StateContainerOpt>();
                        return NUdf::EFetchStatus::Ok;
                    }
                }
            }
        }

        const NUdf::TUnboxedValue Stream_;
        NUdf::TUnboxedValue Iterator_;
        const TCombineCoreNodes Nodes_;
        const ui64 MemLimit_;
        TComputationContext& CompCtx_;
    };

    TCombineCoreWrapper(TComputationMutables& mutables, IComputationNode* stream, const TCombineCoreNodes& nodes, TKeyTypes&& keyTypes, bool isTuple, ui64 memLimit)
        : TBaseComputation(mutables)
        , Stream_(stream)
        , Nodes_(nodes)
        , KeyTypes_(std::move(keyTypes))
        , IsTuple_(isTuple)
        , MemLimit_(memLimit)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Combine_) {
            return ctx.HolderFactory.Create<TCodegenValue>(Combine_, &ctx, Stream_->GetValue(ctx),
                                                           ctx.ExecuteLLVM && Hash_ ? THashFunc(std::ptr_fun(Hash_)) : THashFunc(TValueHasher(KeyTypes_, IsTuple_, /*hash=*/nullptr)),
                                                           ctx.ExecuteLLVM && Equals_ ? TEqualsFunc(std::ptr_fun(Equals_)) : TEqualsFunc(TValueEqual(KeyTypes_, IsTuple_, /*equate=*/nullptr)));
        }
#endif
        return ctx.HolderFactory.Create<TStreamValue>(Stream_->GetValue(ctx), Nodes_, MemLimit_, ctx,
                                                      TValueHasher(KeyTypes_, IsTuple_, /*hash=*/nullptr), TValueEqual(KeyTypes_, IsTuple_, /*equate=*/nullptr));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
        Nodes_.RegisterDependencies(
            [this](IComputationNode* node) { this->DependsOn(node); },
            [this](IComputationExternalNode* node) { this->Own(node); });
    }

#ifndef MKQL_DISABLE_CODEGEN
    template <bool EqualsOrHash>
    TString MakeFuncName() const {
        TStringStream out;
        out << this->DebugString() << "::" << (EqualsOrHash ? "Equals" : "Hash") << "_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        codegen.ExportSymbol(CombineFunc_ = GenerateCombine(codegen));
        codegen.ExportSymbol(EqualsFunc_ = GenerateEqualsFunction(codegen, MakeFuncName<true>(), IsTuple_, KeyTypes_));
        codegen.ExportSymbol(HashFunc_ = GenerateHashFunction(codegen, MakeFuncName<false>(), IsTuple_, KeyTypes_));
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (CombineFunc_) {
            Combine_ = reinterpret_cast<TCombinePtr>(codegen.GetPointerToFunction(CombineFunc_));
        }
        if (EqualsFunc_) {
            Equals_ = reinterpret_cast<TEqualsPtr>(codegen.GetPointerToFunction(EqualsFunc_));
        }
        if (HashFunc_) {
            Hash_ = reinterpret_cast<THashPtr>(codegen.GetPointerToFunction(HashFunc_));
        }
    }

    Function* GenerateCombine(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(Nodes_.ItemNode);
        const auto codegenKeyArg = dynamic_cast<ICodegeneratorExternalNode*>(Nodes_.KeyNode);
        const auto codegenStateArg = dynamic_cast<ICodegeneratorExternalNode*>(Nodes_.StateNode);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenKeyArg, "Key arg must be codegenerator node.");
        MKQL_ENSURE(codegenStateArg, "State arg must be codegenerator node.");

        const auto& name = this->MakeName("Fetch");
        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto containerType = static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);

        TLLVMFieldsStructureState fieldsStruct(context);
        const auto stateType = StructType::get(context, fieldsStruct.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);
        const auto funcType = IsMultiRowState ? FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, statePtrType, ptrValueType, ptrValueType}, /*isVarArg=*/false) : FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, statePtrType, ptrValueType}, /*isVarArg=*/false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto stateArg = &*++args;
        const auto currArg = IsMultiRowState ? &*++args : nullptr;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        auto block = main;

        const auto onePtr = new AllocaInst(valueType, 0U, "one_ptr", block);
        const auto twoPtr = new AllocaInst(valueType, 0U, "two_ptr", block);
        new StoreInst(ConstantInt::get(valueType, 0), onePtr, block);
        new StoreInst(ConstantInt::get(valueType, 0), twoPtr, block);

        BranchInst::Create(more, block);

        block = more;

        if constexpr (IsMultiRowState) {
            const auto pull = BasicBlock::Create(context, "pull", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

            const auto current = new LoadInst(valueType, currArg, "current", block);
            BranchInst::Create(skip, pull, IsEmpty(current, block, context), block);

            block = pull;

            const auto status = StateContainerOpt ? CallBoxedValueFetch(current, ctx, block, valuePtr) : CallBoxedValueNext(current, ctx, block, valuePtr);

            const auto icmp = StateContainerOpt ? CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, status, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Finish)), "cond", block) : status;

            BranchInst::Create(good, next, icmp, block);

            block = good;
            ReturnInst::Create(context, StateContainerOpt ? status : ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), block);

            block = next;
            UnRefBoxed(current, ctx, block);
            new StoreInst(ConstantInt::get(current->getType(), 0), currArg, block);
            BranchInst::Create(skip, block);

            block = skip;
        }

        const auto empty = EmitFunctionCall<&TState::IsEmpty>(Type::getInt1Ty(context), {stateArg}, ctx, block);

        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto full = BasicBlock::Create(context, "full", ctx.Func);

        BranchInst::Create(next, full, empty, block);

        {
            block = next;

            const auto rest = BasicBlock::Create(context, "rest", ctx.Func);
            const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
            const auto pull = BasicBlock::Create(context, "pull", ctx.Func);
            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);

            const auto statusPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {fieldsStruct.This(), fieldsStruct.GetStatus()}, "last", block);

            const auto last = new LoadInst(statusType, statusPtr, "last", block);

            const auto choise = SwitchInst::Create(last, pull, 2U, block);
            choise->addCase(ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), rest);
            choise->addCase(ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), exit);

            block = rest;
            new StoreInst(ConstantInt::get(last->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), statusPtr, block);
            BranchInst::Create(exit, block);

            block = exit;
            ReturnInst::Create(context, last, block);

            block = pull;

            const auto used = GetMemoryUsed(MemLimit_, ctx, block);

            const auto stream = static_cast<Value*>(containerArg);

            BranchInst::Create(loop, block);

            block = loop;

            const auto fetch = CallBoxedValueFetch(stream, ctx, block, onePtr);

            const auto ok = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, fetch, ConstantInt::get(fetch->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), "ok", block);
            new StoreInst(fetch, statusPtr, block);

            BranchInst::Create(good, done, ok, block);

            block = good;

            codegenItemArg->CreateSetValue(ctx, block, onePtr);
            const auto key = GetNodeValue(Nodes_.KeyResultNode, ctx, block);
            codegenKeyArg->CreateSetValue(ctx, block, key);

            const auto place = EmitFunctionCall<&TState::At>(ptrValueType, {stateArg, key}, ctx, block);

            const auto init = BasicBlock::Create(context, "init", ctx.Func);
            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            const auto test = BasicBlock::Create(context, "test", ctx.Func);

            BranchInst::Create(init, next, IsInvalid(place, block, context), block);

            block = init;
            GetNodeValue(place, Nodes_.InitResultNode, ctx, block);
            BranchInst::Create(test, block);

            block = next;
            codegenStateArg->CreateSetValue(ctx, block, place);
            GetNodeValue(place, Nodes_.UpdateResultNode, ctx, block);
            BranchInst::Create(test, block);

            block = test;

            const auto check = CheckAdjustedMemLimit<TrackRss>(MemLimit_, used, ctx, block);
            BranchInst::Create(done, loop, check, block);

            block = done;

            EmitFunctionCall<&TState::PushStat>(Type::getVoidTy(context), {stateArg, ctx.GetStat()}, ctx, block);

            BranchInst::Create(full, block);
        }

        {
            block = full;

            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            const auto has = EmitFunctionCall<&TState::Extract>(Type::getInt1Ty(context), {stateArg, onePtr, twoPtr}, ctx, block);

            BranchInst::Create(good, more, has, block);

            block = good;

            codegenKeyArg->CreateSetValue(ctx, block, onePtr);
            codegenStateArg->CreateSetValue(ctx, block, twoPtr);

            if constexpr (IsMultiRowState) {
                if constexpr (StateContainerOpt) {
                    GetNodeValue(currArg, Nodes_.FinishResultNode, ctx, block);
                    BranchInst::Create(more, block);
                } else {
                    const auto list = GetNodeValue(Nodes_.FinishResultNode, ctx, block);
                    CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(currArg, list, codegen, block);
                    if (Nodes_.FinishResultNode->IsTemporaryValue()) {
                        CleanupBoxed(list, ctx, block);
                    }
                    BranchInst::Create(more, block);
                }
            } else {
                SafeUnRefUnboxedOne(valuePtr, ctx, block);
                GetNodeValue(valuePtr, Nodes_.FinishResultNode, ctx, block);
                const auto value = new LoadInst(valueType, valuePtr, "value", block);

                const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
                BranchInst::Create(more, exit, IsEmpty(value, block, context), block);

                block = exit;

                if constexpr (StateContainerOpt) {
                    const auto strip = GetOptionalValue(context, value, block);
                    new StoreInst(strip, valuePtr, block);
                }

                ReturnInst::Create(context, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), block);
            }
        }

        return ctx.Func;
    }

    using TCombinePtr = typename TCodegenValue::TFetchPtr;

    Function* CombineFunc_ = nullptr;
    Function* EqualsFunc_ = nullptr;
    Function* HashFunc_ = nullptr;

    TCombinePtr Combine_ = nullptr;
    TEqualsPtr Equals_ = nullptr;
    THashPtr Hash_ = nullptr;
#endif
    IComputationNode* const Stream_;
    const TCombineCoreNodes Nodes_;
    const TKeyTypes KeyTypes_;
    const bool IsTuple_;
    const ui64 MemLimit_;
};

} // namespace

IComputationNode* WrapCombineCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 9U, "Expected 9 args");

    const auto type = callable.GetType()->GetReturnType();
    const auto finishResultType = callable.GetInput(7).GetStaticType();
    MKQL_ENSURE(finishResultType->IsList() || finishResultType->IsOptional() || finishResultType->IsStream(), "Expected list, stream or optional");

    const auto keyType = callable.GetInput(2).GetStaticType();
    TKeyTypes keyTypes;
    bool isTuple;
    bool encoded;
    bool useIHash;
    GetDictionaryKeyTypes(keyType, keyTypes, isTuple, encoded, useIHash);
    Y_ENSURE(!encoded, "TODO");
    const auto memLimit = AS_VALUE(TDataLiteral, callable.GetInput(8))->AsValue().Get<ui64>();
    const bool trackRss = EGraphPerProcess::Single == ctx.GraphPerProcess;

    const auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto keyExtractorResultNode = LocateNode(ctx.NodeLocator, callable, 2);
    const auto initResultNode = LocateNode(ctx.NodeLocator, callable, 4);
    const auto updateResultNode = LocateNode(ctx.NodeLocator, callable, 6);
    const auto finishResultNode = LocateNode(ctx.NodeLocator, callable, 7);

    const auto itemNode = LocateExternalNode(ctx.NodeLocator, callable, 1);
    const auto keyNode = LocateExternalNode(ctx.NodeLocator, callable, 3);
    const auto stateNode = LocateExternalNode(ctx.NodeLocator, callable, 5);

    const TCombineCoreNodes nodes = {
        .ItemNode = itemNode,
        .KeyNode = keyNode,
        .StateNode = stateNode,
        .KeyResultNode = keyExtractorResultNode,
        .InitResultNode = initResultNode,
        .UpdateResultNode = updateResultNode,
        .FinishResultNode = finishResultNode};

    if (type->IsFlow()) {
        const auto kind = GetValueRepresentation(AS_TYPE(TFlowType, type)->GetItemType());
        if (finishResultType->IsStream()) {
            if (trackRss) {
                return new TCombineCoreFlowWrapper<true, true, true>(ctx.Mutables, kind, stream, nodes, std::move(keyTypes), isTuple, memLimit);
            } else {
                return new TCombineCoreFlowWrapper<true, true, false>(ctx.Mutables, kind, stream, nodes, std::move(keyTypes), isTuple, memLimit);
            }
        } else if (finishResultType->IsList()) {
            if (trackRss) {
                return new TCombineCoreFlowWrapper<true, false, true>(ctx.Mutables, kind, stream, nodes, std::move(keyTypes), isTuple, memLimit);
            } else {
                return new TCombineCoreFlowWrapper<true, false, false>(ctx.Mutables, kind, stream, nodes, std::move(keyTypes), isTuple, memLimit);
            }
        } else if (finishResultType->IsOptional()) {
            if (AS_TYPE(TOptionalType, finishResultType)->GetItemType()->IsOptional()) {
                if (trackRss) {
                    return new TCombineCoreFlowWrapper<false, true, true>(ctx.Mutables, kind, stream, nodes, std::move(keyTypes), isTuple, memLimit);
                } else {
                    return new TCombineCoreFlowWrapper<false, true, false>(ctx.Mutables, kind, stream, nodes, std::move(keyTypes), isTuple, memLimit);
                }
            } else {
                if (trackRss) {
                    return new TCombineCoreFlowWrapper<false, false, true>(ctx.Mutables, kind, stream, nodes, std::move(keyTypes), isTuple, memLimit);
                } else {
                    return new TCombineCoreFlowWrapper<false, false, false>(ctx.Mutables, kind, stream, nodes, std::move(keyTypes), isTuple, memLimit);
                }
            }
        }
    } else if (type->IsStream()) {
        if (finishResultType->IsStream()) {
            if (trackRss) {
                return new TCombineCoreWrapper<true, true, true>(ctx.Mutables, stream, nodes, std::move(keyTypes), isTuple, memLimit);
            } else {
                return new TCombineCoreWrapper<true, true, false>(ctx.Mutables, stream, nodes, std::move(keyTypes), isTuple, memLimit);
            }
        } else if (finishResultType->IsList()) {
            if (trackRss) {
                return new TCombineCoreWrapper<true, false, true>(ctx.Mutables, stream, nodes, std::move(keyTypes), isTuple, memLimit);
            } else {
                return new TCombineCoreWrapper<true, false, false>(ctx.Mutables, stream, nodes, std::move(keyTypes), isTuple, memLimit);
            }
        } else if (finishResultType->IsOptional()) {
            if (AS_TYPE(TOptionalType, finishResultType)->GetItemType()->IsOptional()) {
                if (trackRss) {
                    return new TCombineCoreWrapper<false, true, true>(ctx.Mutables, stream, nodes, std::move(keyTypes), isTuple, memLimit);
                } else {
                    return new TCombineCoreWrapper<false, true, false>(ctx.Mutables, stream, nodes, std::move(keyTypes), isTuple, memLimit);
                }
            } else {
                if (trackRss) {
                    return new TCombineCoreWrapper<false, false, true>(ctx.Mutables, stream, nodes, std::move(keyTypes), isTuple, memLimit);
                } else {
                    return new TCombineCoreWrapper<false, false, false>(ctx.Mutables, stream, nodes, std::move(keyTypes), isTuple, memLimit);
                }
            }
        }
    }

    THROW yexception() << "Expected flow or stream.";
}

} // namespace NKikimr::NMiniKQL
