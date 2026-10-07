#include "mkql_switch.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_llvm_base.h>                // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_stats_registry.h>
#include <yql/essentials/utils/cast.h>
#include <yql/essentials/utils/runtime_dispatch.h>

#include <util/string/cast.h>

#include <utility>

namespace NKikimr::NMiniKQL {

#ifndef MKQL_DISABLE_CODEGEN
using NYql::EnsureDynamicCast;
#endif

namespace {

const TStatKey Switch_FlushesCount("Switch_FlushesCount", /*deriv=*/true);
const TStatKey Switch_MaxRowsCount("Switch_MaxRowsCount", /*deriv=*/false);

using TPagedUnboxedValueList = TPagedList<NUdf::TUnboxedValue>;

struct TSwitchHandler {
    std::vector<ui32, TMKQLAllocator<ui32>> InputIndices;
    IComputationExternalNode* Item = nullptr;
    IComputationNode* NewItem = nullptr;
    std::optional<ui32> ResultVariantOffset;
    bool IsOutputVariant = false;
    EValueRepresentation Kind = EValueRepresentation::Any;
};

using TSwitchHandlersList = std::vector<TSwitchHandler, TMKQLAllocator<TSwitchHandler>>;

class TState: public TComputationValue<TState> {
    using TBase = TComputationValue<TState>;

public:
    TState(TMemoryUsageInfo* memInfo, ui32 size)
        : TBase(memInfo)
        , ChildReadIndex(size)
    {
    }

    ui32 ChildReadIndex = 0;
    bool IsFinished = false;
};

#ifndef MKQL_DISABLE_CODEGEN
class TLLVMFieldsStructureForState: public TLLVMFieldsStructure<TComputationValue<TState>> {
private:
    using TBase = TLLVMFieldsStructure<TComputationValue<TState>>;
    llvm::IntegerType* IndexType_;
    llvm::IntegerType* IsFinishedType_;
    const ui32 FieldsCount_ = 0;

protected:
    using TBase::GetContext;

    ui32 GetFieldsCount() const {
        return FieldsCount_;
    }

    std::vector<llvm::Type*> GetFields() {
        std::vector<llvm::Type*> result = TBase::GetFields();
        result.emplace_back(IndexType_);      // index
        result.emplace_back(IsFinishedType_); // isFinished
        return result;
    }

public:
    std::vector<llvm::Type*> GetFieldsArray() {
        return GetFields();
    }

    llvm::Constant* GetIndex() {
        return ConstantInt::get(Type::getInt32Ty(GetContext()), TBase::GetFieldsCount() + 0);
    }

    llvm::Constant* GetIsFinished() {
        return ConstantInt::get(Type::getInt32Ty(GetContext()), TBase::GetFieldsCount() + 1);
    }

    explicit TLLVMFieldsStructureForState(llvm::LLVMContext& context)
        : TBase(context)
        , IndexType_(Type::getInt32Ty(context))
        , IsFinishedType_(Type::getInt8Ty(context))
        , FieldsCount_(GetFields().size())
    {
    }
};
#endif

template <bool IsInputVariant, bool TrackRss>
class TSwitchFlowWrapper: public TStatefulFlowCodegeneratorNode<TSwitchFlowWrapper<IsInputVariant, TrackRss>> {
    using TBaseComputation = TStatefulFlowCodegeneratorNode<TSwitchFlowWrapper<IsInputVariant, TrackRss>>;

private:
    class TFlowState: public TState {
    public:
        TFlowState(TMemoryUsageInfo* memInfo, TAlignedPagePool& pool, ui32 size)
            : TState(memInfo, size)
            , Buffer_(pool)
        {
        }

        void Add(NUdf::TUnboxedValuePod item) {
            Buffer_.Add(std::move(item));
        }

        void PushStat(IStatsRegistry* stats) const {
            if (const auto size = Buffer_.Size()) {
                MKQL_SET_MAX_STAT(stats, Switch_MaxRowsCount, static_cast<i64>(size));
                MKQL_INC_STAT(stats, Switch_FlushesCount);
            }
        }

        NUdf::TUnboxedValuePod Get(ui32 i) const {
            if (Buffer_.Size() == i) {
                return IsFinished ? NUdf::TUnboxedValuePod::MakeFinish() : NUdf::TUnboxedValuePod::MakeYield();
            }

            return Buffer_[i];
        }

        bool IsBufferConsumed() const {
            return Buffer_.Size() == Position_;
        }

        void Clear() {
            Buffer_.Clear();
        }

        void ResetPosition() {
            Position_ = 0U;
        }

        NUdf::TUnboxedValuePod Handler(const TSwitchHandler& handler, TComputationContext& ctx) {
            while (true) {
                auto current = Get(Position_);
                if (current.IsSpecial()) {
                    return current;
                }

                ++Position_;
                ui32 streamIndex = 0U;

                if constexpr (IsInputVariant) {
                    streamIndex = current.GetVariantIndex();
                    current = current.GetVariantItem().Release();
                }

                for (ui32 var = 0U; var < handler.InputIndices.size(); ++var) {
                    if (handler.InputIndices[var] == streamIndex) {
                        if (handler.InputIndices.size() > 1) {
                            current = ctx.HolderFactory.CreateVariantHolder(std::move(current), var);
                        }

                        return current;
                    }
                }
            }
        }

    private:
        ui32 Position_ = 0U;
        TPagedUnboxedValueList Buffer_;
    };

public:
    TSwitchFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow, ui64 memLimit, TSwitchHandlersList&& handlers)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Any)
        , Flow_(flow)
        , MemLimit_(memLimit)
        , Handlers_(std::move(handlers))
    {
        for (const auto& handler : Handlers_) {
            handler.Item->SetGetter([stateIndex = mutables.CurValueIndex - 1, handler = &handler, this](TComputationContext& context) {
                NUdf::TUnboxedValue& state = context.MutableValues[stateIndex];
                if (state.IsInvalid()) {
                    MakeState(context, state);
                }

                auto ptr = static_cast<TFlowState*>(state.AsBoxed().Get());
                return ptr->Handler(*handler, context);
            });

#ifndef MKQL_DISABLE_CODEGEN
            const auto handlerIndex = static_cast<ui32>(&handler - Handlers_.data());
            EnsureDynamicCast<ICodegeneratorExternalNode*>(handler.Item)->SetValueGetterBuilder([handlerIndex, this](const TCodegenContext& ctx) {
                return GenerateHandler(handlerIndex, ctx.Codegen);
            });
#endif
        }
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);
        }

        auto ptr = static_cast<TFlowState*>(state.AsBoxed().Get());
        bool hasDataInInput = true;
        ui32 finishedHandlers = 0U;
        while (true) {
            if (ptr->ChildReadIndex == Handlers_.size()) {
                if (finishedHandlers == Handlers_.size()) {
                    return NUdf::TUnboxedValuePod::MakeFinish();
                }

                if (!hasDataInInput) {
                    return NUdf::TUnboxedValuePod::MakeYield();
                }

                const auto initUsage = MemLimit_ ? ctx.HolderFactory.GetMemoryUsed() : 0ULL;

                do {
                    auto current = Flow_->GetValue(ctx);
                    if (current.IsSpecial()) {
                        hasDataInInput = false;
                        ptr->IsFinished = current.IsFinish();
                        break;
                    }

                    ptr->Add(current.Release());
                } while (!ctx.CheckAdjustedMemLimit<TrackRss>(MemLimit_, initUsage));

                ptr->ChildReadIndex = 0U;
                ptr->PushStat(ctx.Stats);
                finishedHandlers = 0U;
            }

            const TSwitchHandler& handler = Handlers_[ptr->ChildReadIndex];
            auto childRes = handler.NewItem->GetValue(ctx);
            if (childRes.IsSpecial()) {
                if (childRes.IsYield() && !ptr->IsBufferConsumed()) {
                    return NUdf::TUnboxedValuePod::MakeYield();
                }

                finishedHandlers += childRes.IsFinish();
                ptr->ResetPosition();

                if (++ptr->ChildReadIndex == Handlers_.size()) {
                    ptr->Clear();
                }

                continue;
            }

            if (const auto offset = handler.ResultVariantOffset) {
                ui32 localIndex = 0U;
                if (handler.IsOutputVariant) {
                    localIndex = childRes.GetVariantIndex();
                    childRes = childRes.Release().GetVariantItem();
                }

                childRes = ctx.HolderFactory.CreateVariantHolder(childRes.Release(), *offset + localIndex);
            }

            return childRes.Release();
        }

        MKQL_ENSURE(false, "Unreachable");
    }

#ifndef MKQL_DISABLE_CODEGEN
private:
    class TLLVMFieldsStructureForFlowState: public TLLVMFieldsStructureForState {
    private:
        using TBase = TLLVMFieldsStructureForState;
        llvm::PointerType* StructPtrType_;
        llvm::IntegerType* IndexType_;

    protected:
        using TBase::GetContext;

    public:
        std::vector<llvm::Type*> GetFieldsArray() {
            std::vector<llvm::Type*> result = TBase::GetFields();
            result.emplace_back(IndexType_);     // position
            result.emplace_back(StructPtrType_); // buffer
            return result;
        }

        llvm::Constant* GetPosition() const {
            return ConstantInt::get(Type::getInt32Ty(GetContext()), TBase::GetFieldsCount() + 0);
        }

        llvm::Constant* GetBuffer() const {
            return ConstantInt::get(Type::getInt32Ty(GetContext()), TBase::GetFieldsCount() + 1);
        }

        explicit TLLVMFieldsStructureForFlowState(llvm::LLVMContext& context)
            : TBase(context)
            , StructPtrType_(PointerType::getUnqual(StructType::get(context)))
            , IndexType_(Type::getInt32Ty(context))
        {
        }
    };

    Function* GenerateHandler(ui32 i, NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        TStringStream out;
        out << this->DebugString() << "::Handler_" << i << "_(" << static_cast<const void*>(this) << ").";
        const auto& name = out.Str();
        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto valueType = Type::getInt128Ty(context);
        const auto funcType = FunctionType::get(valueType, {PointerType::getUnqual(GetCompContextType(context))}, /*isVarArg=*/false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        ctx.Ctx = &*ctx.Func->arg_begin();
        ctx.Ctx->addAttr(Attribute::NonNull);

        const auto indexType = Type::getInt32Ty(context);
        TLLVMFieldsStructureForFlowState fieldsStruct(context);
        const auto stateType = StructType::get(context, fieldsStruct.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);

        auto block = main;

        const auto statePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(indexType, static_cast<const IComputationNode*>(this)->GetIndex())}, "state_ptr", block);
        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        const auto posPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {fieldsStruct.This(), fieldsStruct.GetPosition()}, "pos_ptr", block);

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);

        BranchInst::Create(loop, block);

        block = loop;

        const auto pos = new LoadInst(indexType, posPtr, "pos", block);

        const auto input = EmitFunctionCall<&TFlowState::Get>(valueType, {stateArg, pos}, ctx, block);

        const auto special = SwitchInst::Create(input, good, 2U, block);
        special->addCase(GetYield(context), done);
        special->addCase(GetFinish(context), done);

        block = done;
        ReturnInst::Create(context, input, block);

        block = good;

        const auto plus = BinaryOperator::CreateAdd(pos, ConstantInt::get(pos->getType(), 1), "plus", block);
        new StoreInst(plus, posPtr, block);

        const auto unpack = IsInputVariant ? GetVariantParts(input, ctx, block) : std::make_pair(ConstantInt::get(indexType, 0), input);

        const auto& handler = Handlers_[i];
        const auto choise = SwitchInst::Create(unpack.first, loop, handler.InputIndices.size(), block);

        for (ui32 idx = 0U; idx < handler.InputIndices.size(); ++idx) {
            const auto var = BasicBlock::Create(context, (TString("var_") += ToString(idx)).c_str(), ctx.Func);

            choise->addCase(ConstantInt::get(indexType, handler.InputIndices[idx]), var);
            block = var;

            if (handler.InputIndices.size() > 1U) {
                const auto variant = MakeVariant(unpack.second, ConstantInt::get(indexType, idx), ctx, block);
                ReturnInst::Create(context, variant, block);
            } else {
                ReturnInst::Create(context, unpack.second, block);
            }
        }

        return ctx.Func;
    }

public:
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const final {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto isFinishedType = Type::getInt8Ty(context);
        const auto indexType = Type::getInt32Ty(context);
        TLLVMFieldsStructureForFlowState fieldsStruct(context);
        const auto stateType = StructType::get(context, fieldsStruct.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
        const auto result = PHINode::Create(valueType, Handlers_.size() + 3U, "result", exit);

        BranchInst::Create(make, main, IsInvalid(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TSwitchFlowWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto indexPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {fieldsStruct.This(), fieldsStruct.GetIndex()}, "index_ptr", block);
        const auto isFinishedPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {fieldsStruct.This(), fieldsStruct.GetIsFinished()}, "is_finished_ptr", block);

        const auto hasDataPtr = new AllocaInst(Type::getInt1Ty(context), 0U, "has_data_ptr", &ctx.Func->getEntryBlock().back());
        const auto finishedPtr = new AllocaInst(indexType, 0U, "finished_ptr", &ctx.Func->getEntryBlock().back());
        new StoreInst(ConstantInt::get(Type::getInt1Ty(context), 1), hasDataPtr, block);
        new StoreInst(ConstantInt::get(indexType, 0), finishedPtr, block);

        BranchInst::Create(more, block);

        block = more;

        const auto index = new LoadInst(indexType, indexPtr, "index", block);
        const auto empty = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, index, ConstantInt::get(index->getType(), Handlers_.size()), "empty", block);

        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto full = BasicBlock::Create(context, "full", ctx.Func);

        BranchInst::Create(next, full, empty, block);

        {
            block = next;

            const auto live = BasicBlock::Create(context, "live", ctx.Func);
            const auto pull = BasicBlock::Create(context, "pull", ctx.Func);
            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);

            const auto finished = new LoadInst(indexType, finishedPtr, "finished", block);
            const auto allFinished = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, finished, ConstantInt::get(finished->getType(), Handlers_.size()), "all_finished", block);
            result->addIncoming(GetFinish(context), block);
            BranchInst::Create(exit, live, allFinished, block);

            block = live;
            const auto hasData = new LoadInst(Type::getInt1Ty(context), hasDataPtr, "has_data", block);
            result->addIncoming(GetYield(context), block);
            BranchInst::Create(pull, exit, hasData, block);

            block = pull;

            const auto used = GetMemoryUsed(MemLimit_, ctx, block);

            BranchInst::Create(loop, block);

            block = loop;

            const auto item = GetNodeValue(Flow_, ctx, block);
            const auto special = IsSpecial(item, block, context);

            BranchInst::Create(stop, good, special, block);

            block = stop;
            new StoreInst(ConstantInt::get(Type::getInt1Ty(context), 0), hasDataPtr, block);
            const auto finsh = IsFinish(item, block, context);
            const auto finshExt = CastInst::Create(Instruction::ZExt, finsh, isFinishedType, "finsh_ext", block);
            new StoreInst(finshExt, isFinishedPtr, block);
            BranchInst::Create(done, block);

            block = good;

            EmitFunctionCall<&TFlowState::Add>(Type::getVoidTy(context), {stateArg, item}, ctx, block);

            const auto check = CheckAdjustedMemLimit<TrackRss>(MemLimit_, used, ctx, block);
            BranchInst::Create(done, loop, check, block);

            block = done;
            new StoreInst(ConstantInt::get(indexType, 0), indexPtr, block);

            EmitFunctionCall<&TFlowState::PushStat>(Type::getVoidTy(context), {stateArg, ctx.GetStat()}, ctx, block);

            new StoreInst(ConstantInt::get(indexType, 0), finishedPtr, block);

            BranchInst::Create(more, block);
        }

        {
            block = full;

            const auto stub = BasicBlock::Create(context, "stub", ctx.Func);
            const auto special = BasicBlock::Create(context, "special", ctx.Func);
            const auto skip = BasicBlock::Create(context, "skip", ctx.Func);
            const auto drop = BasicBlock::Create(context, "drop", ctx.Func);

            new UnreachableInst(context, stub);

            const auto specialOutput = PHINode::Create(valueType, Handlers_.size(), "special_output", special);

            const auto choise = SwitchInst::Create(index, stub, Handlers_.size(), block);

            for (ui32 i = 0U; i < Handlers_.size(); ++i) {
                const auto idx = ConstantInt::get(indexType, i);

                const auto var = BasicBlock::Create(context, (TString("var_") += ToString(i)).c_str(), ctx.Func);

                choise->addCase(idx, var);
                block = var;

                const auto output = GetNodeValue(Handlers_[i].NewItem, ctx, block);

                if (const auto offset = Handlers_[i].ResultVariantOffset) {
                    const auto good = BasicBlock::Create(context, (TString("good_") += ToString(i)).c_str(), ctx.Func);
                    specialOutput->addIncoming(output, block);
                    BranchInst::Create(special, good, IsSpecial(output, block, context), block);
                    block = good;

                    const auto unpack = Handlers_[i].IsOutputVariant ? GetVariantParts(output, ctx, block) : std::make_pair(ConstantInt::get(indexType, 0), output);
                    const auto reindex = BinaryOperator::CreateAdd(unpack.first, ConstantInt::get(indexType, *offset), "reindex", block);
                    const auto variant = MakeVariant(unpack.second, reindex, ctx, block);
                    result->addIncoming(variant, block);
                    BranchInst::Create(exit, block);
                } else {
                    specialOutput->addIncoming(output, block);
                    result->addIncoming(output, block);
                    BranchInst::Create(special, exit, IsSpecial(output, block, context), block);
                }
            }

            block = special;

            const auto checkBuf = BasicBlock::Create(context, "check_buf", ctx.Func);
            BranchInst::Create(checkBuf, skip, IsYield(specialOutput, block, context), block);

            block = checkBuf;
            const auto consumed = EmitFunctionCall<&TFlowState::IsBufferConsumed>(Type::getInt1Ty(context), {stateArg}, ctx, block);
            result->addIncoming(GetYield(context), block);
            BranchInst::Create(skip, exit, consumed, block);

            block = skip;

            const auto finsh = IsFinish(specialOutput, block, context);
            const auto finshExt = CastInst::Create(Instruction::ZExt, finsh, indexType, "finsh_ext", block);
            const auto finished = new LoadInst(indexType, finishedPtr, "finished", block);
            const auto incFinished = BinaryOperator::CreateAdd(finished, finshExt, "inc_finished", block);
            new StoreInst(incFinished, finishedPtr, block);

            const auto posPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {fieldsStruct.This(), fieldsStruct.GetPosition()}, "pos_ptr", block);
            new StoreInst(ConstantInt::get(indexType, 0), posPtr, block);

            const auto plus = BinaryOperator::CreateAdd(index, ConstantInt::get(index->getType(), 1), "plus", block);
            new StoreInst(plus, indexPtr, block);
            const auto flush = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, plus, ConstantInt::get(plus->getType(), Handlers_.size()), "flush", block);
            BranchInst::Create(drop, more, flush, block);

            block = drop;

            EmitFunctionCall<&TFlowState::Clear>(Type::getVoidTy(context), {stateArg}, ctx, block);

            BranchInst::Create(more, block);

            block = exit;
            return result;
        }
    }
#endif

private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TFlowState>(ctx.HolderFactory.GetPagePool(), Handlers_.size());
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            for (const auto& x : Handlers_) {
                this->Own(flow, x.Item);
                this->DependsOn(flow, x.NewItem);
            }
        }
    }

    IComputationNode* const Flow_;
    const ui64 MemLimit_;
    const TSwitchHandlersList Handlers_;
};

template <bool IsInputVariant, bool TrackRss>
class TSwitchWrapper: public TCustomValueCodegeneratorNode<TSwitchWrapper<IsInputVariant, TrackRss>> {
    using TBaseComputation = TCustomValueCodegeneratorNode<TSwitchWrapper<IsInputVariant, TrackRss>>;

private:
    class TChildStream: public TComputationValue<TChildStream> {
    public:
        using TBase = TComputationValue<TChildStream>;

        TChildStream(TMemoryUsageInfo* memInfo, TSwitchHandler handler,
                     TComputationContext& ctx, const TPagedUnboxedValueList* buffer)
            : TBase(memInfo)
            , Handler_(std::move(handler))
            , Ctx_(ctx)
            , Buffer_(buffer)
        {
        }

        bool IsBufferConsumed() const {
            return BufferIndex_ == Buffer_->Size();
        }

        void Reset(bool isFinished) {
            BufferIndex_ = InputIndex_ = 0U;
            IsFinished_ = isFinished;
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            for (;;) {
                if (IsBufferConsumed()) {
                    return IsFinished_ ? NUdf::EFetchStatus::Finish : NUdf::EFetchStatus::Yield;
                }

                auto current = (*Buffer_)[BufferIndex_];
                ui32 streamIndex = 0;
                if constexpr (IsInputVariant) {
                    streamIndex = current.GetVariantIndex();
                    current = current.Release().GetVariantItem();
                }

                for (; InputIndex_ < Handler_.InputIndices.size(); ++InputIndex_) {
                    if (Handler_.InputIndices[InputIndex_] == streamIndex) {
                        if (Handler_.InputIndices.size() > 1) {
                            current = Ctx_.HolderFactory.CreateVariantHolder(current.Release(), InputIndex_);
                        }

                        result = std::move(current);
                        ++InputIndex_;
                        return NUdf::EFetchStatus::Ok;
                    }
                }

                InputIndex_ = 0;
                ++BufferIndex_;
            }
        }

        const TSwitchHandler Handler_;
        TComputationContext& Ctx_;
        const TPagedUnboxedValueList* const Buffer_;
        ui32 BufferIndex_ = 0U;
        ui32 InputIndex_ = 0U;
        bool IsFinished_ = false;
    };

    class TValueBase: public TState {
    public:
        void Add(NUdf::TUnboxedValue&& item) {
            Buffer_.Add(std::move(item));
        }

        void Reset() {
            if (const auto size = Buffer_.Size()) {
                MKQL_SET_MAX_STAT(Ctx_.Stats, Switch_MaxRowsCount, static_cast<i64>(size));
                MKQL_INC_STAT(Ctx_.Stats, Switch_FlushesCount);
            }

            ChildReadIndex = 0U;

            for (const auto& stream : ChildrenInStreams_) {
                stream->Reset(IsFinished);
            }
        }

        NUdf::EFetchStatus Get(NUdf::TUnboxedValue& result) {
            return ChildrenOutStreams_[ChildReadIndex].Fetch(result);
        }

        bool IsBufferConsumed() const {
            return ChildrenInStreams_[ChildReadIndex]->IsBufferConsumed();
        }

        void AdvanceReadIndex() {
            if (++ChildReadIndex == Handlers_.size()) {
                Buffer_.Clear();
            }
        }

    protected:
        TValueBase(TMemoryUsageInfo* memInfo, const TSwitchHandlersList& handlers, TComputationContext& ctx)
            : TState(memInfo, handlers.size())
            , Handlers_(handlers)
            , Buffer_(ctx.HolderFactory.GetPagePool())
            , Ctx_(ctx)
        {
            ChildrenInStreams_.reserve(Handlers_.size());
            ChildrenOutStreams_.reserve(Handlers_.size());
            for (const auto& handler : Handlers_) {
                const auto stream = Ctx_.HolderFactory.Create<TChildStream>(handler, Ctx_, &Buffer_);
                ChildrenInStreams_.emplace_back(static_cast<TChildStream*>(stream.AsBoxed().Get()));
                handler.Item->SetValue(Ctx_, stream);
                ChildrenOutStreams_.emplace_back(handler.NewItem->GetValue(Ctx_));
            }
        }

        const TSwitchHandlersList Handlers_;
        TPagedUnboxedValueList Buffer_;
        TComputationContext& Ctx_;
        std::vector<NUdf::TRefCountedPtr<TChildStream>, TMKQLAllocator<NUdf::TRefCountedPtr<TChildStream>>> ChildrenInStreams_;
        TUnboxedValueVector ChildrenOutStreams_;
    };

    class TValue: public TValueBase {
    public:
        TValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream,
               const TSwitchHandlersList& handlers, ui64 memLimit, TComputationContext& ctx)
            : TValueBase(memInfo, handlers, ctx)
            , Stream_(std::move(stream))
            , MemLimit_(memLimit)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            bool hasDataInInput = true;
            ui32 finishedHandlers = 0U;
            for (;;) {
                if (this->ChildReadIndex == this->Handlers_.size()) {
                    if (finishedHandlers == this->Handlers_.size()) {
                        return NUdf::EFetchStatus::Finish;
                    }

                    if (!hasDataInInput) {
                        return NUdf::EFetchStatus::Yield;
                    }

                    const auto initUsage = MemLimit_ ? this->Ctx_.HolderFactory.GetMemoryUsed() : 0ULL;

                    do {
                        NUdf::TUnboxedValue current;
                        const auto inputStatus = Stream_.Fetch(current);
                        if (NUdf::EFetchStatus::Ok != inputStatus) {
                            hasDataInInput = false;
                            this->IsFinished = inputStatus == NUdf::EFetchStatus::Finish;
                            break;
                        }

                        this->Add(std::move(current));
                    } while (!this->Ctx_.template CheckAdjustedMemLimit<TrackRss>(MemLimit_, initUsage));

                    this->Reset();
                    finishedHandlers = 0;
                }

                if (const NUdf::EFetchStatus status = this->Get(result); status != NUdf::EFetchStatus::Ok) {
                    if (status == NUdf::EFetchStatus::Yield && !this->IsBufferConsumed()) {
                        return NUdf::EFetchStatus::Yield;
                    }

                    finishedHandlers += status == NUdf::EFetchStatus::Finish;
                    this->AdvanceReadIndex();
                    continue;
                }

                const auto& handler = this->Handlers_[this->ChildReadIndex];
                if (const auto offset = handler.ResultVariantOffset) {
                    ui32 localIndex = 0;
                    if (handler.IsOutputVariant) {
                        localIndex = result.GetVariantIndex();
                        result = result.Release().GetVariantItem();
                    }

                    result = this->Ctx_.HolderFactory.CreateVariantHolder(result.Release(), *offset + localIndex);
                }

                return NUdf::EFetchStatus::Ok;
            }
        }

        const NUdf::TUnboxedValue Stream_;
        const ui64 MemLimit_ = 0;
    };

#ifndef MKQL_DISABLE_CODEGEN
    class TCodegenValue: public TStreamCodegenSelfStateValue<TValueBase> {
    public:
        using TFetchPtr = typename TStreamCodegenSelfStateValue<TValueBase>::TFetchPtr;
        TCodegenValue(TMemoryUsageInfo* memInfo, TFetchPtr fetch, TComputationContext* ctx, NUdf::TUnboxedValue&& stream, const TSwitchHandlersList& handlers)
            : TStreamCodegenSelfStateValue<TValueBase>(memInfo, fetch, ctx, std::move(stream), handlers, *ctx)
        {
        }
    };
#endif
public:
    TSwitchWrapper(TComputationMutables& mutables, IComputationNode* stream, ui64 memLimit, TSwitchHandlersList&& handlers)
        : TBaseComputation(mutables)
        , Stream_(stream)
        , MemLimit_(memLimit)
        , Handlers_(std::move(handlers))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Switch_) {
            return ctx.HolderFactory.Create<TCodegenValue>(Switch_, &ctx, Stream_->GetValue(ctx), Handlers_);
        }
#endif
        return ctx.HolderFactory.Create<TValue>(Stream_->GetValue(ctx), Handlers_, MemLimit_, ctx);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
        for (const auto& handler : Handlers_) {
            this->Own(handler.Item);
            this->DependsOn(handler.NewItem);
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    class TLLVMFieldsStructureForValueBase: public TLLVMFieldsStructureForState {
    private:
        using TBase = TLLVMFieldsStructureForState;

    public:
        std::vector<llvm::Type*> GetFieldsArray() {
            std::vector<llvm::Type*> result = TBase::GetFields();
            return result;
        }

        explicit TLLVMFieldsStructureForValueBase(llvm::LLVMContext& context)
            : TBase(context)
        {
        }
    };

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        SwitchFunc_ = GenerateSwitch(codegen);
        codegen.ExportSymbol(SwitchFunc_);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (SwitchFunc_) {
            Switch_ = reinterpret_cast<TSwitchPtr>(codegen.GetPointerToFunction(SwitchFunc_));
        }
    }

    Function* GenerateSwitch(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = this->MakeName("Fetch");
        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto containerType = static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt32Ty(context);
        TLLVMFieldsStructureForValueBase fieldsStruct(context);
        const auto stateType = StructType::get(context, fieldsStruct.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, statePtrType, ptrValueType}, /*isVarArg=*/false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto stateArg = &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        auto block = main;

        const auto indexPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {fieldsStruct.This(), fieldsStruct.GetIndex()}, "index_ptr", block);
        const auto isFinishedPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, {fieldsStruct.This(), fieldsStruct.GetIsFinished()}, "is_finished_ptr", block);

        const auto itemPtr = new AllocaInst(valueType, 0U, "item_ptr", block);
        new StoreInst(ConstantInt::get(valueType, 0), itemPtr, block);

        const auto hasDataPtr = new AllocaInst(Type::getInt1Ty(context), 0U, "has_data_ptr", block);
        const auto finishedPtr = new AllocaInst(indexType, 0U, "finished_ptr", block);
        new StoreInst(ConstantInt::get(Type::getInt1Ty(context), 1), hasDataPtr, block);
        new StoreInst(ConstantInt::get(indexType, 0), finishedPtr, block);

        BranchInst::Create(more, block);

        block = more;

        const auto index = new LoadInst(indexType, indexPtr, "index", block);
        const auto empty = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, index, ConstantInt::get(index->getType(), Handlers_.size()), "empty", block);

        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto full = BasicBlock::Create(context, "full", ctx.Func);

        BranchInst::Create(next, full, empty, block);

        {
            block = next;

            const auto live = BasicBlock::Create(context, "live", ctx.Func);
            const auto pull = BasicBlock::Create(context, "pull", ctx.Func);
            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);
            const auto retFinish = BasicBlock::Create(context, "ret_finish", ctx.Func);
            const auto retYield = BasicBlock::Create(context, "ret_yield", ctx.Func);

            const auto finished = new LoadInst(indexType, finishedPtr, "finished", block);
            const auto allFinished = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, finished, ConstantInt::get(finished->getType(), Handlers_.size()), "all_finished", block);
            BranchInst::Create(retFinish, live, allFinished, block);

            block = retFinish;
            ReturnInst::Create(context, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), block);

            block = live;
            const auto hasData = new LoadInst(Type::getInt1Ty(context), hasDataPtr, "has_data", block);
            BranchInst::Create(pull, retYield, hasData, block);

            block = retYield;
            ReturnInst::Create(context, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), block);

            block = pull;

            const auto used = GetMemoryUsed(MemLimit_, ctx, block);

            const auto stream = static_cast<Value*>(containerArg);

            BranchInst::Create(loop, block);

            block = loop;

            const auto fetch = CallBoxedValueFetch(stream, ctx, block, itemPtr);

            const auto ok = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, fetch, ConstantInt::get(fetch->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), "ok", block);

            BranchInst::Create(good, stop, ok, block);

            block = good;

            EmitFunctionCall<&TValueBase::Add>(Type::getVoidTy(context), {stateArg, itemPtr}, ctx, block);

            const auto check = CheckAdjustedMemLimit<TrackRss>(MemLimit_, used, ctx, block);
            BranchInst::Create(done, loop, check, block);

            block = stop;
            new StoreInst(ConstantInt::get(Type::getInt1Ty(context), 0), hasDataPtr, block);
            const auto finsh = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, fetch, ConstantInt::get(fetch->getType(), static_cast<ui32>(NUdf::EFetchStatus::Finish)), "finsh", block);
            const auto finshExt = CastInst::Create(Instruction::ZExt, finsh, Type::getInt8Ty(context), "finsh_ext", block);
            new StoreInst(finshExt, isFinishedPtr, block);
            BranchInst::Create(done, block);

            block = done;

            EmitFunctionCall<&TValueBase::Reset>(Type::getVoidTy(context), {stateArg}, ctx, block);

            new StoreInst(ConstantInt::get(indexType, 0), finishedPtr, block);

            BranchInst::Create(more, block);
        }

        {
            block = full;

            const auto stub = BasicBlock::Create(context, "stub", ctx.Func);
            const auto special = BasicBlock::Create(context, "special", ctx.Func);
            const auto skip = BasicBlock::Create(context, "skip", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto retOk = BasicBlock::Create(context, "ret_ok", ctx.Func);

            new UnreachableInst(context, stub);
            ReturnInst::Create(context, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), retOk);

            const auto status = EmitFunctionCall<&TValueBase::Get>(statusType, {stateArg, valuePtr}, ctx, block);
            const auto getOk = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), "get_ok", block);

            BranchInst::Create(good, special, getOk, block);

            block = special;

            const auto checkBuf = BasicBlock::Create(context, "check_buf", ctx.Func);
            const auto yieldExit = BasicBlock::Create(context, "yield_exit", ctx.Func);
            const auto isYield = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Yield)), "is_yield", block);
            BranchInst::Create(checkBuf, skip, isYield, block);

            block = checkBuf;
            const auto consumed = EmitFunctionCall<&TValueBase::IsBufferConsumed>(Type::getInt1Ty(context), {stateArg}, ctx, block);
            BranchInst::Create(skip, yieldExit, consumed, block);

            block = yieldExit;
            ReturnInst::Create(context, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), block);

            block = skip;
            const auto isFin = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Finish)), "is_fin", block);
            const auto finExt = CastInst::Create(Instruction::ZExt, isFin, indexType, "fin_ext", block);
            const auto finished = new LoadInst(indexType, finishedPtr, "finished", block);
            const auto incFinished = BinaryOperator::CreateAdd(finished, finExt, "inc_finished", block);
            new StoreInst(incFinished, finishedPtr, block);

            EmitFunctionCall<&TValueBase::AdvanceReadIndex>(Type::getVoidTy(context), {stateArg}, ctx, block);

            BranchInst::Create(more, block);

            block = good;

            const auto choise = SwitchInst::Create(index, stub, Handlers_.size(), block);

            for (ui32 i = 0U; i < Handlers_.size(); ++i) {
                const auto idx = ConstantInt::get(indexType, i);
                if (const auto offset = Handlers_[i].ResultVariantOffset) {
                    const auto var = BasicBlock::Create(context, (TString("var_") += ToString(i)).c_str(), ctx.Func);
                    choise->addCase(idx, var);
                    block = var;

                    const auto output = new LoadInst(valueType, valuePtr, "output", block);
                    ValueRelease(Handlers_[i].Kind, output, ctx, block);

                    const auto unpack = Handlers_[i].IsOutputVariant ? GetVariantParts(output, ctx, block) : std::make_pair(ConstantInt::get(indexType, 0), output);
                    const auto reindex = BinaryOperator::CreateAdd(unpack.first, ConstantInt::get(indexType, *offset), "reindex", block);
                    const auto variant = MakeVariant(unpack.second, reindex, ctx, block);
                    new StoreInst(variant, valuePtr, block);

                    ValueAddRef(EValueRepresentation::Any, variant, ctx, block);
                    ReturnInst::Create(context, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), block);
                } else {
                    choise->addCase(idx, retOk);
                }
            }
        }

        return ctx.Func;
    }

    using TSwitchPtr = typename TCodegenValue::TFetchPtr;

    Function* SwitchFunc_ = nullptr;

    TSwitchPtr Switch_ = nullptr;
#endif

    IComputationNode* const Stream_;
    const ui64 MemLimit_;
    const TSwitchHandlersList Handlers_;
};

} // namespace

IComputationNode* WrapSwitch(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 6, "Expected at least 6 args");
    MKQL_ENSURE((callable.GetInputsCount() - 2) % 4 == 0, "Corrupted arguments for Switch");
    TSwitchHandlersList handlers;
    handlers.reserve(callable.GetInputsCount() >> 2U);
    const auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto memLimit = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui64>();
    const auto type = callable.GetType()->GetReturnType();

    for (ui32 i = 2; i < callable.GetInputsCount(); i += 4) {
        TSwitchHandler handler;
        const auto tuple = AS_VALUE(TTupleLiteral, callable.GetInput(i));
        for (ui32 tupleIndex = 0; tupleIndex < tuple->GetValuesCount(); ++tupleIndex) {
            handler.InputIndices.emplace_back(AS_VALUE(TDataLiteral, tuple->GetValue(tupleIndex))->AsValue().Get<ui32>());
        }

        const auto itemType = type->IsFlow() ? AS_TYPE(TFlowType, callable.GetInput(i + 2))->GetItemType() : AS_TYPE(TStreamType, callable.GetInput(i + 2))->GetItemType();
        handler.IsOutputVariant = itemType->IsVariant();
        handler.Kind = GetValueRepresentation(itemType);
        handler.NewItem = LocateNode(ctx.NodeLocator, callable, i + 2);
        handler.Item = LocateExternalNode(ctx.NodeLocator, callable, i + 1);
        const auto offsetNode = callable.GetInput(i + 3);
        if (!offsetNode.GetStaticType()->IsVoid()) {
            handler.ResultVariantOffset = AS_VALUE(TDataLiteral, offsetNode)->AsValue().Get<ui32>();
        }

        handlers.emplace_back(std::move(handler));
    }

    const bool trackRss = EGraphPerProcess::Single == ctx.GraphPerProcess;
    if (type->IsFlow()) {
        const bool isInputVariant = AS_TYPE(TFlowType, callable.GetInput(0))->GetItemType()->IsVariant();
        const auto kind = GetValueRepresentation(type);
        return YQL_RUNTIME_DISPATCH_NEW(IComputationNode*, TSwitchFlowWrapper, 2, isInputVariant, trackRss, ctx.Mutables, kind, stream, memLimit, std::move(handlers));
    } else if (type->IsStream()) {
        const bool isInputVariant = AS_TYPE(TStreamType, callable.GetInput(0))->GetItemType()->IsVariant();
        return YQL_RUNTIME_DISPATCH_NEW(IComputationNode*, TSwitchWrapper, 2, isInputVariant, trackRss, ctx.Mutables, stream, memLimit, std::move(handlers));
    }

    THROW yexception() << "Expected flow or stream.";
}

} // namespace NKikimr::NMiniKQL
