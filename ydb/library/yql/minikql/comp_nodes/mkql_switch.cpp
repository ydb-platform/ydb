#include "mkql_switch.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_llvm_base.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_stats_registry.h>
#include <ydb/library/yql/utils/cast.h>

#include <util/string/cast.h>

namespace NKikimr {
namespace NMiniKQL {

using NYql::EnsureDynamicCast;

namespace {

static const TStatKey Switch_FlushesCount("Switch_FlushesCount", true);
static const TStatKey Switch_MaxRowsCount("Switch_MaxRowsCount", false);

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

class TState : public TComputationValue<TState> {
    typedef TComputationValue<TState> TBase;
public:
    TState(TMemoryUsageInfo* memInfo, ui32 size)
        : TBase(memInfo), ChildReadIndex(size)
    {}

    ui32 ChildReadIndex;
    NUdf::EFetchStatus InputStatus = NUdf::EFetchStatus::Ok;
};

#ifndef MKQL_DISABLE_CODEGEN
class TLLVMFieldsStructureForState: public TLLVMFieldsStructure<TComputationValue<TState>> {
private:
    using TBase = TLLVMFieldsStructure<TComputationValue<TState>>;
    llvm::IntegerType* IndexType;
    llvm::IntegerType* StatusType;
    const ui32 FieldsCount = 0;
protected:
    using TBase::Context;
    ui32 GetFieldsCount() const {
        return FieldsCount;
    }

    std::vector<llvm::Type*> GetFields() {
        std::vector<llvm::Type*> result = TBase::GetFields();
        result.emplace_back(IndexType);     // index
        result.emplace_back(StatusType);    // status

        return result;
    }

public:
    std::vector<llvm::Type*> GetFieldsArray() {
        return GetFields();
    }

    llvm::Constant* GetIndex() {
        return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 0);
    }

    llvm::Constant* GetStatus() {
        return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 1);
    }

    TLLVMFieldsStructureForState(llvm::LLVMContext& context)
        : TBase(context)
        , IndexType(Type::getInt32Ty(context))
        , StatusType(Type::getInt32Ty(context))
        , FieldsCount(GetFields().size())
    {
    }
};
#endif

template <bool IsInputVariant, bool TrackRss>
class TSwitchFlowWrapper : public TStatefulFlowCodegeneratorNode<TSwitchFlowWrapper<IsInputVariant, TrackRss>> {
    typedef TStatefulFlowCodegeneratorNode<TSwitchFlowWrapper<IsInputVariant, TrackRss>> TBaseComputation;
private:
    class TFlowState : public TState {
    public:
        TFlowState(TMemoryUsageInfo* memInfo, TAlignedPagePool& pool, ui32 size)
            : TState(memInfo, size), Buffer(pool)
        {}

        void Add(NUdf::TUnboxedValuePod item) {
            Buffer.Add(std::move(item));
        }

        void PushStat(IStatsRegistry* stats) const {
            if (const auto size = Buffer.Size()) {
                MKQL_SET_MAX_STAT(stats, Switch_MaxRowsCount, static_cast<i64>(size));
                MKQL_INC_STAT(stats, Switch_FlushesCount);
            }
        }

        NUdf::TUnboxedValuePod Get(ui32 i) const {
            if (Buffer.Size() == i) {
                return NUdf::EFetchStatus::Finish == InputStatus ?
                    NUdf::TUnboxedValuePod::MakeFinish():
                    NUdf::TUnboxedValuePod::MakeYield();
            }

            return Buffer[i];
        }

        void Clear() {
            Buffer.Clear();
        }

        void ResetPosition() {
            Position = 0U;
        }

        NUdf::TUnboxedValuePod Handler(ui32, const TSwitchHandler& handler, TComputationContext& ctx) {
            while (true) {
                auto current = Get(Position);
                if (current.IsSpecial()) {
                    if (current.IsYield())
                        ResetPosition();
                    return current;
                }
                ++Position;
                ui32 streamIndex = 0U;
                if constexpr (IsInputVariant) {
                    streamIndex = current.GetVariantIndex();
                    current = current.GetVariantItem().Release();
                }

                for (ui32 var = 0U; var < handler.InputIndices.size(); ++var) {
                    if (handler.InputIndices[var] == streamIndex) {
                        if (handler.InputIndices.size() > 1) {
                            current = ctx.HolderFactory.CreateVariantHolder(current, var);
                        }

                        return current;
                    }
                }
            }
        }

        ui32 Position = 0U;
        TPagedUnboxedValueList Buffer;
    };
public:
    TSwitchFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow, ui64 memLimit, TSwitchHandlersList&& handlers)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Any)
        , Flow(flow)
        , MemLimit(memLimit)
        , Handlers(std::move(handlers))
    {
        size_t handlersSize = Handlers.size();
        for (ui32 handlerIndex = 0; handlerIndex < handlersSize; ++handlerIndex) {
            Handlers[handlerIndex].Item->SetGetter([stateIndex = mutables.CurValueIndex - 1, handlerIndex, this](TComputationContext & context) {
                NUdf::TUnboxedValue& state = context.MutableValues[stateIndex];
                if (!state.HasValue()) {
                    MakeState(context, state);
                }

                auto ptr = static_cast<TFlowState*>(state.AsBoxed().Get());
                return ptr->Handler(handlerIndex, Handlers[handlerIndex], context);
            });

#ifndef MKQL_DISABLE_CODEGEN
            EnsureDynamicCast<ICodegeneratorExternalNode*>(Handlers[handlerIndex].Item)->SetValueGetterBuilder([handlerIndex, this](const TCodegenContext& ctx) {
                return GenerateHandler(handlerIndex, ctx.Codegen);
            });
#endif
        }
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            MakeState(ctx, state);
        }

        auto ptr = static_cast<TFlowState*>(state.AsBoxed().Get());
        while (true) {
            if (ptr->ChildReadIndex == Handlers.size()) {
                switch (ptr->InputStatus) {
                    case NUdf::EFetchStatus::Ok: break;
                    case NUdf::EFetchStatus::Yield:
                        ptr->InputStatus = NUdf::EFetchStatus::Ok;
                        return NUdf::TUnboxedValuePod::MakeYield();
                    case NUdf::EFetchStatus::Finish:
                        return NUdf::TUnboxedValuePod::MakeFinish();
                }

                const auto initUsage = MemLimit ? ctx.HolderFactory.GetMemoryUsed() : 0ULL;

                do {
                    auto current = Flow->GetValue(ctx);
                    if (current.IsFinish()) {
                        ptr->InputStatus = NUdf::EFetchStatus::Finish;
                        break;
                    } else if (current.IsYield()) {
                        ptr->InputStatus = NUdf::EFetchStatus::Yield;
                        break;
                    }
                    ptr->Add(current.Release());
                } while (!ctx.CheckAdjustedMemLimit<TrackRss>(MemLimit, initUsage));


                ptr->ChildReadIndex = 0U;
                ptr->PushStat(ctx.Stats);
            }

            const auto& handler = Handlers[ptr->ChildReadIndex];
            auto childRes = handler.NewItem->GetValue(ctx);
            if (childRes.IsSpecial()) {
                ptr->ResetPosition();
                if (++ptr->ChildReadIndex == Handlers.size()) {
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
        Y_UNREACHABLE();
    }
#ifndef MKQL_DISABLE_CODEGEN
private:
    class TLLVMFieldsStructureForFlowState: public TLLVMFieldsStructureForState {
    private:
        using TBase = TLLVMFieldsStructureForState;
        llvm::PointerType* StructPtrType;
        llvm::IntegerType* IndexType;
    protected:
        using TBase::Context;
    public:
        std::vector<llvm::Type*> GetFieldsArray() {
            std::vector<llvm::Type*> result = TBase::GetFields();
            result.emplace_back(IndexType);     // position
            result.emplace_back(StructPtrType); // buffer
            return result;
        }

        llvm::Constant* GetPosition() const {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 0);
        }

        llvm::Constant* GetBuffer() const {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 1);
        }

        TLLVMFieldsStructureForFlowState(llvm::LLVMContext& context)
            : TBase(context)
            , StructPtrType(PointerType::getUnqual(StructType::get(context)))
            , IndexType(Type::getInt32Ty(context)) {
        }
    };

    Function* GenerateHandler(ui32 i, NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        TStringStream out;
        out << this->DebugString() << "::Handler_" << i << "_(" << static_cast<const void*>(this) << ").";
        const auto& name = out.Str();
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto funcType = FunctionType::get(valueType, {PointerType::getUnqual(GetCompContextType(context))}, false);

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

        const auto placeholder = NYql::NCodegen::ETarget::Windows == ctx.Codegen.GetEffectiveTarget() ?
            new AllocaInst(valueType, 0U, "placeholder", block) : nullptr;

        const auto statePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(indexType, static_cast<const IComputationNode*>(this)->GetIndex())}, "state_ptr", block);
        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        const auto posPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { fieldsStruct.This(), fieldsStruct.GetPosition() }, "pos_ptr", block);

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto back = BasicBlock::Create(context, "back", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);

        BranchInst::Create(loop, block);

        block = loop;

        const auto pos = new LoadInst(indexType, posPtr, "pos", block);

        const auto getFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TFlowState::Get));

        Value* input;
        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto getType = FunctionType::get(valueType, {stateArg->getType(), pos->getType()}, false);
            const auto getPtr = CastInst::Create(Instruction::IntToPtr, getFunc, PointerType::getUnqual(getType), "get", block);
            input = CallInst::Create(getType, getPtr, {stateArg, pos}, "input", block);
        } else {
            const auto getType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), placeholder->getType(), pos->getType()}, false);
            const auto getPtr = CastInst::Create(Instruction::IntToPtr, getFunc, PointerType::getUnqual(getType), "get", block);
            CallInst::Create(getType, getPtr, {stateArg, placeholder, pos}, "", block);
            input = new LoadInst(valueType, placeholder, "input", block);
        }

        const auto special = SwitchInst::Create(input, good, 2U, block);
        special->addCase(GetYield(context), back);
        special->addCase(GetFinish(context), done);

        block = back;
        new StoreInst(ConstantInt::get(pos->getType(), 0), posPtr, block);
        BranchInst::Create(done, block);

        block = done;
        ReturnInst::Create(context, input, block);

        block = good;

        const auto plus = BinaryOperator::CreateAdd(pos, ConstantInt::get(pos->getType(), 1), "plus", block);
        new StoreInst(plus, posPtr, block);

        const auto unpack = IsInputVariant ? GetVariantParts(input, ctx, block) : std::make_pair(ConstantInt::get(indexType, 0), input);

        const auto& handler = Handlers[i];
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
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt32Ty(context);
        TLLVMFieldsStructureForFlowState fieldsStruct(context);
        const auto stateType = StructType::get(context, fieldsStruct.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
        const auto result = PHINode::Create(valueType, Handlers.size() + 2U, "result", exit);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSwitchFlowWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto indexPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { fieldsStruct.This(), fieldsStruct.GetIndex() }, "index_ptr", block);

        BranchInst::Create(more, block);

        block = more;

        const auto index = new LoadInst(indexType, indexPtr, "index", block);
        const auto empty = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, index, ConstantInt::get(index->getType(), Handlers.size()), "empty", block);

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

            const auto statusPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { fieldsStruct.This(), fieldsStruct.GetStatus() }, "last", block);

            const auto last = new LoadInst(statusType, statusPtr, "last", block);

            result->addIncoming(GetFinish(context), block);
            const auto choise = SwitchInst::Create(last, pull, 2U, block);
            choise->addCase(ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), rest);
            choise->addCase(ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), exit);

            block = rest;
            new StoreInst(ConstantInt::get(last->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), statusPtr, block);
            result->addIncoming(GetYield(context), block);
            BranchInst::Create(exit, block);

            block = pull;

            const auto used = GetMemoryUsed(MemLimit, ctx, block);

            BranchInst::Create(loop, block);

            block = loop;

            const auto item = GetNodeValue(Flow, ctx, block);

            const auto finsh = IsFinish(item, block);
            const auto yield = IsYield(item, block);
            const auto special = BinaryOperator::CreateOr(finsh, yield, "special", block);

            const auto fin = SelectInst::Create(finsh, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), "fin", block);
            const auto save = SelectInst::Create(yield, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), fin, "save", block);
            new StoreInst(save, statusPtr, block);

            BranchInst::Create(done, good, special, block);

            block = good;

            const auto addFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TFlowState::Add));
            const auto addArg = WrapArgumentForWindows(item, ctx, block);
            const auto addType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), addArg->getType()}, false);
            const auto addPtr = CastInst::Create(Instruction::IntToPtr, addFunc, PointerType::getUnqual(addType), "add", block);
            CallInst::Create(addType, addPtr, {stateArg, addArg}, "", block);

            const auto check = CheckAdjustedMemLimit<TrackRss>(MemLimit, used, ctx, block);
            BranchInst::Create(done, loop, check, block);

            block = done;
            new StoreInst(ConstantInt::get(indexType, 0), indexPtr, block);

            const auto stat = ctx.GetStat();
            const auto statFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TFlowState::PushStat));
            const auto statType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), stat->getType()}, false);
            const auto statPtr = CastInst::Create(Instruction::IntToPtr, statFunc, PointerType::getUnqual(statType), "stat", block);
            CallInst::Create(statType, statPtr, {stateArg, stat}, "", block);

            BranchInst::Create(more, block);
        }

        {
            block = full;

            const auto stub = BasicBlock::Create(context, "stub", ctx.Func);
            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            const auto drop = BasicBlock::Create(context, "drop", ctx.Func);

            new UnreachableInst(context, stub);
            const auto choise = SwitchInst::Create(index, stub, Handlers.size(), block);

            for (ui32 i = 0U; i < Handlers.size(); ++i) {
                const auto idx = ConstantInt::get(indexType, i);

                const auto var = BasicBlock::Create(context, (TString("var_") += ToString(i)).c_str(), ctx.Func);

                choise->addCase(idx, var);
                block = var;

                const auto output = GetNodeValue(Handlers[i].NewItem, ctx, block);

                if (const auto offset = Handlers[i].ResultVariantOffset) {
                    const auto good = BasicBlock::Create(context, (TString("good_") += ToString(i)).c_str(), ctx.Func);
                    BranchInst::Create(next, good, IsSpecial(output, block), block);
                    block = good;

                    const auto unpack = Handlers[i].IsOutputVariant ? GetVariantParts(output, ctx, block) : std::make_pair(ConstantInt::get(indexType, 0), output);
                    const auto reindex = BinaryOperator::CreateAdd(unpack.first, ConstantInt::get(indexType, *offset), "reindex", block);
                    const auto variant = MakeVariant(unpack.second, reindex, ctx, block);
                    result->addIncoming(variant, block);
                    BranchInst::Create(exit, block);
                } else {
                    result->addIncoming(output, block);
                    BranchInst::Create(next, exit, IsSpecial(output, block), block);
                }
            }

            block = next;

            const auto posPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { fieldsStruct.This(), fieldsStruct.GetPosition() }, "pos_ptr", block);
            new StoreInst(ConstantInt::get(indexType, 0), posPtr, block);

            const auto plus = BinaryOperator::CreateAdd(index, ConstantInt::get(index->getType(), 1), "plus", block);
            new StoreInst(plus, indexPtr, block);
            const auto flush = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, plus, ConstantInt::get(plus->getType(), Handlers.size()), "flush", block);
            BranchInst::Create(drop, more, flush, block);

            block = drop;

            const auto clearFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TFlowState::Clear));
            const auto clearType = FunctionType::get(Type::getInt1Ty(context), {stateArg->getType()}, false);
            const auto clearPtr = CastInst::Create(Instruction::IntToPtr, clearFunc, PointerType::getUnqual(clearType), "clear", block);
            CallInst::Create(clearType, clearPtr, {stateArg}, "", block);

            BranchInst::Create(more, block);

            block = exit;
            return result;
        }
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TFlowState>(ctx.HolderFactory.GetPagePool(), Handlers.size());
    }

    void RegisterDependencies() const final {
        if (const auto flow =  this->FlowDependsOn(Flow)) {
            for (const auto& x : Handlers) {
                this->Own(flow, x.Item);
                this->DependsOn(flow, x.NewItem);
            }
        }
    }

    IComputationNode *const Flow;
    const ui64 MemLimit;
    const TSwitchHandlersList Handlers;
};

template <bool IsInputVariant, bool TrackRss>
class TSwitchWrapper : public TCustomValueCodegeneratorNode<TSwitchWrapper<IsInputVariant, TrackRss>> {
    typedef TCustomValueCodegeneratorNode<TSwitchWrapper<IsInputVariant, TrackRss>> TBaseComputation;
private:
    class TChildStream : public TComputationValue<TChildStream> {
    public:
        using TBase = TComputationValue<TChildStream>;

        TChildStream(TMemoryUsageInfo* memInfo, const TSwitchHandler& handler,
            TComputationContext& ctx, const TPagedUnboxedValueList* buffer)
            : TBase(memInfo)
            , Handler(handler)
            , Ctx(ctx)
            , Buffer(buffer)
        {}

        void Reset(bool isFinished) {
            BufferIndex = InputIndex = 0U;
            IsFinished = isFinished;
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            for (;;) {
                if (BufferIndex == Buffer->Size()) {
                    return IsFinished ? NUdf::EFetchStatus::Finish : NUdf::EFetchStatus::Yield;
                }

                auto current = (*Buffer)[BufferIndex];
                ui32 streamIndex = 0;
                if constexpr (IsInputVariant) {
                    streamIndex = current.GetVariantIndex();
                    current = current.Release().GetVariantItem();
                }

                for (; InputIndex < Handler.InputIndices.size(); ++InputIndex) {
                    if (Handler.InputIndices[InputIndex] == streamIndex) {
                        if (Handler.InputIndices.size() > 1) {
                            current = Ctx.HolderFactory.CreateVariantHolder(current.Release(), InputIndex);
                        }

                        result = std::move(current);
                        ++InputIndex;
                        return NUdf::EFetchStatus::Ok;
                    }
                }

                InputIndex = 0;
                ++BufferIndex;
            }
        }

        const TSwitchHandler Handler;
        TComputationContext& Ctx;
        const TPagedUnboxedValueList* const Buffer;
        ui32 BufferIndex = 0U;
        ui32 InputIndex = 0U;
        bool IsFinished = false;
    };

    class TValueBase : public TState {
    public:
        void Add(NUdf::TUnboxedValue&& item) {
            Buffer.Add(std::move(item));
        }

        void Reset() {
            if (const auto size = Buffer.Size()) {
                MKQL_SET_MAX_STAT(Ctx.Stats, Switch_MaxRowsCount, static_cast<i64>(size));
                MKQL_INC_STAT(Ctx.Stats, Switch_FlushesCount);
            }

            ChildReadIndex = 0U;
            for (const auto& stream : ChildrenInStreams) {
                stream->Reset(NUdf::EFetchStatus::Finish == InputStatus);
            }
        }

        bool Get(NUdf::TUnboxedValue& result) {
            if (ChildrenOutStreams[ChildReadIndex].Fetch(result) == NUdf::EFetchStatus::Ok) {
                return true;
            }

            if (++ChildReadIndex == Handlers.size()) {
                Buffer.Clear();
            }

            return false;
        }

    protected:
        TValueBase(TMemoryUsageInfo* memInfo, const TSwitchHandlersList& handlers, TComputationContext& ctx)
            : TState(memInfo, handlers.size())
            , Handlers(handlers)
            , Buffer(ctx.HolderFactory.GetPagePool())
            , Ctx(ctx)
        {
            ChildrenInStreams.reserve(Handlers.size());
            ChildrenOutStreams.reserve(Handlers.size());
            for (const auto& handler : Handlers) {
                const auto stream = Ctx.HolderFactory.Create<TChildStream>(handler, Ctx, &Buffer);
                ChildrenInStreams.emplace_back(static_cast<TChildStream*>(stream.AsBoxed().Get()));
                handler.Item->SetValue(Ctx, stream);
                ChildrenOutStreams.emplace_back(handler.NewItem->GetValue(Ctx));
            }
        }

        const TSwitchHandlersList Handlers;
        TPagedUnboxedValueList Buffer;
        TComputationContext& Ctx;
        std::vector<NUdf::TRefCountedPtr<TChildStream>, TMKQLAllocator<NUdf::TRefCountedPtr<TChildStream>>> ChildrenInStreams;
        TUnboxedValueVector ChildrenOutStreams;
    };

    class TValue : public TValueBase {
    public:
        TValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream,
            const TSwitchHandlersList& handlers, ui64 memLimit, TComputationContext& ctx)
            : TValueBase(memInfo, handlers, ctx)
            , Stream(std::move(stream)), MemLimit(memLimit)
        {}

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            for (;;) {
                if (this->ChildReadIndex == this->Handlers.size()) {
                    switch (this->InputStatus) {
                        case NUdf::EFetchStatus::Ok: break;
                        case NUdf::EFetchStatus::Yield:
                            this->InputStatus = NUdf::EFetchStatus::Ok;
                            return NUdf::EFetchStatus::Yield;
                        case NUdf::EFetchStatus::Finish:
                            return NUdf::EFetchStatus::Finish;
                    }

                    const auto initUsage = this->MemLimit ? this->Ctx.HolderFactory.GetMemoryUsed() : 0ULL;

                    do {
                        NUdf::TUnboxedValue current;
                        this->InputStatus = this->Stream.Fetch(current);
                        if (NUdf::EFetchStatus::Ok != this->InputStatus) {
                            break;
                        }
                        this->Add(std::move(current));
                    } while (!this->Ctx.template CheckAdjustedMemLimit<TrackRss>(this->MemLimit, initUsage));

                    this->Reset();
                }

                if (!this->Get(result)) {
                    continue;
                }

                const auto& handler = this->Handlers[this->ChildReadIndex];
                if (const auto offset = handler.ResultVariantOffset) {
                    ui32 localIndex = 0;
                    if (handler.IsOutputVariant) {
                        localIndex = result.GetVariantIndex();
                        result = result.Release().GetVariantItem();
                    }

                    result = this->Ctx.HolderFactory.CreateVariantHolder(result.Release(), *offset + localIndex);
                }

                return NUdf::EFetchStatus::Ok;
            }
        }

        const NUdf::TUnboxedValue Stream;
        const ui64 MemLimit;
    };

#ifndef MKQL_DISABLE_CODEGEN
    class TCodegenValue : public TStreamCodegenSelfStateValue<TValueBase> {
    public:
    using TFetchPtr = typename TStreamCodegenSelfStateValue<TValueBase>::TFetchPtr;
        TCodegenValue(TMemoryUsageInfo* memInfo, TFetchPtr fetch, TComputationContext* ctx, NUdf::TUnboxedValue&& stream, const TSwitchHandlersList& handlers)
            : TStreamCodegenSelfStateValue<TValueBase>(memInfo, fetch, ctx, std::move(stream), handlers, *ctx)
        {}
    };
#endif
public:
    TSwitchWrapper(TComputationMutables& mutables, IComputationNode* stream, ui64 memLimit, TSwitchHandlersList&& handlers)
        : TBaseComputation(mutables)
        , Stream(stream)
        , MemLimit(memLimit)
        , Handlers(std::move(handlers))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Switch)
            return ctx.HolderFactory.Create<TCodegenValue>(Switch, &ctx, Stream->GetValue(ctx), Handlers);
#endif
        return ctx.HolderFactory.Create<TValue>(Stream->GetValue(ctx), Handlers, MemLimit, ctx);
    }


private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream);
        for (const auto& handler : Handlers) {
            this->Own(handler.Item);
            this->DependsOn(handler.NewItem);
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    class TLLVMFieldsStructureForValueBase: public TLLVMFieldsStructureForState {
    private:
        using TBase = TLLVMFieldsStructureForState;
    protected:
        using TBase::Context;
    public:
        std::vector<llvm::Type*> GetFieldsArray() {
            std::vector<llvm::Type*> result = TBase::GetFields();
            return result;
        }

        TLLVMFieldsStructureForValueBase(llvm::LLVMContext& context)
            : TBase(context) {
        }
    };

    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        SwitchFunc = GenerateSwitch(codegen);
        codegen.ExportSymbol(SwitchFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (SwitchFunc)
            Switch = reinterpret_cast<TSwitchPtr>(codegen.GetPointerToFunction(SwitchFunc));
    }

    Function* GenerateSwitch(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = this->MakeName("Fetch");
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto containerType = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ? static_cast<Type*>(ptrValueType) : static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt32Ty(context);
        TLLVMFieldsStructureForValueBase fieldsStruct(context);
        const auto stateType = StructType::get(context, fieldsStruct.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, statePtrType, ptrValueType}, false);

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

        const auto indexPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { fieldsStruct.This(), fieldsStruct.GetIndex() }, "index_ptr", block);

        const auto itemPtr = new AllocaInst(valueType, 0U, "item_ptr", block);
        new StoreInst(ConstantInt::get(valueType, 0), itemPtr, block);

        BranchInst::Create(more, block);

        block = more;

        const auto index = new LoadInst(indexType, indexPtr, "index", block);
        const auto empty = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, index, ConstantInt::get(index->getType(), Handlers.size()), "empty", block);

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

            const auto statusPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { fieldsStruct.This(), fieldsStruct.GetStatus() }, "last", block);

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

            const auto used = GetMemoryUsed(MemLimit, ctx, block);

            const auto stream = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
                new LoadInst(valueType, containerArg, "load_container", false, block) : static_cast<Value*>(containerArg);

            BranchInst::Create(loop, block);

            block = loop;

            const auto fetch = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, stream, codegen, block, itemPtr);
            new StoreInst(fetch, statusPtr, block);

            const auto ok = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, fetch, ConstantInt::get(fetch->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), "ok", block);

            BranchInst::Create(good, done, ok, block);

            block = good;

            const auto addFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TValueBase::Add));
            const auto addType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), itemPtr->getType()}, false);
            const auto addPtr = CastInst::Create(Instruction::IntToPtr, addFunc, PointerType::getUnqual(addType), "add", block);
            CallInst::Create(addType, addPtr, {stateArg, itemPtr}, "", block);

            const auto check = CheckAdjustedMemLimit<TrackRss>(MemLimit, used, ctx, block);
            BranchInst::Create(done, loop, check, block);

            block = done;

            const auto resetFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TValueBase::Reset));
            const auto resetType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType()}, false);
            const auto resetPtr = CastInst::Create(Instruction::IntToPtr, resetFunc, PointerType::getUnqual(resetType), "reset", block);
            CallInst::Create(resetType, resetPtr, {stateArg}, "", block);

            BranchInst::Create(more, block);
        }

        {
            block = full;

            const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
            const auto stub = BasicBlock::Create(context, "stub", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            ReturnInst::Create(context, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), exit);
            new UnreachableInst(context, stub);

            const auto nextFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TValueBase::Get));
            const auto nextType = FunctionType::get(Type::getInt1Ty(context), {stateArg->getType(), valuePtr->getType()}, false);
            const auto nextPtr = CastInst::Create(Instruction::IntToPtr, nextFunc, PointerType::getUnqual(nextType), "next", block);
            const auto has = CallInst::Create(nextType, nextPtr, {stateArg, valuePtr}, "has", block);

            BranchInst::Create(good, more, has, block);

            block = good;

            const auto choise = SwitchInst::Create(index, stub, Handlers.size(), block);

            for (ui32 i = 0U; i < Handlers.size(); ++i) {
                const auto idx = ConstantInt::get(indexType, i);
                if (const auto offset = Handlers[i].ResultVariantOffset) {
                    const auto var = BasicBlock::Create(context, (TString("var_") += ToString(i)).c_str(), ctx.Func);
                    choise->addCase(idx, var);
                    block = var;

                    const auto output = new LoadInst(valueType, valuePtr, "output", block);
                    ValueRelease(Handlers[i].Kind, output, ctx, block);

                    const auto unpack = Handlers[i].IsOutputVariant ? GetVariantParts(output, ctx, block) : std::make_pair(ConstantInt::get(indexType, 0), output);
                    const auto reindex = BinaryOperator::CreateAdd(unpack.first, ConstantInt::get(indexType, *offset), "reindex", block);
                    const auto variant = MakeVariant(unpack.second, reindex, ctx, block);
                    new StoreInst(variant, valuePtr, block);

                    ValueAddRef(EValueRepresentation::Any, variant, ctx, block);
                    ReturnInst::Create(context, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), block);
                } else {
                    choise->addCase(idx, exit);
                }
            }
        }

        return ctx.Func;
    }

    using TSwitchPtr = typename TCodegenValue::TFetchPtr;

    Function* SwitchFunc = nullptr;

    TSwitchPtr Switch = nullptr;
#endif

    IComputationNode *const Stream;
    const ui64 MemLimit;
    const TSwitchHandlersList Handlers;
};

}

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

        const auto itemType = type->IsFlow() ?
            AS_TYPE(TFlowType, callable.GetInput(i + 2))->GetItemType():
            AS_TYPE(TStreamType, callable.GetInput(i + 2))->GetItemType();
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
        if (isInputVariant && trackRss) {
            return new TSwitchFlowWrapper<true, true>(ctx.Mutables, kind, stream, memLimit, std::move(handlers));
        } else if (isInputVariant) {
            return new TSwitchFlowWrapper<true, false>(ctx.Mutables, kind, stream, memLimit, std::move(handlers));
        } else if (trackRss) {
            return new TSwitchFlowWrapper<false, true>(ctx.Mutables, kind, stream, memLimit, std::move(handlers));
        } else {
            return new TSwitchFlowWrapper<false, false>(ctx.Mutables, kind, stream, memLimit, std::move(handlers));
        }
    } else if (type->IsStream()) {
        const bool isInputVariant = AS_TYPE(TStreamType, callable.GetInput(0))->GetItemType()->IsVariant();
        if (isInputVariant && trackRss) {
            return new TSwitchWrapper<true, true>(ctx.Mutables, stream, memLimit, std::move(handlers));
        } else if (isInputVariant) {
            return new TSwitchWrapper<true, false>(ctx.Mutables, stream, memLimit, std::move(handlers));
        } else if (trackRss) {
            return new TSwitchWrapper<false, true>(ctx.Mutables, stream, memLimit, std::move(handlers));
        } else {
            return new TSwitchWrapper<false, false>(ctx.Mutables, stream, memLimit, std::move(handlers));
        }
    }

    THROW yexception() << "Expected flow or stream.";
}

}
}
