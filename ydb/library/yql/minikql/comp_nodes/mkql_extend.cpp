#include "mkql_extend.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_llvm_base.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <util/string/cast.h>
#include <queue>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TState : public TComputationValue<TState> {
public:
    ssize_t Index;
    std::queue<ssize_t> Queue;

    TState(TMemoryUsageInfo* memInfo, ssize_t count)
        : TComputationValue<TState>(memInfo)
    {
        while (count)
            Queue.push(--count);
        Index = Queue.front();
    }

    void NextFlow() {
        Queue.push(Queue.front());
        Queue.pop();
        Index = Queue.front();
    }

    void FlowOver() {
        Queue.pop();
        Index = Queue.empty() ? -1LL : Queue.front();
    }
};
#ifndef MKQL_DISABLE_CODEGEN
    class TLLVMFieldsStructureState: public TLLVMFieldsStructure<TComputationValue<TState>> {
    private:
        using TBase = TLLVMFieldsStructure<TComputationValue<TState>>;
        llvm::IntegerType*const IndexType;
    protected:
        using TBase::Context;
    public:
        std::vector<llvm::Type*> GetFieldsArray() {
            auto result = TBase::GetFields();
            result.emplace_back(IndexType);
            return result;
        }

        llvm::Constant* GetIndex() {
            return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount());
        }

        TLLVMFieldsStructureState(llvm::LLVMContext& context)
            : TBase(context), IndexType(Type::getInt64Ty(Context))
        {}
    };
#endif

class TExtendWideFlowWrapper : public TStatefulWideFlowCodegeneratorNode<TExtendWideFlowWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TExtendWideFlowWrapper>;
public:
    TExtendWideFlowWrapper(TComputationMutables& mutables, TComputationWideFlowNodePtrVector&& flows, size_t width)
        : TBaseComputation(mutables, this, EValueRepresentation::Boxed)
        , Flows_(std::move(flows)), Width_(width)
    {
#ifdef MKQL_DISABLE_CODEGEN
        Y_UNUSED(Width_);
#endif
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto& s = GetState(state, ctx);
        while (s.Index >= 0) {
            switch (Flows_[s.Index]->FetchValues(ctx, output)) {
                case EFetchResult::One:
                    return EFetchResult::One;
                case EFetchResult::Yield:
                    s.NextFlow();
                    return EFetchResult::Yield;
                case EFetchResult::Finish:
                    s.FlowOver();
                    break;
            }
        }
        return EFetchResult::Finish;
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt64Ty(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto arrayType = ArrayType::get(valueType, Width_);

        const auto arrayPtr = new AllocaInst(arrayType, 0, "array_ptr", &ctx.Func->getEntryBlock().back());

        TLLVMFieldsStructureState stateFields(context);

        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);
        const auto funcType = FunctionType::get(Type::getVoidTy(context), {statePtrType}, false);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TExtendWideFlowWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        const auto indexPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetIndex() }, "index_ptr", block);

        BranchInst::Create(loop, main);

        block = loop;

        const auto index = new LoadInst(indexType, indexPtr, "index", block);

        const auto result = PHINode::Create(statusType, Flows_.size() + 2U, "result", done);

        const auto select = SwitchInst::Create(index, done, Flows_.size(), block);
        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), block);

        for (auto i = 0U; i < Flows_.size(); ++i) {
            const auto flow = BasicBlock::Create(context, (TString("flow_") += ToString(i)).c_str(), ctx.Func);
            const auto save = BasicBlock::Create(context, (TString("save_") += ToString(i)).c_str(), ctx.Func);
            select->addCase(ConstantInt::get(indexType, i), flow);

            block = flow;

            const auto getres = GetNodeValues(Flows_[i], ctx, block);

            const auto way = SwitchInst::Create(getres.first, save, 2U, block);
            way->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), over);
            way->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), next);

            block = save;

            Value* values = UndefValue::get(arrayType);
            for (auto idx = 0U; idx < Width_; ++idx) {
                const auto value = getres.second[idx](ctx, block);
                values = InsertValueInst::Create(values, value, {idx}, (TString("value_") += ToString(idx)).c_str(), block);
            }
            new StoreInst(values, arrayPtr, block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);
            BranchInst::Create(done, block);
        }

        block = next;

        const auto nextFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::NextFlow));
        const auto nextPtr = CastInst::Create(Instruction::IntToPtr, nextFunc, PointerType::getUnqual(funcType), "next_ptr", block);
        CallInst::Create(funcType, nextPtr, {stateArg}, "", block);
        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

        BranchInst::Create(done, block);

        block = over;

        const auto overFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::FlowOver));
        const auto overPtr = CastInst::Create(Instruction::IntToPtr, overFunc, PointerType::getUnqual(funcType), "over_ptr", block);
        CallInst::Create(funcType, overPtr, {stateArg}, "", block);

        BranchInst::Create(loop, block);

        block = done;

        ICodegeneratorInlineWideNode::TGettersList getters(Width_);
        for (size_t idx = 0U; idx < getters.size(); ++idx) {
            getters[idx] = [idx, valueType, arrayType, arrayPtr, indexType](const TCodegenContext& ctx, BasicBlock*& block) {
                Y_UNUSED(ctx);
                const auto valuePtr = GetElementPtrInst::CreateInBounds(arrayType, arrayPtr, { ConstantInt::get(indexType, 0), ConstantInt::get(indexType, idx)}, "value_ptr", block);
                return new LoadInst(valueType, valuePtr, "value", block);
            };
        }
        return {result, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {
        for (auto& flow : Flows_) {
            FlowDependsOn(flow);
        }
    }

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(Flows_.size());
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue())
            MakeState(ctx, state);
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    const TComputationWideFlowNodePtrVector Flows_;
    const size_t Width_;
};

class TExtendFlowWrapper : public TStatefulFlowCodegeneratorNode<TExtendFlowWrapper> {
    typedef TStatefulFlowCodegeneratorNode<TExtendFlowWrapper> TBaseComputation;
public:
     TExtendFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, TComputationNodePtrVector&& flows)
        : TBaseComputation(mutables, this, kind, EValueRepresentation::Boxed), Flows(flows)
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        auto& s = GetState(state, ctx);
        while (s.Index >= 0) {
            auto item = Flows[s.Index]->GetValue(ctx);
            if (item.IsYield())
                s.NextFlow();
            if (item.IsFinish())
                s.FlowOver();
            else
                return item.Release();
        }
        return NUdf::TUnboxedValuePod::MakeFinish();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt64Ty(context);

        TLLVMFieldsStructureState stateFields(context);

        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());
        const auto statePtrType = PointerType::getUnqual(stateType);
        const auto funcType = FunctionType::get(Type::getVoidTy(context), {statePtrType}, false);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TExtendFlowWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        const auto indexPtr = GetElementPtrInst::CreateInBounds(stateType, stateArg, { stateFields.This(), stateFields.GetIndex() }, "index_ptr", block);

        BranchInst::Create(loop, main);

        block = loop;

        const auto index = new LoadInst(indexType, indexPtr, "index", block);

        const auto result = PHINode::Create(valueType, Flows.size() + 2U, "result", done);

        const auto select = SwitchInst::Create(index, done, Flows.size(), block);
        result->addIncoming(GetFinish(context), block);

        for (auto i = 0U; i < Flows.size(); ++i) {
            const auto flow = BasicBlock::Create(context, (TString("flow_") += ToString(i)).c_str(), ctx.Func);
            select->addCase(ConstantInt::get(indexType, i), flow);

            block = flow;
            const auto item = GetNodeValue(Flows[i], ctx, block);
            result->addIncoming(item, block);
            const auto way = SwitchInst::Create(item, done, 2U, block);
            way->addCase(GetFinish(context), over);
            way->addCase(GetYield(context), next);
        }

        block = next;

        const auto nextFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::NextFlow));
        const auto nextPtr = CastInst::Create(Instruction::IntToPtr, nextFunc, PointerType::getUnqual(funcType), "next_ptr", block);
        CallInst::Create(funcType, nextPtr, {stateArg}, "", block);
        result->addIncoming(GetYield(context), block);

        BranchInst::Create(done, block);

        block = over;

        const auto overFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::FlowOver));
        const auto overPtr = CastInst::Create(Instruction::IntToPtr, overFunc, PointerType::getUnqual(funcType), "over_ptr", block);
        CallInst::Create(funcType, overPtr, {stateArg}, "", block);

        BranchInst::Create(loop, block);

        block = done;
        return result;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(Flows.size());
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue())
            MakeState(ctx, state);
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    void RegisterDependencies() const final {
        std::for_each(Flows.cbegin(), Flows.cend(), std::bind(&TExtendFlowWrapper::FlowDependsOn, this, std::placeholders::_1));
    }

    const TComputationNodePtrVector Flows;
};

class TOrderedExtendWideFlowWrapper : public TStatefulWideFlowCodegeneratorNode<TOrderedExtendWideFlowWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TOrderedExtendWideFlowWrapper>;
public:
    TOrderedExtendWideFlowWrapper(TComputationMutables& mutables, TComputationWideFlowNodePtrVector&& flows, size_t width)
        : TBaseComputation(mutables, this, EValueRepresentation::Embedded)
        , Flows_(std::move(flows)), Width_(width)
    {
#ifdef MKQL_DISABLE_CODEGEN
        Y_UNUSED(Width_);
#endif
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        for (ui64 index = state.IsInvalid() ? 0ULL : state.Get<ui64>(); index < Flows_.size(); ++index) {
            if (const auto result = Flows_[index]->FetchValues(ctx, output); EFetchResult::Finish != result) {
                state = NUdf::TUnboxedValuePod(index);
                return result;
            }
        }

        return EFetchResult::Finish;
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt64Ty(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto arrayType = ArrayType::get(valueType, Width_);

        const auto arrayPtr = new AllocaInst(arrayType, 0, "array_ptr", &ctx.Func->getEntryBlock().back());

        TLLVMFieldsStructureState stateFields(context);

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto load = new LoadInst(valueType, statePtr, "load", block);
        const auto start = SelectInst::Create(IsInvalid(load, block), ConstantInt::get(indexType, 0ULL), GetterFor<ui64>(load, context, block), "start", block);
        const auto index = PHINode::Create(indexType, 2U, "index", main);
        index->addIncoming(start, block);

        BranchInst::Create(main, block);

        block = main;

        const auto result = PHINode::Create(statusType, Flows_.size() + 2U, "result", done);

        const auto select = SwitchInst::Create(index, done, Flows_.size(), block);
        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), block);

        for (auto i = 0U; i < Flows_.size(); ++i) {
            const auto flow = BasicBlock::Create(context, (TString("flow_") += ToString(i)).c_str(), ctx.Func);
            const auto save = BasicBlock::Create(context, (TString("save_") += ToString(i)).c_str(), ctx.Func);
            select->addCase(ConstantInt::get(indexType, i), flow);

            block = flow;

            const auto getres = GetNodeValues(Flows_[i], ctx, block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

            const auto way = SwitchInst::Create(getres.first, save, 2U, block);
            way->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), next);
            way->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), done);

            block = save;

            Value* values = UndefValue::get(arrayType);
            for (auto idx = 0U; idx < Width_; ++idx) {
                const auto value = getres.second[idx](ctx, block);
                values = InsertValueInst::Create(values, value, {idx}, (TString("value_") += ToString(idx)).c_str(), block);
            }
            new StoreInst(values, arrayPtr, block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);
            BranchInst::Create(done, block);
        }

        block = next;

        const auto plus = BinaryOperator::CreateAdd(index, ConstantInt::get(indexType, 1ULL), "plus", block);
        index->addIncoming(plus, block);
        BranchInst::Create(main, block);

        block = done;

        new StoreInst(SetterFor<ui64>(index, context, block), statePtr, block);

        ICodegeneratorInlineWideNode::TGettersList getters(Width_);
        for (size_t idx = 0U; idx < getters.size(); ++idx) {
            getters[idx] = [idx, valueType, arrayType, arrayPtr, indexType](const TCodegenContext& ctx, BasicBlock*& block) {
                Y_UNUSED(ctx);
                const auto valuePtr = GetElementPtrInst::CreateInBounds(arrayType, arrayPtr, { ConstantInt::get(indexType, 0), ConstantInt::get(indexType, idx)}, "value_ptr", block);
                return new LoadInst(valueType, valuePtr, "value", block);
            };
        }
        return {result, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {
        for (auto& flow : Flows_) {
            FlowDependsOn(flow);
        }
    }

    const TComputationWideFlowNodePtrVector Flows_;
    const size_t Width_;
};

class TOrderedExtendFlowWrapper : public TStatefulFlowCodegeneratorNode<TOrderedExtendFlowWrapper> {
using TBaseComputation = TStatefulFlowCodegeneratorNode<TOrderedExtendFlowWrapper>;
public:
     TOrderedExtendFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, TComputationNodePtrVector&& flows)
        : TBaseComputation(mutables, this, kind, EValueRepresentation::Embedded), Flows_(flows)
    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        for (ui64 index = state.IsInvalid() ? 0ULL : state.Get<ui64>(); index < Flows_.size(); ++index) {
            const auto item = Flows_[index]->GetValue(ctx);

            if (!item.IsFinish()) {
                state = NUdf::TUnboxedValuePod(index);
                return item;
            }
        }

        return NUdf::TUnboxedValuePod::MakeFinish();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt64Ty(context);

        const auto load = new LoadInst(valueType, statePtr, "load", block);
        const auto state = SelectInst::Create(IsInvalid(load, block), ConstantInt::get(indexType, 0ULL), GetterFor<ui64>(load, context, block), "index", block);

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(valueType, Flows_.size() + 1U, "result", done);

        const auto index = PHINode::Create(indexType, 2U, "index", main);
        index->addIncoming(state, block);
        BranchInst::Create(main, block);

        block = main;

        const auto select = SwitchInst::Create(index, done, Flows_.size(), block);
        result->addIncoming(GetFinish(context), block);

        for (auto i = 0U; i < Flows_.size(); ++i) {
            const auto flow = BasicBlock::Create(context, "flow", ctx.Func);
            select->addCase(ConstantInt::get(indexType, i), flow);

            block = flow;
            const auto item = GetNodeValue(Flows_[i], ctx, block);
            result->addIncoming(item, block);
            BranchInst::Create(next, done, IsFinish(item, block), block);
        }

        block = next;
        const auto plus = BinaryOperator::CreateAdd(index, ConstantInt::get(indexType, 1ULL), "plus", block);
        index->addIncoming(plus, block);
        BranchInst::Create(main, block);

        block = done;
        new StoreInst(SetterFor<ui64>(index, context, block), statePtr, block);
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        std::for_each(Flows_.cbegin(), Flows_.cend(), std::bind(&TOrderedExtendFlowWrapper::FlowDependsOn, this, std::placeholders::_1));
    }

    const TComputationNodePtrVector Flows_;
};

template <bool IsStream>
class TOrderedExtendWrapper : public TMutableCodegeneratorNode<TOrderedExtendWrapper<IsStream>> {
using TBaseComputation = TMutableCodegeneratorNode<TOrderedExtendWrapper<IsStream>>;
public:
    TOrderedExtendWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& lists)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Lists(std::move(lists))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        TUnboxedValueVector values;
        values.reserve(Lists.size());
        std::transform(Lists.cbegin(), Lists.cend(), std::back_inserter(values),
            std::bind(&IComputationNode::GetValue, std::placeholders::_1, std::ref(ctx))
        );

        return IsStream ?
            ctx.HolderFactory.ExtendStream(values.data(), values.size()):
            ctx.HolderFactory.ExtendList<false>(values.data(), values.size());
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto sizeType = Type::getInt64Ty(context);
        const auto size = ConstantInt::get(sizeType, Lists.size());

        const auto arrayType = ArrayType::get(valueType, Lists.size());
        const auto array = *this->Stateless || ctx.AlwaysInline ?
            new AllocaInst(arrayType, 0U, "array", &ctx.Func->getEntryBlock().back()):
            new AllocaInst(arrayType, 0U, "array", block);

        for (size_t i = 0U; i < Lists.size(); ++i) {
            const auto ptr = GetElementPtrInst::CreateInBounds(arrayType, array, {ConstantInt::get(sizeType, 0), ConstantInt::get(sizeType, i)}, (TString("ptr_") += ToString(i)).c_str(), block);
            GetNodeValue(ptr, Lists[i], ctx, block);
        }

        const auto factory = ctx.GetFactory();
        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(IsStream ? &THolderFactory::ExtendStream : &THolderFactory::ExtendList<false>));

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(valueType, {factory->getType(), array->getType(), size->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            const auto res = CallInst::Create(funType, funcPtr, {factory, array, size}, "res", block);
            return res;
        } else {
            const auto retPtr = new AllocaInst(valueType, 0U, "ret_ptr", block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {factory->getType(), retPtr->getType(), array->getType(), size->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            CallInst::Create(funType, funcPtr, {factory, retPtr, array, size}, "", block);
            const auto res = new LoadInst(valueType, retPtr, "res", block);
            return res;
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        std::for_each(Lists.cbegin(), Lists.cend(), std::bind(&TOrderedExtendWrapper::DependsOn, this, std::placeholders::_1));
    }

    const TComputationNodePtrVector Lists;
};

template<bool Ordered>
IComputationNode* WrapExtendT(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 1, "Expected at least 1 list");
    const auto type = callable.GetType()->GetReturnType();

    TComputationNodePtrVector flows;
    flows.reserve(callable.GetInputsCount());
    for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
        flows.emplace_back(LocateNode(ctx.NodeLocator, callable, i));
    }

    if (type->IsFlow()) {
        if (dynamic_cast<IComputationWideFlowNode*>(flows.front())) {
            const auto width = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));
            TComputationWideFlowNodePtrVector wideFlows;
            wideFlows.reserve(callable.GetInputsCount());
            for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
                wideFlows.emplace_back(dynamic_cast<IComputationWideFlowNode*>(flows[i]));
                MKQL_ENSURE_S(wideFlows.back());
            }
            if constexpr (Ordered)
                return new TOrderedExtendWideFlowWrapper(ctx.Mutables, std::move(wideFlows), width);
            else
                return new TExtendWideFlowWrapper(ctx.Mutables, std::move(wideFlows), width);
        }
        if constexpr (Ordered)
            return new TOrderedExtendFlowWrapper(ctx.Mutables, GetValueRepresentation(AS_TYPE(TFlowType, type)->GetItemType()), std::move(flows));
        else
            return new TExtendFlowWrapper(ctx.Mutables, GetValueRepresentation(AS_TYPE(TFlowType, type)->GetItemType()), std::move(flows));
    } else if (type->IsStream()) {
        return new TOrderedExtendWrapper<true>(ctx.Mutables, std::move(flows));
    } else if (type->IsList()) {
        return new TOrderedExtendWrapper<false>(ctx.Mutables, std::move(flows));
    }

    THROW yexception() << "Expected either flow, list or stream.";
}

}

IComputationNode* WrapExtend(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapExtendT<false>(callable, ctx);
}

IComputationNode* WrapOrderedExtend(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapExtendT<true>(callable, ctx);
}

}
}
