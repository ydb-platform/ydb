#include "mkql_squeeze_to_list.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_llvm_base.h>  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TSqueezeToListWrapper: public TStatefulFlowCodegeneratorNode<TSqueezeToListWrapper> {
    using TBase = TStatefulFlowCodegeneratorNode<TSqueezeToListWrapper>;
public:
    class TState: public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;
    public:
        TState(TMemoryUsageInfo* memInfo, ui64 limit)
            : TBase(memInfo), Limit(limit) {
        }

        NUdf::TUnboxedValuePod Pull(TComputationContext& ctx) {
            if (Accumulator.empty())
                return ctx.HolderFactory.GetEmptyContainerLazy();

            NUdf::TUnboxedValue* items = nullptr;
            const auto list = ctx.HolderFactory.CreateDirectArrayHolder(Accumulator.size(), items);
            std::move(Accumulator.begin(), Accumulator.end(), items);
            Accumulator.clear();
            return list;
        }

        bool Put(NUdf::TUnboxedValuePod value) {
            Accumulator.emplace_back(std::move(value));
            return Limit != 0 && Limit <= Accumulator.size();
        }
    private:
        const ui64 Limit;
        TUnboxedValueDeque Accumulator;
    };

    TSqueezeToListWrapper(TComputationMutables& mutables, IComputationNode* flow, IComputationNode* limit)
        : TBase(mutables, flow, EValueRepresentation::Boxed, EValueRepresentation::Any)
        , Flow(flow), Limit(limit) {
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return NUdf::TUnboxedValuePod::MakeFinish();
        } else if (!state.HasValue()) {
            MakeState(ctx, Limit->GetValue(ctx).GetOrDefault(std::numeric_limits<ui64>::max()), state);
        }

        while (const auto statePtr = static_cast<TState*>(state.AsBoxed().Get())) {
            if (auto item = Flow->GetValue(ctx); item.IsYield()) {
                return item.Release();
            } else if (item.IsFinish() || statePtr->Put(item.Release())) {
                const auto list = statePtr->Pull(ctx);
                state = NUdf::TUnboxedValuePod::MakeFinish();
                return list;
            }
        }
        Y_UNREACHABLE();
    }

#ifndef MKQL_DISABLE_CODEGEN
    class TLLVMFieldsStructureForState: public TLLVMFieldsStructure<TComputationValue<TState>> {
    private:
        using TBase = TLLVMFieldsStructure<TComputationValue<TState>>;
        llvm::PointerType* StructPtrType;
    protected:
        using TBase::Context;
    public:
        std::vector<llvm::Type*> GetFieldsArray() {
            std::vector<llvm::Type*> result = TBase::GetFields();
            return result;
        }

        TLLVMFieldsStructureForState(llvm::LLVMContext& context)
            : TBase(context)
            , StructPtrType(PointerType::getUnqual(StructType::get(context))) {
        }
    };

    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);

        TLLVMFieldsStructureForState fieldsStruct(context);
        const auto stateType = StructType::get(context, fieldsStruct.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        BranchInst::Create(make, main, IsInvalid(statePtr, block), block);
        block = make;

        const auto value = GetNodeValue(Limit, ctx, block);
        const auto limit = SelectInst::Create(IsExists(value, block), GetterFor<ui64>(value, context, block), ConstantInt::get(Type::getInt64Ty(context), std::numeric_limits<ui64>::max()), "limit", block);

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TSqueezeToListWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), limit->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, limit, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto plus = BasicBlock::Create(context, "plus", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        const auto result = PHINode::Create(valueType, 3U, "result", over);

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        result->addIncoming(GetFinish(context), block);

        BranchInst::Create(over, more, IsFinish(state, block), block);

        block = more;

        const auto item = GetNodeValue(Flow, ctx, block);
        result->addIncoming(GetYield(context), block);

        const auto choise = SwitchInst::Create(item, plus, 2U, block);
        choise->addCase(GetFinish(context), done);
        choise->addCase(GetYield(context), over);

        block = plus;

        const auto push = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Put));

        const auto arg = WrapArgumentForWindows(item, ctx, block);

        const auto pushType = FunctionType::get(Type::getInt1Ty(context), {stateArg->getType(), arg->getType()}, false);
        const auto pushPtr = CastInst::Create(Instruction::IntToPtr, push, PointerType::getUnqual(pushType), "push", block);
        const auto stop = CallInst::Create(pushType, pushPtr, {stateArg, arg}, "stop", block);

        BranchInst::Create(done, more, stop, block);

        block = done;

        const auto pull = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Pull));

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto pullType = FunctionType::get(valueType, {stateArg->getType(), ctx.Ctx->getType()}, false);
            const auto pullPtr = CastInst::Create(Instruction::IntToPtr, pull, PointerType::getUnqual(pullType), "pull", block);
            const auto list = CallInst::Create(pullType, pullPtr, {stateArg, ctx.Ctx}, "list", block);
            UnRefBoxed(state, ctx, block);
            result->addIncoming(list, block);
        } else {
            const auto ptr = new AllocaInst(valueType, 0U, "ptr", block);
            const auto pullType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType(), ptr->getType(), ctx.Ctx->getType()}, false);
            const auto pullPtr = CastInst::Create(Instruction::IntToPtr, pull, PointerType::getUnqual(pullType), "pull", block);
            CallInst::Create(pullType, pullPtr, {stateArg, ptr, ctx.Ctx}, "", block);
            const auto list = new LoadInst(valueType, ptr, "list", block);
            UnRefBoxed(state, ctx, block);
            result->addIncoming(list, block);
        }

        new StoreInst(GetFinish(context), statePtr, block);
        BranchInst::Create(over, block);

        block = over;
        return result;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, const ui64 limit, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(limit);
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            this->DependsOn(flow, Limit);
        }
    }

    IComputationNode* const Flow;
    IComputationNode* const Limit;
};

}

IComputationNode* WrapSqueezeToList(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected pair of arguments.");
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    const auto limit = LocateNode(ctx.NodeLocator, callable, 1);
    return new TSqueezeToListWrapper(ctx.Mutables, flow, limit);
}

}
}
