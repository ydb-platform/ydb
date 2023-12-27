#include "yql_mkql_input.h"
#include "yql_mkql_table.h"

#include <ydb/library/yql/providers/yt/codec/yt_codec_io.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <algorithm>
#include <functional>
#include <array>

namespace NYql {

using namespace NKikimr::NMiniKQL;

class TInputStateBase: public TComputationValue<TInputStateBase> {
public:
    TInputStateBase(TMemoryUsageInfo* memInfo,
        const TMkqlIOSpecs& specs, NYT::IReaderImplBase* input, TComputationContext& ctx,
        const std::array<IComputationExternalNode*, 5>& argNodes)
        : TComputationValue<TInputStateBase>(memInfo)
        , SpecsCache_(specs, ctx.HolderFactory)
        , TableState_(specs.TableNames, specs.TableOffsets, ctx, argNodes)
        , Input_(input)
        , IsValid_(input->IsValid())
    {
    }
    virtual ~TInputStateBase() = default;

    NUdf::TUnboxedValuePod FetchRecord() {
        if (AtStart_) {
            UpdateTableState(true);
        } else {
            ReadNext();
        }
        AtStart_ = false;

        if (!IsValid_) {
            return NUdf::TUnboxedValuePod();
        }

        const size_t tableIndex = Input_->GetTableIndex();
        NUdf::TUnboxedValue result = GetCurrent(tableIndex);

        auto& specs = SpecsCache_.GetSpecs();
        if (!specs.InputGroups.empty()) {
            result = SpecsCache_.GetHolderFactory().CreateVariantHolder(result.Release(), specs.InputGroups.at(tableIndex));
        }
        return result.Release().MakeOptional();
    }

protected:
    virtual NUdf::TUnboxedValue GetCurrent(size_t tableIndex) = 0;

    void ReadNext() {
        if (Y_LIKELY(IsValid_)) {
            Input_->Next();
            IsValid_ = Input_->IsValid();
            bool keySwitch = false;
            if (!IsValid_) {
                Input_->NextKey();
                if (Input_->IsValid()) {
                    Input_->Next();
                    keySwitch = true;
                    IsValid_ = Input_->IsValid();
                }
            }
            UpdateTableState(keySwitch);
        }
    }

    void UpdateTableState(bool keySwitch) {
        if (IsValid_) {
            TableState_.Update(Input_->GetTableIndex(), Input_->GetRowIndex() + 1, keySwitch);
        } else {
            TableState_.Reset();
        }
    }

protected:
    TMkqlIOCache SpecsCache_;
    TTableState TableState_;
    NYT::IReaderImplBase* Input_;
    bool IsValid_;
    bool AtStart_ = true;
};

class TYamrInputState: public TInputStateBase {
public:
    TYamrInputState(TMemoryUsageInfo* memInfo, const TMkqlIOSpecs& specs, NYT::IYaMRReaderImpl* input, TComputationContext& ctx,
        const std::array<IComputationExternalNode*, 5>& argNodes)
        : TInputStateBase(memInfo, specs, input, ctx, argNodes)
    {
    }

protected:
    NUdf::TUnboxedValue GetCurrent(size_t tableIndex) final {
        return NYql::DecodeYamr(SpecsCache_, tableIndex, static_cast<NYT::IYaMRReaderImpl*>(Input_)->GetRow());
    }
};

class TYtInputState: public TInputStateBase {
public:
    TYtInputState(TMemoryUsageInfo* memInfo, const TMkqlIOSpecs& specs, TMkqlReaderImpl* input, TComputationContext& ctx,
        const std::array<IComputationExternalNode*, 5>& argNodes)
        : TInputStateBase(memInfo, specs, input, ctx, argNodes)
    {
    }

protected:
    NUdf::TUnboxedValue GetCurrent(size_t tableIndex) final {
        Y_UNUSED(tableIndex);
        return static_cast<TMkqlReaderImpl*>(Input_)->GetRow();
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TYtInputWrapper : public TMutableComputationNode<TYtInputWrapper> {
    typedef TMutableComputationNode<TYtInputWrapper> TBaseComputation;
public:
    TYtInputWrapper(TComputationMutables& mutables, const TMkqlIOSpecs& specs, NYT::IReaderImplBase* input,
        std::array<IComputationExternalNode*, 5>&& argNodes, TComputationNodePtrVector&& dependentNodes)
        : TBaseComputation(mutables)
        , Spec_(specs)
        , Input_(input)
        , ArgNodes_(std::move(argNodes))
        , DependentNodes_(std::move(dependentNodes))
        , StateIndex_(mutables.CurValueIndex++)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto& state = ctx.MutableValues[StateIndex_];
        if (!state.HasValue()) {
            MakeState(ctx, state);
        }

        return static_cast<TInputStateBase&>(*state.AsBoxed()).FetchRecord();
    }
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        if (const auto mkqlReader = dynamic_cast<TMkqlReaderImpl*>(Input_)) {
            state = ctx.HolderFactory.Create<TYtInputState>(Spec_, mkqlReader, ctx, ArgNodes_);
        } else if (const auto yamrReader = dynamic_cast<NYT::IYaMRReaderImpl*>(Input_)) {
            state = ctx.HolderFactory.Create<TYamrInputState>(Spec_, yamrReader, ctx, ArgNodes_);
        }
    }

    void RegisterDependencies() const final {
        std::for_each(ArgNodes_.cbegin(), ArgNodes_.cend(), std::bind(&TYtInputWrapper::Own, this, std::placeholders::_1));
        std::for_each(DependentNodes_.cbegin(), DependentNodes_.cend(), std::bind(&TYtInputWrapper::DependsOn, this, std::placeholders::_1));
    }

    const TMkqlIOSpecs& Spec_;
    NYT::IReaderImplBase*const Input_;
    const std::array<IComputationExternalNode*, 5> ArgNodes_;
    const TComputationNodePtrVector DependentNodes_;
    const ui32 StateIndex_;
};

class TYtBaseInputWrapper  {
protected:
    TYtBaseInputWrapper(const TMkqlIOSpecs& specs, NYT::IReaderImplBase* input)
        : Spec_(specs), Input_(input)
    {}

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        std::array<IComputationExternalNode*, 5U> stub;
        stub.fill(nullptr);
        if (const auto mkqlReader = dynamic_cast<TMkqlReaderImpl*>(Input_)) {
            state = ctx.HolderFactory.Create<TYtInputState>(Spec_, mkqlReader, ctx, stub);
        } else if (const auto yamrReader = dynamic_cast<NYT::IYaMRReaderImpl*>(Input_)) {
            state = ctx.HolderFactory.Create<TYamrInputState>(Spec_, yamrReader, ctx, stub);
        }
    }
private:
    const TMkqlIOSpecs& Spec_;
    NYT::IReaderImplBase*const Input_;
};

class TYtFlowInputWrapper : public TStatefulFlowCodegeneratorNode<TYtFlowInputWrapper>, private TYtBaseInputWrapper {
using TBaseComputation = TStatefulFlowCodegeneratorNode<TYtFlowInputWrapper>;
public:
    TYtFlowInputWrapper(TComputationMutables& mutables, EValueRepresentation kind, const TMkqlIOSpecs& specs, NYT::IReaderImplBase* input)
        : TBaseComputation(mutables, this, kind, EValueRepresentation::Boxed), TYtBaseInputWrapper(specs, input)
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            MakeState(ctx, state);
        }

        if (const auto value = static_cast<TInputStateBase&>(*state.AsBoxed()).FetchRecord())
            return value;
        else
            return NUdf::TUnboxedValuePod::MakeFinish();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto structPtrType = PointerType::getUnqual(StructType::get(context));

        const auto stateType = StructType::get(context, {
            structPtrType,              // vtbl
            Type::getInt32Ty(context),  // ref
            Type::getInt16Ty(context),  // abi
            Type::getInt16Ty(context),  // reserved
            structPtrType               // meminfo
        });

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(static_cast<const TYtBaseInputWrapper*>(this))), structPtrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TYtFlowInputWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TInputStateBase::FetchRecord));
        const auto funcType = FunctionType::get(valueType, { statePtrType }, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funcType), "fetch_func", block);
        const auto fetch = CallInst::Create(funcType, funcPtr, { stateArg }, "fetch", block);
        const auto result = SelectInst::Create(IsExists(fetch, block), fetch, GetFinish(context), "result", block);

        return result;
    }
#endif
private:
    void RegisterDependencies() const final {}
};

class TYtWideInputWrapper : public TPairStateWideFlowCodegeneratorNode<TYtWideInputWrapper>, private TYtBaseInputWrapper {
using TBaseComputation = TPairStateWideFlowCodegeneratorNode<TYtWideInputWrapper>;
public:
    TYtWideInputWrapper(TComputationMutables& mutables, ui32 width, const TMkqlIOSpecs& specs, NYT::IReaderImplBase* input)
        : TBaseComputation(mutables, this, EValueRepresentation::Boxed, EValueRepresentation::Embedded)
        , TYtBaseInputWrapper(specs, input), Width(width)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, NUdf::TUnboxedValue& current, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.HasValue()) {
            MakeState(ctx, state);
        }

        if (const auto value = static_cast<TInputStateBase&>(*state.AsBoxed()).FetchRecord()) {
            const auto elements = value.GetElements();
            current = NUdf::TUnboxedValuePod(reinterpret_cast<ui64>(elements));
            for (ui32 i = 0U; i < Width; ++i)
                if (const auto out = *output++)
                    *out = elements[i];

            return EFetchResult::One;
        }

        return EFetchResult::Finish;
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, Value* currentPtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto arrayType = ArrayType::get(valueType, Width);
        const auto pointerType = PointerType::getUnqual(arrayType);
        const auto structPtrType = PointerType::getUnqual(StructType::get(context));
        const auto statusType = Type::getInt32Ty(context);

        const auto stateType = StructType::get(context, {
            structPtrType,              // vtbl
            Type::getInt32Ty(context),  // ref
            Type::getInt16Ty(context),  // abi
            Type::getInt16Ty(context),  // reserved
            structPtrType               // meminfo
        });

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto placeholder = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(statusType,  static_cast<const IComputationNode*>(this)->GetIndex() + 1U)}, "placeholder", &ctx.Func->getEntryBlock().back());

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(static_cast<const TYtBaseInputWrapper*>(this))), structPtrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TYtWideInputWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TInputStateBase::FetchRecord));
        const auto funcType = FunctionType::get(valueType, { statePtrType }, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funcType), "fetch_func", block);
        const auto fetch = CallInst::Create(funcType, funcPtr, { stateArg }, "fetch", block);

        const auto result = PHINode::Create(statusType, 2U, "result", done);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), block);

        BranchInst::Create(good, done, IsExists(fetch, block), block);

        block = good;

        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(pointerType, fetch, ctx.Codegen, block);
        const auto integer = CastInst::Create(Instruction::PtrToInt, elements, Type::getInt64Ty(context), "integer", block);
        const auto stored = SetterFor<ui64>(integer, context, block);
        new StoreInst(stored, currentPtr, block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);

        BranchInst::Create(done, block);

        block = done;

        TGettersList getters;
        getters.reserve(Width);
        for (ui32 i = 0U; i < Width; ++i) {
            getters.emplace_back([i, placeholder, pointerType, valueType, arrayType](const TCodegenContext& ctx, BasicBlock*& block) {
                auto& context = ctx.Codegen.GetContext();
                const auto current = new LoadInst(valueType, placeholder, (TString("current_") += ToString(i)).c_str(), block);
                const auto integer = GetterFor<ui64>(current, context, block);
                const auto pointer = CastInst::Create(Instruction::IntToPtr, integer, pointerType, (TString("pointer_") += ToString(i)).c_str(), block);
                const auto indexType = Type::getInt32Ty(context);
                const auto ptr = GetElementPtrInst::CreateInBounds(arrayType, pointer, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), block);
                const auto load = new LoadInst(valueType, ptr, (TString("load_") += ToString(i)).c_str(), block);
                return load;
            });
        }

        return {result, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {}
    const ui32 Width;
};

IComputationNode* WrapYtInput(TCallable& callable, const TComputationNodeFactoryContext& ctx, const TMkqlIOSpecs& specs, NYT::IReaderImplBase* input)
{
    if (!callable.GetInputsCount()) {
        if (const auto type = AS_TYPE(TFlowType, callable.GetType()->GetReturnType())->GetItemType(); type->IsTuple())
            return new TYtWideInputWrapper(ctx.Mutables, AS_TYPE(TTupleType, type)->GetElementsCount(), specs, input);
        else if (type->IsMulti())
            return new TYtWideInputWrapper(ctx.Mutables, AS_TYPE(TMultiType, type)->GetElementsCount(), specs, input);
        else if (type->IsStruct())
            return new TYtFlowInputWrapper(ctx.Mutables, GetValueRepresentation(type), specs, input);

        THROW yexception() << "Expected tuple or struct as flow item type.";
    }

    YQL_ENSURE(callable.GetInputsCount() >= 5, "Expected at least 5 args");

    std::array<IComputationExternalNode*, 5> argNodes;
    for (size_t i = 0; i < argNodes.size(); ++i) {
        argNodes[i] = LocateExternalNode(ctx.NodeLocator, callable, i, false);
    }

    TComputationNodePtrVector dependentNodes(callable.GetInputsCount() - argNodes.size());
    for (ui32 i = argNodes.size(); i < callable.GetInputsCount(); ++i) {
        dependentNodes[i - argNodes.size()] = LocateNode(ctx.NodeLocator, callable, i);
    }

    return new TYtInputWrapper(ctx.Mutables, specs, input, std::move(argNodes), std::move(dependentNodes));
}

} // NYql
