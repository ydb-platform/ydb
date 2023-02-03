#include "mkql_wide_top_sort.h"
#include "mkql_llvm_base.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/utils/cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

struct TMyValueCompare {
    TMyValueCompare(const TKeyTypes& types)
        : Types(types)
    {}

    int operator()(const bool* directions, const NUdf::TUnboxedValuePod* left, const NUdf::TUnboxedValuePod* right) const {
        return CompareValues(left, right, Types, directions);
    }

    const TKeyTypes& Types;
};

using TComparePtr = int(*)(const bool*, const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*);
using TCompareFunc = std::function<int(const bool*, const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)>;

class TState : public TComputationValue<TState> {
using TBase = TComputationValue<TState>;
public:
using TLLVMBase = TLLVMFieldsStructure<TComputationValue<TState>>;
private:
    using TStorage = std::vector<NUdf::TUnboxedValue, TMKQLAllocator<NUdf::TUnboxedValue, EMemorySubPool::Temporary>>;
    using TFields = std::vector<NUdf::TUnboxedValue*, TMKQLAllocator<NUdf::TUnboxedValue*, EMemorySubPool::Temporary>>;
    using TPointers = std::vector<NUdf::TUnboxedValuePod*, TMKQLAllocator<NUdf::TUnboxedValuePod*, EMemorySubPool::Temporary>>;

    size_t GetStorageSize() const {
        return std::max<size_t>(Count << 2ULL, 1ULL << 8ULL);
    }

    void ResetFields() {
        auto ptr = Tongue = Free.back();
        std::for_each(Indexes.cbegin(), Indexes.cend(), [&](ui32 index) { Fields[index] = static_cast<NUdf::TUnboxedValue*>(ptr++); });
    }
public:
    TState(TMemoryUsageInfo* memInfo, ui64 count, const bool* directons, size_t keyWidth, const TCompareFunc& compare, const std::vector<ui32>& indexes)
        : TBase(memInfo), Count(count), Indexes(indexes), Directions(directons, directons + keyWidth)
        , LessFunc(std::bind(std::less<int>(), std::bind(compare, Directions.data(), std::placeholders::_1, std::placeholders::_2), 0))
        , Storage(GetStorageSize() * Indexes.size()), Free(GetStorageSize(), nullptr), Fields(Indexes.size(), nullptr)
    {
        if (Count) {
            Full.reserve(GetStorageSize());
            auto ptr = Storage.data();
            std::generate(Free.begin(), Free.end(), [&ptr, this]() {
                const auto p = ptr;
                ptr += Indexes.size();
                return p;
            });
            ResetFields();
        } else
            InputStatus = EFetchResult::Finish;
    }

    NUdf::TUnboxedValue*const* GetFields() const {
        return Fields.data();
    }

    bool Push() {
        if (Full.size() + 1U == GetStorageSize()) {
            Free.pop_back();

            std::nth_element(Full.begin(), Full.begin() + Count, Full.end(), LessFunc);
            std::copy(Full.cbegin() + Count, Full.cend(), std::back_inserter(Free));
            Full.resize(Count);

            std::for_each(Free.cbegin(), Free.cend(), [this](NUdf::TUnboxedValuePod* ptr) {
                std::fill_n(static_cast<NUdf::TUnboxedValue*>(ptr), Indexes.size(), NUdf::TUnboxedValuePod());
            });
            Free.emplace_back(Tongue);
            Throat = nullptr;
        }

        if (Full.size() >= Count) {
            if (!Throat)
                Throat = *std::max_element(Full.cbegin(), Full.cend(), LessFunc);

            if (!LessFunc(Tongue, Throat))
                return false;
        }

        Full.emplace_back(Free.back());
        Free.pop_back();
        ResetFields();
        return true;
    }

    template<bool Sort>
    void Seal() {
        Free.clear();
        Free.shrink_to_fit();

        if (Full.size() > Count) {
            std::nth_element(Full.begin(), Full.begin() + Count, Full.end(), LessFunc);
            Full.resize(Count);
        }

        if constexpr (Sort)
            std::sort(Full.rbegin(), Full.rend(), LessFunc);
    }

    NUdf::TUnboxedValue* Extract() {
        if (Full.empty())
            return nullptr;

        const auto ptr = Full.back();
        Full.pop_back();
        return static_cast<NUdf::TUnboxedValue*>(ptr);
    }

    EFetchResult InputStatus = EFetchResult::One;
    NUdf::TUnboxedValuePod* Tongue = nullptr;
    NUdf::TUnboxedValuePod* Throat = nullptr;
private:
    const ui64 Count;
    const std::vector<ui32> Indexes;
    const std::vector<bool> Directions;
    const std::function<bool(const NUdf::TUnboxedValuePod*, const NUdf::TUnboxedValuePod*)> LessFunc;
    TStorage Storage;
    TPointers Free, Full;
    TFields Fields;
};

#ifndef MKQL_DISABLE_CODEGEN
class TLLVMFieldsStructureState: public TState::TLLVMBase {
private:
    using TBase = TState::TLLVMBase;
    llvm::IntegerType* ValueType;
    llvm::PointerType* PtrValueType;
    llvm::IntegerType* StatusType;
protected:
    using TBase::Context;
public:
    std::vector<llvm::Type*> GetFieldsArray() {
        std::vector<llvm::Type*> result = TBase::GetFields();
        result.emplace_back(StatusType); //status
        result.emplace_back(PtrValueType); //tongue
        result.emplace_back(PtrValueType); //throat
        result.emplace_back(Type::getInt64Ty(Context)); //count
        return result;
    }

    llvm::Constant* GetStatus() {
        return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 0);
    }

    llvm::Constant* GetTongue() {
        return ConstantInt::get(Type::getInt32Ty(Context), TBase::GetFieldsCount() + 1);
    }

    TLLVMFieldsStructureState(llvm::LLVMContext& context)
        : TBase(context)
        , ValueType(Type::getInt128Ty(Context))
        , PtrValueType(PointerType::getUnqual(ValueType))
        , StatusType(Type::getInt32Ty(Context)) {

    }
};
#endif

template<bool Sort>
class TWideTopWrapper: public TStatefulWideFlowCodegeneratorNode<TWideTopWrapper<Sort>>
#ifndef MKQL_DISABLE_CODEGEN
    , public ICodegeneratorRootNode
#endif
{
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideTopWrapper<Sort>>;
public:
    TWideTopWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, IComputationNode* count, TComputationNodePtrVector&& directions, TKeyTypes&& keyTypes, std::vector<ui32>&& indexes, std::vector<EValueRepresentation>&& representations)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed), Flow(flow), Count(count), Directions(std::move(directions)), KeyTypes(std::move(keyTypes)), Indexes(std::move(indexes)), Representations(std::move(representations))
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.HasValue()) {
            const auto count = Count->GetValue(ctx).Get<ui64>();
            std::vector<bool> dirs(Directions.size());
            std::transform(Directions.cbegin(), Directions.cend(), dirs.begin(), [&ctx](IComputationNode* dir){ return dir->GetValue(ctx).Get<bool>(); });
            MakeState(ctx, state, count, dirs.data());
        }

        if (const auto ptr = static_cast<TState*>(state.AsBoxed().Get())) {
            while (EFetchResult::Finish != ptr->InputStatus) {
                switch (ptr->InputStatus = Flow->FetchValues(ctx, ptr->GetFields())) {
                    case EFetchResult::One:
                        ptr->Push();
                        continue;
                    case EFetchResult::Yield:
                        return EFetchResult::Yield;
                    case EFetchResult::Finish:
                        ptr->Seal<Sort>();
                        break;
                }
            }

            if (auto extract = ptr->Extract()) {
                for (const auto index : Indexes)
                    if (const auto to = output[index])
                        *to = std::move(*extract++);
                    else
                        ++extract;
                return EFetchResult::One;
            }

            return EFetchResult::Finish;
        }

        Y_UNREACHABLE();
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen->GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto structPtrType = PointerType::getUnqual(StructType::get(context));
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt32Ty(ctx.Codegen->GetContext());

        TLLVMFieldsStructureState stateFields(context);
        const auto stateType = StructType::get(context, stateFields.GetFieldsArray());

        const auto statePtrType = PointerType::getUnqual(stateType);

        const auto outputPtrType = PointerType::getUnqual(ArrayType::get(valueType, Representations.size()));
        const auto outs = new AllocaInst(outputPtrType, 0U, "outs", &ctx.Func->getEntryBlock().back());

        ICodegeneratorInlineWideNode::TGettersList getters(Representations.size());

        for (auto i = 0U; i < getters.size(); ++i) {
            getters[Indexes[i]] = [i, outs, indexType](const TCodegenContext& ctx, BasicBlock*& block) {
                const auto values = new LoadInst(outs, "values", block);
                const auto pointer = GetElementPtrInst::CreateInBounds(values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), block);
                return new LoadInst(pointer, (TString("load_") += ToString(i)).c_str(), block);
            };
        }

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto more = BasicBlock::Create(context, "more", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto count = GetNodeValue(Count, ctx, block);
        const auto trunc = GetterFor<ui64>(count, context, block);

        const auto dirs = new AllocaInst(ArrayType::get(Type::getInt1Ty(context), Directions.size()), 0U, "dirs", block);
        for (auto i = 0U; i < Directions.size(); ++i) {
            const auto dir = GetNodeValue(Directions[i], ctx, block);
            const auto cut = GetterFor<bool>(dir, context, block);
            const auto ptr = GetElementPtrInst::CreateInBounds(dirs, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, "ptr", block);
            new StoreInst(cut, ptr, block);
        }

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TWideTopWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType(), trunc->getType(), dirs->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeFuncPtr, {self, ctx.Ctx, statePtr, trunc, dirs}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);
        BranchInst::Create(more, block);

        block = more;

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto full = BasicBlock::Create(context, "full", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);
        const auto result = PHINode::Create(statusType, 3U, "result", over);

        const auto statusPtr = GetElementPtrInst::CreateInBounds(stateArg, {stateFields.This(), stateFields.GetStatus()}, "last", block);
        const auto last = new LoadInst(statusPtr, "last", block);
        const auto finish = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, last, ConstantInt::get(last->getType(), static_cast<i32>(EFetchResult::Finish)), "finish", block);

        BranchInst::Create(full, loop, finish, block);

        {
            const auto rest = BasicBlock::Create(context, "rest", ctx.Func);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            block = loop;

            const auto getres = GetNodeValues(Flow, ctx, block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), block);

            const auto choise = SwitchInst::Create(getres.first, good, 2U, block);
            choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), over);
            choise->addCase(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), rest);

            block = rest;

            new StoreInst(ConstantInt::get(last->getType(), static_cast<i32>(EFetchResult::Finish)), statusPtr, block);
            const auto sealFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Seal<Sort>));
            const auto sealType = FunctionType::get(Type::getVoidTy(context), {stateArg->getType()}, false);
            const auto sealPtr = CastInst::Create(Instruction::IntToPtr, sealFunc, PointerType::getUnqual(sealType), "seal", block);
            CallInst::Create(sealPtr, {stateArg}, "", block);

            BranchInst::Create(full, block);

            block = good;

            const auto tonguePtr = GetElementPtrInst::CreateInBounds(stateArg, { stateFields.This(), stateFields.GetTongue() }, "tongue_ptr", block);
            const auto tongue = new LoadInst(tonguePtr, "tongue", block);

            std::vector<Value*> placeholders(Representations.size());
            for (auto i = 0U; i < placeholders.size(); ++i) {
                placeholders[i] = GetElementPtrInst::CreateInBounds(tongue, {ConstantInt::get(indexType, i)}, (TString("placeholder_") += ToString(i)).c_str(), block);
            }

            for (auto i = 0U; i < KeyTypes.size(); ++i) {
                const auto item = getres.second[Indexes[i]](ctx, block);
                new StoreInst(item, placeholders[i], block);
            }

            const auto pushFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Push));
            const auto pushType = FunctionType::get(Type::getInt1Ty(context), {stateArg->getType()}, false);
            const auto pushPtr = CastInst::Create(Instruction::IntToPtr, pushFunc, PointerType::getUnqual(pushType), "function", block);
            const auto accepted = CallInst::Create(pushPtr, {stateArg}, "accepted", block);

            const auto push = BasicBlock::Create(context, "push", ctx.Func);
            const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

            BranchInst::Create(push, skip, accepted, block);

            block = push;

            for (auto i = 0U; i < KeyTypes.size(); ++i) {
                ValueAddRef(Representations[i], placeholders[i], ctx, block);
            }

            for (auto i = KeyTypes.size(); i < Representations.size(); ++i) {
                const auto item = getres.second[Indexes[i]](ctx, block);
                ValueAddRef(Representations[i], item, ctx, block);
                new StoreInst(item, placeholders[i], block);
            }

            BranchInst::Create(loop, block);

            block = skip;

            for (auto i = 0U; i < KeyTypes.size(); ++i) {
                ValueCleanup(Representations[i], placeholders[i], ctx, block);
                new StoreInst(ConstantInt::get(valueType, 0), placeholders[i], block);
            }

            BranchInst::Create(loop, block);
        }

        {
            block = full;

            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            const auto extractFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Extract));
            const auto extractType = FunctionType::get(outputPtrType, {stateArg->getType()}, false);
            const auto extractPtr = CastInst::Create(Instruction::IntToPtr, extractFunc, PointerType::getUnqual(extractType), "extract", block);
            const auto out = CallInst::Create(extractPtr, {stateArg}, "out", block);
            const auto has = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, out, ConstantPointerNull::get(outputPtrType), "has", block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), block);

            BranchInst::Create(good, over, has, block);

            block = good;

            new StoreInst(out, outs, block);

            result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);
            BranchInst::Create(over, block);
        }

        block = over;
        return {result, std::move(getters)};
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state, ui64 count, const bool* directions) const {
#ifdef MKQL_DISABLE_CODEGEN
        state = ctx.HolderFactory.Create<TState>(count, directions, Directions.size(), TMyValueCompare(KeyTypes), Indexes);
#else
        state = ctx.HolderFactory.Create<TState>(count, directions, Directions.size(), ctx.ExecuteLLVM && Compare ? TCompareFunc(Compare) : TCompareFunc(TMyValueCompare(KeyTypes)), Indexes);
#endif
    }

    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            TWideTopWrapper::DependsOn(flow, Count);
            std::for_each(Directions.cbegin(), Directions.cend(), std::bind(&TWideTopWrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationWideFlowNode *const Flow;
    IComputationNode *const Count;
    const TComputationNodePtrVector Directions;
    const TKeyTypes KeyTypes;
    const std::vector<ui32> Indexes;
    const std::vector<EValueRepresentation> Representations;
#ifndef MKQL_DISABLE_CODEGEN
    TComparePtr Compare = nullptr;

    Function* CompareFunc = nullptr;

    TString MakeName() const {
        TStringStream out;
        out << this->DebugString() << "::Compare_(" << static_cast<const void*>(this) << ").";
        return out.Str();
    }

    void FinalizeFunctions(const NYql::NCodegen::ICodegen::TPtr& codegen) final {
        if (CompareFunc) {
            Compare = reinterpret_cast<TComparePtr>(codegen->GetPointerToFunction(CompareFunc));
        }
    }

    void GenerateFunctions(const NYql::NCodegen::ICodegen::TPtr& codegen) final {
        codegen->ExportSymbol(CompareFunc = GenerateCompareFunction(codegen, MakeName(), KeyTypes));
    }
#endif
};

}

template<bool Sort>
IComputationNode* WrapWideTopT(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() > 2U && !(callable.GetInputsCount() % 2U), "Expected more arguments.");

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    const auto count = LocateNode(ctx.NodeLocator, callable, 1);
    const auto keyWidth = (callable.GetInputsCount() >> 1U) - 1U;
    const auto inputType = AS_TYPE(TTupleType, AS_TYPE(TFlowType, callable.GetType()->GetReturnType())->GetItemType());
    std::vector<ui32> indexes(inputType->GetElementsCount());
    std::iota(indexes.begin(), indexes.end(), 0U);

    TKeyTypes keyTypes(keyWidth);
    for (auto i = 0U; i < keyTypes.size(); ++i) {
        const auto keyIndex = AS_VALUE(TDataLiteral, callable.GetInput((i + 1U) << 1U))->AsValue().Get<ui32>();
        std::swap(indexes[i], indexes[indexes[keyIndex]]);
        keyTypes[i].first = *UnpackOptionalData(inputType->GetElementType(keyIndex), keyTypes[i].second)->GetDataSlot();
    }

    std::vector<EValueRepresentation> representations(inputType->GetElementsCount());
    for (auto i = 0U; i < representations.size(); ++i)
        representations[i] = GetValueRepresentation(inputType->GetElementType(indexes[i]));

    TComputationNodePtrVector directions(keyWidth);
    auto index = 1U;
    std::generate(directions.begin(), directions.end(), [&](){ return LocateNode(ctx.NodeLocator, callable, ++++index); });

    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        return new TWideTopWrapper<Sort>(ctx.Mutables, wide, count, std::move(directions), std::move(keyTypes), std::move(indexes), std::move(representations));
    }

    THROW yexception() << "Expected wide flow.";
}

IComputationNode* WrapWideTop(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapWideTopT<false>(callable, ctx);
}

IComputationNode* WrapWideTopSort(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapWideTopT<true>(callable, ctx);
}

}
}
