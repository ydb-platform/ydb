#pragma once

#include "dq_yt_reader.h"

#include <ydb/library/yql/providers/yt/comp_nodes/yql_mkql_file_input_state.h>
#include <ydb/library/yql/providers/yt/codec/yt_codec.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>

#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/errors.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/serialize.h>
#include <yt/cpp/mapreduce/interface/config.h>
#include <yt/cpp/mapreduce/common/wait_proxy.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/yson/node/node_io.h>

#include <util/generic/size_literals.h>
#include <util/stream/output.h>

namespace NYql::NDqs {

using namespace NKikimr::NMiniKQL;

template<typename T, typename IS>
class TDqYtReadWrapperBase : public TStatefulWideFlowCodegeneratorNode<TDqYtReadWrapperBase<T, IS>> {
using TBaseComputation =  TStatefulWideFlowCodegeneratorNode<TDqYtReadWrapperBase<T, IS>>;
public:
    TDqYtReadWrapperBase(const TComputationNodeFactoryContext& ctx, const TString& clusterName,
        const TString& token, const NYT::TNode& inputSpec, const NYT::TNode& samplingSpec,
        const TVector<ui32>& inputGroups,
        TType* itemType, const TVector<TString>& tableNames, TVector<std::pair<NYT::TRichYPath, NYT::TFormat>>&& tables,
        NKikimr::NMiniKQL::IStatsRegistry* jobStats, size_t inflight, size_t timeout, const TVector<ui64>& tableOffsets)
        : TBaseComputation(ctx.Mutables, this, EValueRepresentation::Boxed, EValueRepresentation::Boxed)
        , Width(AS_TYPE(TStructType, itemType)->GetMembersCount())
        , CodecCtx(ctx.Env, ctx.FunctionRegistry, &ctx.HolderFactory)
        , ClusterName(clusterName)
        , Token(token)
        , SamplingSpec(samplingSpec)
        , Tables(std::move(tables))
        , Inflight(inflight)
        , Timeout(timeout)
    {
        Specs.SetUseSkiff("", TMkqlIOSpecs::ESystemField::RowIndex | TMkqlIOSpecs::ESystemField::RangeIndex);
        Specs.Init(CodecCtx, inputSpec, inputGroups, tableNames, itemType, {}, {}, jobStats);
        Specs.SetTableOffsets(tableOffsets);
    }

    class TState: public TComputationValue<TState>, public IS {
    public:

        template<typename... Args>
        TState(TMemoryUsageInfo* memInfo, Args&&... args)
            : TComputationValue<TState>(memInfo)
            , IS(std::forward<Args>(args)...)
        {
            IS::SetNextBlockCallback([this]() { Yield_ = true; });
        }

        virtual ~TState() = default;

        NUdf::TUnboxedValuePod FetchRecord() {
            if (!AtStart_) {
                IS::Next();
            }
            AtStart_ = false;

            if (!IS::IsValid()) {
                IS::Finish();
                return NUdf::TUnboxedValuePod::MakeFinish();
            }

            if (Yield_) {
                Yield_ = false;
                AtStart_ = true;
                return NUdf::TUnboxedValuePod::MakeYield();
            }

            return IS::GetCurrent().Release();
        }

    private:
        bool AtStart_ = true;
        bool Yield_ = false;
    };

    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        static_cast<const T*>(this)->MakeState(ctx, state);
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!state.HasValue()) {
            MakeState(ctx, state);
        }

        if (const auto value = static_cast<TState&>(*state.AsBoxed()).FetchRecord(); value.IsFinish())
            return EFetchResult::Finish;
        else if (value.IsYield())
            return EFetchResult::Yield;
        else {
            const auto elements = value.GetElements();
            for (ui32 i = 0U; i < Width; ++i)
                if (const auto out = *output++)
                    *out = elements[i];

        }

        return EFetchResult::One;
    }

#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
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

        const auto placeholder = new AllocaInst(pointerType, 0U, "paceholder", &ctx.Func->getEntryBlock().back());

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(static_cast<const TDqYtReadWrapperBase<T, IS>*>(this))), structPtrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TDqYtReadWrapperBase<T, IS>::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::FetchRecord));
        const auto funcType = FunctionType::get(valueType, { statePtrType }, false);
        const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funcType), "fetch_func", block);
        const auto fetch = CallInst::Create(funcType, funcPtr, { stateArg }, "fetch", block);

        const auto result = PHINode::Create(statusType, 2U, "result", done);
        const auto special = SelectInst::Create(IsYield(fetch, block), ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Yield)), ConstantInt::get(statusType, static_cast<i32>(EFetchResult::Finish)), "special", block);
        result->addIncoming(special, block);

        BranchInst::Create(done, good, IsSpecial(fetch, block), block);

        block = good;

        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(pointerType, fetch, ctx.Codegen, block);
        new StoreInst(elements, placeholder, block);

        result->addIncoming(ConstantInt::get(statusType, static_cast<i32>(EFetchResult::One)), block);

        BranchInst::Create(done, block);

        block = done;

        ICodegeneratorInlineWideNode::TGettersList getters;
        getters.reserve(Width);
        for (ui32 i = 0U; i < Width; ++i) {
            getters.emplace_back([i, placeholder, pointerType, arrayType, valueType](const TCodegenContext& ctx, BasicBlock*& block) {
                const auto indexType = Type::getInt32Ty(ctx.Codegen.GetContext());
                const auto pointer = new LoadInst(pointerType, placeholder, (TString("pointer_") += ToString(i)).c_str(), block);
                const auto ptr = GetElementPtrInst::CreateInBounds(arrayType, pointer, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, (TString("ptr_") += ToString(i)).c_str(), block);
                const auto load = new LoadInst(valueType, ptr, (TString("load_") += ToString(i)).c_str(), block);
                return load;
            });
        }

        return {result, std::move(getters)};
    }
#endif
    void RegisterDependencies() const final {}

    const ui32 Width;
    NCommon::TCodecContext CodecCtx;
    TMkqlIOSpecs Specs;

    TString ClusterName;
    TString Token;
    NYT::TNode SamplingSpec;
    TVector<std::pair<NYT::TRichYPath, NYT::TFormat>> Tables;
    size_t Inflight;
    size_t Timeout;
};
}
