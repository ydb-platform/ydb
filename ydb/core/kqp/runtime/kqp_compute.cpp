#include "kqp_compute.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/public/udf/udf_terminator.h>
#include <yql/essentials/public/udf/udf_type_builder.h>

namespace NKikimr {
namespace NMiniKQL {

TComputationNodeFactory GetKqpBaseComputeFactory(const TKqpComputeContextBase* computeCtx) {
    return NYql::NDq::GetDqBaseComputeFactory(computeCtx);
}

namespace {

class TKqpEnsureWrapper : public TMutableCodegeneratorNode<TKqpEnsureWrapper> {
    using TBaseComputation = TMutableCodegeneratorNode<TKqpEnsureWrapper>;
public:
    TKqpEnsureWrapper(TComputationMutables& mutables, IComputationNode* value, IComputationNode* predicate,
        IComputationNode* issueCode, IComputationNode* message)
        : TBaseComputation(mutables, value->GetRepresentation())
        , Arg(value)
        , Predicate(predicate)
        , IssueCode(issueCode)
        , Message(message)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& predicate = Predicate->GetValue(ctx);
        if (predicate && predicate.Get<bool>()) {
            return Arg->GetValue(ctx).Release();
        }

        Throw(this, &ctx);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto predicate = GetNodeValue(Predicate, ctx, block);
        const auto pass = CastInst::Create(Instruction::Trunc, predicate, Type::getInt1Ty(context), "bool", block);

        const auto kill = BasicBlock::Create(context, "kill", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);

        BranchInst::Create(good, kill, pass, block);

        block = kill;
        const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr<&TKqpEnsureWrapper::Throw>());
        const auto doFuncArg = ConstantInt::get(Type::getInt64Ty(context), (ui64)this);
        const auto doFuncType = FunctionType::get(Type::getVoidTy(context), { Type::getInt64Ty(context), ctx.Ctx->getType() }, false);
        const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(doFuncType), "thrower", block);
        CallInst::Create(doFuncType, doFuncPtr, { doFuncArg, ctx.Ctx }, "", block)->setTailCall();
        new UnreachableInst(context, block);

        block = good;
        return GetNodeValue(Arg, ctx, block);;
    }
#endif

private:
    [[noreturn]]
    static void Throw(TKqpEnsureWrapper const* thisPtr, TComputationContext* ctxPtr) {
        auto issueCode = thisPtr->IssueCode->GetValue(*ctxPtr);
        auto message = thisPtr->Message->GetValue(*ctxPtr);

        throw TKqpEnsureFail(issueCode.Get<ui32>(), TString(TStringBuf(message.AsStringRef())));
    }

    void RegisterDependencies() const final {
        DependsOn(Arg);
        DependsOn(Predicate);
    }

    IComputationNode* const Arg;
    IComputationNode* const Predicate;
    IComputationNode* const IssueCode;
    IComputationNode* const Message;
};

class TKqpIndexLookupJoinWrapper : public TMutableComputationNode<TKqpIndexLookupJoinWrapper> {
public:
    struct TState : public TComputationValue<TState> {
        using TComputationValue::TComputationValue;

        std::optional<NUdf::TUnboxedValue> PrevEmptyLeftRow;
        std::optional<NUdf::TUnboxedValue> UnprocessedRow;
        bool Finish = false;
    };

    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream, TComputationContext& ctx,
            const TKqpIndexLookupJoinWrapper* self, ui32 stateIndex)
            : TComputationValue<TStreamValue>(memInfo)
            , Stream(std::move(stream))
            , Self(self)
            , Ctx(ctx)
            , StateIndex(stateIndex)
        {
        }

    private:
        enum class EOutputMode {
            OnlyLeftRow,
            Both
        };

        TState& GetState() const {
            auto& result = Ctx.MutableValues[StateIndex];
            if (!result.HasValue()) {
                result = Ctx.HolderFactory.Create<TState>();
            }
            return *static_cast<TState*>(result.AsBoxed().Get());
        }

        NUdf::TUnboxedValue FillResultItems(NUdf::TUnboxedValue leftRow, NUdf::TUnboxedValue rightRow, EOutputMode mode) {
            auto resultRowSize = (mode == EOutputMode::OnlyLeftRow) ? Self->LeftColumnsIndices.size()
                : Self->LeftColumnsIndices.size() + Self->RightColumnsIndices.size();
            auto resultRow = Self->ResultRowCache.NewArray(Ctx, resultRowSize, ResultItems);

            if (mode == EOutputMode::OnlyLeftRow || mode == EOutputMode::Both) {
                for (size_t i = 0; i < Self->LeftColumnsIndices.size(); ++i) {
                    ResultItems[Self->LeftColumnsIndices[i]] = std::move(leftRow.GetElement(i));
                }
            }

            if (mode == EOutputMode::Both) {
                if (rightRow.HasValue()) {
                    for (size_t i = 0; i < Self->RightColumnsIndices.size(); ++i) {
                        ResultItems[Self->RightColumnsIndices[i]] = std::move(rightRow.GetElement(i));
                    }
                } else {
                    for (size_t i = 0; i < Self->RightColumnsIndices.size(); ++i) {
                        ResultItems[Self->RightColumnsIndices[i]] = NUdf::TUnboxedValuePod();
                    }
                }
            }

            return resultRow;
        }

        bool TryBuildResultRow(TState& state, NUdf::TUnboxedValue inputRow, NUdf::TUnboxedValue& result, ui64 rowNumber) {
            auto leftRow = inputRow.GetElement(0);
            auto rightRow = inputRow.GetElement(1);

            bool ok = true;
            switch (Self->JoinType) {
                case EJoinKind::Inner: {
                    if (!rightRow.HasValue()) {
                        ok = false;
                        break;
                    }

                    result = FillResultItems(std::move(leftRow), std::move(rightRow), EOutputMode::Both);
                    break;
                }
                case EJoinKind::Left: {
                    if (!rightRow.HasValue()) {
                        if (rowNumber == 1) {
                            state.PrevEmptyLeftRow = std::move(leftRow);
                        }

                        ok = false;
                        break;
                    }

                    if (rowNumber >= 2 && rightRow.HasValue()) {
                        state.PrevEmptyLeftRow.reset();
                    }

                    result = FillResultItems(std::move(leftRow), std::move(rightRow), EOutputMode::Both);
                    break;
                }
                case EJoinKind::LeftOnly: {
                    if (rightRow.HasValue()) {
                        ok = false;
                        break;
                    }

                    result = FillResultItems(std::move(leftRow), std::move(rightRow), EOutputMode::OnlyLeftRow);
                    break;
                }
                case EJoinKind::LeftSemi: {
                    if (!rightRow.HasValue()) {
                        ok = false;
                        break;
                    }

                    result = FillResultItems(std::move(leftRow), std::move(rightRow), EOutputMode::OnlyLeftRow);
                    break;
                }
                default:
                    MKQL_ENSURE(false, "Unsupported join kind");
            }

            return ok;
        }

        void BuildFromPrevEmptyLeftRow(TState& state, NUdf::TUnboxedValue& result) {
            result = FillResultItems(std::move(*state.PrevEmptyLeftRow), NUdf::TUnboxedValuePod(), EOutputMode::Both);
            state.PrevEmptyLeftRow.reset();
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            auto& state = GetState();
            for(;;) {
                if (state.Finish) {
                    if (state.PrevEmptyLeftRow.has_value()) {
                        BuildFromPrevEmptyLeftRow(state, result);
                        return NUdf::EFetchStatus::Ok;
                    }

                    return NUdf::EFetchStatus::Finish;
                }

                NUdf::TUnboxedValue item;
                if (state.UnprocessedRow) {
                    item = std::move(*state.UnprocessedRow);
                    state.UnprocessedRow.reset();
                } else {
                    auto status = Stream.Fetch(item);

                    if (status == NUdf::EFetchStatus::Yield) {
                        return status;
                    }

                    if (status == NUdf::EFetchStatus::Finish) {
                        state.Finish = true;
                        continue;
                    }
                }

                ui64 rowNumber = item.GetElement(2).Get<ui64>();
                if (rowNumber == 1 && state.PrevEmptyLeftRow.has_value()) {
                    BuildFromPrevEmptyLeftRow(state, result);
                    state.UnprocessedRow = std::move(item);
                    return NUdf::EFetchStatus::Ok;
                }

                bool buildRow = TryBuildResultRow(state, item, result, rowNumber);
                if (buildRow) {
                    return NUdf::EFetchStatus::Ok;
                }
            }
            return NUdf::EFetchStatus::Ok;
        }

    private:
        NUdf::TUnboxedValue Stream;
        const TKqpIndexLookupJoinWrapper* Self;
        TComputationContext& Ctx;
        NUdf::TUnboxedValue* ResultItems = nullptr;
        ui32 StateIndex;
    };

public:
    TKqpIndexLookupJoinWrapper(TComputationMutables& mutables, IComputationNode* inputNode,
        EJoinKind joinType, TVector<ui32>&& leftColumnsIndices, TVector<ui32>&& rightColumnsIndices)
        : TMutableComputationNode<TKqpIndexLookupJoinWrapper>(mutables)
        , InputNode(inputNode)
        , JoinType(joinType)
        , LeftColumnsIndices(std::move(leftColumnsIndices))
        , RightColumnsIndices(std::move(rightColumnsIndices))
        , ResultRowCache(mutables)
        , StateIndex(mutables.CurValueIndex++)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(InputNode->GetValue(ctx), ctx, this, StateIndex);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(InputNode);
    }

private:
    IComputationNode* InputNode;
    const EJoinKind JoinType;
    const TVector<ui32> LeftColumnsIndices;
    const TVector<ui32> RightColumnsIndices;
    const TContainerCacheOnContext ResultRowCache;
    const ui32 StateIndex;
};

} // namespace

IComputationNode* WrapKqpEnsure(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");
    bool isOptional;
    auto unpackedType = UnpackOptionalData(callable.GetInput(1), isOptional);
    MKQL_ENSURE(unpackedType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool");

    auto value = LocateNode(ctx.NodeLocator, callable, 0);
    auto predicate = LocateNode(ctx.NodeLocator, callable, 1);
    auto issueCode = LocateNode(ctx.NodeLocator, callable, 2);
    auto message = LocateNode(ctx.NodeLocator, callable, 3);

    return new TKqpEnsureWrapper(ctx.Mutables, value, predicate, issueCode, message);
}

IComputationNode* WrapKqpIndexLookupJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");

    auto inputNode = LocateNode(ctx.NodeLocator, callable, 0);
    ui32 joinKind = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui32>();
    auto leftColumnsIndicesMap = AS_VALUE(TDictLiteral, callable.GetInput(2));
    auto rightColumnsIndicesMap = AS_VALUE(TDictLiteral, callable.GetInput(3));

    TVector<ui32> leftColumnsIndices(leftColumnsIndicesMap->GetItemsCount());
    for (ui32 i = 0; i < leftColumnsIndicesMap->GetItemsCount(); ++i) {
        auto item = leftColumnsIndicesMap->GetItem(i);
        ui32 leftIndex = AS_VALUE(TDataLiteral, item.first)->AsValue().Get<ui32>();
        ui32 resultIndex = AS_VALUE(TDataLiteral, item.second)->AsValue().Get<ui32>();
        leftColumnsIndices[leftIndex] = resultIndex;
    }

    TVector<ui32> rightColumnsIndices(rightColumnsIndicesMap->GetItemsCount());
    for (ui32 i = 0; i < rightColumnsIndicesMap->GetItemsCount(); ++i) {
        auto item = rightColumnsIndicesMap->GetItem(i);
        ui32 rightIndex = AS_VALUE(TDataLiteral, item.first)->AsValue().Get<ui32>();
        ui32 resultIndex = AS_VALUE(TDataLiteral, item.second)->AsValue().Get<ui32>();
        rightColumnsIndices[rightIndex] = resultIndex;
    }

    return new TKqpIndexLookupJoinWrapper(ctx.Mutables, inputNode, GetJoinKind(joinKind), std::move(leftColumnsIndices), std::move(rightColumnsIndices));
}

} // namespace NMiniKQL
} // namespace NKikimr
