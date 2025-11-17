#include "dq_scalar_hash_join.h"
#include "dq_join_common.h"
#include <dq_hash_join_table.h>
#include <ranges>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

namespace NKikimr::NMiniKQL {

namespace {
class TScalarRowSource : public NNonCopyable::TMoveOnly {
  public:
    TScalarRowSource(IComputationWideFlowNode* flow, const TMKQLVector<TType*>& types)
        : Flow_(flow)
        , ConsumeBuff_(types.size())
        , Pointers_(types.size())
    {
        for (int index = 0; index < std::ssize(types); ++index) {
            Pointers_[index] = &ConsumeBuff_[index];
        }
        MKQL_ENSURE(std::ranges::is_permutation(
                        ConsumeBuff_ | std::views::transform([](auto& value) { return &value; }), Pointers_),
                    "Pointers_ should be a permutation of ConsumeBuff_ addresses");
    }

    bool Finished() const {
        return Finished_;
    }

    int UserDataSize() const {
        return ConsumeBuff_.size();
    }

    NYql::NUdf::EFetchStatus ForEachRow(TComputationContext& ctx, std::invocable<NJoinTable::TTuple> auto consume) {
        auto res = Flow_->FetchValues(ctx, Pointers_.data());
        switch (res) {
        case EFetchResult::Finish: {
            Finished_ = true;
            return NYql::NUdf::EFetchStatus::Finish;
        }
        case EFetchResult::Yield: {
            return NYql::NUdf::EFetchStatus::Yield;
        }
        case EFetchResult::One: {
            consume(ConsumeBuff_.data());
            return NYql::NUdf::EFetchStatus::Ok;
        }
        }
    }

  private:
    bool Finished_ = false;
    IComputationWideFlowNode* Flow_;
    TMKQLVector<NYql::NUdf::TUnboxedValue> ConsumeBuff_;
    TMKQLVector<NYql::NUdf::TUnboxedValue*> Pointers_;
};

template <EJoinKind Kind> class TScalarHashJoinState : public TComputationValue<TScalarHashJoinState<Kind>> {
  public:
    TScalarHashJoinState(TMemoryUsageInfo* memInfo, IComputationWideFlowNode* leftFlow,
                         IComputationWideFlowNode* rightFlow, const TMKQLVector<ui32>& leftKeyColumns,
                         const TMKQLVector<ui32>& rightKeyColumns, const TMKQLVector<TType*>& leftColumnTypes,
                         const TMKQLVector<TType*>& rightColumnTypes, NUdf::TLoggerPtr logger, TString componentName,
                         TDqUserRenames renames)
        : NKikimr::NMiniKQL::TComputationValue<TScalarHashJoinState>(memInfo)
        , Join_(memInfo, TScalarRowSource{leftFlow, leftColumnTypes}, TScalarRowSource{rightFlow, rightColumnTypes},
                TJoinMetadata{TColumnsMetadata{rightKeyColumns, rightColumnTypes},
                              TColumnsMetadata{leftKeyColumns, leftColumnTypes},
                              KeyTypesFromColumns(leftColumnTypes, leftKeyColumns)}, logger, componentName)
        , Output_(std::move(renames), leftColumnTypes, rightColumnTypes)
    {}

    EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) {
        while (Output_.SizeTuples() == 0) {
            auto res = Join_.MatchRows(ctx, Output_.MakeConsumeFn());
            switch (res) {
            case EFetchResult::Finish:
                return res;
            case EFetchResult::Yield:
                return res;
            case EFetchResult::One:
                break;
            }
        }
        const int outputTupleSize = Output_.TupleSize();
        MKQL_ENSURE(std::ssize(Output_.OutputBuffer) >= outputTupleSize, "Output_ must contain at least one tuple");
        for (int index = 0; index < outputTupleSize; ++index) {
            int myIndex = std::ssize(Output_.OutputBuffer) - outputTupleSize + index;
            int theirIndex = index;
            *output[theirIndex] = Output_.OutputBuffer[myIndex];
        }
        Output_.OutputBuffer.resize(std::ssize(Output_.OutputBuffer) - outputTupleSize);
        return EFetchResult::One;
    }

  private:
    TJoin<TScalarRowSource, Kind> Join_;
    TRenamedOutput<Kind> Output_;
};

template <EJoinKind Kind>
class TScalarHashJoinWrapper : public TStatefulWideFlowComputationNode<TScalarHashJoinWrapper<Kind>> {
  private:
    using TBaseComputation = TStatefulWideFlowComputationNode<TScalarHashJoinWrapper>;

  public:
    TScalarHashJoinWrapper(TComputationMutables& mutables, IComputationWideFlowNode* leftFlow,
                           IComputationWideFlowNode* rightFlow,
                           TMKQLVector<TType*>&& resultItemTypes,
                           TMKQLVector<TType*>&& leftColumnTypes,
                           TMKQLVector<ui32>&& leftKeyColumns,
                           TMKQLVector<TType*>&& rightColumnTypes,
                           TMKQLVector<ui32>&& rightKeyColumns, TDqUserRenames renames)
        : TBaseComputation(mutables, nullptr, EValueRepresentation::Boxed)
        , LeftFlow_(leftFlow)
        , RightFlow_(rightFlow)
        , ResultItemTypes_(std::move(resultItemTypes))
        , LeftColumnTypes_(std::move(leftColumnTypes))
        , LeftKeyColumns_(std::move(leftKeyColumns))
        , RightColumnTypes_(std::move(rightColumnTypes))
        , RightKeyColumns_(std::move(rightKeyColumns))
        , Renames_(std::move(renames))
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx,
                             NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);
        }
        return static_cast<TScalarHashJoinState<Kind>*>(state.AsBoxed().Get())->FetchValues(ctx, output);
    }

  private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        NYql::NUdf::TLoggerPtr logger = ctx.MakeLogger();

        state = ctx.HolderFactory.Create<TScalarHashJoinState<Kind>>(
            LeftFlow_, RightFlow_, LeftKeyColumns_, RightKeyColumns_, LeftColumnTypes_, RightColumnTypes_, logger,
            "ScalarHashJoin", Renames_);
    }

    void RegisterDependencies() const final {
        this->FlowDependsOnBoth(LeftFlow_, RightFlow_);
    }

  private:
    IComputationWideFlowNode* const LeftFlow_;
    IComputationWideFlowNode* const RightFlow_;

    const TMKQLVector<TType*> ResultItemTypes_;
    const TMKQLVector<TType*> LeftColumnTypes_;
    const TMKQLVector<ui32> LeftKeyColumns_;
    const TMKQLVector<TType*> RightColumnTypes_;
    const TMKQLVector<ui32> RightKeyColumns_;
    const TDqUserRenames Renames_;
};

} // namespace

IComputationWideFlowNode* WrapDqScalarHashJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 7, "Expected 7 args");

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsFlow(), "Expected WideFlow as a resulting flow");
    const auto joinComponents = GetWideComponents(joinType);
    MKQL_ENSURE(joinComponents.size() > 0, "Expected at least one column");
    TMKQLVector<TType*> joinItems(joinComponents.cbegin(), joinComponents.cend());

    const auto leftType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(leftType->IsFlow(), "Expected WideFlow as a left flow");
    const auto leftFlowType = AS_TYPE(TFlowType, leftType);
    MKQL_ENSURE(leftFlowType->GetItemType()->IsMulti(), "Expected Multi as a left flow item type");
    const auto leftFlowComponents = GetWideComponents(leftFlowType);
    MKQL_ENSURE(leftFlowComponents.size() > 0, "Expected at least one column");
    TMKQLVector<TType*> leftFlowItems(leftFlowComponents.cbegin(), leftFlowComponents.cend());

    const auto rightType = callable.GetInput(1).GetStaticType();
    MKQL_ENSURE(rightType->IsFlow(), "Expected WideFlow as a right flow");
    const auto rightFlowType = AS_TYPE(TFlowType, rightType);
    MKQL_ENSURE(rightFlowType->GetItemType()->IsMulti(), "Expected Multi as a right flow item type");
    const auto rightFlowComponents = GetWideComponents(rightFlowType);
    MKQL_ENSURE(rightFlowComponents.size() > 0, "Expected at least one column");
    TMKQLVector<TType*> rightFlowItems(rightFlowComponents.cbegin(), rightFlowComponents.cend());

    const auto joinKindNode = callable.GetInput(2);
    const auto rawKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();
    const auto joinKind = GetJoinKind(rawKind);

    const auto leftKeyColumnsLiteral = callable.GetInput(3);
    const auto leftKeyColumnsTuple = AS_VALUE(TTupleLiteral, leftKeyColumnsLiteral);
    TMKQLVector<ui32> leftKeyColumns;
    leftKeyColumns.reserve(leftKeyColumnsTuple->GetValuesCount());
    for (ui32 i = 0; i < leftKeyColumnsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, leftKeyColumnsTuple->GetValue(i));
        leftKeyColumns.emplace_back(item->AsValue().Get<ui32>());
    }

    

    const auto rightKeyColumnsLiteral = callable.GetInput(4);
    const auto rightKeyColumnsTuple = AS_VALUE(TTupleLiteral, rightKeyColumnsLiteral);
    TMKQLVector<ui32> rightKeyColumns;
    rightKeyColumns.reserve(rightKeyColumnsTuple->GetValuesCount());
    for (ui32 i = 0; i < rightKeyColumnsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, rightKeyColumnsTuple->GetValue(i));
        rightKeyColumns.emplace_back(item->AsValue().Get<ui32>());
    }

    MKQL_ENSURE(leftKeyColumns.size() == rightKeyColumns.size(), "Key columns mismatch");

    const auto leftFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    const auto rightFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 1));

    MKQL_ENSURE(leftFlow, "Expected WideFlow as a left input");
    MKQL_ENSURE(rightFlow, "Expected WideFlow as a right input");
    MKQL_ENSURE(joinKind == EJoinKind::Inner, "Only inner is supported, see gh#26780 for details.");

    TDqUserRenames renames =
        FromGraceFormat(TGraceJoinRenames::FromRuntimeNodes(callable.GetInput(5), callable.GetInput(6)));
    ValidateRenames(renames, joinKind, std::ssize(leftFlowItems), std::ssize(rightFlowItems));
    return new TScalarHashJoinWrapper<EJoinKind::Inner>(ctx.Mutables, leftFlow, rightFlow, std::move(joinItems),
                                                        std::move(leftFlowItems), std::move(leftKeyColumns),
                                                        std::move(rightFlowItems), std::move(rightKeyColumns), renames);
}
} // namespace NKikimr::NMiniKQL
