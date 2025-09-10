#include "dq_scalar_hash_join.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

namespace NKikimr::NMiniKQL {

namespace {

class TScalarHashJoinState : public TComputationValue<TScalarHashJoinState> {
    using TBase = TComputationValue<TScalarHashJoinState>;
public:

    TScalarHashJoinState(TMemoryUsageInfo* memInfo,
        IComputationWideFlowNode* leftFlow, IComputationWideFlowNode* rightFlow,
        const std::vector<ui32>& leftKeyColumns, const std::vector<ui32>& rightKeyColumns,
        const std::vector<TType*>& leftColumnTypes, const std::vector<TType*>& rightColumnTypes, [[maybe_unused]] TComputationContext& ctx,
        NUdf::TLoggerPtr logger, NUdf::TLogComponentId logComponent)
    :   TBase(memInfo)
    ,   LeftFlow_(leftFlow)
    ,   RightFlow_(rightFlow)
    ,   LeftKeyColumns_(leftKeyColumns)
    ,   RightKeyColumns_(rightKeyColumns)
    ,   LeftColumnTypes_(leftColumnTypes)
    ,   RightColumnTypes_(rightColumnTypes)
    ,   Logger_(logger)
    ,   LogComponent_(logComponent)
    {
        UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Debug, "TScalarHashJoinState created");
    }

    EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) {
        if (!LeftFinished_) {
            auto result = LeftFlow_->FetchValues(ctx, output);
            if (result == EFetchResult::One) {
                return EFetchResult::One;
            } else if (result == EFetchResult::Finish) {
                LeftFinished_ = true;
            } else if (result == EFetchResult::Yield) {
                return EFetchResult::Yield;
            }
        }

        if (!RightFinished_) {
            auto result = RightFlow_->FetchValues(ctx, output);
            if (result == EFetchResult::One) {
                return EFetchResult::One;
            } else if (result == EFetchResult::Finish) {
                RightFinished_ = true;
            } else if (result == EFetchResult::Yield) {
                return EFetchResult::Yield;
            }
        }

        if (LeftFinished_ && RightFinished_) {
            return EFetchResult::Finish;
        }

        return EFetchResult::Yield;
    }

private:
    IComputationWideFlowNode* const LeftFlow_;
    IComputationWideFlowNode* const RightFlow_;

    bool LeftFinished_ = false;
    bool RightFinished_ = false;

    const std::vector<ui32> LeftKeyColumns_;
    const std::vector<ui32> RightKeyColumns_;
    const std::vector<TType*> LeftColumnTypes_;
    const std::vector<TType*> RightColumnTypes_;

    const NUdf::TLoggerPtr Logger_;
    const NUdf::TLogComponentId LogComponent_;
};

class TScalarHashJoinWrapper : public TStatefulWideFlowComputationNode<TScalarHashJoinWrapper> {
private:
    using TBaseComputation = TStatefulWideFlowComputationNode<TScalarHashJoinWrapper>;

public:
    TScalarHashJoinWrapper(
        TComputationMutables&       mutables,
        IComputationWideFlowNode*   leftFlow,
        IComputationWideFlowNode*   rightFlow,
        const TVector<TType*>&&     resultItemTypes,
        const TVector<TType*>&&     leftColumnTypes,
        const TVector<ui32>&&       leftKeyColumns,
        const TVector<TType*>&&     rightColumnTypes,
        const TVector<ui32>&&       rightKeyColumns
    )
        : TBaseComputation(mutables, nullptr, EValueRepresentation::Boxed)
        , LeftFlow_(leftFlow)
        , RightFlow_(rightFlow)
        , ResultItemTypes_(std::move(resultItemTypes))
        , LeftColumnTypes_(std::move(leftColumnTypes))
        , LeftKeyColumns_(std::move(leftKeyColumns))
        , RightColumnTypes_(std::move(rightColumnTypes))
        , RightKeyColumns_(std::move(rightKeyColumns))

    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (state.IsInvalid()) {
            MakeState(ctx, state);
        }
        return static_cast<TScalarHashJoinState*>(state.AsBoxed().Get())->FetchValues(ctx, output);
    }

private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
            NYql::NUdf::TLoggerPtr logger = ctx.MakeLogger();
            NYql::NUdf::TLogComponentId logComponent = logger->RegisterComponent("ScalarHashJoin");
            UDF_LOG(logger, logComponent, NUdf::ELogLevel::Debug, TStringBuilder() << "State initialized");

            state = ctx.HolderFactory.Create<TScalarHashJoinState>(
                LeftFlow_, RightFlow_, LeftKeyColumns_, RightKeyColumns_,
                LeftColumnTypes_, RightColumnTypes_,
                ctx, logger, logComponent);
    }

    void RegisterDependencies() const final {
        FlowDependsOnBoth(LeftFlow_, RightFlow_);
    }

private:
    IComputationWideFlowNode* const LeftFlow_;
    IComputationWideFlowNode* const RightFlow_;

    const TVector<TType*>   ResultItemTypes_;
    const TVector<TType*>   LeftColumnTypes_;
    const TVector<ui32>     LeftKeyColumns_;
    const TVector<TType*>   RightColumnTypes_;
    const TVector<ui32>     RightKeyColumns_;
};

} // namespace


IComputationWideFlowNode* WrapDqScalarHashJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsFlow(), "Expected WideFlow as a resulting flow");
    const auto joinComponents = GetWideComponents(joinType);
    MKQL_ENSURE(joinComponents.size() > 0, "Expected at least one column");
    const TVector<TType*> joinItems(joinComponents.cbegin(), joinComponents.cend());

    const auto leftType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(leftType->IsFlow(), "Expected WideFlow as a left flow");
    const auto leftFlowType = AS_TYPE(TFlowType, leftType);
    MKQL_ENSURE(leftFlowType->GetItemType()->IsMulti(),
                "Expected Multi as a left flow item type");
    const auto leftFlowComponents = GetWideComponents(leftFlowType);
    MKQL_ENSURE(leftFlowComponents.size() > 0, "Expected at least one column");
    const TVector<TType*> leftFlowItems(leftFlowComponents.cbegin(), leftFlowComponents.cend());

    const auto rightType = callable.GetInput(1).GetStaticType();
    MKQL_ENSURE(rightType->IsFlow(), "Expected WideFlow as a right flow");
    const auto rightFlowType = AS_TYPE(TFlowType, rightType);
    MKQL_ENSURE(rightFlowType->GetItemType()->IsMulti(),
                "Expected Multi as a right flow item type");
    const auto rightFlowComponents = GetWideComponents(rightFlowType);
    MKQL_ENSURE(rightFlowComponents.size() > 0, "Expected at least one column");
    const TVector<TType*> rightFlowItems(rightFlowComponents.cbegin(), rightFlowComponents.cend());

    const auto joinKindNode = callable.GetInput(2);
    const auto rawKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();
    const auto joinKind = GetJoinKind(rawKind);
    MKQL_ENSURE(joinKind == EJoinKind::Inner,
                "Only inner join is supported in scalar hash join prototype");

    const auto leftKeyColumnsLiteral = callable.GetInput(3);
    const auto leftKeyColumnsTuple = AS_VALUE(TTupleLiteral, leftKeyColumnsLiteral);
    TVector<ui32> leftKeyColumns;
    leftKeyColumns.reserve(leftKeyColumnsTuple->GetValuesCount());
    for (ui32 i = 0; i < leftKeyColumnsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, leftKeyColumnsTuple->GetValue(i));
        leftKeyColumns.emplace_back(item->AsValue().Get<ui32>());
    }

    const auto rightKeyColumnsLiteral = callable.GetInput(4);
    const auto rightKeyColumnsTuple = AS_VALUE(TTupleLiteral, rightKeyColumnsLiteral);
    TVector<ui32> rightKeyColumns;
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

    return new TScalarHashJoinWrapper(
        ctx.Mutables,
        leftFlow,
        rightFlow,
        std::move(joinItems),
        std::move(leftFlowItems),
        std::move(leftKeyColumns),
        std::move(rightFlowItems),
        std::move(rightKeyColumns)
    );
}
} // namespace NKikimr::NMiniKQL

