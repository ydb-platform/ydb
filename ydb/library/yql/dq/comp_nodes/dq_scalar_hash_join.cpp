#include "dq_scalar_hash_join.h"

#include <dq_hash_join_table.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <ranges>

namespace NKikimr::NMiniKQL {

namespace {
TKeyTypes KeyTypesFromColumns(const std::vector<TType*>& types, const std::vector<ui32>& keyIndexes) {
    TKeyTypes kt;
    std::ranges::copy(keyIndexes | std::views::transform([&types](ui32 typeIndex) {
                          const TType* type = types[typeIndex];
                          MKQL_ENSURE(type->IsData(), "exepected data type");
                          return std::pair{*static_cast<const TDataType*>(type)->GetDataSlot(), false};
                      }), std::back_inserter(kt));
    return kt;
}


class TScalarHashJoinState : public TComputationValue<TScalarHashJoinState> {
    using TBase = TComputationValue<TScalarHashJoinState>;
    IComputationWideFlowNode* BuildSide() const {
        return RightFinished_ ? nullptr : RightFlow_;
    }

    IComputationWideFlowNode* ProbeSide() const {
        return LeftFinished_ ? nullptr : LeftFlow_;
    }

    void AppendTuple(NJoinTable::TTuple probe, NJoinTable::TTuple build, std::vector<NUdf::TUnboxedValue>& output){
        std::copy(probe, probe + LeftColumnTypes_.size(), std::back_inserter(output));
        for(int index = 0; index < std::ssize(RightColumnTypes_); ++index){
            if (std::ranges::find(RightKeyColumns_, index) == RightKeyColumns_.end()){
                output.push_back(build[index]);
            }
        }
    }

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
    ,   KeyTypes_(KeyTypesFromColumns(leftColumnTypes, leftKeyColumns))
    ,   Table_(std::ssize(leftColumnTypes), TWideUnboxedEqual{KeyTypes_}, TWideUnboxedHasher{KeyTypes_})
    ,   Values_(rightColumnTypes.size())
    ,   Pointers_()
    ,   Output_()
    {
        MKQL_ENSURE(RightColumnTypes_.size() == LeftColumnTypes_.size(), "unimplemented");
        Pointers_.resize(LeftColumnTypes_.size());
        for (int index = 0; index < std::ssize(LeftKeyColumns_); ++index) {
            Pointers_[LeftKeyColumns_[index]] = &Values_[index];
        }
        int valuesIndex = 0;
        for(int index = 0; index < std::ssize(Pointers_); ++index){
            if (!Pointers_[index]){
                Pointers_[index] = &Values_[ std::ssize(LeftKeyColumns_) + valuesIndex];
                valuesIndex++;
            }
        }
        MKQL_ENSURE(std::ranges::is_permutation(Values_ | std::views::transform([](auto& value){return &value;}), Pointers_), "Pointers_ should be a permutation of Values_ addresses");

        UDF_LOG(Logger_, LogComponent_, NUdf::ELogLevel::Debug, "TScalarHashJoinState created");
    }

    EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) {
        const int outputTupleSize = std::ssize(RightColumnTypes_) + std::ssize(LeftColumnTypes_) - std::ssize(LeftKeyColumns_);
        if (auto* buildSide = BuildSide()) {
            auto res = buildSide->FetchValues(ctx, Pointers_.data());
            switch (res) {

            case EFetchResult::Finish: {
                Table_.Build();
                RightFinished_ = true;
                return EFetchResult::Yield;
            }
            case EFetchResult::Yield: {
                return EFetchResult::Yield;
            }
            case EFetchResult::One: {
                Table_.Add(Values_);
                return EFetchResult::Yield;
            }
            default:
                MKQL_ENSURE(false, "unreachable");
            }
        }
        if (!Output_.empty()) {
            MKQL_ENSURE(std::ssize(Output_) >= outputTupleSize,
                       "Output_ must contain at least one tuple");
            for (int index = 0; index < outputTupleSize; ++index) {
                int myIndex = std::ssize(Output_) - outputTupleSize + index;
                int theirIndex = index;
                *output[theirIndex] = Output_[myIndex];
            }
            Output_.resize(std::ssize(Output_) - outputTupleSize);
            return EFetchResult::One;
        }
        if (auto* probeSide = ProbeSide()) {
            auto result = LeftFlow_->FetchValues(ctx, Pointers_.data());
            switch (result) {
            case EFetchResult::Finish: {
                LeftFinished_ = true;
                return EFetchResult::Finish;
            }
            case EFetchResult::Yield: {
                return EFetchResult::Yield;
            }
            case EFetchResult::One: {
                Table_.Lookup(Values_.data(), [this](NJoinTable::TTuple matched) {
                    AppendTuple(Values_.data(),matched,Output_);
                });
                return EFetchResult::Yield;
            }
            default:
                MKQL_ENSURE(false, "unreachable");
            }
        }
        MKQL_ENSURE(false, "unreachable");
    }

private:
    IComputationWideFlowNode* const LeftFlow_;
    IComputationWideFlowNode* const RightFlow_;

    const std::vector<ui32> LeftKeyColumns_;
    const std::vector<ui32> RightKeyColumns_;
    const std::vector<TType*> LeftColumnTypes_;
    const std::vector<TType*> RightColumnTypes_;

    const NUdf::TLoggerPtr Logger_;
    const NUdf::TLogComponentId LogComponent_;
    const TKeyTypes KeyTypes_;
    bool LeftFinished_ = false;
    bool RightFinished_ = false;
    NJoinTable::TStdJoinTable Table_;
    std::vector<NUdf::TUnboxedValue> Values_;
    std::vector<NUdf::TUnboxedValue*> Pointers_;
    std::vector<NUdf::TUnboxedValue> Output_;
};

class TScalarHashJoinWrapper : public TStatefulWideFlowComputationNode<TScalarHashJoinWrapper> {
private:
    using TBaseComputation = TStatefulWideFlowComputationNode<TScalarHashJoinWrapper>;

public:
    TScalarHashJoinWrapper(
        TComputationMutables&       mutables,
        IComputationWideFlowNode*   leftFlow,
        IComputationWideFlowNode*   rightFlow,
        TVector<TType*>&&           resultItemTypes,
        TVector<TType*>&&           leftColumnTypes,
        TVector<ui32>&&             leftKeyColumns,
        TVector<TType*>&&           rightColumnTypes,
        TVector<ui32>&&             rightKeyColumns
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
    TVector<TType*> joinItems(joinComponents.cbegin(), joinComponents.cend());

    const auto leftType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(leftType->IsFlow(), "Expected WideFlow as a left flow");
    const auto leftFlowType = AS_TYPE(TFlowType, leftType);
    MKQL_ENSURE(leftFlowType->GetItemType()->IsMulti(),
                "Expected Multi as a left flow item type");
    const auto leftFlowComponents = GetWideComponents(leftFlowType);
    MKQL_ENSURE(leftFlowComponents.size() > 0, "Expected at least one column");
    TVector<TType*> leftFlowItems(leftFlowComponents.cbegin(), leftFlowComponents.cend());

    const auto rightType = callable.GetInput(1).GetStaticType();
    MKQL_ENSURE(rightType->IsFlow(), "Expected WideFlow as a right flow");
    const auto rightFlowType = AS_TYPE(TFlowType, rightType);
    MKQL_ENSURE(rightFlowType->GetItemType()->IsMulti(),
                "Expected Multi as a right flow item type");
    const auto rightFlowComponents = GetWideComponents(rightFlowType);
    MKQL_ENSURE(rightFlowComponents.size() > 0, "Expected at least one column");
    TVector<TType*> rightFlowItems(rightFlowComponents.cbegin(), rightFlowComponents.cend());

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

