#include "dq_scalar_hash_join.h"

#include "dq_join_common.h"
#include <dq_hash_join_table.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

namespace NKikimr::NMiniKQL {

namespace {
class TScalarRowSource : public NNonCopyable::TMoveOnly {
  public:
    TScalarRowSource(IComputationWideFlowNode* flow, const std::vector<TType*>& types)
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
    std::vector<NYql::NUdf::TUnboxedValue> ConsumeBuff_;
    std::vector<NYql::NUdf::TUnboxedValue*> Pointers_;
};

template <EJoinKind Kind> class TScalarHashJoinState : public TComputationValue<TScalarHashJoinState<Kind>> {
  public:
    TScalarHashJoinState(TMemoryUsageInfo* memInfo, IComputationWideFlowNode* leftFlow,
                         IComputationWideFlowNode* rightFlow, const std::vector<ui32>& leftKeyColumns,
                         const std::vector<ui32>& rightKeyColumns, const std::vector<TType*>& leftColumnTypes,
                         const std::vector<TType*>& rightColumnTypes, NUdf::TLoggerPtr logger, TString componentName)
        : NKikimr::NMiniKQL::TComputationValue<TScalarHashJoinState>(memInfo)
        , Join_(memInfo, TScalarRowSource{leftFlow, leftColumnTypes}, TScalarRowSource{rightFlow, rightColumnTypes},
                TJoinMetadata{TColumnsMetadata{rightKeyColumns, rightColumnTypes},
                              TColumnsMetadata{leftKeyColumns, leftColumnTypes},
                              KeyTypesFromColumns(leftColumnTypes, leftKeyColumns)}, logger, componentName)
    {}

    int TupleSize() const {
        if constexpr (Kind == EJoinKind::LeftOnly || Kind == EJoinKind::LeftSemi) {
            return Join_.ProbeSize();
        } else {
            if constexpr (Kind == EJoinKind::RightOnly || Kind == EJoinKind::RightSemi) {
                return Join_.BuildSize();
            } else {
                return Join_.BuildSize() + Join_.ProbeSize();
            }
        }
    }

    int SizeTuples() const {
        MKQL_ENSURE(OutputBuffer_.size() % TupleSize() == 0, "buffer contains tuple parts??");
        return OutputBuffer_.size() / TupleSize();
    }

    EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) {
        while (SizeTuples() == 0) {
            auto consumeOneOrTwo = [&] {
                if constexpr (SemiOrOnlyJoin(Kind)) {
                    return [&](NJoinTable::TTuple tuple) {
                        auto out = std::back_inserter(OutputBuffer_);
                        if (!tuple) {
                            tuple = NullTuples_.data();
                        }
                        std::copy_n(tuple, Join_.ProbeSize(), out);
                    };
                } else {
                    return [&](NJoinTable::TTuple probe, NJoinTable::TTuple build) {
                        auto out = std::back_inserter(OutputBuffer_);
                        if (!probe) {
                            probe = NullTuples_.data();
                        }
                        std::copy_n(probe, Join_.ProbeSize(), out);

                        if (!build) {
                            build = NullTuples_.data();
                        }
                        std::copy_n(build, Join_.BuildSize(), out);
                    };
                }
            }();
            auto res = Join_.MatchRows(ctx, consumeOneOrTwo);
            switch (res) {

            case EFetchResult::Finish:
                return res;
            case EFetchResult::Yield:
                return res;
            case EFetchResult::One:
                break;
            }
        }
        const int outputTupleSize = TupleSize();
        MKQL_ENSURE(std::ssize(OutputBuffer_) >= outputTupleSize, "Output_ must contain at least one tuple");
        for (int index = 0; index < outputTupleSize; ++index) {
            int myIndex = std::ssize(OutputBuffer_) - outputTupleSize + index;
            int theirIndex = index;
            *output[theirIndex] = OutputBuffer_[myIndex];
        }
        OutputBuffer_.resize(std::ssize(OutputBuffer_) - outputTupleSize);
        return EFetchResult::One;
    }

  private:
    TJoin<TScalarRowSource, Kind> Join_;
    std::vector<NUdf::TUnboxedValue> OutputBuffer_;
    const std::vector<NYql::NUdf::TUnboxedValue> NullTuples_{
        static_cast<size_t>(std::max(Join_.BuildSize(), Join_.ProbeSize())), NYql::NUdf::TUnboxedValuePod{}};
};

template <EJoinKind Kind>
class TScalarHashJoinWrapper : public TStatefulWideFlowComputationNode<TScalarHashJoinWrapper<Kind>> {
  private:
    using TBaseComputation = TStatefulWideFlowComputationNode<TScalarHashJoinWrapper>;

  public:
    TScalarHashJoinWrapper(TComputationMutables& mutables, IComputationWideFlowNode* leftFlow,
                           IComputationWideFlowNode* rightFlow,
                           TVector<TType*>&& resultItemTypes,
                           TVector<TType*>&& leftColumnTypes,
                           TVector<ui32>&& leftKeyColumns,
                           TVector<TType*>&& rightColumnTypes,
                           TVector<ui32>&& rightKeyColumns)
        : TBaseComputation(mutables, nullptr, EValueRepresentation::Boxed)
        , LeftFlow_(leftFlow)
        , RightFlow_(rightFlow)
        , ResultItemTypes_(std::move(resultItemTypes))
        , LeftColumnTypes_(std::move(leftColumnTypes))
        , LeftKeyColumns_(std::move(leftKeyColumns))
        , RightColumnTypes_(std::move(rightColumnTypes))
        , RightKeyColumns_(std::move(rightKeyColumns))
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

        state = ctx.HolderFactory.Create<TScalarHashJoinState<Kind>>(LeftFlow_, RightFlow_, LeftKeyColumns_,
                                                                     RightKeyColumns_, LeftColumnTypes_,
                                                                     RightColumnTypes_, logger, "ScalarHashJoin");
    }

    void RegisterDependencies() const final {
        this->FlowDependsOnBoth(LeftFlow_, RightFlow_);
    }

  private:
    IComputationWideFlowNode* const LeftFlow_;
    IComputationWideFlowNode* const RightFlow_;

    const TVector<TType*> ResultItemTypes_;
    const TVector<TType*> LeftColumnTypes_;
    const TVector<ui32> LeftKeyColumns_;
    const TVector<TType*> RightColumnTypes_;
    const TVector<ui32> RightKeyColumns_;
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
    MKQL_ENSURE(leftFlowType->GetItemType()->IsMulti(), "Expected Multi as a left flow item type");
    const auto leftFlowComponents = GetWideComponents(leftFlowType);
    MKQL_ENSURE(leftFlowComponents.size() > 0, "Expected at least one column");
    TVector<TType*> leftFlowItems(leftFlowComponents.cbegin(), leftFlowComponents.cend());

    const auto rightType = callable.GetInput(1).GetStaticType();
    MKQL_ENSURE(rightType->IsFlow(), "Expected WideFlow as a right flow");
    const auto rightFlowType = AS_TYPE(TFlowType, rightType);
    MKQL_ENSURE(rightFlowType->GetItemType()->IsMulti(), "Expected Multi as a right flow item type");
    const auto rightFlowComponents = GetWideComponents(rightFlowType);
    MKQL_ENSURE(rightFlowComponents.size() > 0, "Expected at least one column");
    TVector<TType*> rightFlowItems(rightFlowComponents.cbegin(), rightFlowComponents.cend());

    const auto joinKindNode = callable.GetInput(2);
    const auto rawKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();
    const auto joinKind = GetJoinKind(rawKind);

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
    return std::visit([&](auto kind) -> IComputationWideFlowNode* {
        return new TScalarHashJoinWrapper<decltype(kind)::Kind_>(
            ctx.Mutables, leftFlow, rightFlow, std::move(joinItems), std::move(leftFlowItems),
            std::move(leftKeyColumns), std::move(rightFlowItems), std::move(rightKeyColumns));
    }, TypifyJoinKind(joinKind));
}
} // namespace NKikimr::NMiniKQL
