#include "dq_block_hash_join.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_program_builder.h>

namespace NKikimr::NMiniKQL {

namespace {

class TStreamValue : public TComputationValue<TStreamValue> {
private:
    using TBase = TComputationValue<TStreamValue>;
    
public:
    TStreamValue(
        TMemoryUsageInfo*       memInfo,
        TComputationContext&    ctx,
        const TVector<TType*>&  resultItemTypes,
        NUdf::TUnboxedValue&&   leftStream,
        const TVector<TType*>&  leftItemTypes,
        const TVector<ui32>&    leftKeyColumns,
        NUdf::TUnboxedValue&&   rightStream,
        const TVector<TType*>&  rightItemTypes,
        const TVector<ui32>&    rightKeyColumns
    )
        : TBase(memInfo)
        , Ctx_(ctx)
        , ResultItemTypes_(resultItemTypes)
        , LeftStream_(std::move(leftStream))
        , LeftItemTypes_(leftItemTypes)
        , LeftKeyColumns_(leftKeyColumns)
        , RightStream_(std::move(rightStream))
        , RightItemTypes_(rightItemTypes)
        , RightKeyColumns_(rightKeyColumns)
    { }

private:
    NUdf::EFetchStatus WideFetch(NUdf::TUnboxedValue* output, ui32 width) override {
        if (!LeftFinished_) {
            auto status = LeftStream_.WideFetch(output, width);
            if (status == NUdf::EFetchStatus::Ok) {
                return NUdf::EFetchStatus::Ok;
            } else if (status == NUdf::EFetchStatus::Finish) {
                LeftFinished_ = true;
            } 
        }

        if (!RightFinished_) {
            auto status = RightStream_.WideFetch(output, width);
            if (status == NUdf::EFetchStatus::Ok) {
                return NUdf::EFetchStatus::Ok;
            } else if (status == NUdf::EFetchStatus::Finish) {
                RightFinished_ = true;
            } else if (status == NUdf::EFetchStatus::Yield) {
                return NUdf::EFetchStatus::Yield;
            }
        }

        if (LeftFinished_ && RightFinished_) {
            return NUdf::EFetchStatus::Finish;
        }

        return NUdf::EFetchStatus::Yield;
    }

private:
    bool LeftFinished_ = false;
    bool RightFinished_ = false;

private:
    [[maybe_unused]] TComputationContext&    Ctx_;
    [[maybe_unused]] const TVector<TType*>&  ResultItemTypes_;

    NUdf::TUnboxedValue                      LeftStream_;
    [[maybe_unused]] const TVector<TType*>&  LeftItemTypes_;
    [[maybe_unused]] const TVector<ui32>&    LeftKeyColumns_;

    NUdf::TUnboxedValue                      RightStream_;
    [[maybe_unused]] const TVector<TType*>&  RightItemTypes_;
    [[maybe_unused]] const TVector<ui32>&    RightKeyColumns_;
};

class TBlockHashJoinWraper : public TMutableComputationNode<TBlockHashJoinWraper> {
private:
    using TBaseComputation = TMutableComputationNode<TBlockHashJoinWraper>;

public:
    TBlockHashJoinWraper(
        TComputationMutables&   mutables,
        const TVector<TType*>&& resultItemTypes,
        const TVector<TType*>&& leftItemTypes,
        const TVector<ui32>&&   leftKeyColumns,
        const TVector<TType*>&& rightItemTypes,
        const TVector<ui32>&&   rightKeyColumns,
        IComputationNode*       leftStream,
        IComputationNode*       rightStream
    )
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , ResultItemTypes_(std::move(resultItemTypes))
        , LeftItemTypes_(std::move(leftItemTypes))
        , LeftKeyColumns_(std::move(leftKeyColumns))
        , RightItemTypes_(std::move(rightItemTypes))
        , RightKeyColumns_(std::move(rightKeyColumns))
        , LeftStream_(leftStream)
        , RightStream_(rightStream)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(
            ctx,
            ResultItemTypes_,
            std::move(LeftStream_->GetValue(ctx)),
            LeftItemTypes_,
            LeftKeyColumns_,
            std::move(RightStream_->GetValue(ctx)),
            RightItemTypes_,
            RightKeyColumns_
        );
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(LeftStream_);
        this->DependsOn(RightStream_);
    }

private:
    const TVector<TType*>   ResultItemTypes_;

    const TVector<TType*>   LeftItemTypes_;
    const TVector<ui32>     LeftKeyColumns_;

    const TVector<TType*>   RightItemTypes_;
    const TVector<ui32>     RightKeyColumns_;

    IComputationNode*       LeftStream_;
    IComputationNode*       RightStream_;
};

} // namespace

IComputationNode* WrapDqBlockHashJoin(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 6 args");

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsStream(), "Expected WideStream as a resulting stream");
    const auto joinStreamType = AS_TYPE(TStreamType, joinType);
    MKQL_ENSURE(joinStreamType->GetItemType()->IsMulti(),
                "Expected Multi as a resulting item type");
    const auto joinComponents = GetWideComponents(joinStreamType);
    MKQL_ENSURE(joinComponents.size() > 0, "Expected at least one column");
    const TVector<TType*> joinItems(joinComponents.cbegin(), joinComponents.cend());

    const auto leftType = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(leftType->IsStream(), "Expected WideStream as a left stream");
    const auto leftStreamType = AS_TYPE(TStreamType, leftType);
    MKQL_ENSURE(leftStreamType->GetItemType()->IsMulti(),
                "Expected Multi as a left stream item type");
    const auto leftStreamComponents = GetWideComponents(leftStreamType);
    MKQL_ENSURE(leftStreamComponents.size() > 0, "Expected at least one column");
    const TVector<TType*> leftStreamItems(leftStreamComponents.cbegin(), leftStreamComponents.cend());

    const auto rightType = callable.GetInput(1).GetStaticType();
    MKQL_ENSURE(rightType->IsStream(), "Expected WideStream as a right stream");
    const auto rightStreamType = AS_TYPE(TStreamType, rightType);
    MKQL_ENSURE(rightStreamType->GetItemType()->IsMulti(),
                "Expected Multi as a right stream item type");
    const auto rightStreamComponents = GetWideComponents(rightStreamType);
    MKQL_ENSURE(rightStreamComponents.size() > 0, "Expected at least one column");
    const TVector<TType*> rightStreamItems(rightStreamComponents.cbegin(), rightStreamComponents.cend());

    const auto joinKindNode = callable.GetInput(2);
    const auto rawKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();
    const auto joinKind = GetJoinKind(rawKind);
    MKQL_ENSURE(joinKind == EJoinKind::Inner,
                "Only inner join is supported in block grace hash join prototype");

    const auto leftKeyColumnsLiteral = callable.GetInput(3);
    const auto leftKeyColumnsTuple = AS_VALUE(TTupleLiteral, leftKeyColumnsLiteral);
    TVector<ui32> leftKeyColumns;
    leftKeyColumns.reserve(leftKeyColumnsTuple->GetValuesCount());
    for (ui32 i = 0; i < leftKeyColumnsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, leftKeyColumnsTuple->GetValue(i));
        leftKeyColumns.emplace_back(item->AsValue().Get<ui32>());
    }
    const THashSet<ui32> leftKeySet(leftKeyColumns.cbegin(), leftKeyColumns.cend());

    const auto rightKeyColumnsLiteral = callable.GetInput(4);
    const auto rightKeyColumnsTuple = AS_VALUE(TTupleLiteral, rightKeyColumnsLiteral);
    TVector<ui32> rightKeyColumns;
    rightKeyColumns.reserve(rightKeyColumnsTuple->GetValuesCount());
    for (ui32 i = 0; i < rightKeyColumnsTuple->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, rightKeyColumnsTuple->GetValue(i));
        rightKeyColumns.emplace_back(item->AsValue().Get<ui32>());
    }
    const THashSet<ui32> rightKeySet(rightKeyColumns.cbegin(), rightKeyColumns.cend());

    MKQL_ENSURE(leftKeyColumns.size() == rightKeyColumns.size(), "Key columns mismatch");

    const auto leftStream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto rightStream = LocateNode(ctx.NodeLocator, callable, 1);

    return new TBlockHashJoinWraper(
        ctx.Mutables,
        std::move(joinItems),
        std::move(leftStreamItems),
        std::move(leftKeyColumns),
        std::move(rightStreamItems),
        std::move(rightKeyColumns),
        leftStream,
        rightStream
    );
}

} // namespace NKikimr::NMiniKQL

