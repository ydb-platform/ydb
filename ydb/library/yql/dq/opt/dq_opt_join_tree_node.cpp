#include "dq_opt_join_tree_node.h"

namespace NYql::NDq {

std::shared_ptr<TJoinOptimizerNodeInternal> MakeJoinInternal(
    TOptimizerStatistics&& stats,
    std::shared_ptr<IBaseOptimizerNode> left,
    std::shared_ptr<IBaseOptimizerNode> right,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys,
    EJoinKind joinKind,
    EJoinAlgoType joinAlgo,
    bool leftAny,
    bool rightAny,
    const std::optional<TOrderingsStateMachine::TLogicalOrderings>& logicalOrderings
) {
    auto res = std::make_shared<TJoinOptimizerNodeInternal>(left, right, leftJoinKeys, rightJoinKeys, joinKind, joinAlgo, leftAny, rightAny);
    res->Stats = std::move(stats);
    if (logicalOrderings.has_value()) {
        res->Stats.LogicalOrderings = logicalOrderings.value();
    }
    return res;
}

std::shared_ptr<TJoinOptimizerNode> ConvertFromInternal(
    const std::shared_ptr<IBaseOptimizerNode>& internal,
    bool enableShuffleElimination,
    const TFDStorage* fdStorage
) {
    Y_ENSURE(internal->Kind == EOptimizerNodeKind::JoinNodeType);

    if (dynamic_cast<TJoinOptimizerNode*>(internal.get()) != nullptr) {
        return  std::static_pointer_cast<TJoinOptimizerNode>(internal);
    }

    auto join = std::static_pointer_cast<TJoinOptimizerNodeInternal>(internal);

    auto left = join->LeftArg;
    auto right = join->RightArg;

    if (left->Kind == EOptimizerNodeKind::JoinNodeType) {
        left = ConvertFromInternal(left, enableShuffleElimination, fdStorage);
    }
    if (right->Kind == EOptimizerNodeKind::JoinNodeType) {
        right = ConvertFromInternal(right, enableShuffleElimination, fdStorage);
    }

    auto newJoin = std::make_shared<TJoinOptimizerNode>(left, right, join->LeftJoinKeys, join->RightJoinKeys, join->JoinType, join->JoinAlgo, join->LeftAny, join->RightAny);
    newJoin->Stats = std::move(join->Stats);

    if (
        !enableShuffleElimination && join->JoinAlgo == EJoinAlgoType::GraceJoin ||
        join->ShuffleLeftSideByOrderingIdx == TJoinOptimizerNodeInternal::NoOrdering
    ) {
        left->Stats.ShuffledByColumns =
            TIntrusivePtr<TOptimizerStatistics::TShuffledByColumns>(
                new TOptimizerStatistics::TShuffledByColumns(join->LeftJoinKeys)
            );
    } else if (join->ShuffleLeftSideByOrderingIdx >= 0 && fdStorage) {
        auto shuffledBy = fdStorage->GetInterestingOrderingsColumnNamesByIdx(join->ShuffleLeftSideByOrderingIdx);

        left->Stats.ShuffledByColumns =
            TIntrusivePtr<TOptimizerStatistics::TShuffledByColumns>(
                new TOptimizerStatistics::TShuffledByColumns(std::move(shuffledBy))
            );
    } else {
        left->Stats.ShuffledByColumns = nullptr;
    }

    if (
        (!enableShuffleElimination && join->JoinAlgo == EJoinAlgoType::GraceJoin) ||
        join->ShuffleRightSideByOrderingIdx == TJoinOptimizerNodeInternal::NoOrdering
    ) {
        right->Stats.ShuffledByColumns =
            TIntrusivePtr<TOptimizerStatistics::TShuffledByColumns>(
                new TOptimizerStatistics::TShuffledByColumns(join->RightJoinKeys)
            );
    } else if (join->ShuffleRightSideByOrderingIdx >= 0 && fdStorage) {
        auto shuffledBy = fdStorage->GetInterestingOrderingsColumnNamesByIdx(join->ShuffleRightSideByOrderingIdx);

        right->Stats.ShuffledByColumns =
            TIntrusivePtr<TOptimizerStatistics::TShuffledByColumns>(
                new TOptimizerStatistics::TShuffledByColumns(std::move(shuffledBy))
            );
    } else {
        right->Stats.ShuffledByColumns = nullptr;
    }

    return newJoin;
}

} // namespace NYql::NDq
