#include "dq_opt_join_tree_node.h"

namespace NYql::NDq {

std::shared_ptr<TJoinOptimizerNodeInternal> MakeJoinInternal(
    std::shared_ptr<IBaseOptimizerNode> left,
    std::shared_ptr<IBaseOptimizerNode> right,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys,
    EJoinKind joinKind,
    EJoinAlgoType joinAlgo,
    bool leftAny,
    bool rightAny,
    IProviderContext& ctx,
    TCardinalityHints::TCardinalityHint* maybeHint) {

    auto res = std::make_shared<TJoinOptimizerNodeInternal>(left, right, leftJoinKeys, rightJoinKeys, joinKind, joinAlgo, leftAny, rightAny);
    res->Stats = std::make_shared<TOptimizerStatistics>(ctx.ComputeJoinStats(*left->Stats, *right->Stats, leftJoinKeys, rightJoinKeys, joinAlgo, joinKind, maybeHint));
    return res;
}

std::shared_ptr<TJoinOptimizerNode> ConvertFromInternal(const std::shared_ptr<IBaseOptimizerNode> internal) {
    Y_ENSURE(internal->Kind == EOptimizerNodeKind::JoinNodeType);

    if (dynamic_cast<TJoinOptimizerNode*>(internal.get()) != nullptr) {
        return  std::static_pointer_cast<TJoinOptimizerNode>(internal);
    }

    auto join = std::static_pointer_cast<TJoinOptimizerNodeInternal>(internal);

    auto left = join->LeftArg;
    auto right = join->RightArg;

    if (left->Kind == EOptimizerNodeKind::JoinNodeType) {
        left = ConvertFromInternal(left);
    }
    if (right->Kind == EOptimizerNodeKind::JoinNodeType) {
        right = ConvertFromInternal(right);
    }

    auto newJoin = std::make_shared<TJoinOptimizerNode>(left, right, join->LeftJoinKeys, join->RightJoinKeys, join->JoinType, join->JoinAlgo, join->LeftAny, join->RightAny);
    newJoin->Stats = join->Stats;
    return newJoin;
}

} // namespace NYql::NDq
