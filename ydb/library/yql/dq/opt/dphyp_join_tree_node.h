#pragma once

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h> 

namespace NYql::NDq {

struct TJoinOptimizerNodeInternal : public IBaseOptimizerNode {
    std::shared_ptr<IBaseOptimizerNode> LeftArg;
    std::shared_ptr<IBaseOptimizerNode> RightArg;
    const std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>>& JoinConditions;
    const TVector<TString>& LeftJoinKeys;
    const TVector<TString>& RightJoinKeys;
    EJoinKind JoinType;
    EJoinAlgoType JoinAlgo;
    bool IsReorderable;

    TJoinOptimizerNodeInternal(const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>>& joinConditions,
        const TVector<TString>& leftJoinKeys,
        const TVector<TString>& rightJoinKeys,
        const EJoinKind joinType,
        const EJoinAlgoType joinAlgo,
        bool nonReorderable=false);

    virtual ~TJoinOptimizerNodeInternal() {}
    virtual TVector<TString> Labels();
    virtual void Print(std::stringstream& stream, int ntabs=0);
};

/**
 * Create a new internal join node and compute its statistics and cost
*/
std::shared_ptr<TJoinOptimizerNodeInternal> MakeJoinInternal(std::shared_ptr<IBaseOptimizerNode> left,
    std::shared_ptr<IBaseOptimizerNode> right,
    const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions,
    const TVector<TString>& leftJoinKeys,
    const TVector<TString>& rightJoinKeys,
    EJoinKind joinKind,
    EJoinAlgoType joinAlgo,
    IProviderContext& ctx) {

    auto res = std::make_shared<TJoinOptimizerNodeInternal>(left, right, joinConditions, leftJoinKeys, rightJoinKeys, joinKind, joinAlgo);
    res->Stats = std::make_shared<TOptimizerStatistics>(ComputeJoinStats(*left->Stats, *right->Stats, leftJoinKeys, rightJoinKeys, joinAlgo, ctx));
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

    auto newJoin = std::make_shared<TJoinOptimizerNode>(left, right, join->JoinConditions, join->JoinType, join->JoinAlgo, !join->IsReorderable);
    newJoin->Stats = join->Stats;
    return newJoin;
}


}