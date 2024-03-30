#pragma once

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h> 

namespace NYql::NDq::NDphyp {

struct TJoinOptimizerNodeInternal : public IBaseOptimizerNode {
    TJoinOptimizerNodeInternal(
        const std::shared_ptr<IBaseOptimizerNode>& left, 
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions,
        const TVector<TString>& leftJoinKeys,
        const TVector<TString>& rightJoinKeys, 
        const EJoinKind joinType, 
        const EJoinAlgoType joinAlgo
    ) 
        : IBaseOptimizerNode(JoinNodeType)
        , LeftArg(left)
        , RightArg(right)
        , JoinConditions(joinConditions)
        , LeftJoinKeys(leftJoinKeys)
        , RightJoinKeys(rightJoinKeys)
        , JoinType(joinType)
        , JoinAlgo(joinAlgo)
    {}

    virtual ~TJoinOptimizerNodeInternal() = default;
    virtual TVector<TString> Labels() {
        auto res = LeftArg->Labels();
        auto rightLabels = RightArg->Labels();
        res.insert(res.begin(),rightLabels.begin(),rightLabels.end());
        return res;
    }

    virtual void Print(std::stringstream&, int) {
    }

    std::shared_ptr<IBaseOptimizerNode> LeftArg;
    std::shared_ptr<IBaseOptimizerNode> RightArg;
    const std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>>& JoinConditions;
    const TVector<TString>& LeftJoinKeys;
    const TVector<TString>& RightJoinKeys;
    EJoinKind JoinType;
    EJoinAlgoType JoinAlgo;
};

/**
 * Create a new internal join node and compute its statistics and cost
*/
std::shared_ptr<TJoinOptimizerNodeInternal> MakeJoinInternal(
    std::shared_ptr<IBaseOptimizerNode> left,
    std::shared_ptr<IBaseOptimizerNode> right,
    const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions,
    const TVector<TString>& leftJoinKeys,
    const TVector<TString>& rightJoinKeys,
    EJoinKind joinKind,
    EJoinAlgoType joinAlgo,
    IProviderContext& ctx
);

std::shared_ptr<TJoinOptimizerNode> ConvertFromInternal(const std::shared_ptr<IBaseOptimizerNode> internal);

}