#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/cbo/cbo_optimizer_new.h>

#include <ydb/core/kqp/opt/kqp_opt.h>

namespace NKikimr::NKqp::NOpt {

/**
 * KQP specific Rel node, includes a pointer to ExprNode
*/
struct TKqpRelOptimizerNode : public NYql::TRelOptimizerNode {
    const NYql::TExprNode::TPtr Node;

    TKqpRelOptimizerNode(TString label, NYql::TOptimizerStatistics stats, const NYql::TExprNode::TPtr node) : 
        TRelOptimizerNode(label, std::move(stats)), Node(node) { }
};

/**
 * KQP Specific cost function and join applicability cost function
*/
struct TKqpProviderContext : public NYql::TBaseProviderContext {
    TKqpProviderContext(const TKqpOptimizeContext& kqpCtx, const int optLevel) : KqpCtx(kqpCtx), OptLevel(optLevel) {}
    TKqpProviderContext(const TKqpOptimizeContext& kqpCtx, const int optLevel, const NYql::TKikimrConfiguration::TPtr& config) : 
        KqpCtx(kqpCtx), OptLevel(optLevel) {
            SetConstants(config);
    }

    void SetConstants(const NYql::TKikimrConfiguration::TPtr& config);

    virtual bool IsJoinApplicable(
        const std::shared_ptr<NYql::IBaseOptimizerNode>& left, 
        const std::shared_ptr<NYql::IBaseOptimizerNode>& right, 
        const TVector<NYql::NDq::TJoinColumn>& leftJoinKeys, 
        const TVector<NYql::NDq::TJoinColumn>& rightJoinKeys,
        NYql::EJoinAlgoType joinAlgo,  
        NYql::EJoinKind joinKind
    ) override;

    virtual double ComputeJoinCost(
        const NYql::TOptimizerStatistics& leftStats, 
        const NYql::TOptimizerStatistics& rightStats, 
        const double outputRows, 
        const double outputByteSize, 
        NYql::EJoinAlgoType joinAlgo
    ) const override;

    virtual NYql::TOptimizerStatistics ComputeJoinStats(
        const NYql::TOptimizerStatistics& leftStats,
        const NYql::TOptimizerStatistics& rightStats,
        const TVector<NYql::NDq::TJoinColumn>& leftJoinKeys,
        const TVector<NYql::NDq::TJoinColumn>& rightJoinKeys,
        NYql::EJoinAlgoType joinAlgo,
        NYql::EJoinKind joinKind,
        NYql::TCardinalityHints::TCardinalityHint* maybeHint = nullptr) const override;

    virtual NYql::TOptimizerStatistics ComputeJoinStatsV1(
        const NYql::TOptimizerStatistics& leftStats,
        const NYql::TOptimizerStatistics& rightStats,
        const TVector<NYql::NDq::TJoinColumn>& leftJoinKeys,
        const TVector<NYql::NDq::TJoinColumn>& rightJoinKeys,
        NYql::EJoinAlgoType joinAlgo,
        NYql::EJoinKind joinKind,
        NYql::TCardinalityHints::TCardinalityHint* maybeHint,
        bool shuffleLeftSide,
        bool shuffleRightSide) const override;

    virtual NYql::TOptimizerStatistics ComputeJoinStatsV2(
        const NYql::TOptimizerStatistics& leftStats,
        const NYql::TOptimizerStatistics& rightStats,
        const TVector<NYql::NDq::TJoinColumn>& leftJoinKeys,
        const TVector<NYql::NDq::TJoinColumn>& rightJoinKeys,
        NYql::EJoinAlgoType joinAlgo,
        NYql::EJoinKind joinKind,
        NYql::TCardinalityHints::TCardinalityHint* maybeHint,
        bool shuffleLeftSide,
        bool shuffleRightSide,
        NYql::TCardinalityHints::TCardinalityHint* maybeBytesHint) const override;

    const TKqpOptimizeContext& KqpCtx;
    int OptLevel;

    ui32 CONSTS_MAX_DEPTH = 3;
    double CONSTS_SEL_MULT = 0.5;
    double CONSTS_SEL_POW = 0.5;

    double CONSTS_SHUFFLE_LEFT_SIDE_MULT = 0.5;
    double CONSTS_SHUFFLE_LEFT_SIDE_POW = 1.0;
    double CONSTS_SHUFFLE_RIGHT_SIDE_MULT = 0.5;
    double CONSTS_SHUFFLE_RIGHT_SIDE_POW = 1.0;

    double CONSTS_INTERACTION_MULT = 0.5;
    double CONSTS_INTERACTION_POW = 0.5;

    double CONSTS_MAPJOIN_LEFT_SIDE_MULT = 1.0;
    double CONSTS_MAPJOIN_LEFT_SIDE_POW = 1.0;
    double CONSTS_MAPJOIN_RIGHT_SIDE_MULT = 1.8;
    double CONSTS_MAPJOIN_RIGHT_SIDE_POW = 1.0;
    double CONSTS_MAPJOIN_OUTPUT_MULT = 0.5;
    double CONSTS_MAPJOIN_OUTPUT_POW = 1.0;

    double CONSTS_GRACEJOIN_LEFT_SIDE_MULT = 2.5;
    double CONSTS_GRACEJOIN_LEFT_SIDE_POW = 1.2;
    double CONSTS_GRACEJOIN_RIGHT_SIDE_MULT = 3.0;
    double CONSTS_GRACEJOIN_RIGHT_SIDE_POW = 1.2;
    double CONSTS_GRACEJOIN_OUTPUT_MULT = 0.5;
    double CONSTS_GRACEJOIN_OUTPUT_POW = 1.0;
    
};

}
