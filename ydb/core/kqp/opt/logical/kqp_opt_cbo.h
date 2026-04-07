#pragma once

#include <yql/essentials/ast/yql_expr.h>
#include <ydb/core/kqp/opt/cbo/cbo_optimizer_new.h>

#include <ydb/core/kqp/opt/kqp_opt.h>

namespace NKikimr::NKqp::NOpt {

/**
 * KQP specific Rel node, includes a pointer to ExprNode
*/
struct TKqpRelOptimizerNode : public TRelOptimizerNode {
    const NYql::TExprNode::TPtr Node;

    TKqpRelOptimizerNode(TString label, TOptimizerStatistics stats, const NYql::TExprNode::TPtr node) :
        TRelOptimizerNode(label, std::move(stats)), Node(node) { }
};

/**
 * KQP Specific cost function and join applicability cost function
*/
struct TKqpProviderContext : public TBaseProviderContext {
    TKqpProviderContext(const TKqpOptimizeContext& kqpCtx, const int optLevel, bool blockJoinEnabled) : KqpCtx(kqpCtx), OptLevel(optLevel), BlockJoinEnabled(blockJoinEnabled) {}
    TKqpProviderContext(const TKqpOptimizeContext& kqpCtx, const int optLevel, bool blockJoinEnabled, const NYql::TKikimrConfiguration::TPtr& config) : 
        KqpCtx(kqpCtx), OptLevel(optLevel), BlockJoinEnabled(blockJoinEnabled) {
            SetConstants(config);
    }

    void SetConstants(const NYql::TKikimrConfiguration::TPtr& config);

    virtual bool IsJoinApplicable(
        const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind
    ) override;

    virtual double ComputeJoinCost(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const double outputRows,
        const double outputByteSize,
        EJoinAlgoType joinAlgo
    ) const override;

    virtual TOptimizerStatistics ComputeJoinStats(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint = nullptr) const override;

    virtual TOptimizerStatistics ComputeJoinStatsV1(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint,
        bool shuffleLeftSide,
        bool shuffleRightSide
    ) const override;

    virtual TOptimizerStatistics ComputeJoinStatsV2(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const TVector<TJoinColumn>& leftJoinKeys,
        const TVector<TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint,
        bool shuffleLeftSide,
        bool shuffleRightSide,
        TCardinalityHints::TCardinalityHint* maybeBytesHint
    ) const override;

    const TKqpOptimizeContext& KqpCtx;
    int OptLevel;
    bool BlockJoinEnabled;

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
