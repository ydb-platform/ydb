#pragma once
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalQueryBuilder: public NNonCopyable::TNonCopyable {
public:
    TPhysicalQueryBuilder(TOpRoot& root, TStageGraph&& graph, THashMap<int, TExprNode::TPtr>&& stages, THashMap<int, TVector<TExprNode::TPtr>>&& stageArgs,
                          THashMap<int, TPositionHandle>&& stagePos, TRBOContext& rboCtx)
        : Root(root)
        , Graph(std::move(graph))
        , Stages(std::move(stages))
        , StageArgs(std::move(stageArgs))
        , StagePos(std::move(stagePos))
        , RBOCtx(rboCtx) {
    }

    TExprNode::TPtr BuildPhysicalQuery();
    TPhysicalQueryBuilder() = delete;
    ~TPhysicalQueryBuilder() = default;

private:
    TVector<std::pair<TExprNode::TPtr, TPositionHandle>> BuildPhysicalStageGraph();
    TVector<TExprNode::TPtr> PeepHoleOptimizePhysicalStages(TVector<std::pair<TExprNode::TPtr, TPositionHandle>>&& physicalStages);
    TExprNode::TPtr BuildPhysicalQuery(TVector<TExprNode::TPtr>&& physicalStages);
    TExprNode::TPtr PeepHoleOptimizeStageLambda(TExprNode::TPtr stageLambda);
    void TypeAnnotate(TExprNode::TPtr input);
    bool CanApplyPeepHole(TExprNode::TPtr input, const std::initializer_list<std::string_view>& callableNames);
    TExprNode::TPtr BuildDqPhyStage(const TVector<TExprNode::TPtr>& inputs, const TVector<TExprNode::TPtr>& args, TExprNode::TPtr physicalStageBody,
                                    TExprContext& ctx, TPositionHandle pos);

    TOpRoot& Root;
    TStageGraph Graph;
    THashMap<int, TExprNode::TPtr> Stages;
    THashMap<int, TVector<TExprNode::TPtr>> StageArgs;
    THashMap<int, TPositionHandle> StagePos;
    TRBOContext& RBOCtx;
};
