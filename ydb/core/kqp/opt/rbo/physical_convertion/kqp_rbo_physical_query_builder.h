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
    TVector<TExprNode::TPtr> BuildPhysicalStageGraph();
    TVector<TExprNode::TPtr> PeepHoleOptimizePhysicalStages(TVector<TExprNode::TPtr>&& physicalStages);
    TVector<TExprNode::TPtr> EnableWideChannelsPhysicalStages(TVector<TExprNode::TPtr>&& physicalStages);
    TExprNode::TPtr BuildPhysicalQuery(TVector<TExprNode::TPtr>&& physicalStages);
    TExprNode::TPtr PeepHoleOptimizeStageLambda(TExprNode::TPtr stageLambda) const;
    void TypeAnnotate(TExprNode::TPtr& input);
    bool CanApplyPeepHole(TExprNode::TPtr input, const std::initializer_list<std::string_view>& callableNames) const;
    TExprNode::TPtr BuildDqPhyStage(const TVector<TExprNode::TPtr>& inputs, const TVector<TExprNode::TPtr>& args, TExprNode::TPtr physicalStageBody,
                                    NNodes::TCoNameValueTupleList&& setings, TExprContext& ctx, TPositionHandle pos) const;
    void TopologicalSort(TDqPhyStage& dqStage, TVector<TExprNode::TPtr>& result, THashSet<const TExprNode*>& visited) const;
    void TopologicalSort(TDqPhyStage&& dqStage, TVector<TExprNode::TPtr>& result) const;
    void KeepTypeAnnotationForStageAndFirstLevelChilds(TDqPhyStage& newStage, const TDqPhyStage& oldStage) const;

    TOpRoot& Root;
    TStageGraph Graph;
    THashMap<int, TExprNode::TPtr> Stages;
    THashMap<int, TVector<TExprNode::TPtr>> StageArgs;
    THashMap<int, TPositionHandle> StagePos;
    TRBOContext& RBOCtx;
};
