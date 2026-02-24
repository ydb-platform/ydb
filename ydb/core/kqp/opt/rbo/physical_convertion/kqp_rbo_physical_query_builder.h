#pragma once
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalQueryBuilder: public NNonCopyable::TNonCopyable {
public:
    TPhysicalQueryBuilder(TOpRoot& root, TStageGraph&& graph, THashMap<ui32, TExprNode::TPtr>&& stages, THashMap<ui32, TVector<TExprNode::TPtr>>&& stageArgs,
                          THashMap<ui32, TPositionHandle>&& stagePos, TRBOContext& rboCtx)
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
    TExprNode::TPtr PeepHoleOptimize(TExprNode::TPtr input, const TVector<const TTypeAnnotationNode*>& argsType) const;
    bool CanApplyPeepHole(TExprNode::TPtr input, const std::initializer_list<std::string_view>& callableNames) const;
    TExprNode::TPtr BuildDqPhyStage(const TVector<TExprNode::TPtr>& inputs, const TVector<TExprNode::TPtr>& args, TExprNode::TPtr physicalStageBody,
                                    NNodes::TCoNameValueTupleList&& setings, TExprContext& ctx, TPositionHandle pos) const;
    void TopologicalSort(TDqPhyStage& dqStage, TVector<TExprNode::TPtr>& result, THashSet<const TExprNode*>& visited) const;
    void TopologicalSort(TDqPhyStage&& dqStage, TVector<TExprNode::TPtr>& result) const;
    void KeepTypeAnnotationForStageAndFirstLevelChilds(TDqPhyStage& newStage, const TDqPhyStage& oldStage) const;
    TVector<const TTypeAnnotationNode*> GetArgsType(TExprNode::TPtr input) const;
    bool IsCompatibleWithBlocks(const TStructExprType& type, TPositionHandle pos) const;
    bool IsSuitableToPropagateWideBlocksThroughConnection(const TDqOutput& output) const;
    bool IsSuitableToPropagateWideBlocksThroughHashShuffleConnections(const TDqPhyStage& stage) const;
    TExprNode::TPtr TypeAnnotateProgram(TExprNode::TPtr input, const TVector<const TTypeAnnotationNode*>& argsType);
    void TypeAnnotate(TExprNode::TPtr& input);

    TOpRoot& Root;
    TStageGraph Graph;
    THashMap<ui32, TExprNode::TPtr> Stages;
    THashMap<ui32, TVector<TExprNode::TPtr>> StageArgs;
    THashMap<ui32, TPositionHandle> StagePos;
    TRBOContext& RBOCtx;
};
