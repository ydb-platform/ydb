#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>

namespace NYql::NDq {

IGraphTransformer::TStatus AnnotateDqStage(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqPhyLength(const TExprNode::TPtr& node, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqPhyStage(const TExprNode::TPtr & input, TExprContext & ctx);
IGraphTransformer::TStatus AnnotateDqOutput(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqCnHashShuffle(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqCnValue(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqCnResult(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqReplicate(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqConnection(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqCnMerge(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqJoin(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqMapOrDictJoin(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqCrossJoin(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqSource(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqSink(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqQuery(const TExprNode::TPtr& input, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqPrecompute(const TExprNode::TPtr& node, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqPhyPrecompute(const TExprNode::TPtr& node, TExprContext& ctx);
IGraphTransformer::TStatus AnnotateDqTransform(const TExprNode::TPtr& input, TExprContext& ctx);

THolder<IGraphTransformer> CreateDqTypeAnnotationTransformer(NYql::TTypeAnnotationContext& typesCtx);

bool IsTypeSupportedInMergeCn(EDataSlot type);
bool IsTypeSupportedInMergeCn(const TDataExprType* dataType);
bool IsMergeConnectionApplicable(const TVector<const TTypeAnnotationNode*>& sortKeyTypes);

struct TDqStageSettings {
    static constexpr TStringBuf LogicalIdSettingName = "_logical_id";
    static constexpr TStringBuf IdSettingName = "_id";
    static constexpr TStringBuf SinglePartitionSettingName = "_single_partition";
    static constexpr TStringBuf WideChannelsSettingName = "_wide_channels";

    ui64 LogicalId = 0;
    TString Id;
    bool SinglePartition = false;

    bool WideChannels = false;
    const TStructExprType* OutputNarrowType = nullptr;

    TDqStageSettings& SetSinglePartition(bool value = true) { SinglePartition = value; return *this; }
    TDqStageSettings& SetWideChannels(const TStructExprType& narrowType) { WideChannels = true; OutputNarrowType = &narrowType; return *this; }

    static TDqStageSettings New(const NNodes::TDqStageBase& node);
    static TDqStageSettings New();

    static TDqStageSettings Parse(const NNodes::TDqStageBase& node);
    static bool Validate(const TExprNode& stage, TExprContext& ctx);
    NNodes::TCoNameValueTupleList BuildNode(TExprContext& ctx, TPositionHandle pos) const;
};


TString PrintDqStageOnly(const NNodes::TDqStageBase& stage, TExprContext& ctx);

} // namespace NYql::NDq
