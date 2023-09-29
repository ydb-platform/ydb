#include "dq_opt.h"

#include <ydb/library/yql/core/yql_type_annotation.h>

namespace NYql::NDq {

void InferStatisticsForFlatMap(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForFilter(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForSkipNullMembers(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForExtractMembers(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForAggregateCombine(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForAggregateMergeFinalize(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void PropagateStatisticsToLambdaArgument(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void PropagateStatisticsToStageArguments(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForStage(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForDqSource(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForGraceJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForMapJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);

} // namespace NYql::NDq {
