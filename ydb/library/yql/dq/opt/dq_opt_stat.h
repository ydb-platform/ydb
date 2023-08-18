#include "dq_opt.h"

#include <ydb/library/yql/core/yql_type_annotation.h>

namespace NYql::NDq {

void InferStatisticsForFlatMap(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);
void InferStatisticsForSkipNullMembers(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);

} // namespace NYql::NDq {