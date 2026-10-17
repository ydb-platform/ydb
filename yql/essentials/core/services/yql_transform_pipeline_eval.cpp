#include "yql_transform_pipeline.h"
#include "yql_eval_expr.h"

namespace NYql {

TTransformationPipeline& TTransformationPipeline::AddExpressionEvaluation(const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
                                                                          IGraphTransformer* calcTransformer, EYqlIssueCode issueCode) {
    Transformers_.push_back(TTransformStage(
        CreateEvaluateExpressionTransformer(*TypeAnnotationContext_, functionRegistry, calcTransformer, TypeAnnCallableFactory_),
        "EvaluateExpression",
        issueCode));

    return *this;
}

} // namespace NYql
