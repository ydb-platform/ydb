#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <yql/essentials/core/type_ann/type_ann_core.h>
#include "yql/essentials/core/type_ann/type_ann_impl.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <yql/essentials/utils/log/log.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

using TStatus = IGraphTransformer::TStatus;

namespace {
TStatus AnnotateKqpOlapPredicateClosure(const TExprNode::TPtr& node, TExprContext& ctx) {
    if (!EnsureArgsCount(*node, 2, ctx)) {
        return TStatus::Error;
    }

    auto* argsType = node->Child(TKqpOlapPredicateClosure::idx_ArgsType);

    if (!EnsureType(*argsType, ctx)) {
        return TStatus::Error;
    }
    auto argTypesTupleRaw = argsType->GetTypeAnn()->Cast<TTypeExprType>()->GetType();

    if (!EnsureTupleType(node->Pos(), *argTypesTupleRaw, ctx)) {
        return TStatus::Error;
    }
    auto argTypesTuple = argTypesTupleRaw->Cast<TTupleExprType>();

    std::vector<const TTypeAnnotationNode*> argTypes;
    argTypes.reserve(argTypesTuple->GetSize());

    for (const auto& argTypeRaw : argTypesTuple->GetItems()) {
        if (!EnsureStructType(node->Pos(), *argTypeRaw, ctx)) {
            return TStatus::Error;
        }
        argTypes.push_back(argTypeRaw);
    }

    auto& lambda = node->ChildRef(TKqpOlapPredicateClosure::idx_Lambda);
    if (!EnsureLambda(*lambda, ctx)) {
        return TStatus::Error;
    }

    if (!UpdateLambdaAllArgumentsTypes(lambda, argTypes, ctx)) {
        return TStatus::Error;
    }

    if (!lambda->GetTypeAnn()) {
        return TStatus::Repeat;
    }

    node->SetTypeAnn(ctx.MakeType<TVoidExprType>());
    return TStatus::Ok;
}
} // namespace

TAutoPtr<IGraphTransformer> CreateKqpTypeAnnotationTransformerMini(const TString& cluster,
    TIntrusivePtr<TKikimrTablesData> tablesData, TTypeAnnotationContext& typesCtx, TKikimrConfiguration::TPtr config)
{
    TAutoPtr<IGraphTransformer> dqTransformer = CreateDqTypeAnnotationTransformer(typesCtx);
    Y_UNUSED(typesCtx);

    return CreateFunctorTransformer(
        [cluster, tablesData, dqTransformer, config](const TExprNode::TPtr& input, TExprNode::TPtr& output,
            TExprContext& ctx) -> TStatus
        {
            output = input;

            TIssueScopeGuard issueScope(ctx.IssueManager, [&input, &ctx] {
                return MakeIntrusive<TIssue>(ctx.GetPosition(input->Pos()),
                        TStringBuilder() << "At function: " << input->Content());
            });

            if (TKqpOlapPredicateClosure::Match(input.Get())) {
                return AnnotateKqpOlapPredicateClosure(input, ctx);
            }

            return dqTransformer->Transform(input, output, ctx);
        });
}
} // namespace NKikimr
