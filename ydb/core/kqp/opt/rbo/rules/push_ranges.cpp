#include "kqp_rules_include.h"
#include <yql/essentials/core/extract_predicate/extract_predicate.h>

namespace {
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

bool IsValidForRange(const NYql::TExprNode::TPtr& node) {
    TExprBase expr(node);
    if (auto sqlin = expr.Maybe<TCoSqlIn>()) {
        auto collection = sqlin.Cast().Collection().Ptr();
        bool result = true;
        VisitExpr(collection,
            [&](const TExprNode::TPtr& node) {
                if (node->IsCallable() && (node->Content().StartsWith("Dq") || node->Content().StartsWith("Kql") || node->Content().StartsWith("Kqp"))) {
                    result = false;
                    return false;
                }
                return true;
            });
        return result;
    }
    return true;
}

bool IsLambdaOptionalType(TExprNode::TPtr node, const TTypeAnnotationNode* structType, TRBOContext& ctx) {
    Y_ENSURE(node);
    auto lambda = ctx.ExprCtx.DeepCopyLambda(*node);
    if (!UpdateLambdaAllArgumentsTypes(lambda, {structType}, ctx.ExprCtx)) {
        return false;
    }

    ctx.TypeAnnTransformer.Rewind();
    IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
    do {
        status = ctx.TypeAnnTransformer.Transform(lambda, lambda, ctx.ExprCtx);
    } while (status == IGraphTransformer::TStatus::Repeat);

    const TTypeAnnotationNode* lambdaType = lambda->GetTypeAnn();
    if (!lambdaType) {
        return false;
    }

    return !lambdaType->IsOptionalOrNull();
}

bool IsSuitableToExtractAndPushRanges(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx) {
    if (input->Kind != EOperator::Filter) {
        return false;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto maybeRead = filter->GetInput();
    if (maybeRead->Kind != EOperator::Source) {
        return false;
    }

    const auto read = CastOperator<TOpRead>(maybeRead);
    // Currently supported only for cs.
    if (read->GetRanges() || read->GetTableStorageType() != NYql::EStorageType::ColumnStorage || !filter->GetTypeAnn()) {
        return false;
    }

    const TTypeAnnotationNode* inputType = read->Type;
    if (!inputType) {
        return false;
    }

    auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    auto lambda = filter->FilterExpr.Node;
    // FIXME: We should be able to work with optional<bool> by inserting coalesce.
    return IsLambdaOptionalType(lambda, structType, ctx);
}

TPredicateExtractorSettings PrepareExtractorSettings(TKqpOptimizeContext& kqpCtx) {
    TPredicateExtractorSettings settings;
    settings.MergeAdjacentPointRanges = true;
    settings.HaveNextValueCallable = true;
    settings.BuildLiteralRange = false;
    settings.IsValidForRange = IsValidForRange;

    if (kqpCtx.Config->GetExtractPredicateRangesLimit() != 0) {
        settings.MaxRanges = kqpCtx.Config->GetExtractPredicateRangesLimit();
    } else {
        settings.MaxRanges = Nothing();
    }

    if (kqpCtx.QueryCtx->RuntimeParameterSizeLimitSatisfied && kqpCtx.QueryCtx->RuntimeParameterSizeLimit > 0) {
        settings.ExternalParameterMaxSize = kqpCtx.QueryCtx->RuntimeParameterSizeLimit;
    }
    return settings;
}

} // namespace

namespace NKikimr {
namespace NKqp {

TIntrusivePtr<IOperator> TPushRangesRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx, TPlanProps& props) {
    Y_UNUSED(props);
    auto& kqpCtx = rboCtx.KqpCtx;
    auto& ctx = rboCtx.ExprCtx;
    auto& typeCtx = rboCtx.TypeCtx;

    auto predicateExtractSetting = kqpCtx.Config->GetOptPredicateExtract();
    if (predicateExtractSetting == EOptionalFlag::Disabled) {
        return input;
    }

    if (!IsSuitableToExtractAndPushRanges(input, rboCtx)) {
        return input;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto read = CastOperator<TOpRead>(filter->GetInput());
    const auto tablePath = TExprBase(read->GetTable()).Cast<TKqpTable>().Path().StringValue();

    // Check for table.
    const auto tableDesc = kqpCtx.Tables->EnsureTableExists(kqpCtx.Cluster, tablePath, read->Pos, ctx);
    if (!tableDesc) {
        return input;
    }

    THashSet<TString> possibleKeys;
    auto settings = PrepareExtractorSettings(kqpCtx);
    auto extractor = MakePredicateRangeExtractor(settings);

    YQL_ENSURE(tableDesc->SchemeNode);
    auto lambda = TCoLambda(filter->FilterExpr.Node);
    // Predicate extract lib requires constraints.
    auto arg = lambda.Args().Arg(0).Ptr();
    arg->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());

    const bool prepareSuccess = extractor->Prepare(lambda.Ptr(), *arg->GetTypeAnn(), possibleKeys, ctx, typeCtx);
    YQL_ENSURE(prepareSuccess);

    TVector<TString> keyColumns;
    if (std::any_of(possibleKeys.begin(), possibleKeys.end(), [](const TString& key) { return key.find(".") != TString::npos; })) {
        for (const auto& key : tableDesc->Metadata->KeyColumnNames) {
            auto newName = read->Alias + "." + key;
            keyColumns.emplace_back(std::move(newName));
        }
    } else {
        keyColumns = tableDesc->Metadata->KeyColumnNames;
    }

    auto buildResult = extractor->BuildComputeNode(keyColumns, ctx, typeCtx);
    TExprNode::TPtr ranges = buildResult.ComputeNode;
    if (!ranges) {
        return input;
    }

    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Extracted ranges: " << KqpExprToPrettyString(*ranges, ctx);
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Pruned lambda: " << KqpExprToPrettyString(*buildResult.PrunedLambda, ctx);

    auto newRead = MakeIntrusive<TOpRead>(read->Alias, read->Columns, read->GetOutputIUs(), read->StorageType, read->TableCallable, read->OlapFilterLambda,
                                          read->Limit, ranges, read->SortDir, read->Props, read->Pos);
    return MakeIntrusive<TOpFilter>(newRead, filter->Pos, filter->Props, TExpression(buildResult.PrunedLambda, &ctx, &props));
}
} // namespace NKqp
}