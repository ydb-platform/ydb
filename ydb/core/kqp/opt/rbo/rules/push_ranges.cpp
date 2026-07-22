#include "kqp_rbo_range_extraction.h"

#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/core/extract_predicate/extract_predicate.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp::NRangeExtraction;

bool IsSuitableToExtractAndPushRanges(const TIntrusivePtr<IOperator>& input) {
    if (input->Kind != EOperator::Filter) {
        return false;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto maybeRead = filter->GetInput();
    if (maybeRead->Kind != EOperator::Source) {
        return false;
    }

    const auto type = filter->FilterExpr.Node->GetTypeAnn();
    if (!type || type->GetKind() == ETypeAnnotationKind::Pg) {
        return false;
    }

    const auto read = CastOperator<TOpRead>(maybeRead);
    const auto tableType = read->GetTableStorageType();
    return !read->GetRanges() && (tableType == NYql::EStorageType::ColumnStorage || tableType == NYql::EStorageType::RowStorage);
}

} // anonymous namespace

bool TPushRangesRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter &&
        input->Children.front()->Kind == EOperator::Source;
}

TIntrusivePtr<IOperator> TPushRangesRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx, TPlanProps& props) {
    Y_UNUSED(props);
    auto& kqpCtx = rboCtx.KqpCtx;
    auto& ctx = rboCtx.ExprCtx;
    auto& typeCtx = rboCtx.TypeCtx;

    auto predicateExtractSetting = kqpCtx.Config->GetOptPredicateExtract();
    if (predicateExtractSetting == EOptionalFlag::Disabled) {
        return input;
    }

    if (!IsSuitableToExtractAndPushRanges(input)) {
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

    auto lambda = TCoLambda(GetLambdaForRangeExtractor(filter->FilterExpr.Node, read->Type, rboCtx));
    auto originalLambda = rboCtx.ExprCtx.DeepCopyLambda(*lambda.Ptr());
    // Predicate extract lib requires constraints.
    auto arg = lambda.Args().Arg(0).Ptr();
    arg->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());

    THashSet<TString> possibleKeys;
    auto settings = PrepareExtractorSettings(kqpCtx);
    auto extractor = MakePredicateRangeExtractor(settings);
    auto schemeType = PrepareSchemeType(*read, tableDesc->SchemeNode, ctx);
    const bool prepareSuccess = extractor->Prepare(lambda.Ptr(), *schemeType, possibleKeys, ctx, typeCtx);
    YQL_ENSURE(prepareSuccess);

    // Key columns must be named exactly as the scheme type exposes them, so the compute node lines up
    // with the names the predicate extractor resolved.
    const auto keyColumns = ResolveExposedKeyColumns(*read, tableDesc->Metadata->KeyColumnNames);

    auto buildResult = extractor->BuildComputeNode(keyColumns, ctx, typeCtx);
    TExprNode::TPtr ranges = buildResult.ComputeNode;
    if (!ranges) {
        return input;
    }

    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Extracted ranges: " << KqpExprToPrettyString(*ranges, ctx);
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Pruned lambda: " << KqpExprToPrettyString(*buildResult.PrunedLambda, ctx);

    TOpRead::TRangeInfo rangeInfo{
        .ComputeNode = ranges,
        .KeyColumns = keyColumns,
        .UsedPrefixLen = buildResult.UsedPrefixLen,
        .ExpectedMaxRanges = buildResult.ExpectedMaxRanges
            ? TMaybe<size_t>(*buildResult.ExpectedMaxRanges)
            : TMaybe<size_t>(),
    };
    auto newRead = MakeIntrusive<TOpRead>(read->Alias, read->Columns, read->GetOutputIUs(), read->StorageType, read->TableCallable, read->OlapFilterLambda,
                                          read->Limit, std::move(rangeInfo), TExpression(originalLambda, &ctx, &props), read->SortDir, read->Props, read->Pos);
    return MakeIntrusive<TOpFilter>(newRead, filter->Pos, filter->Props, TExpression(buildResult.PrunedLambda, &ctx, &props));
}
} // namespace NKikimr::NKqp
