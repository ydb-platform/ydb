#include "kqp_rbo_range_extraction.h"

#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NKikimr::NKqp::NRangeExtraction {

namespace {

using namespace NYql::NNodes;

bool IsValidForRange(const NYql::TExprNode::TPtr& node) {
    TExprBase expr(node);
    if (auto sqlin = expr.Maybe<TCoSqlIn>()) {
        auto collection = sqlin.Cast().Collection().Ptr();
        bool result = true;
        VisitExpr(collection, [&](const TExprNode::TPtr& node) {
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

    return lambdaType->IsOptionalOrNull();
}

// Map a physical table column name to the name the read actually exposes. Projection elimination
// can rename read outputs (most commonly by stripping the alias), so Columns[i] (physical) is
// aligned with OutputIUs[i] (exposed). Columns not selected by the read are absent from the map.
THashMap<TString, TString> BuildPhysicalToExposedName(const TOpRead& read) {
    THashMap<TString, TString> result;
    const size_t count = std::min(read.Columns.size(), read.OutputIUs.size());
    for (size_t i = 0; i < count; ++i) {
        result[read.Columns[i]] = read.OutputIUs[i].GetFullName();
    }
    return result;
}

// Resolve the name a physical column should carry so it stays consistent with the read's exposed
// outputs and the lambda the predicate extractor is typed against.
TString ResolveExposedName(const TString& physicalName, const TOpRead& read,
                           const THashMap<TString, TString>& physicalToExposed, bool exposesQualified) {
    if (const auto it = physicalToExposed.find(physicalName); it != physicalToExposed.end()) {
        return it->second;
    }
    if (exposesQualified && !read.Alias.empty()) {
        return read.Alias + "." + physicalName;
    }
    return physicalName;
}

bool ExposesQualifiedNames(const TOpRead& read) {
    return std::any_of(read.OutputIUs.begin(), read.OutputIUs.end(), [](const TInfoUnit& iu) { return !iu.GetAlias().empty(); });
}

} // anonymous namespace

TPredicateExtractorSettings PrepareExtractorSettings(NOpt::TKqpOptimizeContext& kqpCtx) {
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

TExprNode::TPtr GetLambdaForRangeExtractor(TExprNode::TPtr node, const TTypeAnnotationNode* inputType, TRBOContext& rboCtx) {
    if (!inputType) {
        return node;
    }

    auto& ctx = rboCtx.ExprCtx;
    auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    if (!IsLambdaOptionalType(node, structType, rboCtx)) {
        return node;
    }

    auto lambda = TCoLambda(node);
    // clang-format off
    auto newBody = Build<TCoCoalesce>(ctx, node->Pos())
        .Predicate(lambda.Body())
        .Value<TCoBool>()
            .Literal().Build("false")
        .Build()
    .Done();
    // clang-format on

    // clang-format off
    auto newLambda = Build<TCoLambda>(ctx, node->Pos())
        .Args({"arg"})
        .Body<TExprApplier>()
            .Apply(newBody)
            .With(lambda.Args().Arg(0), "arg")
        .Build()
    .Done();
    // clang-format on

    TVector<const TTypeAnnotationNode*> argTypes{structType};
    // clang-format off
    auto predicateClosure = Build<TKqpPredicateClosure>(ctx, node->Pos())
        .Lambda(newLambda)
        .ArgsType(ExpandType(node->Pos(), *ctx.MakeType<TTupleExprType>(argTypes), ctx))
    .Done();
    // clang-format on

    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Range exctractor, before peephole: " << KqpExprToPrettyString(predicateClosure, ctx);

    TExprNode::TPtr afterPeephole;
    bool hasNonDeterministicFunctions;
    if (const auto status = PeepHoleOptimizeNode(predicateClosure.Ptr(), afterPeephole, ctx, rboCtx.TypeCtx, nullptr,
                                                 hasNonDeterministicFunctions);
        status != IGraphTransformer::TStatus::Ok) {
        YQL_CLOG(ERROR, ProviderKqp) << "[NEW RBO] Peephole failed with status: " << status << Endl;
        afterPeephole = nullptr;
    }
    Y_ENSURE(afterPeephole);
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Range exctractor, after peephole: " << KqpExprToPrettyString(TExprBase(afterPeephole), ctx);

    return TExprBase(afterPeephole).Cast<TKqpPredicateClosure>().Lambda().Ptr();
}


const TStructExprType* PrepareSchemeType(const TOpRead& read, const TStructExprType* schemeType, TExprContext& ctx) {
    const auto physicalToExposed = BuildPhysicalToExposedName(read);
    const bool exposesQualified = ExposesQualifiedNames(read);

    TVector<const TItemExprType*> newItemTypes;
    bool changed = false;
    for (const auto itemType : schemeType->GetItems()) {
        const TString physicalName(itemType->GetName());
        const auto newName = ResolveExposedName(physicalName, read, physicalToExposed, exposesQualified);
        changed |= newName != physicalName;
        newItemTypes.push_back(ctx.MakeType<TItemExprType>(newName, itemType->GetItemType()));
    }

    return changed ? ctx.MakeType<TStructExprType>(newItemTypes) : schemeType;
}

TVector<TString> ResolveExposedKeyColumns(const TOpRead& read, const TVector<TString>& physicalKeyColumns) {
    const auto physicalToExposed = BuildPhysicalToExposedName(read);
    const bool exposesQualified = ExposesQualifiedNames(read);

    TVector<TString> keyColumns;
    keyColumns.reserve(physicalKeyColumns.size());
    for (const auto& key : physicalKeyColumns) {
        keyColumns.emplace_back(ResolveExposedName(key, read, physicalToExposed, exposesQualified));
    }
    return keyColumns;
}

} // namespace NKikimr::NKqp::NRangeExtraction
