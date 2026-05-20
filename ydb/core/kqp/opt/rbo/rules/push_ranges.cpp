#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/core/extract_predicate/extract_predicate.h>
#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

namespace NKikimr::NKqp {

namespace {

using namespace NYql::NNodes;
using namespace NKikimr;

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
    // Currently supported only for cs.
    return !read->GetRanges() && read->GetTableStorageType() == NYql::EStorageType::ColumnStorage;
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
TString StripAliasPrefix(const TString& column) {
    const auto dot = column.rfind('.');
    return (dot != TString::npos) ? column.substr(dot + 1) : column;
}

TString NormalizeLiteralText(TString value) {
    if (value.size() >= 2 && value.front() == '"' && value.back() == '"') {
        return value.substr(1, value.size() - 2);
    }
    return value;
}

bool IsNothingLike(const TExprNode::TPtr& node) {
    if (!node) {
        return false;
    }

    if (node->IsCallable("Nothing")) {
        return true;
    }

    if (node->IsCallable({"Just", "SafeCast", "StrictCast"}) && node->ChildrenSize() > 0) {
        return IsNothingLike(node->HeadPtr());
    }

    return false;
}

std::optional<TString> FormatLiteralValue(const TExprNode::TPtr& node) {
    if (!node || IsNothingLike(node)) {
        return std::nullopt;
    }

    if (node->IsCallable({"Just", "SafeCast", "StrictCast"}) && node->ChildrenSize() > 0) {
        return FormatLiteralValue(node->HeadPtr());
    }

    if (auto data = TExprBase(node).Maybe<TCoDataCtor>()) {
        return NormalizeLiteralText(TString(data.Cast().Literal().Value()));
    }

    if (auto pg = TExprBase(node).Maybe<TCoPgConst>()) {
        return NormalizeLiteralText(TString(pg.Cast().Value().Value()));
    }

    return std::nullopt;
}

struct TExplainRangeBound {
    std::optional<TString> Value;
    bool Inclusive = false;
};

struct TExplainColumnRange {
    TExplainRangeBound From;
    TExplainRangeBound To;
};

using TExplainRange = TVector<TExplainColumnRange>;
using TExplainRanges = TVector<TExplainRange>;

TExplainColumnRange MakeFullExplainRange() {
    return {};
}

std::optional<TExplainRangeBound> IntersectFromBound(const TExplainRangeBound& left, const TExplainRangeBound& right) {
    if (!left.Value) {
        return right;
    }
    if (!right.Value) {
        return left;
    }
    if (*left.Value != *right.Value) {
        return std::nullopt;
    }

    return TExplainRangeBound{.Value = left.Value, .Inclusive = left.Inclusive && right.Inclusive};
}

std::optional<TExplainRangeBound> IntersectToBound(const TExplainRangeBound& left, const TExplainRangeBound& right) {
    if (!left.Value) {
        return right;
    }
    if (!right.Value) {
        return left;
    }
    if (*left.Value != *right.Value) {
        return std::nullopt;
    }

    return TExplainRangeBound{.Value = left.Value, .Inclusive = left.Inclusive && right.Inclusive};
}

std::optional<TExplainColumnRange> IntersectColumnRange(const TExplainColumnRange& left, const TExplainColumnRange& right) {
    auto from = IntersectFromBound(left.From, right.From);
    auto to = IntersectToBound(left.To, right.To);
    if (!from || !to) {
        return std::nullopt;
    }

    return TExplainColumnRange{.From = std::move(*from), .To = std::move(*to)};
}

std::optional<TExplainRange> IntersectExplainRange(const TExplainRange& left, const TExplainRange& right) {
    TExplainRange result;
    const size_t size = Max(left.size(), right.size());
    result.reserve(size);

    for (size_t i = 0; i < size; ++i) {
        const auto& leftColumn = i < left.size() ? left[i] : MakeFullExplainRange();
        const auto& rightColumn = i < right.size() ? right[i] : MakeFullExplainRange();
        auto column = IntersectColumnRange(leftColumn, rightColumn);
        if (!column) {
            return std::nullopt;
        }
        result.push_back(std::move(*column));
    }

    return result;
}

std::optional<TExplainRanges> IntersectExplainRanges(const TExplainRanges& left, const TExplainRanges& right) {
    TExplainRanges result;
    for (const auto& leftRange : left) {
        for (const auto& rightRange : right) {
            auto range = IntersectExplainRange(leftRange, rightRange);
            if (!range) {
                return std::nullopt;
            }
            result.push_back(std::move(*range));
        }
    }
    return result;
}

TExplainRanges MultiplyExplainRanges(const TExplainRanges& left, const TExplainRanges& right) {
    TExplainRanges result;
    for (const auto& leftRange : left) {
        for (const auto& rightRange : right) {
            TExplainRange range = leftRange;
            range.insert(range.end(), rightRange.begin(), rightRange.end());
            result.push_back(std::move(range));
        }
    }
    return result;
}

std::optional<TExplainRanges> ParseAsRangeForExplain(const TExprNode::TPtr& node) {
    TExplainRanges result;
    for (const auto& range : node->ChildrenList()) {
        if (!range->IsList() || range->ChildrenSize() != 2) {
            return std::nullopt;
        }

        const auto& from = range->Head();
        const auto& to = range->Tail();
        if (!from.IsList() || !to.IsList() || from.ChildrenSize() == 0 || from.ChildrenSize() != to.ChildrenSize()) {
            return std::nullopt;
        }

        const size_t columnCount = from.ChildrenSize() - 1;
        auto fromInclusive = FormatLiteralValue(from.ChildPtr(columnCount));
        auto toInclusive = FormatLiteralValue(to.ChildPtr(columnCount));
        if (!fromInclusive || !toInclusive) {
            return std::nullopt;
        }

        TExplainRange explainRange;
        explainRange.reserve(columnCount);
        for (size_t i = 0; i < columnCount; ++i) {
            TExplainColumnRange columnRange;
            if (!IsNothingLike(from.ChildPtr(i))) {
                columnRange.From.Value = FormatLiteralValue(from.ChildPtr(i));
                if (!columnRange.From.Value) {
                    return std::nullopt;
                }
            }
            if (!IsNothingLike(to.ChildPtr(i))) {
                columnRange.To.Value = FormatLiteralValue(to.ChildPtr(i));
                if (!columnRange.To.Value) {
                    return std::nullopt;
                }
            }
            columnRange.From.Inclusive = *fromInclusive == "1";
            columnRange.To.Inclusive = *toInclusive == "1";
            explainRange.push_back(std::move(columnRange));
        }

        result.push_back(std::move(explainRange));
    }
    return result;
}

std::optional<TExplainRanges> ParseRangeForExplain(const TExprNode::TPtr& node) {
    const TString op = TString(node->Head().Content());
    if (op == "Exists" || op == "NotExists" || op == "StartsWith" || op == "NotStartsWith") {
        return std::nullopt;
    }

    auto value = FormatLiteralValue(node->ChildPtr(1));
    if (!value) {
        return std::nullopt;
    }

    auto makeSingleColumnRange = [](TExplainRangeBound from, TExplainRangeBound to) {
        TExplainRange range;
        range.push_back(TExplainColumnRange{.From = std::move(from), .To = std::move(to)});
        return range;
    };

    if (op == "==" || op == "===") {
        TExplainRangeBound bound{.Value = value, .Inclusive = true};
        return TExplainRanges{makeSingleColumnRange(bound, bound)};
    }
    if (op == "!=") {
        TExplainRangeBound bound{.Value = value, .Inclusive = false};
        return TExplainRanges{
            makeSingleColumnRange({}, bound),
            makeSingleColumnRange(bound, {}),
        };
    }
    if (op == "<" || op == "<=") {
        return TExplainRanges{makeSingleColumnRange({}, {.Value = value, .Inclusive = op == "<="})};
    }
    if (op == ">" || op == ">=") {
        return TExplainRanges{makeSingleColumnRange({.Value = value, .Inclusive = op == ">="}, {})};
    }

    return std::nullopt;
}

std::optional<TExplainRanges> ParseRangesForExplain(const TExprNode::TPtr& node) {
    if (!node) {
        return std::nullopt;
    }

    if (node->IsCallable("RangeFinalize") || node->IsCallable("RangeToPg")) {
        return ParseRangesForExplain(node->HeadPtr());
    }

    if (node->IsCallable("RangeEmpty")) {
        return TExplainRanges{};
    }

    if (node->IsCallable("AsRange")) {
        return ParseAsRangeForExplain(node);
    }

    if (node->IsCallable("RangeFor")) {
        return ParseRangeForExplain(node);
    }

    if (node->IsCallable("RangeUnion")) {
        TExplainRanges result;
        for (const auto& child : node->ChildrenList()) {
            auto childRanges = ParseRangesForExplain(child);
            if (!childRanges) {
                return std::nullopt;
            }
            result.insert(result.end(), childRanges->begin(), childRanges->end());
        }
        return result;
    }

    if (node->IsCallable("RangeIntersect")) {
        std::optional<TExplainRanges> result;
        for (const auto& child : node->ChildrenList()) {
            auto childRanges = ParseRangesForExplain(child);
            if (!childRanges) {
                return std::nullopt;
            }
            if (!result) {
                result = std::move(childRanges);
            } else {
                result = IntersectExplainRanges(*result, *childRanges);
                if (!result) {
                    return std::nullopt;
                }
            }
        }
        if (result) {
            return result;
        }
        return TExplainRanges{};
    }

    if (node->IsCallable("RangeMultiply")) {
        TExplainRanges result{{}};
        for (size_t i = 1; i < node->ChildrenSize(); ++i) {
            auto childRanges = ParseRangesForExplain(node->ChildPtr(i));
            if (!childRanges) {
                return std::nullopt;
            }
            result = MultiplyExplainRanges(result, *childRanges);
        }
        return result;
    }

    return std::nullopt;
}

TVector<TString> BuildReadRangeDescriptions(const TExprNode::TPtr& ranges, const TVector<TString>& keyColumns, size_t usedPrefixLen) {
    auto parsedRanges = ParseRangesForExplain(ranges);
    if (!parsedRanges) {
        return {};
    }

    TVector<TString> result;
    for (const auto& range : *parsedRanges) {
        size_t columnCount = Min(range.size(), keyColumns.size());
        columnCount = Min(columnCount, usedPrefixLen);
        for (size_t i = 0; i < columnCount; ++i) {
            const auto& columnRange = range[i];
            if (!columnRange.From.Value && !columnRange.To.Value) {
                continue;
            }

            TStringBuilder desc;
            desc << StripAliasPrefix(keyColumns[i]) << " "
                 << (columnRange.From.Inclusive ? "[" : "(")
                 << (columnRange.From.Value ? *columnRange.From.Value : "-∞") << ", "
                 << (columnRange.To.Value ? *columnRange.To.Value : "+∞")
                 << (columnRange.To.Inclusive ? "]" : ")");
            result.push_back(desc);
        }
    }
    return result;
}

const TStructExprType* PrepareSchemeType(const TOpRead& read, const TStructExprType* schemeType, TExprContext& ctx) {
    const auto physicalToExposed = BuildPhysicalToExposedName(read);
    const bool exposesQualified = std::any_of(read.OutputIUs.begin(), read.OutputIUs.end(),
                                              [](const TInfoUnit& iu) { return !iu.GetAlias().empty(); });

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

} // anonymous namespace

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
    const auto physicalToExposed = BuildPhysicalToExposedName(*read);
    const bool exposesQualified = std::any_of(read->OutputIUs.begin(), read->OutputIUs.end(),
                                              [](const TInfoUnit& iu) { return !iu.GetAlias().empty(); });
    TVector<TString> keyColumns;
    for (const auto& key : tableDesc->Metadata->KeyColumnNames) {
        keyColumns.emplace_back(ResolveExposedName(key, *read, physicalToExposed, exposesQualified));
    }

    auto buildResult = extractor->BuildComputeNode(keyColumns, ctx, typeCtx);
    TExprNode::TPtr ranges = buildResult.ComputeNode;
    if (!ranges) {
        return input;
    }

    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Extracted ranges: " << KqpExprToPrettyString(*ranges, ctx);
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Pruned lambda: " << KqpExprToPrettyString(*buildResult.PrunedLambda, ctx);

    TOpRead::TRangeInfo rangeInfo{
        .KeyColumns = keyColumns,
        .ReadRangeDescriptions = BuildReadRangeDescriptions(ranges, keyColumns, buildResult.UsedPrefixLen),
        .UsedPrefixLen = buildResult.UsedPrefixLen,
        .ExpectedMaxRanges = buildResult.ExpectedMaxRanges
            ? TMaybe<size_t>(*buildResult.ExpectedMaxRanges)
            : TMaybe<size_t>(),
    };
    auto newRead = MakeIntrusive<TOpRead>(read->Alias, read->Columns, read->GetOutputIUs(), read->StorageType, read->TableCallable, read->OlapFilterLambda,
                                          read->Limit, ranges, TExpression(originalLambda, &ctx, &props), read->SortDir, read->Props, read->Pos, std::move(rangeInfo));
    return MakeIntrusive<TOpFilter>(newRead, filter->Pos, filter->Props, TExpression(buildResult.PrunedLambda, &ctx, &props));
}
} // namespace NKikimr::NKqp
