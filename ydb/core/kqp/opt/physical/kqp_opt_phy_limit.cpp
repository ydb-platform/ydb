#include "kqp_opt_phy_rules.h"
#include "kqp_opt_phy_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

TExprBase KqpApplyLimitToFullTextIndex(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    Y_UNUSED(kqpCtx);

    if (!node.Maybe<TCoTake>()) {
        return node;
    }
    auto take = node.Cast<TCoTake>();

    auto maybeSkip = take.Input().Maybe<TCoSkip>();
    if (maybeSkip) {
        return node;
    }

    auto input = maybeSkip ? maybeSkip.Cast().Input() : take.Input();

    bool isReadTable = input.Maybe<TKqpReadTableFullTextIndex>().IsValid();

    if (!isReadTable) {
        return node;
    }

    auto settings = TKqpReadTableFullTextIndexSettings::Parse(input.Cast<TKqpReadTableFullTextIndex>().Settings());

    if (settings.ItemsLimit) {
        return node; // already set?
    }

    settings.SetItemsLimit(take.Count().Ptr());

    auto newSettings = settings.BuildNode(ctx, input.Pos());

    auto newInput = ctx.ChangeChild(
        input.Ref(), TKqpReadTableFullTextIndex::idx_Settings, newSettings.Ptr());

    return Build<TCoTake>(ctx, take.Pos())
        .Input(newInput)
        .Count(take.Count())
        .Done();
}

TExprBase KqpApplyLimitToReadTable(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TCoTake>()) {
        return node;
    }
    auto take = node.Cast<TCoTake>();

    auto maybeSkip = take.Input().Maybe<TCoSkip>();
    auto input = maybeSkip ? maybeSkip.Cast().Input() : take.Input();

    bool isReadTable = input.Maybe<TKqpReadTable>().IsValid();
    bool isReadTableRanges = input.Maybe<TKqpReadTableRanges>().IsValid() || input.Maybe<TKqpReadOlapTableRanges>().IsValid();

    if (!isReadTable && !isReadTableRanges) {
        return node;
    }

    if (kqpCtx.IsScanQuery()) {
        auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, GetReadTablePath(input, isReadTableRanges));

        if (tableDesc.Metadata->Kind != EKikimrTableKind::Olap) {
            return node;
        }
    }

    auto settings = GetReadTableSettings(input, isReadTableRanges);
    if (settings.ItemsLimit) {
        return node; // already set?
    }

    settings.SequentialInFlight = 1;

    TMaybeNode<TExprBase> limitValue;
    auto maybeTakeCount = take.Count().Maybe<TCoUint64>();
    auto maybeSkipCount = maybeSkip.Count().Maybe<TCoUint64>();

    if (maybeTakeCount && (!maybeSkip || maybeSkipCount)) {
        ui64 totalLimit = FromString<ui64>(maybeTakeCount.Cast().Literal().Value());

        if (maybeSkipCount) {
            totalLimit += FromString<ui64>(maybeSkipCount.Cast().Literal().Value());
        }

        limitValue = Build<TCoUint64>(ctx, node.Pos())
            .Literal<TCoAtom>()
            .Value(ToString(totalLimit)).Build()
            .Done();
    } else {
        limitValue = take.Count();
        if (maybeSkip) {
            limitValue = Build<TCoAggrAdd>(ctx, node.Pos())
                .Left(limitValue.Cast())
                .Right(maybeSkip.Cast().Count())
                .Done();
        }
    }

    YQL_CLOG(TRACE, ProviderKqp) << "-- set limit items value to " << limitValue.Cast().Ref().Dump();

    if (limitValue.Maybe<TCoUint64>()) {
        settings.SetItemsLimit(limitValue.Cast().Ptr());
    } else {
        settings.SetItemsLimit(Build<TDqPrecompute>(ctx, node.Pos())
            .Input(limitValue.Cast())
            .Done().Ptr());
    }

    input = BuildReadNode(node.Pos(), ctx, input, settings);

    if (maybeSkip) {
        input = Build<TCoSkip>(ctx, node.Pos())
            .Input(input)
            .Count(maybeSkip.Cast().Count())
            .Done();
    }

    return Build<TCoTake>(ctx, take.Pos())
        .Input(input)
        .Count(take.Count())
        .Done();
}

TExprBase KqpApplyLimitToOlapReadTable(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TCoTopSort>()) {
        return node;
    }
    auto topSort = node.Cast<TCoTopSort>();

    // Column Shards always return result sorted by PK in ASC order
    ESortDirection direction = GetSortDirection(topSort.SortDirections());
    if (direction != ESortDirection::Forward && direction != ESortDirection::Reverse) {
        return node;
    }

    auto maybeSkip = topSort.Input().Maybe<TCoSkip>();
    auto input = maybeSkip ? maybeSkip.Cast().Input() : topSort.Input();

    bool isReadTable = input.Maybe<TKqpReadOlapTableRanges>().IsValid();

    if (!isReadTable) {
        return node;
    }

    const bool isReadTableRanges = true;
    auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, GetReadTablePath(input, isReadTableRanges));

    if (tableDesc.Metadata->Kind != EKikimrTableKind::Olap) {
        return node;
    }

    auto settings = GetReadTableSettings(input, isReadTableRanges);
    if (settings.ItemsLimit) {
        return node; // already set
    }
    if (direction == ESortDirection::Reverse) {
        settings.SetSorting(ERequestSorting::DESC);
    } else if (direction == ESortDirection::Forward) {
        settings.SetSorting(ERequestSorting::ASC);
    }

    auto keySelector = topSort.KeySelectorLambda();
    if (!IsSortKeyPrimary(keySelector, tableDesc)) {
        // Column shards return data sorted by PK
        // So we can pushdown limit only if query has sort by PK
        return node;
    }

    TMaybeNode<TExprBase> limitValue;
    auto maybeTopSortCount = topSort.Count().Maybe<TCoUint64>();
    auto maybeSkipCount = maybeSkip.Count().Maybe<TCoUint64>();

    if (maybeTopSortCount && (!maybeSkip || maybeSkipCount)) {
        ui64 totalLimit = FromString<ui64>(maybeTopSortCount.Cast().Literal().Value());

        if (maybeSkipCount) {
            totalLimit += FromString<ui64>(maybeSkipCount.Cast().Literal().Value());
        }

        limitValue = Build<TCoUint64>(ctx, node.Pos())
            .Literal<TCoAtom>()
            .Value(ToString(totalLimit)).Build()
            .Done();
    } else {
        limitValue = topSort.Count();
        if (maybeSkip) {
            limitValue = Build<TCoAggrAdd>(ctx, node.Pos())
                .Left(limitValue.Cast())
                .Right(maybeSkip.Cast().Count())
                .Done();
        }
    }

    YQL_CLOG(TRACE, ProviderKqp) << "-- set limit items value to " << limitValue.Cast().Ref().Dump();

    if (limitValue.Maybe<TCoUint64>()) {
        settings.SetItemsLimit(limitValue.Cast().Ptr());
    } else {
        settings.SetItemsLimit(Build<TDqPrecompute>(ctx, node.Pos())
            .Input(limitValue.Cast())
            .Done().Ptr());
    }

    input = BuildReadNode(node.Pos(), ctx, input, settings);

    if (maybeSkip) {
        input = Build<TCoSkip>(ctx, node.Pos())
            .Input(input)
            .Count(maybeSkip.Cast().Count())
            .Done();
    }

    return Build<TCoTopSort>(ctx, topSort.Pos())
        .Input(input)
        .Count(topSort.Count())
        .SortDirections(topSort.SortDirections())
        .KeySelectorLambda(topSort.KeySelectorLambda())
        .Done();
}

namespace {

// Helper function to extract KNN distance info from an Apply node.
// Returns candidate interpretations: {column, method, target} tuples where one arg is a Member (table column).
// Multiple candidates are returned because either Apply argument could be the column reference.
TVector<std::tuple<TString, TString, TExprNode::TPtr>> ExtractKnnDistanceCandidates(
    const TCoApply& apply,
    const TNodeMap<TExprNode::TPtr>& argSubstitutions = {}) {

    TVector<std::tuple<TString, TString, TExprNode::TPtr>> result;

    auto udf = apply.Callable().Maybe<TCoUdf>();
    if (!udf || !udf.Cast().MethodName().Value().StartsWith("Knn.")) {
        return result;
    }

    const auto methodName = TString(udf.Cast().MethodName().Value());

    // Collect resolved arguments (skip the callable itself)
    TVector<std::pair<TExprNode::TPtr, TExprBase>> resolvedArgs;
    for (const auto& arg : apply.Args()) {
        if (arg.Raw() == apply.Callable().Raw()) continue;
        auto it = argSubstitutions.find(arg.Raw());
        auto resolved = (it != argSubstitutions.end()) ? it->second : arg.Ptr();
        resolvedArgs.emplace_back(resolved, TExprBase(resolved));
    }

    if (resolvedArgs.size() != 2) {
        return result;
    }

    // Return interpretations where one argument is a Member (column reference)
    for (size_t i = 0; i < 2; ++i) {
        if (auto member = resolvedArgs[i].second.Maybe<TCoMember>()) {
            result.emplace_back(TString(member.Cast().Name().Value()), methodName, resolvedArgs[1 - i].first);
        }
    }
    return result;
}

// Recursively extract KNN distance info, unwrapping Just/FlatMap wrappers.
// Returns candidate interpretations for the innermost KNN distance Apply node.
TVector<std::tuple<TString, TString, TExprNode::TPtr>> UnwrapAndExtractKnnDistance(
    const TExprBase& expr, TNodeMap<TExprNode::TPtr> argSubstitutions = {}) {

    if (auto apply = expr.Maybe<TCoApply>()) {
        return ExtractKnnDistanceCandidates(apply.Cast(), argSubstitutions);
    }
    if (auto just = expr.Maybe<TCoJust>()) {
        return UnwrapAndExtractKnnDistance(just.Cast().Input(), argSubstitutions);
    }
    if (auto flatMap = expr.Maybe<TCoFlatMap>()) {
        if (flatMap.Cast().Lambda().Args().Size() == 1) {
            argSubstitutions[flatMap.Cast().Lambda().Args().Arg(0).Raw()] = flatMap.Cast().Input().Ptr();
        }
        return UnwrapAndExtractKnnDistance(flatMap.Cast().Lambda().Body(), argSubstitutions);
    }
    return {};
}

// Find column expression in AsStruct by name
TMaybeNode<TExprBase> FindColumnExprInStruct(const TExprBase& structExpr, const TStringBuf& columnName) {
    if (auto asStruct = structExpr.Maybe<TCoAsStruct>()) {
        for (const auto& item : asStruct.Cast()) {
            if (auto tuple = item.Maybe<TCoNameValueTuple>()) {
                if (tuple.Cast().Name().Value() == columnName) {
                    return tuple.Cast().Value();
                }
            }
        }
    }
    return {};
}

// Unwrap Just/FlatMap to find AsStruct
TMaybeNode<TCoAsStruct> UnwrapToStruct(const TExprBase& body) {
    if (auto asStruct = body.Maybe<TCoAsStruct>()) {
        return asStruct;
    }
    if (auto just = body.Maybe<TCoJust>()) {
        return just.Cast().Input().Maybe<TCoAsStruct>();
    }
    if (auto flatMap = body.Maybe<TCoFlatMap>()) {
        return UnwrapToStruct(flatMap.Cast().Lambda().Body());
    }
    return {};
}

// Find KNN info in computed column (alias) within Map/FlatMap structures
TVector<std::tuple<TString, TString, TExprNode::TPtr>> FindKnnInfoInComputedColumn(
    const TExprBase& body, const TStringBuf& aliasName) {

    if (auto asStruct = UnwrapToStruct(body)) {
        if (auto columnExpr = FindColumnExprInStruct(asStruct.Cast(), aliasName)) {
            return UnwrapAndExtractKnnDistance(columnExpr.Cast());
        }
    }
    if (auto map = body.Maybe<TCoMap>()) {
        return FindKnnInfoInComputedColumn(map.Cast().Lambda().Body(), aliasName);
    }
    if (auto flatMap = body.Maybe<TCoFlatMapBase>()) {
        return FindKnnInfoInComputedColumn(flatMap.Cast().Lambda().Body(), aliasName);
    }
    return {};
}

// Get metric name from Knn method if sort direction matches
TString GetMetricFromMethodName(const TString& methodName, bool isAsc) {
    static const std::tuple<TStringBuf, TStringBuf, bool> metrics[] = {
        {"Knn.CosineDistance", "CosineDistance", true},
        {"Knn.CosineSimilarity", "CosineSimilarity", false},
        {"Knn.InnerProductSimilarity", "InnerProductSimilarity", false},
        {"Knn.ManhattanDistance", "ManhattanDistance", true},
        {"Knn.EuclideanDistance", "EuclideanDistance", true},
    };
    for (auto [method, metric, requiresAsc] : metrics) {
        if (methodName == method && isAsc == requiresAsc) {
            return TString(metric);
        }
    }
    return {};
}

// Find first valid KNN candidate (column exists, metric valid for direction)
TMaybe<std::tuple<TString, TString, TExprNode::TPtr>> FindValidKnnCandidate(
    const TVector<std::tuple<TString, TString, TExprNode::TPtr>>& candidates,
    const TKikimrTableDescription& tableDesc, bool isAsc, TString& outMetric) {

    for (const auto& [col, method, target] : candidates) {
        if (!tableDesc.Metadata->Columns.contains(col)) continue;
        TString metric = GetMetricFromMethodName(method, isAsc);
        if (!metric.empty()) {
            outMetric = metric;
            return std::make_tuple(col, method, target);
        }
    }
    return {};
}

// Extract KNN info from TopSort key selector
TVector<std::tuple<TString, TString, TExprNode::TPtr>> ExtractKnnInfoFromTopSort(
    const TCoTopSort& topSort, const TMaybeNode<TCoFlatMapBase>& maybeFlatMap = {}) {

    auto lambdaBody = topSort.KeySelectorLambda().Body();
    auto result = UnwrapAndExtractKnnDistance(lambdaBody);
    if (result.empty()) {
        if (auto member = lambdaBody.Maybe<TCoMember>()) {
            auto aliasName = member.Cast().Name().Value();
            result = maybeFlatMap ? FindKnnInfoInComputedColumn(maybeFlatMap.Cast(), aliasName)
                                  : FindKnnInfoInComputedColumn(topSort.Input(), aliasName);
        }
    }
    return result;
}

// Wrap expression in precompute if not a simple literal/parameter
TExprNode::TPtr WrapInPrecompute(const TExprBase& expr, TExprContext& ctx) {
    if (expr.Maybe<TCoString>() || expr.Maybe<TCoParameter>() ||
        expr.Maybe<TCoUint64>() || expr.Maybe<TCoArgument>()) {
        return expr.Ptr();
    }
    return KqpPrecomputeParameter(expr, ctx).Ptr();
}

// Replace stage input at given index
TVector<TExprBase> ReplaceStageInput(const TDqStage& stage, size_t index, TExprBase newInput) {
    TVector<TExprBase> inputs;
    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        inputs.push_back(i == index ? newInput : stage.Inputs().Item(i));
    }
    return inputs;
}

} // anonymous namespace

TExprBase KqpApplyVectorTopKToReadTable(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TCoTopSort>()) {
        return node;
    }
    auto topSort = node.Cast<TCoTopSort>();

    auto input = topSort.Input();

    // Check if input is directly TKqpReadTableRanges
    bool isReadTableRanges = input.Maybe<TKqpReadTableRanges>().IsValid();

    // Track various intermediate nodes
    TMaybeNode<TDqCnUnionAll> maybeCnUnionAll;
    TMaybeNode<TDqStage> maybeStage;
    TMaybeNode<TKqpReadTableRanges> maybeReadTableRanges;
    TMaybeNode<TDqSource> maybeDqSource;
    TMaybeNode<TKqpReadRangesSourceSettings> maybeSourceSettings;
    TMaybeNode<TCoFlatMapBase> maybeFlatMap;
    size_t sourceInputIndex = 0;
    TExprBase flatMapInput = input;

    // Check for FlatMap/Map (used when ORDER BY references an alias/computed column or subquery)
    if (input.Maybe<TCoFlatMapBase>()) {
        maybeFlatMap = input.Cast<TCoFlatMapBase>();
        flatMapInput = maybeFlatMap.Cast().Input();
    } else if (input.Maybe<TCoMap>()) {
        // TCoMap is used when the target vector comes from a subquery
        flatMapInput = input.Cast<TCoMap>().Input();
        if (flatMapInput.Maybe<TCoFlatMapBase>()) {
            flatMapInput = flatMapInput.Cast<TCoFlatMapBase>().Input();
        }
    }

    if (!isReadTableRanges && flatMapInput.Maybe<TDqCnUnionAll>()) {
        maybeCnUnionAll = flatMapInput.Cast<TDqCnUnionAll>();
        maybeStage = maybeCnUnionAll.Cast().Output().Stage().Maybe<TDqStage>();
        if (maybeStage) {
            auto stage = maybeStage.Cast();
            auto stageBody = stage.Program().Body();

            maybeReadTableRanges = stageBody.Maybe<TKqpReadTableRanges>();
            if (maybeReadTableRanges) {
                isReadTableRanges = true;
                input = maybeReadTableRanges.Cast();
            }

            // Check if the stage body is a FlatMap over TKqpReadTableRanges
            // This happens when ORDER BY references an alias computed in the FlatMap
            if (!isReadTableRanges && stageBody.Maybe<TCoFlatMapBase>()) {
                auto stageFlatMap = stageBody.Cast<TCoFlatMapBase>();
                maybeReadTableRanges = stageFlatMap.Input().Maybe<TKqpReadTableRanges>();
                if (maybeReadTableRanges) {
                    isReadTableRanges = true;
                    input = maybeReadTableRanges.Cast();
                    if (!maybeFlatMap) {
                        maybeFlatMap = stageFlatMap;
                    }
                }
            }

            // Check if the stage has a DqSource with KqpReadRangesSourceSettings
            // This happens for data queries where reads are converted to sources
            if (!isReadTableRanges) {
                for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
                    if (auto dqSource = stage.Inputs().Item(i).Maybe<TDqSource>()) {
                        if (auto sourceSettings = dqSource.Cast().Settings().Maybe<TKqpReadRangesSourceSettings>()) {
                            maybeDqSource = dqSource.Cast();
                            maybeSourceSettings = sourceSettings.Cast();
                            sourceInputIndex = i;
                            isReadTableRanges = true;
                            break;
                        }
                    }
                }
                if (isReadTableRanges && stageBody.Maybe<TCoFlatMapBase>() && !maybeFlatMap) {
                    maybeFlatMap = stageBody.Cast<TCoFlatMapBase>();
                }
            }
        }
    }

    if (!isReadTableRanges) {
        return node;
    }

    // Get table path and settings
    TCoAtom tablePath = maybeSourceSettings
        ? maybeSourceSettings.Cast().Table().Path()
        : GetReadTablePath(input, true);
    TKqpReadTableSettings settings = maybeSourceSettings
        ? TKqpReadTableSettings::Parse(maybeSourceSettings.Cast().Settings())
        : GetReadTableSettings(input, true);
    TExprBase rangesExpr = maybeSourceSettings
        ? maybeSourceSettings.Cast().RangesExpr()
        : input.Cast<TKqpReadTableRanges>().Ranges();

    // Only apply to full table scans (no WHERE clause filtering)
    if (!TCoVoid::Match(rangesExpr.Raw())) {
        return node;
    }

    // Reject filtering FlatMaps (OptionalIf/ListIf), allow projection FlatMaps (ORDER BY alias)
    if (maybeFlatMap) {
        auto flatMapBody = maybeFlatMap.Cast().Lambda().Body();
        if (flatMapBody.Maybe<TCoOptionalIf>() || flatMapBody.Maybe<TCoListIf>()) {
            return node;
        }
    }

    auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, tablePath);

    // Only for datashard tables
    if (tableDesc.Metadata->Kind != EKikimrTableKind::Datashard) {
        return node;
    }

    if (settings.VectorTopKColumn) {
        return node; // already set
    }

    // Check sort direction
    ESortDirection direction = GetSortDirection(topSort.SortDirections());
    if (direction != ESortDirection::Forward && direction != ESortDirection::Reverse) {
        return node;
    }
    const bool isAsc = (direction == ESortDirection::Forward);

    // Extract and validate KNN info
    TString metric;
    auto knnInfo = FindValidKnnCandidate(ExtractKnnInfoFromTopSort(topSort, maybeFlatMap), tableDesc, isAsc, metric);
    if (!knnInfo) {
        return node;
    }

    auto [columnName, _, targetExpr] = *knnInfo;
    settings.VectorTopKColumn = columnName;
    settings.VectorTopKMetric = metric;
    settings.VectorTopKTarget = WrapInPrecompute(TExprBase(targetExpr), ctx);
    settings.VectorTopKLimit = WrapInPrecompute(topSort.Count(), ctx);

    // Handle source-based read (data queries)
    if (maybeSourceSettings && maybeStage) {
        auto stage = maybeStage.Cast();
        auto oldSourceSettings = maybeSourceSettings.Cast();

        auto newSettingsNode = settings.BuildNode(ctx, oldSourceSettings.Settings().Pos());
        auto newSourceSettings = Build<TKqpReadRangesSourceSettings>(ctx, oldSourceSettings.Pos())
            .Table(oldSourceSettings.Table())
            .Columns(oldSourceSettings.Columns())
            .Settings(newSettingsNode)
            .RangesExpr(oldSourceSettings.RangesExpr())
            .ExplainPrompt(oldSourceSettings.ExplainPrompt())
            .Done();
        auto newSource = Build<TDqSource>(ctx, maybeDqSource.Cast().Pos())
            .Settings(newSourceSettings)
            .DataSource(maybeDqSource.Cast().DataSource())
            .Done();
        auto newStage = Build<TDqStage>(ctx, stage.Pos())
            .Inputs()
                .Add(ReplaceStageInput(stage, sourceInputIndex, newSource))
                .Build()
            .Program(stage.Program())
            .Settings(stage.Settings())
            .Done();
        auto newCnUnionAll = Build<TDqCnUnionAll>(ctx, maybeCnUnionAll.Cast().Pos())
            .Output()
                .Stage(newStage)
                .Index(maybeCnUnionAll.Cast().Output().Index())
                .Build()
            .Done();

        TExprBase newTopSortInput = newCnUnionAll;
        if (maybeFlatMap && maybeFlatMap.Cast().Raw() != stage.Program().Body().Raw()) {
            if (maybeFlatMap.Cast().Maybe<TCoOrderedFlatMap>()) {
                newTopSortInput = Build<TCoOrderedFlatMap>(ctx, maybeFlatMap.Cast().Pos())
                    .Input(newCnUnionAll)
                    .Lambda(maybeFlatMap.Cast().Lambda())
                    .Done();
            } else {
                newTopSortInput = Build<TCoFlatMap>(ctx, maybeFlatMap.Cast().Pos())
                    .Input(newCnUnionAll)
                    .Lambda(maybeFlatMap.Cast().Lambda())
                    .Done();
            }
        }

        return Build<TCoTopSort>(ctx, topSort.Pos())
            .Input(newTopSortInput)
            .Count(topSort.Count())
            .SortDirections(topSort.SortDirections())
            .KeySelectorLambda(topSort.KeySelectorLambda())
            .Done();
    }

    auto newReadNode = BuildReadNode(node.Pos(), ctx, input, settings);

    if (maybeStage) {
        auto stage = maybeStage.Cast();
        auto stageBody = stage.Program().Body();
        TExprBase newStageBody = newReadNode;

        // Preserve FlatMap structure if present
        if (auto orderedFlatMap = stageBody.Maybe<TCoOrderedFlatMap>()) {
            if (orderedFlatMap.Cast().Input().Maybe<TKqpReadTableRanges>()) {
                newStageBody = Build<TCoOrderedFlatMap>(ctx, orderedFlatMap.Cast().Pos())
                    .Input(newReadNode)
                    .Lambda(orderedFlatMap.Cast().Lambda())
                    .Done();
            }
        } else if (auto flatMap = stageBody.Maybe<TCoFlatMap>()) {
            if (flatMap.Cast().Input().Maybe<TKqpReadTableRanges>()) {
                newStageBody = Build<TCoFlatMap>(ctx, flatMap.Cast().Pos())
                    .Input(newReadNode)
                    .Lambda(flatMap.Cast().Lambda())
                    .Done();
            }
        }

        auto newStage = Build<TDqStage>(ctx, stage.Pos())
            .Inputs(stage.Inputs())
            .Program()
                .Args(stage.Program().Args())
                .Body(newStageBody)
                .Build()
            .Settings(stage.Settings())
            .Done();
        auto newCnUnionAll = Build<TDqCnUnionAll>(ctx, maybeCnUnionAll.Cast().Pos())
            .Output()
                .Stage(newStage)
                .Index(maybeCnUnionAll.Cast().Output().Index())
                .Build()
            .Done();

        return Build<TCoTopSort>(ctx, topSort.Pos())
            .Input(newCnUnionAll)
            .Count(topSort.Count())
            .SortDirections(topSort.SortDirections())
            .KeySelectorLambda(topSort.KeySelectorLambda())
            .Done();
    }

    return Build<TCoTopSort>(ctx, topSort.Pos())
        .Input(newReadNode)
        .Count(topSort.Count())
        .SortDirections(topSort.SortDirections())
        .KeySelectorLambda(topSort.KeySelectorLambda())
        .Done();
}

namespace {

// Find TopSort in stage body (through Map/FlatMap/ToFlow wrappers)
TMaybeNode<TCoTopSort> FindTopSortInBody(const TExprBase& body) {
    if (auto t = body.Maybe<TCoTopSort>()) {
        return t;
    }
    if (auto m = body.Maybe<TCoMap>()) {
        return FindTopSortInBody(m.Cast().Lambda().Body());
    }
    if (auto f = body.Maybe<TCoFlatMapBase>()) {
        return FindTopSortInBody(f.Cast().Lambda().Body());
    }
    if (auto t = body.Maybe<TCoToFlow>()) {
        return FindTopSortInBody(t.Cast().Input());
    }
    return {};
}

// Build precompute to extract column from subquery result
TExprNode::TPtr BuildSubqueryTargetPrecompute(
    const TCoMember& member, const TDqPhyPrecompute& precompute, TExprContext& ctx) {

    auto newArg = Build<TCoArgument>(ctx, member.Pos())
        .Name("_kqp_extract_arg")
        .Done();

    // Build: Unwrap(Member(Unwrap(arg), "column_name"))
    auto memberExpr = Build<TCoMember>(ctx, member.Pos())
        .Struct<TCoUnwrap>()
            .Optional(newArg)
            .Build()
        .Name(member.Name())
        .Done();
    auto extractExpr = Build<TCoUnwrap>(ctx, member.Pos())
        .Optional(memberExpr)
        .Done();

    return Build<TDqPhyPrecompute>(ctx, member.Pos())
        .Connection<TDqCnValue>()
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(precompute)
                        .Build()
                    .Program()
                        .Args({newArg})
                        .Body<TCoToStream>()
                            .Input<TCoAsList>()
                                .Add(extractExpr)
                                .Build()
                            .Build()
                        .Build()
                    .Settings().Build()
                    .Build()
                .Index().Build("0")
                .Build()
            .Build()
        .Done().Ptr();
}

// Resolve target expression for stage-based queries (handles subquery Member references)
TExprNode::TPtr ResolveStageTarget(const TExprBase& expr, const TDqStage& stage, TExprContext& ctx) {
    // Handle subquery: Member(arg, "column") where arg maps to DqPhyPrecompute input
    if (auto member = expr.Maybe<TCoMember>()) {
        if (auto arg = member.Cast().Struct().Maybe<TCoArgument>()) {
            auto stageArgs = stage.Program().Args();
            for (size_t i = 0; i < stageArgs.Size() && i < stage.Inputs().Size(); ++i) {
                if (stageArgs.Arg(i).Raw() == arg.Cast().Raw()) {
                    if (auto precompute = stage.Inputs().Item(i).Maybe<TDqPhyPrecompute>()) {
                        return BuildSubqueryTargetPrecompute(member.Cast(), precompute.Cast(), ctx);
                    }
                }
            }
        }
    }

    // Reject expressions containing unresolved arguments
    bool hasArgument = false;
    VisitExpr(expr.Ref(), [&](const TExprNode& n) {
        if (n.IsArgument()) { hasArgument = true; return false; }
        return true;
    });
    if (hasArgument) {
        return nullptr;
    }

    return WrapInPrecompute(expr, ctx);
}

} // anonymous namespace

TExprBase KqpApplyVectorTopKToStageWithSource(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TDqStage>()) {
        return node;
    }
    auto stage = node.Cast<TDqStage>();

    // Find DqSource with KqpReadRangesSourceSettings
    TMaybeNode<TDqSource> maybeDqSource;
    TMaybeNode<TKqpReadRangesSourceSettings> maybeSourceSettings;
    size_t sourceInputIndex = 0;

    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        if (auto dqSource = stage.Inputs().Item(i).Maybe<TDqSource>()) {
            if (auto sourceSettings = dqSource.Cast().Settings().Maybe<TKqpReadRangesSourceSettings>()) {
                maybeDqSource = dqSource.Cast();
                maybeSourceSettings = sourceSettings.Cast();
                sourceInputIndex = i;
                break;
            }
        }
    }

    if (!maybeSourceSettings) {
        return node;
    }

    auto sourceSettings = maybeSourceSettings.Cast();
    auto settings = TKqpReadTableSettings::Parse(sourceSettings.Settings());

    // Skip if already has VectorTopK or not a full table scan
    if (settings.VectorTopKColumn || !TCoVoid::Match(sourceSettings.RangesExpr().Raw())) {
        return node;
    }

    auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, sourceSettings.Table().Path());
    if (tableDesc.Metadata->Kind != EKikimrTableKind::Datashard) {
        return node;
    }

    // Find TopSort in stage body
    auto maybeTopSort = FindTopSortInBody(stage.Program().Body());
    if (!maybeTopSort) {
        return node;
    }
    auto topSort = maybeTopSort.Cast();

    // Check sort direction
    ESortDirection direction = GetSortDirection(topSort.SortDirections());
    if (direction != ESortDirection::Forward && direction != ESortDirection::Reverse) {
        return node;
    }
    const bool isAsc = (direction == ESortDirection::Forward);

    // Extract and validate KNN info
    TString metric;
    auto knnInfo = FindValidKnnCandidate(ExtractKnnInfoFromTopSort(topSort), tableDesc, isAsc, metric);
    if (!knnInfo) {
        return node;
    }

    auto [columnName, _, targetExpr] = *knnInfo;
    TExprNode::TPtr resolvedTarget = ResolveStageTarget(TExprBase(targetExpr), stage, ctx);
    if (!resolvedTarget) {
        return node;
    }

    settings.VectorTopKColumn = columnName;
    settings.VectorTopKMetric = metric;
    settings.VectorTopKTarget = resolvedTarget;
    settings.VectorTopKLimit = WrapInPrecompute(topSort.Count(), ctx);

    auto newSettingsNode = settings.BuildNode(ctx, sourceSettings.Settings().Pos());
    auto newSourceSettings = Build<TKqpReadRangesSourceSettings>(ctx, sourceSettings.Pos())
        .Table(sourceSettings.Table())
        .Columns(sourceSettings.Columns())
        .Settings(newSettingsNode)
        .RangesExpr(sourceSettings.RangesExpr())
        .ExplainPrompt(sourceSettings.ExplainPrompt())
        .Done();
    auto newSource = Build<TDqSource>(ctx, maybeDqSource.Cast().Pos())
        .Settings(newSourceSettings)
        .DataSource(maybeDqSource.Cast().DataSource())
        .Done();

    return Build<TDqStage>(ctx, stage.Pos())
        .Inputs()
            .Add(ReplaceStageInput(stage, sourceInputIndex, newSource))
            .Build()
        .Program(stage.Program())
        .Settings(stage.Settings())
        .Done();
}

} // namespace NKikimr::NKqp::NOpt

