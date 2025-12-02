#include "kqp_opt_phy_rules.h"
#include "kqp_opt_phy_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

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

// Helper function to extract info from a Knn::*Distance Apply node
// argSubstitutions maps lambda arguments to their corresponding input expressions
// Returns: vector of {column name, method name, target expression} for all possible interpretations
// For subquery cases, both arguments might be Members, so we return all possibilities
TVector<std::tuple<TString, TString, TExprNode::TPtr>> ExtractKnnDistanceFromApplyAll(
    const TCoApply& apply,
    const TNodeMap<TExprNode::TPtr>& argSubstitutions = {}) {
    TVector<std::tuple<TString, TString, TExprNode::TPtr>> result;

    auto udf = apply.Callable().Maybe<TCoUdf>();
    if (!udf) {
        return result;
    }

    const auto methodName = TString(udf.Cast().MethodName().Value());
    if (!methodName.StartsWith("Knn.")) {
        return result;
    }

    TVector<std::pair<TExprNode::TPtr, TExprBase>> resolvedArgs;

    for (const auto& arg : apply.Args()) {
        if (arg.Raw() == apply.Callable().Raw()) {
            continue;
        }

        TExprNode::TPtr resolvedArg = arg.Ptr();
        auto it = argSubstitutions.find(arg.Raw());
        if (it != argSubstitutions.end()) {
            resolvedArg = it->second;
        }
        resolvedArgs.emplace_back(resolvedArg, TExprBase(resolvedArg));
    }

    if (resolvedArgs.size() != 2) {
        return result;
    }

    // Return all possible interpretations where one argument is a Member
    for (size_t i = 0; i < 2; ++i) {
        if (auto member = resolvedArgs[i].second.Maybe<TCoMember>()) {
            TString memberName = TString(member.Cast().Name().Value());
            result.emplace_back(memberName, methodName, resolvedArgs[1 - i].first);
        }
    }

    return result;
}

// Try to find a Knn distance expression in a struct literal by column name
// Returns the expression if found, nullopt otherwise
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

// Check if the lambda body is a Knn::*Distance function call and extract ALL possible interpretations
// Returns: vector of {column name, metric name, target expression}
// Handles both direct Apply and FlatMap-wrapped cases (for nullable columns)
TVector<std::tuple<TString, TString, TExprNode::TPtr>> ExtractKnnDistanceInfoAll(const TExprBase& lambdaBody) {
    // Case 1: Direct Apply - Knn::Distance(Member(input, 'emb'), expr)
    if (auto apply = lambdaBody.Maybe<TCoApply>()) {
        return ExtractKnnDistanceFromApplyAll(apply.Cast());
    }

    // Case 2: FlatMap-wrapped (for optional target expression like String::HexDecode)
    if (auto flatMap = lambdaBody.Maybe<TCoFlatMap>()) {
        auto input = flatMap.Cast().Input();
        auto lambda = flatMap.Cast().Lambda();
        auto innerBody = lambda.Body();

        TNodeMap<TExprNode::TPtr> argSubstitutions;
        if (lambda.Args().Size() == 1) {
            argSubstitutions[lambda.Args().Arg(0).Raw()] = input.Ptr();
        }

        if (auto just = innerBody.Maybe<TCoJust>()) {
            if (auto apply = just.Cast().Input().Maybe<TCoApply>()) {
                return ExtractKnnDistanceFromApplyAll(apply.Cast(), argSubstitutions);
            }
        }

        if (auto apply = innerBody.Maybe<TCoApply>()) {
            return ExtractKnnDistanceFromApplyAll(apply.Cast(), argSubstitutions);
        }

        // Case 3: Nested FlatMap (for cases with multiple nullables)
        if (auto innerFlatMap = innerBody.Maybe<TCoFlatMap>()) {
            auto innerInput = innerFlatMap.Cast().Input();
            auto innerLambda = innerFlatMap.Cast().Lambda();
            auto innerInnerBody = innerLambda.Body();

            if (innerLambda.Args().Size() == 1) {
                argSubstitutions[innerLambda.Args().Arg(0).Raw()] = innerInput.Ptr();
            }

            if (auto just = innerInnerBody.Maybe<TCoJust>()) {
                if (auto apply = just.Cast().Input().Maybe<TCoApply>()) {
                    return ExtractKnnDistanceFromApplyAll(apply.Cast(), argSubstitutions);
                }
            }
            if (auto apply = innerInnerBody.Maybe<TCoApply>()) {
                return ExtractKnnDistanceFromApplyAll(apply.Cast(), argSubstitutions);
            }
        }
    }

    return {};
}

// Get the metric enum value from the method name
TString GetMetricFromMethodName(const TString& methodName, bool isAsc) {
    static const std::pair<TStringBuf, std::pair<TStringBuf, bool>> metrics[] = {
        {"Knn.CosineDistance", {"CosineDistance", true}},
        {"Knn.CosineSimilarity", {"CosineSimilarity", false}},
        {"Knn.InnerProductSimilarity", {"InnerProductSimilarity", false}},
        {"Knn.ManhattanDistance", {"ManhattanDistance", true}},
        {"Knn.EuclideanDistance", {"EuclideanDistance", true}},
    };
    for (const auto& [name, info] : metrics) {
        if (methodName == name && isAsc == info.second) {
            return TString(info.first);
        }
    }
    return {};
}

// Helper to find KNN distance info from a computed column (alias) in FlatMap/Map structures
TVector<std::tuple<TString, TString, TExprNode::TPtr>> FindKnnInfoInComputedColumn(
    const TExprBase& body, const TStringBuf& aliasName) {

    // Check if body contains a FlatMap/Map that builds a struct with our alias
    if (auto map = body.Maybe<TCoMap>()) {
        auto mapBody = map.Cast().Lambda().Body();
        if (auto asStruct = mapBody.Maybe<TCoAsStruct>()) {
            auto maybeColumnExpr = FindColumnExprInStruct(asStruct.Cast(), aliasName);
            if (maybeColumnExpr) {
                return ExtractKnnDistanceInfoAll(maybeColumnExpr.Cast());
            }
        }
        return FindKnnInfoInComputedColumn(mapBody, aliasName);
    }

    if (auto flatMap = body.Maybe<TCoFlatMapBase>()) {
        auto fmBody = flatMap.Cast().Lambda().Body();

        // Check for Just(AsStruct(...))
        if (auto just = fmBody.Maybe<TCoJust>()) {
            if (auto asStruct = just.Cast().Input().Maybe<TCoAsStruct>()) {
                auto maybeColumnExpr = FindColumnExprInStruct(asStruct.Cast(), aliasName);
                if (maybeColumnExpr) {
                    return ExtractKnnDistanceInfoAll(maybeColumnExpr.Cast());
                }
            }
        }

        // Check for nested FlatMap (nullable subquery case)
        if (auto innerFlatMap = fmBody.Maybe<TCoFlatMap>()) {
            auto innerBody = innerFlatMap.Cast().Lambda().Body();
            if (auto innerJust = innerBody.Maybe<TCoJust>()) {
                if (auto asStruct = innerJust.Cast().Input().Maybe<TCoAsStruct>()) {
                    auto maybeColumnExpr = FindColumnExprInStruct(asStruct.Cast(), aliasName);
                    if (maybeColumnExpr) {
                        TNodeMap<TExprNode::TPtr> argSubstitutions;
                        auto innerLambda = innerFlatMap.Cast().Lambda();
                        if (innerLambda.Args().Size() == 1) {
                            argSubstitutions[innerLambda.Args().Arg(0).Raw()] = innerFlatMap.Cast().Input().Ptr();
                        }

                        if (auto apply = maybeColumnExpr.Cast().Maybe<TCoApply>()) {
                            return ExtractKnnDistanceFromApplyAll(apply.Cast(), argSubstitutions);
                        }
                        if (auto innerExprFm = maybeColumnExpr.Cast().Maybe<TCoFlatMap>()) {
                            auto ieBody = innerExprFm.Cast().Lambda().Body();
                            if (innerExprFm.Cast().Lambda().Args().Size() == 1) {
                                argSubstitutions[innerExprFm.Cast().Lambda().Args().Arg(0).Raw()] = innerExprFm.Cast().Input().Ptr();
                            }
                            if (auto ieJust = ieBody.Maybe<TCoJust>()) {
                                if (auto ieApply = ieJust.Cast().Input().Maybe<TCoApply>()) {
                                    return ExtractKnnDistanceFromApplyAll(ieApply.Cast(), argSubstitutions);
                                }
                            }
                        }
                        return ExtractKnnDistanceInfoAll(maybeColumnExpr.Cast());
                    }
                }
            }
        }
        return FindKnnInfoInComputedColumn(fmBody, aliasName);
    }

    return {};
}

// Find valid KNN candidate from a list (column exists in table, metric is valid for sort direction)
TMaybe<std::tuple<TString, TString, TExprNode::TPtr>> FindValidKnnCandidate(
    const TVector<std::tuple<TString, TString, TExprNode::TPtr>>& candidates,
    const TKikimrTableDescription& tableDesc, bool isAsc, TString& outMetric) {
    for (const auto& [col, method, target] : candidates) {
        if (!tableDesc.Metadata->Columns.contains(col)) {
            continue;
        }
        TString metric = GetMetricFromMethodName(method, isAsc);
        if (!metric.empty()) {
            outMetric = metric;
            return std::make_tuple(col, method, target);
        }
    }
    return {};
}

// Extract KNN info from TopSort's key selector, with optional FlatMap for alias lookup
TVector<std::tuple<TString, TString, TExprNode::TPtr>> ExtractKnnInfoFromTopSort(
    const TCoTopSort& topSort, const TMaybeNode<TCoFlatMapBase>& maybeFlatMap = {}) {
    auto lambdaBody = topSort.KeySelectorLambda().Body();
    auto result = ExtractKnnDistanceInfoAll(lambdaBody);
    if (result.empty()) {
        if (auto member = lambdaBody.Maybe<TCoMember>()) {
            auto aliasName = member.Cast().Name().Value();
            if (maybeFlatMap) {
                result = FindKnnInfoInComputedColumn(maybeFlatMap.Cast(), aliasName);
            } else {
                result = FindKnnInfoInComputedColumn(topSort.Input(), aliasName);
            }
        }
    }
    return result;
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

    // Also check if input is TDqCnUnionAll with a stage containing TKqpReadTableRanges
    TMaybeNode<TDqCnUnionAll> maybeCnUnionAll;
    TMaybeNode<TDqStage> maybeStage;
    TMaybeNode<TKqpReadTableRanges> maybeReadTableRanges;

    // Track if there's a DqSource with KqpReadRangesSourceSettings
    TMaybeNode<TDqSource> maybeDqSource;
    TMaybeNode<TKqpReadRangesSourceSettings> maybeSourceSettings;
    size_t sourceInputIndex = 0;

    // Track if there's a FlatMap between TopSort and read (for ORDER BY alias case)
    TMaybeNode<TCoFlatMapBase> maybeFlatMap;
    TExprBase flatMapInput = input;

    // Check for FlatMap/Map (used when ORDER BY references an alias/computed column or subquery)
    // The FlatMap/Map could be directly under TopSort or inside a TDqCnUnionAll stage
    if (input.Maybe<TCoFlatMapBase>()) {
        maybeFlatMap = input.Cast<TCoFlatMapBase>();
        flatMapInput = maybeFlatMap.Cast().Input();
    } else if (input.Maybe<TCoMap>()) {
        // TCoMap is used when the target vector comes from a subquery
        // We treat it similarly to FlatMap for pattern matching
        auto mapNode = input.Cast<TCoMap>();
        flatMapInput = mapNode.Input();
        // The Map's input might be another FlatMap wrapping DqCnUnionAll
        if (flatMapInput.Maybe<TCoFlatMapBase>()) {
            auto innerFlatMap = flatMapInput.Cast<TCoFlatMapBase>();
            flatMapInput = innerFlatMap.Input();
        }
    }

    if (!isReadTableRanges && flatMapInput.Maybe<TDqCnUnionAll>()) {
        maybeCnUnionAll = flatMapInput.Cast<TDqCnUnionAll>();
        maybeStage = maybeCnUnionAll.Cast().Output().Stage().Maybe<TDqStage>();
        if (maybeStage) {
            auto stage = maybeStage.Cast();
            auto stageBody = stage.Program().Body();

            // Check if the stage program body is TKqpReadTableRanges directly
            maybeReadTableRanges = stageBody.Maybe<TKqpReadTableRanges>();
            if (maybeReadTableRanges) {
                isReadTableRanges = true;
                input = maybeReadTableRanges.Cast();
            }

            // Also check if the stage body is a FlatMap over TKqpReadTableRanges
            // This happens when ORDER BY references an alias computed in the FlatMap
            if (!isReadTableRanges && stageBody.Maybe<TCoFlatMapBase>()) {
                auto stageFlatMap = stageBody.Cast<TCoFlatMapBase>();
                maybeReadTableRanges = stageFlatMap.Input().Maybe<TKqpReadTableRanges>();
                if (maybeReadTableRanges) {
                    isReadTableRanges = true;
                    input = maybeReadTableRanges.Cast();
                    // Use the FlatMap from the stage for alias resolution
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

                // Check if there's a FlatMap in the stage body for alias resolution
                if (isReadTableRanges && stageBody.Maybe<TCoFlatMapBase>() && !maybeFlatMap) {
                    maybeFlatMap = stageBody.Cast<TCoFlatMapBase>();
                }
            }
        }
    }

    if (!isReadTableRanges) {
        return node;
    }

    // Get table path and settings depending on whether we have direct read or source-based read
    TCoAtom tablePath = maybeSourceSettings
        ? maybeSourceSettings.Cast().Table().Path()
        : GetReadTablePath(input, true);

    TKqpReadTableSettings settings = maybeSourceSettings
        ? TKqpReadTableSettings::Parse(maybeSourceSettings.Cast().Settings())
        : GetReadTableSettings(input, true);

    // Check ranges - only apply to full table scans (no WHERE clause filtering)
    // When there's a WHERE clause, the Ranges() will not be TCoVoid
    TExprBase rangesExpr = maybeSourceSettings
        ? maybeSourceSettings.Cast().RangesExpr()
        : input.Cast<TKqpReadTableRanges>().Ranges();

    if (!TCoVoid::Match(rangesExpr.Raw())) {
        return node;
    }

    // If there's a FlatMap, check if it's filtering (WHERE on non-key columns)
    // We allow FlatMaps that add computed columns (ORDER BY alias), but reject filtering FlatMaps
    // Filtering FlatMaps use OptionalIf/ListIf, while projection FlatMaps use Just/SingleAsList
    if (maybeFlatMap) {
        auto flatMapBody = maybeFlatMap.Cast().Lambda().Body();
        // Check if the FlatMap body is conditional (filtering)
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

    // Check sort direction - only handle single direction (either all forward or all reverse)
    ESortDirection direction = GetSortDirection(topSort.SortDirections());
    if (direction != ESortDirection::Forward && direction != ESortDirection::Reverse) {
        return node;
    }
    const bool isAsc = (direction == ESortDirection::Forward);

    // Extract and validate KNN info
    auto knnInfoAll = ExtractKnnInfoFromTopSort(topSort, maybeFlatMap);
    TString metric;
    auto knnInfo = FindValidKnnCandidate(knnInfoAll, tableDesc, isAsc, metric);
    if (!knnInfo) {
        return node;
    }

    auto [columnName, methodName, targetExpr] = *knnInfo;

    YQL_CLOG(TRACE, ProviderKqp) << "-- applying vector top-K pushdown for column " << columnName << " with metric " << metric;

    // Set the vector top-K settings
    settings.VectorTopKColumn = columnName;
    settings.VectorTopKMetric = metric;

    // Target expression - wrap in precompute if not a simple type
    TExprBase targetExprBase(targetExpr);
    if (targetExprBase.Maybe<TCoString>() || targetExprBase.Maybe<TCoParameter>() || targetExprBase.Maybe<TCoArgument>()) {
        // TCoArgument represents a stage input (e.g., precomputed subquery result)
        settings.VectorTopKTarget = targetExpr;
    } else {
        // Wrap non-simple expressions in precompute (TDqPhyPrecompute)
        settings.VectorTopKTarget = KqpPrecomputeParameter(targetExprBase, ctx).Ptr();
    }

    // Limit expression - wrap in precompute if not a simple type
    auto limitExpr = topSort.Count();
    if (limitExpr.Maybe<TCoUint64>() || limitExpr.Maybe<TCoParameter>()) {
        settings.VectorTopKLimit = limitExpr.Ptr();
    } else {
        settings.VectorTopKLimit = KqpPrecomputeParameter(limitExpr, ctx).Ptr();
    }

    // Handle source-based read (data queries)
    if (maybeSourceSettings && maybeStage) {
        auto stage = maybeStage.Cast();
        auto oldSourceSettings = maybeSourceSettings.Cast();

        // Build new settings node
        auto newSettingsNode = settings.BuildNode(ctx, oldSourceSettings.Settings().Pos());

        // Build new source settings with updated read settings
        auto newSourceSettings = Build<TKqpReadRangesSourceSettings>(ctx, oldSourceSettings.Pos())
            .Table(oldSourceSettings.Table())
            .Columns(oldSourceSettings.Columns())
            .Settings(newSettingsNode)
            .RangesExpr(oldSourceSettings.RangesExpr())
            .ExplainPrompt(oldSourceSettings.ExplainPrompt())
            .Done();

        // Build new source
        auto newSource = Build<TDqSource>(ctx, maybeDqSource.Cast().Pos())
            .Settings(newSourceSettings)
            .DataSource(maybeDqSource.Cast().DataSource())
            .Done();

        // Rebuild inputs with the new source
        TVector<TExprBase> newInputs;
        for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
            if (i == sourceInputIndex) {
                newInputs.push_back(newSource);
            } else {
                newInputs.push_back(stage.Inputs().Item(i));
            }
        }

        // Rebuild the stage with the new inputs
        auto newStage = Build<TDqStage>(ctx, stage.Pos())
            .Inputs()
                .Add(newInputs)
                .Build()
            .Program(stage.Program())
            .Settings(stage.Settings())
            .Done();

        // Rebuild the connection
        auto newCnUnionAll = Build<TDqCnUnionAll>(ctx, maybeCnUnionAll.Cast().Pos())
            .Output()
                .Stage(newStage)
                .Index(maybeCnUnionAll.Cast().Output().Index())
                .Build()
            .Done();

        // If there was a FlatMap between TopSort and connection, preserve it
        TExprBase newTopSortInput = newCnUnionAll;
        if (maybeFlatMap && maybeFlatMap.Cast().Raw() != stage.Program().Body().Raw()) {
            // FlatMap was outside the stage (between TopSort and CnUnionAll)
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

    // If we found the read inside a stage, we need to rebuild the stage and connection
    if (maybeStage) {
        auto stage = maybeStage.Cast();
        auto stageBody = stage.Program().Body();

        // Determine the new stage body
        TExprBase newStageBody = newReadNode;

        // If the stage body was a FlatMap over the read, preserve the FlatMap structure and type
        if (auto orderedFlatMap = stageBody.Maybe<TCoOrderedFlatMap>()) {
            if (orderedFlatMap.Cast().Input().Maybe<TKqpReadTableRanges>()) {
                // Rebuild as OrderedFlatMap to preserve ordering semantics
                newStageBody = Build<TCoOrderedFlatMap>(ctx, orderedFlatMap.Cast().Pos())
                    .Input(newReadNode)
                    .Lambda(orderedFlatMap.Cast().Lambda())
                    .Done();
            }
        } else if (auto flatMap = stageBody.Maybe<TCoFlatMap>()) {
            if (flatMap.Cast().Input().Maybe<TKqpReadTableRanges>()) {
                // Rebuild as regular FlatMap
                newStageBody = Build<TCoFlatMap>(ctx, flatMap.Cast().Pos())
                    .Input(newReadNode)
                    .Lambda(flatMap.Cast().Lambda())
                    .Done();
            }
        }

        // Rebuild the stage with the new body
        auto newStage = Build<TDqStage>(ctx, stage.Pos())
            .Inputs(stage.Inputs())
            .Program()
                .Args(stage.Program().Args())
                .Body(newStageBody)
                .Build()
            .Settings(stage.Settings())
            .Done();

        // Rebuild the connection
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

// Helper to find TopSort in a stage body (recursively looking through Map/FlatMap)
TMaybeNode<TCoTopSort> FindTopSortInBody(const TExprBase& body) {
    if (auto topSort = body.Maybe<TCoTopSort>()) {
        return topSort;
    }
    if (auto map = body.Maybe<TCoMap>()) {
        return FindTopSortInBody(map.Cast().Lambda().Body());
    }
    if (auto flatMap = body.Maybe<TCoFlatMapBase>()) {
        return FindTopSortInBody(flatMap.Cast().Lambda().Body());
    }
    if (auto toFlow = body.Maybe<TCoToFlow>()) {
        return FindTopSortInBody(toFlow.Cast().Input());
    }
    return {};
}

// Helper to build a precompute that extracts a column from a subquery result
TExprNode::TPtr BuildSubqueryTargetPrecompute(
    const TCoMember& member, const TDqPhyPrecompute& precompute, TExprContext& ctx) {

    auto memberName = member.Name();
    auto newArg = Build<TCoArgument>(ctx, member.Pos())
        .Name("_kqp_extract_arg")
        .Done();

    // Build: Unwrap(Member(Unwrap(arg), "column_name"))
    auto memberExpr = Build<TCoMember>(ctx, member.Pos())
        .Struct<TCoUnwrap>()
            .Optional(newArg)
            .Build()
        .Name(memberName)
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
    auto knnInfoAll = ExtractKnnInfoFromTopSort(topSort);
    TString validMetric;
    auto validKnnInfo = FindValidKnnCandidate(knnInfoAll, tableDesc, isAsc, validMetric);
    if (!validKnnInfo) {
        return node;
    }

    auto [columnName, methodName, targetExpr] = *validKnnInfo;
    TExprBase targetExprBase(targetExpr);
    TExprNode::TPtr resolvedTarget;

    // Handle subquery targets: Member(arg, "column") where arg -> DqPhyPrecompute
    if (auto member = targetExprBase.Maybe<TCoMember>()) {
        if (auto arg = member.Cast().Struct().Maybe<TCoArgument>()) {
            auto stageArgs = stage.Program().Args();
            for (size_t i = 0; i < stageArgs.Size() && i < stage.Inputs().Size(); ++i) {
                if (stageArgs.Arg(i).Raw() == arg.Cast().Raw()) {
                    if (auto precompute = stage.Inputs().Item(i).Maybe<TDqPhyPrecompute>()) {
                        resolvedTarget = BuildSubqueryTargetPrecompute(member.Cast(), precompute.Cast(), ctx);
                        break;
                    }
                }
            }
        }
    }

    // For non-subquery cases
    if (!resolvedTarget) {
        bool hasArgument = false;
        VisitExpr(*targetExpr, [&](const TExprNode& n) {
            if (n.IsArgument()) { hasArgument = true; return false; }
            return true;
        });
        if (hasArgument) {
            return node;
        }
        resolvedTarget = targetExprBase.Maybe<TCoString>() || targetExprBase.Maybe<TCoParameter>()
            ? targetExpr
            : KqpPrecomputeParameter(targetExprBase, ctx).Ptr();
    }
    YQL_CLOG(TRACE, ProviderKqp) << "-- applying vector top-K pushdown (stage) for column " << columnName << " with metric " << validMetric;

    // Set the vector top-K settings
    settings.VectorTopKColumn = columnName;
    settings.VectorTopKMetric = validMetric;
    settings.VectorTopKTarget = resolvedTarget;

    // Limit expression
    auto limitExpr = topSort.Count();
    if (limitExpr.Maybe<TCoUint64>() || limitExpr.Maybe<TCoParameter>()) {
        settings.VectorTopKLimit = limitExpr.Ptr();
    } else {
        settings.VectorTopKLimit = KqpPrecomputeParameter(limitExpr, ctx).Ptr();
    }

    // Build new settings node
    auto newSettingsNode = settings.BuildNode(ctx, sourceSettings.Settings().Pos());

    // Build new source settings
    auto newSourceSettings = Build<TKqpReadRangesSourceSettings>(ctx, sourceSettings.Pos())
        .Table(sourceSettings.Table())
        .Columns(sourceSettings.Columns())
        .Settings(newSettingsNode)
        .RangesExpr(sourceSettings.RangesExpr())
        .ExplainPrompt(sourceSettings.ExplainPrompt())
        .Done();

    // Build new source
    auto newSource = Build<TDqSource>(ctx, maybeDqSource.Cast().Pos())
        .Settings(newSourceSettings)
        .DataSource(maybeDqSource.Cast().DataSource())
        .Done();

    // Rebuild inputs
    TVector<TExprBase> newInputs;
    for (size_t i = 0; i < stage.Inputs().Size(); ++i) {
        if (i == sourceInputIndex) {
            newInputs.push_back(newSource);
        } else {
            newInputs.push_back(stage.Inputs().Item(i));
        }
    }

    // Rebuild the stage
    return Build<TDqStage>(ctx, stage.Pos())
        .Inputs()
            .Add(newInputs)
            .Build()
        .Program(stage.Program())
        .Settings(stage.Settings())
        .Done();
}

} // namespace NKikimr::NKqp::NOpt

