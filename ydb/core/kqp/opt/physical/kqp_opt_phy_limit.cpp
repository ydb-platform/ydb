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
// Returns: {column name, method name, target expression} or nullopt if not a valid Knn distance call
TMaybe<std::tuple<TString, TString, TExprNode::TPtr>> ExtractKnnDistanceFromApply(
    const TCoApply& apply,
    const TNodeMap<TExprNode::TPtr>& argSubstitutions = {}) {
    auto udf = apply.Callable().Maybe<TCoUdf>();
    if (!udf) {
        return {};
    }

    const auto methodName = TString(udf.Cast().MethodName().Value());
    if (!methodName.StartsWith("Knn.")) {
        return {};
    }

    // Find the member (column) and target expression
    // Knn distance functions have exactly 2 arguments: column and target vector
    TMaybe<TString> columnName;
    TExprNode::TPtr targetExpr;
    size_t argCount = 0;

    for (const auto& arg : apply.Args()) {
        // Skip the callable itself (first element in Args)
        if (arg.Raw() == apply.Callable().Raw()) {
            continue;
        }
        argCount++;

        // Try to resolve the argument through substitutions first
        TExprNode::TPtr resolvedArg = arg.Ptr();
        auto it = argSubstitutions.find(arg.Raw());
        if (it != argSubstitutions.end()) {
            resolvedArg = it->second;
        }

        // Check if the (possibly resolved) argument is a Member - that's the column
        TExprBase resolvedExpr(resolvedArg);
        if (auto member = resolvedExpr.Maybe<TCoMember>()) {
            columnName = TString(member.Cast().Name().Value());
        } else {
            // Not a column reference - it's the target expression
            targetExpr = resolvedArg;
        }
    }

    // Knn distance functions should have exactly 2 arguments
    if (!columnName || !targetExpr || argCount != 2) {
        return {};
    }

    return std::make_tuple(*columnName, methodName, targetExpr);
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

// Check if the lambda body is a Knn::*Distance function call and extract information
// Returns: {column name, metric name, target expression} or nullopt if not a Knn distance function
// Handles both direct Apply and FlatMap-wrapped cases (for nullable columns)
TMaybe<std::tuple<TString, TString, TExprNode::TPtr>> ExtractKnnDistanceInfo(const TExprBase& lambdaBody) {
    // Case 1: Direct Apply - Knn::Distance(Member(input, 'emb'), expr)
    if (auto apply = lambdaBody.Maybe<TCoApply>()) {
        return ExtractKnnDistanceFromApply(apply.Cast());
    }

    // Case 2: FlatMap-wrapped (for optional target expression like String::HexDecode)
    // FlatMap(<input>, lambda(arg): Knn::Distance(Member(lambda_arg, 'emb'), arg))
    // Where <input> is the actual target expression and 'arg' is bound to it
    if (auto flatMap = lambdaBody.Maybe<TCoFlatMap>()) {
        auto input = flatMap.Cast().Input();
        auto lambda = flatMap.Cast().Lambda();
        auto innerBody = lambda.Body();

        // Build substitution map: lambda arg -> FlatMap input
        TNodeMap<TExprNode::TPtr> argSubstitutions;
        if (lambda.Args().Size() == 1) {
            argSubstitutions[lambda.Args().Arg(0).Raw()] = input.Ptr();
        }

        // Check if the inner body is a Just(Apply(...))
        if (auto just = innerBody.Maybe<TCoJust>()) {
            if (auto apply = just.Cast().Input().Maybe<TCoApply>()) {
                return ExtractKnnDistanceFromApply(apply.Cast(), argSubstitutions);
            }
        }

        // Check if the inner body is directly an Apply
        if (auto apply = innerBody.Maybe<TCoApply>()) {
            return ExtractKnnDistanceFromApply(apply.Cast(), argSubstitutions);
        }

        // Case 3: Nested FlatMap (for cases with multiple nullables)
        // FlatMap(expr, lambda(arg1): FlatMap(input, lambda(arg2): Knn::Distance(...)))
        if (auto innerFlatMap = innerBody.Maybe<TCoFlatMap>()) {
            auto innerInput = innerFlatMap.Cast().Input();
            auto innerLambda = innerFlatMap.Cast().Lambda();
            auto innerInnerBody = innerLambda.Body();

            // Add substitution for inner lambda arg
            if (innerLambda.Args().Size() == 1) {
                argSubstitutions[innerLambda.Args().Arg(0).Raw()] = innerInput.Ptr();
            }

            if (auto just = innerInnerBody.Maybe<TCoJust>()) {
                if (auto apply = just.Cast().Input().Maybe<TCoApply>()) {
                    return ExtractKnnDistanceFromApply(apply.Cast(), argSubstitutions);
                }
            }
            if (auto apply = innerInnerBody.Maybe<TCoApply>()) {
                return ExtractKnnDistanceFromApply(apply.Cast(), argSubstitutions);
            }
        }
    }

    return {};
}

// Get the metric enum value from the method name
TString GetMetricFromMethodName(const TString& methodName, bool isAsc) {
    if (methodName == "Knn.CosineDistance" && isAsc) {
        return "CosineDistance";
    }
    if (methodName == "Knn.CosineSimilarity" && !isAsc) {
        return "CosineSimilarity";
    }
    if (methodName == "Knn.InnerProductSimilarity" && !isAsc) {
        return "InnerProductSimilarity";
    }
    if (methodName == "Knn.ManhattanDistance" && isAsc) {
        return "ManhattanDistance";
    }
    if (methodName == "Knn.EuclideanDistance" && isAsc) {
        return "EuclideanDistance";
    }
    return {};
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

    // Track if there's a FlatMap between TopSort and read (for ORDER BY alias case)
    TMaybeNode<TCoFlatMapBase> maybeFlatMap;
    TExprBase flatMapInput = input;

    // Check for FlatMap (used when ORDER BY references an alias/computed column)
    // The FlatMap could be directly under TopSort or inside a TDqCnUnionAll stage
    if (input.Maybe<TCoFlatMapBase>()) {
        maybeFlatMap = input.Cast<TCoFlatMapBase>();
        flatMapInput = maybeFlatMap.Cast().Input();
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
        }
    }

    if (!isReadTableRanges) {
        return node;
    }

    // Only apply to full table scans (no WHERE clause filtering)
    // When there's a WHERE clause, the Ranges() will not be TCoVoid
    auto readTableRanges = input.Cast<TKqpReadTableRanges>();
    if (!TCoVoid::Match(readTableRanges.Ranges().Raw())) {
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

    auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, GetReadTablePath(input, true));

    // Only for datashard tables
    if (tableDesc.Metadata->Kind != EKikimrTableKind::Datashard) {
        return node;
    }

    auto settings = GetReadTableSettings(input, true);
    if (settings.VectorTopKColumn) {
        return node; // already set
    }

    // Check sort direction - only handle single direction (either all forward or all reverse)
    ESortDirection direction = GetSortDirection(topSort.SortDirections());
    if (direction != ESortDirection::Forward && direction != ESortDirection::Reverse) {
        return node;
    }
    const bool isAsc = (direction == ESortDirection::Forward);

    // Extract Knn distance info from the key selector lambda
    auto lambdaBody = topSort.KeySelectorLambda().Body();
    auto knnInfo = ExtractKnnDistanceInfo(lambdaBody);

    // If not found directly, check if it's a Member accessing a computed column from FlatMap
    if (!knnInfo && maybeFlatMap) {
        if (auto member = lambdaBody.Maybe<TCoMember>()) {
            auto columnName = member.Cast().Name().Value();
            // Look for this column in the FlatMap's lambda body
            auto flatMapLambda = maybeFlatMap.Cast().Lambda();
            auto flatMapBody = flatMapLambda.Body();

            // The FlatMap body might be Just(AsStruct(...)) or AsStruct(...)
            TExprBase structExpr = flatMapBody;
            if (auto just = flatMapBody.Maybe<TCoJust>()) {
                structExpr = just.Cast().Input();
            }

            auto maybeColumnExpr = FindColumnExprInStruct(structExpr, columnName);
            if (maybeColumnExpr.IsValid()) {
                knnInfo = ExtractKnnDistanceInfo(maybeColumnExpr.Cast());
            }
        }
    }

    if (!knnInfo) {
        return node;
    }

    auto [columnName, methodName, targetExpr] = *knnInfo;

    // Check if the metric is valid for the sort direction
    TString metric = GetMetricFromMethodName(methodName, isAsc);
    if (metric.empty()) {
        return node;
    }

    // Check if the column exists in the table
    if (!tableDesc.Metadata->Columns.contains(columnName)) {
        return node;
    }

    YQL_CLOG(TRACE, ProviderKqp) << "-- applying vector top-K pushdown for column " << columnName << " with metric " << metric;

    // Set the vector top-K settings
    settings.VectorTopKColumn = columnName;
    settings.VectorTopKMetric = metric;

    // Target expression - wrap in precompute if not a simple type
    TExprBase targetExprBase(targetExpr);
    if (targetExprBase.Maybe<TCoString>() || targetExprBase.Maybe<TCoParameter>()) {
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

} // namespace NKikimr::NKqp::NOpt

