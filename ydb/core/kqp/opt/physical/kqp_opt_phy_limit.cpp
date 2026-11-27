#include "kqp_opt_phy_rules.h"
#include "kqp_opt_phy_impl.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>

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

namespace {

bool IsSuitableToDisableOlapBlocks(TExprBase node, TTypeAnnotationContext& typesCtx, ui32 columnsLimit) {
    if (columnsLimit == 0 || !node.Maybe<TCoTake>() || typesCtx.BlockEngineMode == NYql::EBlockEngineMode::Disable) {
        return false;
    }

    return !!FindNode(node.Ptr(), [](const TExprNode::TPtr& node) { return !!TMaybeNode<TKqpReadOlapTableRanges>(node); });
}

} // anonymous namespace

TExprBase KqpDisableOlapBlocksOnLimit(TExprBase node, TTypeAnnotationContext& typesCtx, ui32 columnsLimit) {
    if (!IsSuitableToDisableOlapBlocks(node, typesCtx, columnsLimit)) {
        return node;
    }

    const auto takeType = node.Ref().GetTypeAnn();
    if (!takeType || takeType->GetKind() != ETypeAnnotationKind::List) {
        return node;
    }

    const auto maybeStructType = takeType->Cast<TListExprType>()->GetItemType();
    if (!maybeStructType || maybeStructType->GetKind() != ETypeAnnotationKind::Struct) {
        return node;
    }

    if (maybeStructType->Cast<TStructExprType>()->GetItems().size() >= static_cast<size_t>(columnsLimit)) {
        typesCtx.BlockEngineMode = NYql::EBlockEngineMode::Disable;
    }

    return node;
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

// Only follow operators that preserve the input rows one-for-one.
TMaybeNode<TExprBase> GetRowPreservingInput(const TExprBase& node) {
    if (node.Maybe<TCoToFlow>() || node.Maybe<TCoFromFlow>() || node.Maybe<TCoToStream>()
        || node.Maybe<TCoExtractMembers>())
    {
        return TExprBase(node.Ref().HeadPtr());
    }
    return {};
}

TMaybeNode<TExprBase> GetSingletonItem(const TExprBase& node) {
    if (auto just = node.Maybe<TCoJust>()) {
        return just.Cast().Input();
    }
    if (auto list = node.Maybe<TCoAsList>(); list && list.Cast().ArgCount() == 1) {
        return list.Cast().Arg(0);
    }
    return {};
}

TMaybeNode<TCoMapBase> GetMap(const TExprBase& node) {
    if (node.Maybe<TCoMap>() || node.Maybe<TCoOrderedMap>()) {
        return node.Cast<TCoMapBase>();
    }
    return {};
}

struct TVectorProjection {
    TCoArgument Row;
    TExprBase Value;
};

struct TVectorReadPath {
    TVector<TVectorProjection> Projections;

    TExprBase Follow(TExprBase node) {
        for (;;) {
            if (auto input = GetRowPreservingInput(node)) {
                node = input.Cast();
                continue;
            }

            const auto map = GetMap(node);
            const auto flatMap = node.Maybe<TCoFlatMapBase>();
            if (!map && !flatMap) {
                return node;
            }
            const auto lambda = map ? map.Cast().Lambda() : flatMap.Cast().Lambda();
            const auto value = map ? TMaybeNode<TExprBase>(lambda.Body()) : GetSingletonItem(lambda.Body());
            if (!value || lambda.Args().Size() != 1 || lambda.Ref().ChildrenSize() != 2) {
                return node;
            }
            Projections.push_back({lambda.Args().Arg(0), value.Cast()});
            node = map ? map.Cast().Input() : flatMap.Cast().Input();
        }
    }
};

// Resolve a field through the projections between the sort and the read.
bool ResolveVectorMember(TExprBase& expr, TCoArgument& row, TConstArrayRef<TVectorProjection>& projections,
    TExprContext& ctx, bool preserveColumn = false)
{
    while (const auto member = expr.Maybe<TCoMember>()) {
        if (member.Cast().Struct().Raw() != row.Raw() || projections.empty()) {
            break;
        }

        const auto& projection = projections.front();
        row = projection.Row;
        projections = projections.subspan(1);
        if (projection.Value.Raw() == row.Raw()) {
            expr = TExprBase(ctx.ChangeChild(member.Cast().Ref(), TCoMember::idx_Struct, row.Ptr()));
            continue;
        }
        const auto fields = projection.Value.Maybe<TCoAsStruct>();
        if (!fields) {
            return false;
        }
        bool found = false;
        for (const auto& field : fields.Cast()) {
            const auto item = field.Cast<TCoNameValueTuple>();
            if (item.Name().Value() == member.Cast().Name().Value()) {
                expr = item.Value().Cast();
                if (preserveColumn) {
                    const auto projectedMember = expr.Maybe<TCoMember>();
                    if (!projectedMember || projectedMember.Cast().Name().Value() != member.Cast().Name().Value()) {
                        return false;
                    }
                }
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
    }
    return true;
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

struct TVectorSortKey {
    TString Column;
    TString Metric;
    TExprBase Target;
};

TMaybe<TVectorSortKey> GetVectorSortKey(const TCoTopSort& topSort, const TVectorReadPath& path,
    const TCoAtomList& columns, TExprContext& ctx)
{
    const auto direction = GetSortDirection(topSort.SortDirections());
    if (direction != ESortDirection::Forward && direction != ESortDirection::Reverse) {
        return {};
    }

    const auto lambda = topSort.KeySelectorLambda();
    if (lambda.Args().Size() != 1 || lambda.Ref().ChildrenSize() != 2) {
        return {};
    }
    auto expr = lambda.Body();
    auto row = lambda.Args().Arg(0);
    TConstArrayRef<TVectorProjection> projections = path.Projections;
    if (!ResolveVectorMember(expr, row, projections, ctx)) {
        return {};
    }

    // Only unwrap optional maps that supply one of the two distance operands.
    TNodeMap<TExprBase> bindings;
    while (true) {
        if (auto just = expr.Maybe<TCoJust>()) {
            expr = just.Cast().Input();
        } else if (auto flatMap = expr.Maybe<TCoFlatMap>()) {
            const auto input = flatMap.Cast().Input();
            const auto fn = flatMap.Cast().Lambda();
            const auto type = input.Ref().GetTypeAnn();
            if (!type || type->GetKind() != ETypeAnnotationKind::Optional || fn.Args().Size() != 1 || fn.Ref().ChildrenSize() != 2) {
                return {};
            }
            bindings.emplace(fn.Args().Arg(0).Raw(), input);
            expr = fn.Body();
        } else {
            break;
        }
    }

    const auto apply = expr.Maybe<TCoApply>();
    if (!apply || apply.Cast().Ref().ChildrenSize() != 3) {
        return {};
    }
    const auto udf = apply.Cast().Callable().Maybe<TCoUdf>();
    if (!udf) {
        return {};
    }
    const auto metric = GetMetricFromMethodName(udf.Cast().MethodName().StringValue(), direction == ESortDirection::Forward);
    if (!metric) {
        return {};
    }

    auto unwrapOperand = [&](TExprBase operand) {
        for (auto it = bindings.find(operand.Raw()); it != bindings.end(); it = bindings.find(operand.Raw())) {
            operand = it->second;
            bindings.erase(it);
        }
        return operand;
    };
    const auto left = unwrapOperand(TExprBase(apply.Cast().Ref().ChildPtr(1)));
    const auto right = unwrapOperand(TExprBase(apply.Cast().Ref().ChildPtr(2)));
    if (!bindings.empty()) {
        return {};
    }

    for (const auto& [operand, targetExpr] : {std::pair{left, right}, std::pair{right, left}}) {
        auto column = operand;
        auto columnRow = row;
        auto remaining = projections;
        if (!ResolveVectorMember(column, columnRow, remaining, ctx, true) || !remaining.empty()) {
            continue;
        }
        const auto member = column.Maybe<TCoMember>();
        if (member && member.Cast().Struct().Raw() == columnRow.Raw()) {
            const auto name = member.Cast().Name().Value();
            for (const auto& readColumn : columns) {
                if (readColumn.Value() == name) {
                    return TVectorSortKey{TString(name), metric, targetExpr};
                }
            }
        }
    }
    return {};
}

// A source used by another branch of the stage cannot be truncated.
bool HasSingleUse(const TExprBase& body, const TExprBase& input) {
    TNodeMap<size_t> uses;
    const auto countUses = [&](const auto& self, const TExprNode& node) -> size_t {
        if (&node == input.Raw()) {
            return 1;
        }
        if (node.IsArguments()) {
            return 0;
        }
        if (const auto it = uses.find(&node); it != uses.end()) {
            return it->second;
        }
        size_t count = 0;
        for (const auto& child : node.Children()) {
            count += self(self, *child);
            if (count > 1) {
                break;
            }
        }
        uses.emplace(&node, count);
        return count;
    };
    return countUses(countUses, body.Ref()) == 1;
}

TMaybe<size_t> FindReadSource(const TDqStage& stage, const TExprBase& input) {
    const auto args = stage.Program().Args();
    for (size_t i = 0; i < args.Size() && i < stage.Inputs().Size(); ++i) {
        if (input.Raw() == args.Arg(i).Raw()) {
            const auto source = stage.Inputs().Item(i).Maybe<TDqSource>();
            if (source && source.Cast().Settings().Maybe<TKqpReadRangesSourceSettings>()
                && HasSingleUse(stage.Program().Body(), input))
            {
                return i;
            }
            break;
        }
    }
    return {};
}

TMaybeNode<TCoTopSort> FindStageTopSort(TExprBase node) {
    for (;;) {
        if (auto top = node.Maybe<TCoTopSort>()) {
            return top;
        }
        if (auto input = GetRowPreservingInput(node)) {
            node = input.Cast();
            continue;
        }
        if (auto item = GetSingletonItem(node)) {
            node = item.Cast();
            continue;
        }

        const auto map = GetMap(node);
        const auto flatMap = node.Maybe<TCoFlatMapBase>();
        if (!map && !flatMap) {
            return {};
        }
        const auto input = map ? map.Cast().Input() : flatMap.Cast().Input();
        if (const auto item = GetSingletonItem(input)) {
            if (const auto top = FindStageTopSort(item.Cast())) {
                return top;
            }
            // With a singleton input the sort in the lambda runs only once.
            const auto lambda = map ? map.Cast().Lambda() : flatMap.Cast().Lambda();
            if (lambda.Ref().ChildrenSize() != 2) {
                return {};
            }
            node = lambda.Body();
        } else {
            node = input;
        }
    }
}

// Read a scalar subquery field in a precompute independent of the source.
TExprNode::TPtr BuildSubqueryReadValue(
    const TCoMember& member, const TDqPhyPrecompute& precompute, TExprContext& ctx)
{
    auto arg = Build<TCoArgument>(ctx, member.Pos()).Name("_kqp_extract_arg").Done();
    auto value = Build<TCoMember>(ctx, member.Pos()).Struct(arg).Name(member.Name()).Done();

    return Build<TDqPhyPrecompute>(ctx, member.Pos())
        .Connection<TDqCnValue>()
            .Output()
                .Stage<TDqStage>()
                    .Inputs()
                        .Add(precompute)
                        .Build()
                    .Program()
                        .Args({arg})
                        .Body<TCoToStream>()
                            .Input<TCoAsList>()
                                .Add(value)
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

TExprNode::TPtr ResolveReadValue(const TExprBase& expr, const TMaybeNode<TDqStage>& stage, TExprContext& ctx,
    bool target)
{
    auto type = expr.Ref().GetTypeAnn();
    if (!type) {
        return {};
    }

    if (target && type->GetKind() == ETypeAnnotationKind::Optional) {
        type = type->Cast<TOptionalExprType>()->GetItemType();
    }
    if (target && type->GetKind() == ETypeAnnotationKind::Tagged) {
        type = type->Cast<TTaggedExprType>()->GetBaseType();
    }
    if (type->GetKind() != ETypeAnnotationKind::Data
        || type->Cast<TDataExprType>()->GetSlot() != (target ? EDataSlot::String : EDataSlot::Uint64))
    {
        return {};
    }

    const auto member = expr.Maybe<TCoMember>();
    const auto dependency = member ? member.Cast().Struct() : expr;
    if (stage && dependency.Maybe<TCoArgument>()) {
        const auto args = stage.Cast().Program().Args();
        const auto inputs = stage.Cast().Inputs();
        for (size_t i = 0; i < args.Size() && i < inputs.Size(); ++i) {
            if (args.Arg(i).Raw() == dependency.Raw()) {
                if (auto precompute = inputs.Item(i).Maybe<TDqPhyPrecompute>()) {
                    return member ? BuildSubqueryReadValue(member.Cast(), precompute.Cast(), ctx) : precompute.Cast().Ptr();
                }
                return {};
            }
        }
    }

    if (!expr.Ref().IsComplete()) {
        return {};
    }
    if (expr.Maybe<TCoString>() || expr.Maybe<TCoUint64>() || expr.Maybe<TCoParameter>() ||
        expr.Maybe<TDqPhyPrecompute>())
    {
        return expr.Ptr();
    }
    return KqpPrecomputeParameter(expr, ctx, /*unwrapOptional=*/false).Ptr();
}

bool SetVectorTopK(const TCoTopSort& topSort, const TVectorReadPath& path, const TCoAtomList& columns,
    const TExprBase& ranges, const TCoAtom& table, const TMaybeNode<TDqStage>& stage,
    TKqpReadTableSettings& settings, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx)
{
    if (!ranges.Maybe<TCoVoid>() || settings.Sampling || settings.VectorTopKColumn || settings.ItemsLimit
        || kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, table).Metadata->Kind != EKikimrTableKind::Datashard)
    {
        return false;
    }
    const auto key = GetVectorSortKey(topSort, path, columns, ctx);
    if (!key) {
        return false;
    }

    auto target = ResolveReadValue(key->Target, stage, ctx, true);
    auto limit = ResolveReadValue(topSort.Count(), stage, ctx, false);
    if (!target || !limit) {
        return false;
    }
    settings.VectorTopKColumn = key->Column;
    settings.VectorTopKMetric = key->Metric;
    settings.VectorTopKTarget = std::move(target);
    settings.VectorTopKLimit = std::move(limit);
    return true;
}

TMaybeNode<TDqStage> PushVectorTopKToSource(const TDqStage& stage, const TCoTopSort& topSort,
    const TVectorReadPath& path, const TExprBase& input, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx)
{
    const auto index = FindReadSource(stage, input);
    if (!index) {
        return {};
    }
    const auto source = stage.Inputs().Item(*index).Cast<TDqSource>();
    const auto read = source.Settings().Cast<TKqpReadRangesSourceSettings>();
    auto settings = TKqpReadTableSettings::Parse(read.Settings());
    if (!SetVectorTopK(topSort, path, read.Columns(), read.RangesExpr(), read.Table().Path(), stage, settings, ctx, kqpCtx)) {
        return {};
    }

    auto newRead = ctx.ChangeChild(read.Ref(), TKqpReadRangesSourceSettings::idx_Settings,
        settings.BuildNode(ctx, read.Settings().Pos()).Ptr());
    auto newSource = ctx.ChangeChild(source.Ref(), TDqSource::idx_Settings, std::move(newRead));
    auto inputs = ctx.ChangeChild(stage.Inputs().Ref(), *index, std::move(newSource));
    return TDqStage(ctx.ChangeChild(stage.Ref(), TDqStage::idx_Inputs, std::move(inputs)));
}

} // anonymous namespace

TExprBase KqpApplyVectorTopKToReadTable(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    const auto topSort = node.Cast<TCoTopSort>();
    TVectorReadPath path;
    const auto outerInput = path.Follow(topSort.Input());
    auto input = outerInput;
    TMaybeNode<TDqStage> stage;
    const auto connection = outerInput.Maybe<TDqCnUnionAll>();
    if (connection) {
        stage = connection.Cast().Output().Stage().Maybe<TDqStage>();
        if (!stage || stage.Cast().Program().Ref().ChildrenSize() != 2
            || connection.Cast().Output().Index().Value() != "0") {
            return node;
        }
        input = path.Follow(stage.Cast().Program().Body());
    }

    if (!HasSingleUse(topSort.Input(), outerInput)) {
        return node;
    }
    TExprNode::TPtr newInput;
    if (const auto read = input.Maybe<TKqpReadTableRanges>()) {
        const auto tableRead = read.Cast();
        auto settings = TKqpReadTableSettings::Parse(tableRead.Settings());
        // The inline read's settings cannot refer to owning stage arguments.
        if (!SetVectorTopK(topSort, path, tableRead.Columns(), tableRead.Ranges(), tableRead.Table().Path(),
            {}, settings, ctx, kqpCtx))
        {
            return node;
        }
        newInput = BuildReadNode(node.Pos(), ctx, tableRead, settings).Ptr();
    } else if (!stage) {
        return node;
    }

    if (stage) {
        TExprNode::TPtr newStage;
        if (newInput) {
            const auto program = stage.Cast().Program();
            if (!HasSingleUse(program.Body(), input)) {
                return node;
            }
            auto newProgram = ctx.ReplaceNode(program.Ptr(), input.Ref(), std::move(newInput));
            newStage = ctx.ChangeChild(stage.Cast().Ref(), TDqStage::idx_Program, std::move(newProgram));
        } else if (auto rewritten = PushVectorTopKToSource(stage.Cast(), topSort, path, input, ctx, kqpCtx)) {
            newStage = rewritten.Cast().Ptr();
        } else {
            return node;
        }
        const auto output = connection.Cast().Output();
        auto newOutput = ctx.ChangeChild(output.Ref(), TDqOutput::idx_Stage, std::move(newStage));
        newInput = ctx.ChangeChild(connection.Cast().Ref(), TDqCnUnionAll::idx_Output, std::move(newOutput));
    }

    auto newTopInput = ctx.ReplaceNodes(topSort.Input().Ptr(), {{outerInput.Raw(), newInput}});
    return TExprBase(ctx.ChangeChild(topSort.Ref(), TCoTopSort::idx_Input, std::move(newTopInput)));
}

TExprBase KqpApplyVectorTopKToStageWithSource(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    const auto stage = node.Cast<TDqStage>();
    if (stage.Program().Ref().ChildrenSize() != 2) {
        return node;
    }
    if (const auto topSort = FindStageTopSort(stage.Program().Body())) {
        TVectorReadPath path;
        const auto input = path.Follow(topSort.Cast().Input());
        if (auto rewritten = PushVectorTopKToSource(stage, topSort.Cast(), path, input, ctx, kqpCtx)) {
            return rewritten.Cast();
        }
    }
    return node;
}

} // namespace NKikimr::NKqp::NOpt
