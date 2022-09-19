#include "yql_kikimr_provider_impl.h"

#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/common_opt/yql_co.h>
#include<ydb/library/yql/core/yql_aggregate_expander.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NYql {
namespace {

using namespace NNodes;
using namespace NCommon;

bool CanPushPartialSort(const TKiPartialSort& node, const TKikimrTableDescription& tableDesc, TVector<TString>* columns) {
    return IsKeySelectorPkPrefix(node.KeySelectorLambda(), tableDesc, columns);
}

TExprNode::TPtr KiTrimReadTableWorld(TExprBase node) {
    if (auto maybeRead = node.Maybe<TCoLeft>().Input().Maybe<TKiReadTable>()) {
        YQL_CLOG(INFO, ProviderKikimr) << "KiTrimReadTableWorld";
        return maybeRead.Cast().World().Ptr();
    }

    return node.Ptr();
}

TExprNode::TPtr KiEmptyCommit(TExprBase node) {
    if (!node.Maybe<TCoCommit>().World().Maybe<TCoCommit>()) {
        return node.Ptr();
    }

    auto commit = node.Cast<TCoCommit>();
    if (!commit.DataSink().Maybe<TKiDataSink>()) {
        return node.Ptr();
    }

    auto innerCommit = commit.World().Cast<TCoCommit>();
    if (!innerCommit.DataSink().Maybe<TKiDataSink>()) {
        return node.Ptr();
    }

    return innerCommit.Ptr();
}

TExprNode::TPtr KiEraseOverSelectRow(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoFlatMap>().Input().Maybe<TKiSelectRow>()) {
        return node.Ptr();
    }

    auto map = node.Cast<TCoFlatMap>();

    if (auto maybeErase = map.Lambda().Body().Maybe<TCoJust>().Input().Maybe<TKiEraseRow>()) {
        auto selectRow = map.Input().Cast<TKiSelectRow>();
        auto eraseRow = maybeErase.Cast();

        YQL_ENSURE(selectRow.Cluster().Raw() == eraseRow.Cluster().Raw());

        if (selectRow.Table().Raw() == eraseRow.Table().Raw()) {
            auto ret = Build<TCoJust>(ctx, node.Pos())
                .Input<TKiEraseRow>()
                    .Cluster(selectRow.Cluster())
                    .Table(selectRow.Table())
                    .Key(selectRow.Key())
                    .Build()
                .Done();

            YQL_CLOG(INFO, ProviderKikimr) << "KiEraseOverSelectRow";
            return ret.Ptr();
        }
    }

    if (auto maybeAsList = map.Lambda().Body().Maybe<TCoAsList>()) {
        auto asList = maybeAsList.Cast();
        if (asList.ArgCount() != 1) {
            return node.Ptr();
        }

        if (auto maybeErase = asList.Arg(0).Maybe<TKiEraseRow>()) {
            auto selectRow = map.Input().Cast<TKiSelectRow>();
            auto eraseRow = maybeErase.Cast();

            YQL_ENSURE(selectRow.Cluster().Raw() == eraseRow.Cluster().Raw());

            if (selectRow.Table().Raw() == eraseRow.Table().Raw()) {

                auto ret = Build<TCoAsList>(ctx, node.Pos())
                    .Add<TKiEraseRow>()
                        .Cluster(selectRow.Cluster())
                        .Table(selectRow.Table())
                        .Key(selectRow.Key())
                        .Build()
                    .Done();

                YQL_CLOG(INFO, ProviderKikimr) << "KiEraseOverSelectRow";
                return ret.Ptr();
            }
        }
    }

    return node.Ptr();
}

TExprNode::TPtr KiRewriteAggregate(TExprBase node, TExprContext& ctx, TTypeAnnotationContext& typesCtx) {
    if (!node.Maybe<TCoAggregate>()) {
        return node.Ptr();
    }

    auto agg = node.Cast<TCoAggregate>();
    if (!agg.Input().Maybe<TKiSelectRange>() && !agg.Input().Maybe<TCoFlatMap>().Input().Maybe<TKiSelectRange>()) {
        return node.Ptr();
    }

    for (size_t i = 0; i < agg.Handlers().Size(); ++i) {
        auto aggHandler = agg.Handlers().Item(i);

        // Need to get rid of Unwraps in AggregateToPartitionByKeyWithCombine for DISTINCT case
        if (aggHandler.DistinctName().IsValid()) {
            return node.Ptr();
        }
    }

    YQL_CLOG(INFO, ProviderKikimr) << "KiRewriteAggregate";
    TAggregateExpander aggExpander(true, node.Ptr(), ctx, typesCtx);
    return aggExpander.ExpandAggregate();
}

TExprNode::TPtr KiRedundantSortByPk(TExprBase node, TExprContext& ctx,
    const TKikimrTablesData& tablesData, const TKikimrConfiguration& config)
{
    auto maybeSort = node.Maybe<TCoSort>();
    auto maybePartialSort = node.Maybe<TKiPartialSort>();

    if (!maybeSort && !maybePartialSort) {
        return node.Ptr();
    }

    auto input = maybeSort ? maybeSort.Cast().Input() : maybePartialSort.Cast().Input();
    auto sortDirections = maybeSort ? maybeSort.Cast().SortDirections() : maybePartialSort.Cast().SortDirections();
    auto keySelector = maybeSort ? maybeSort.Cast().KeySelectorLambda() : maybePartialSort.Cast().KeySelectorLambda();

    auto read = input;
    TMaybe<THashSet<TStringBuf>> passthroughFields;
    if (maybePartialSort && input.Maybe<TCoFlatMap>()) {
        auto flatmap = input.Cast<TCoFlatMap>();

        if (!IsPassthroughFlatMap(flatmap, &passthroughFields)) {
            return node.Ptr();
        }

        read = flatmap.Input();
    }

    if (!read.Maybe<TKiSelectRange>()) {
        return node.Ptr();
    }

    auto selectRange = read.Cast<TKiSelectRange>();

    if (HasSetting(selectRange.Settings().Ref(), "Reverse")) {
        // N.B. when SelectRange has a Reverse option we cannot optimize
        // sort without complex analysis of how it interacts with sorting
        return node.Ptr();
    }

    enum : ui32 {
        SortDirectionNone = 0,
        SortDirectionForward = 1,
        SortDirectionReverse = 2,
        SortDirectionUnknown = 4,
    };

    auto getDirection = [] (TExprBase expr) -> ui32 {
        if (!expr.Maybe<TCoBool>()) {
            return SortDirectionUnknown;
        }

        if (!FromString<bool>(expr.Cast<TCoBool>().Literal().Value())) {
            return SortDirectionReverse;
        }

        return SortDirectionForward;
    };

    ui32 direction = SortDirectionNone;

    if (auto maybeList = sortDirections.Maybe<TExprList>()) {
        for (const auto& expr : maybeList.Cast()) {
            direction |= getDirection(expr);
            if (direction != SortDirectionForward && direction != SortDirectionReverse) {
                return node.Ptr();
            }
        }
    } else {
        direction |= getDirection(sortDirections);
        if (direction != SortDirectionForward && direction != SortDirectionReverse) {
            return node.Ptr();
        }
    }

    auto& tableData = tablesData.ExistingTable(selectRange.Cluster().StringValue(), selectRange.Table().Path().StringValue());

    auto checkKey = [keySelector, &tableData, &passthroughFields] (TExprBase key, ui32 index) {
        if (!key.Maybe<TCoMember>()) {
            return false;
        }

        auto member = key.Cast<TCoMember>();
        if (member.Struct().Raw() != keySelector.Args().Arg(0).Raw()) {
            return false;
        }

        auto column = TString(member.Name().Value());
        auto columnIndex = tableData.GetKeyColumnIndex(column);
        if (!columnIndex || *columnIndex != index) {
            return false;
        }

        if (passthroughFields && !passthroughFields->contains(column)) {
            return false;
        }

        return true;
    };

    auto lambdaBody = keySelector.Body();
    if (auto maybeTuple = lambdaBody.Maybe<TExprList>()) {
        auto tuple = maybeTuple.Cast();
        for (size_t i = 0; i < tuple.Size(); ++i) {
            if (!checkKey(tuple.Item(i), i)) {
                return node.Ptr();
            }
        }
    } else {
        if (!checkKey(lambdaBody, 0)) {
            return node.Ptr();
        }
    }

    if (direction == SortDirectionReverse) {
        if (!config.AllowReverseRange()) {
            return node.Ptr();
        }

        auto reverseValue = Build<TCoBool>(ctx, node.Pos())
            .Literal().Build("true")
            .Done();

        auto newSettings = Build<TCoNameValueTupleList>(ctx, selectRange.Settings().Pos())
            .Add(TVector<TExprBase>(selectRange.Settings().begin(), selectRange.Settings().end()))
            .Add<TCoNameValueTuple>()
                .Name().Build("Reverse")
                .Value(reverseValue)
                .Build()
            .Done();

        TExprNode::TPtr newSelect = Build<TKiSelectRange>(ctx, selectRange.Pos())
            .Cluster(selectRange.Cluster())
            .Table(selectRange.Table())
            .Range(selectRange.Range())
            .Select(selectRange.Select())
            .Settings(newSettings)
            .Done()
            .Ptr();

        YQL_CLOG(INFO, ProviderKikimr) << "KiRedundantSortByPkReverse";

        if (input.Maybe<TCoFlatMap>()) {
            auto flatmap = input.Cast<TCoFlatMap>();

            return Build<TCoFlatMap>(ctx, flatmap.Pos())
                .Input(newSelect)
                .Lambda(flatmap.Lambda())
                .Done().Ptr();
        } else {
            return newSelect;
        }
    }

    YQL_CLOG(INFO, ProviderKikimr) << "KiRedundantSortByPk";
    return input.Ptr();
}

TExprNode::TPtr KiTopSort(TExprBase node, TExprContext& ctx, const TOptimizeContext& optCtx,
    const TKikimrConfiguration& config)
{
    if (config.HasOptDisableTopSort()) {
        return node.Ptr();
    }

    if (!node.Maybe<TCoTake>()) {
        return node.Ptr();
    }

    auto take = node.Cast<TCoTake>();
    TCoInputBase top = take;
    if (!optCtx.IsSingleUsage(take.Input().Ref())) {
        return node.Ptr();
    }

    if (!IsKqlPureExpr(take.Count())) {
        return node.Ptr();
    }

    auto skip = take.Input().Maybe<TCoSkip>();
    if (skip) {
        if (!optCtx.IsSingleUsage(skip.Cast().Input().Ref())) {
            return node.Ptr();
        }

        if (!IsKqlPureExpr(skip.Cast().Count())) {
            return node.Ptr();
        }

        top = skip.Cast();
    }

    auto sort = top.Input().Maybe<TCoSort>();
    if (sort) {
        if (!optCtx.IsSingleUsage(sort.Cast().Input().Ref())) {
            return node.Ptr();
        }

        top = sort.Cast();
    }

    bool hasExtend = false;
    TVector<TExprBase> inputs;
    if (auto maybeExtend = top.Input().Maybe<TCoExtend>()) {
        for (const auto& input : maybeExtend.Cast()) {
            inputs.push_back(input);
        }
        hasExtend = true;
    } else {
        inputs.push_back(top.Input());
    }

    TExprBase takeCount = take.Count();
    if (skip) {
        takeCount = Build<TCoPlus>(ctx, node.Pos())
            .Left(take.Count())
            .Right(skip.Cast().Count())
            .Done();
    }

    bool hasFlatMaps = false;
    TVector<TExprBase> partialOutputs;
    for (auto& input : inputs) {
        auto read = input;
        if (auto maybeFlatmap = input.Maybe<TCoFlatMap>()) {
            auto flatmap = maybeFlatmap.Cast();
            if (!optCtx.IsSingleUsage(flatmap.Input().Ref())) {
                return node.Ptr();
            }

            if (!IsKqlPureLambda(flatmap.Lambda())) {
                return node.Ptr();
            }

            hasFlatMaps = true;
            read = flatmap.Input();
        }

        if (!read.Maybe<TKiSelectRangeBase>()) {
            return node.Ptr();
        }

        auto takeInput = input;
        if (sort) {
            takeInput = Build<TKiPartialSort>(ctx, node.Pos())
                .Input(input)
                .SortDirections(sort.Cast().SortDirections())
                .KeySelectorLambda(sort.Cast().KeySelectorLambda())
                .Done();
        }

        auto output = Build<TKiPartialTake>(ctx, node.Pos())
            .Input(takeInput)
            .Count(takeCount)
            .Done();

        partialOutputs.push_back(output);
    }

    TExprBase merged = Build<TCoExtend>(ctx, node.Pos())
        .Add(partialOutputs)
        .Done();

    if (sort) {
        merged = Build<TCoSort>(ctx, node.Pos())
            .Input(merged)
            .SortDirections(sort.Cast().SortDirections())
            .KeySelectorLambda(sort.Cast().KeySelectorLambda())
            .Done();
    } else {
        auto canRewrite = hasExtend || hasFlatMaps;
        if (!canRewrite || take.Count().Maybe<TCoUint64>()) {
            return node.Ptr();
        }
    }

    YQL_CLOG(INFO, ProviderKikimr) << "KiTopSort";

    if (skip) {
        return Build<TCoTake>(ctx, node.Pos())
            .Input<TCoSkip>()
                .Input(merged)
                .Count(skip.Count().Cast())
                .Build()
            .Count(take.Count())
            .Done()
            .Ptr();
    } else {
        return Build<TCoTake>(ctx, node.Pos())
            .Input(merged)
            .Count(take.Count())
            .Done()
            .Ptr();
    }
}

TMaybeNode<TCoNameValueTupleList> SimplifyKeyTuples(TCoNameValueTupleList keyTupleList, TExprContext& ctx) {
    TVector<TCoNameValueTuple> newTuples(keyTupleList.begin(), keyTupleList.end());

    bool hasChanges = false;
    for (ui32 i = 0; i < keyTupleList.Size(); ++i) {
        const auto& tuple = keyTupleList.Item(i);
        YQL_ENSURE(tuple.Value().IsValid());
        if (auto maybeJust = tuple.Value().Maybe<TCoJust>()) {
            hasChanges = true;
            newTuples[i] = Build<TCoNameValueTuple>(ctx, maybeJust.Cast().Pos())
                .Name(tuple.Name())
                .Value(maybeJust.Cast().Input())
                .Done();
        }
    }

    if (hasChanges) {
        return Build<TCoNameValueTupleList>(ctx, keyTupleList.Pos())
            .Add(newTuples)
            .Done();
    }

    return TMaybeNode<TCoNameValueTupleList>();
}

TExprNode::TPtr KiSimplifyRowKey(TExprBase node, TExprContext& ctx) {
    if (auto maybeSelectRow = node.Maybe<TKiSelectRow>()) {
        auto selectRow = maybeSelectRow.Cast();

        if (auto newKey = SimplifyKeyTuples(selectRow.Key(), ctx)) {
            return Build<TKiSelectRow>(ctx, node.Pos())
                .Cluster(selectRow.Cluster())
                .Table(selectRow.Table())
                .Key(newKey.Cast())
                .Select(selectRow.Select())
                .Done()
                .Ptr();
        }
    }

    if (auto maybeEraseRow = node.Maybe<TKiEraseRow>()) {
        auto eraseRow = maybeEraseRow.Cast();

        if (auto newKey = SimplifyKeyTuples(eraseRow.Key(), ctx)) {
            return Build<TKiEraseRow>(ctx, node.Pos())
                .Cluster(eraseRow.Cluster())
                .Table(eraseRow.Table())
                .Key(newKey.Cast())
                .Done()
                .Ptr();
        }
    }

    if (auto maybeUpdateRow = node.Maybe<TKiUpdateRow>()) {
        auto updateRow = maybeUpdateRow.Cast();

        if (auto newKey = SimplifyKeyTuples(updateRow.Key(), ctx)) {
            return Build<TKiUpdateRow>(ctx, node.Pos())
                .Cluster(updateRow.Cluster())
                .Table(updateRow.Table())
                .Key(newKey.Cast())
                .Update(updateRow.Update())
                .Done()
                .Ptr();
        }
    }

    return node.Ptr();
}

TExprNode::TPtr DoRewriteSelectIndexRange(const TKiSelectIndexRange& selectIndexRange,
    const TKikimrTablesData& tablesData, TExprContext& ctx,
    const TVector<TString>& extraColumns, const std::function<TExprBase(const TExprBase&)>& middleFilter = {})
{
    const auto pos = selectIndexRange.Pos();

    const auto& cluster = selectIndexRange.Cluster().Value();
    const auto& versionedTable = selectIndexRange.Table();
    const auto& indexTableName = selectIndexRange.IndexName().Value();

    const auto& tableDesc = tablesData.ExistingTable(TString(cluster), TString(versionedTable.Path()));
    const auto& indexTableDesc = tablesData.ExistingTable(TString(cluster), TString(indexTableName));

    const auto& fetchItemArg = Build<TCoArgument>(ctx, pos)
        .Name("fetchItem")
        .Done();

    bool needDataRead = false;
    for (const auto& col : selectIndexRange.Select()) {
        if (!indexTableDesc.Metadata->Columns.contains(TString(col.Value()))) {
            needDataRead = true;
            break;
        }
    }

    const bool indexFullScan = TKikimrKeyRange::IsFull(selectIndexRange.Range());
    // Fullscan from index table but without reading data from main is OK
    if (indexFullScan && needDataRead) {
        auto issue = TIssue(ctx.GetPosition(pos), "Given predicate is not suitable for used index");
        SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_KIKIMR_WRONG_INDEX_USAGE, issue);
        if (!ctx.AddWarning(issue)) {
            return nullptr;
        }
    }

    auto keyColumnsList = needDataRead ? BuildKeyColumnsList(tableDesc, pos, ctx) : selectIndexRange.Select();
    auto columns = MergeColumns(keyColumnsList, extraColumns, ctx);

    TExprBase selectKeyRange = Build<TKiSelectRange>(ctx, pos)
        .Cluster(selectIndexRange.Cluster())
        .Table(BuildVersionedTable(*indexTableDesc.Metadata, pos, ctx))
        .Range(selectIndexRange.Range())
        .Select(columns)
        .Settings(selectIndexRange.Settings())
        .Done();

    if (middleFilter) {
        selectKeyRange = middleFilter(selectKeyRange);
    }

    if (!needDataRead) {
        return TExprBase(selectKeyRange).Ptr();
    }

    const auto& fetchLambda = Build<TCoLambda>(ctx, pos)
        .Args(fetchItemArg)
        .Body<TKiSelectRow>()
            .Cluster()
                .Value(cluster)
                .Build()
            .Table(versionedTable)
            .Key(ExtractNamedKeyTuples(fetchItemArg, tableDesc, ctx))
            .Select(selectIndexRange.Select())
            .Build()
        .Done();

    const auto& flatMap = Build<TCoFlatMap>(ctx, pos)
            .Input(selectKeyRange)
            .Lambda(fetchLambda)
            .Done();

    YQL_CLOG(INFO, ProviderKikimr) << "KiRewriteSelectIndexRange";
    return TExprBase(flatMap).Ptr();
}

TExprNode::TPtr KiRewritePartialTakeSortOverSelectIndexRange(TExprBase node, const TKikimrTablesData& tablesData, TExprContext& ctx) {
    auto maybePartialTake = node.Maybe<TKiPartialTake>();
    if (!maybePartialTake) {
        return node.Ptr();
    }

    auto partialTake = maybePartialTake.Cast();

    auto maybePartialSort = partialTake.Input().Maybe<TKiPartialSort>();
    if (!maybePartialSort) {
        return node.Ptr();
    }

    auto partialSort = maybePartialSort.Cast();

    auto maybeSelectIndexRange = partialSort.Input().Maybe<TKiSelectIndexRange>();
    if (!maybeSelectIndexRange) {
        return node.Ptr();
    }

    auto selectIndexRange = maybeSelectIndexRange.Cast();

    const auto cluster = selectIndexRange.Cluster().StringValue();
    const auto indexTableName = selectIndexRange.IndexName().StringValue();

    const auto& indexDesc = tablesData.ExistingTable(cluster, indexTableName);

    TVector<TString> sortByColumns;
    if (!CanPushPartialSort(maybePartialSort.Cast(), indexDesc, &sortByColumns)) {
        return node.Ptr();
    }

    auto filter = [&ctx, &node, &partialSort, &partialTake](const TExprBase& in) mutable {
        auto out = Build<TKiPartialTake>(ctx, node.Pos())
            .Input<TKiPartialSort>()
                .Input(in)
                .SortDirections(partialSort.SortDirections())
                .KeySelectorLambda(ctx.DeepCopyLambda(partialSort.KeySelectorLambda().Ref()))
                .Build()
            .Count(partialTake.Count())
            .Done();
        return TExprBase(out);
    };

    return DoRewriteSelectIndexRange(selectIndexRange, tablesData, ctx, sortByColumns, filter);
}

TExprNode::TPtr KiRewriteSelectIndexRange(TExprBase node, const TKikimrTablesData& tablesData, TExprContext& ctx) {
    if (auto maybeSelectIndexRange = node.Maybe<TKiSelectIndexRange>()) {
        return DoRewriteSelectIndexRange(maybeSelectIndexRange.Cast(), tablesData, ctx, {});
    }

    return node.Ptr();
}

TExprNode::TPtr KiApplyExtractMembersToSelectRange(TExprBase node, TExprContext& ctx) {
    if (!node.Maybe<TCoExtractMembers>().Input().Maybe<TKiSelectRangeBase>()) {
        return node.Ptr();
    }

    auto extract = node.Cast<TCoExtractMembers>();

    if (node.Maybe<TCoExtractMembers>().Input().Maybe<TKiSelectRange>()) {
        auto range = extract.Input().Cast<TKiSelectRange>();

        YQL_CLOG(INFO, ProviderKikimr) << "KiApplyExtractMembersToSelectRange";
        return Build<TKiSelectRange>(ctx, node.Pos())
            .Cluster(range.Cluster())
            .Table(range.Table())
            .Range(range.Range())
            .Select(extract.Members())
            .Settings(range.Settings())
            .Done().Ptr();
    } else if (node.Maybe<TCoExtractMembers>().Input().Maybe<TKiSelectIndexRange>()) {
        auto range = extract.Input().Cast<TKiSelectIndexRange>();

        YQL_CLOG(INFO, ProviderKikimr) << "KiApplyExtractMembersToSelectRange";
        return Build<TKiSelectIndexRange>(ctx, node.Pos())
            .Cluster(range.Cluster())
            .Table(range.Table())
            .Range(range.Range())
            .Select(extract.Members())
            .Settings(range.Settings())
            .IndexName(range.IndexName())
            .Done().Ptr();
    } else {
        ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << "Unexpected callable"));
        return nullptr;
    }
}

} // namespace

TAutoPtr<IGraphTransformer> CreateKiLogicalOptProposalTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx, TTypeAnnotationContext& types) {
    return CreateFunctorTransformer([sessionCtx, &types](const TExprNode::TPtr& input, TExprNode::TPtr& output,
        TExprContext& ctx)
    {
        typedef IGraphTransformer::TStatus TStatus;

        TParentsMap parentsMap;
        TOptimizeContext optCtx;
        GatherParents(*input, parentsMap);
        optCtx.ParentsMap = &parentsMap;

        TStatus status = OptimizeExpr(input, output, [sessionCtx, &optCtx, &types](const TExprNode::TPtr& inputNode, TExprContext& ctx) {
            auto ret = inputNode;
            TExprBase node(inputNode);

            ret = KiSqlInToEquiJoin(node, sessionCtx->Tables(), sessionCtx->Config(), ctx);
            if (ret != inputNode) {
                return ret;
            }

            ret = KiApplyExtractMembersToSelectRange(node, ctx);
            if (ret != inputNode) {
                return ret;
            }

            ret = KiApplyExtractMembersToSelectRow(node, ctx);
            if (ret != inputNode) {
                return ret;
            }

            ret = KiRedundantSortByPk(node, ctx, sessionCtx->Tables(), sessionCtx->Config());
            if (ret != inputNode) {
                return ret;
            }

            ret = KiApplyLimitToSelectRange(node, ctx);
            if (ret != inputNode) {
                return ret;
            }

            ret = KiPushPredicateToSelectRange(node, ctx, sessionCtx->Tables(), sessionCtx->Config());
            if (ret != inputNode) {
                return ret;
            }

            ret = KiEraseOverSelectRow(node, ctx);
            if (ret != inputNode) {
                return ret;
            }

            ret = KiRewriteEquiJoin(node, sessionCtx->Tables(), sessionCtx->Config(), ctx);
            if (ret != inputNode) {
                return ret;
            }

            ret = KiRewriteAggregate(node, ctx, types);
            if (ret != inputNode) {
                return ret;
            }

            ret = KiTopSort(node, ctx, optCtx, sessionCtx->Config());
            if (ret != inputNode) {
                return ret;
            }

            ret = KiSimplifyRowKey(node, ctx);
            if (ret != inputNode) {
                return ret;
            }

            ret = KiRewritePartialTakeSortOverSelectIndexRange(node, sessionCtx->Tables(), ctx);
            if (ret != inputNode) {
                return ret;
            }

            return ret;
        }, ctx, TOptimizeExprSettings(nullptr));

        return status;
    });
}

TAutoPtr<IGraphTransformer> CreateKiPhysicalOptProposalTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx) {
    return CreateFunctorTransformer([sessionCtx](const TExprNode::TPtr& input, TExprNode::TPtr& output,
        TExprContext& ctx)
    {
        typedef IGraphTransformer::TStatus TStatus;

        TStatus status = OptimizeExpr(input, output, [sessionCtx](const TExprNode::TPtr& inputNode, TExprContext& ctx) {
            auto ret = inputNode;
            TExprBase node(inputNode);

            ret = KiEmptyCommit(node);
            if (ret != inputNode) {
                return ret;
            }

            if (auto maybeDatasink = node.Maybe<TCoCommit>().DataSink().Maybe<TKiDataSink>()) {
                auto cluster = TString(maybeDatasink.Cast().Cluster());
                auto useNewEngine = sessionCtx->Config().UseNewEngine.Get();
                if (!useNewEngine.Defined() && sessionCtx->Config().HasKqpForceNewEngine()) {
                    useNewEngine = true;
                }

                ret = KiBuildQuery(node, useNewEngine, ctx);

                if (ret != inputNode) {
                    return ret;
                }
            }

            if (sessionCtx->Config().HasDefaultCluster()) {
                auto defaultCluster = sessionCtx->Config()._DefaultCluster.Get().GetRef();
                ret = KiBuildResult(node, defaultCluster, ctx);
                if (ret != inputNode) {
                    return ret;
                }
            }

            return ret;
        }, ctx, TOptimizeExprSettings(nullptr));

        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }

        status = OptimizeExpr(input, output, [sessionCtx](const TExprNode::TPtr& inputNode, TExprContext& ctx) {
            Y_UNUSED(ctx);

            auto ret = inputNode;
            TExprBase node(inputNode);

            ret = KiTrimReadTableWorld(node);
            if (ret != inputNode) {
                return ret;
            }

            ret = KiRewriteSelectIndexRange(node, sessionCtx->Tables(), ctx);
            if (ret != inputNode) {
                return ret;
            }

            return ret;
        }, ctx, TOptimizeExprSettings(nullptr));

        return status;
    });
}

bool IsKqlPureExpr(NNodes::TExprBase expr) {
    auto node = FindNode(expr.Ptr(), [] (const TExprNode::TPtr& node) {
        if (!node->IsCallable()) {
            return false;
        }

        if (!KikimrKqlFunctions().contains(node->Content())) {
            return false;
        }

        return true;
    });

    return !node;
}

bool IsKqlPureLambda(TCoLambda lambda) {
    return IsKqlPureExpr(lambda.Body());
}

} // namespace NYql
