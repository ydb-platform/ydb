#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/dq/opt/dq_opt_phy.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

bool CanPushTopSort(const TCoTopSort& node, const TKikimrTableDescription& tableDesc, TVector<TString>* columns) {
    return IsKeySelectorPkPrefix(node.KeySelectorLambda(), tableDesc, columns);
}

template<typename TRead>
bool CheckIndexCovering(const TRead& read, const TIntrusivePtr<TKikimrTableMetadata>& indexMeta) {
    for (const auto& col : read.Columns()) {
        if (!indexMeta->Columns.contains(col.StringValue())) {
            return true;
        }
    }
    return false;
}

TExprBase DoRewriteIndexRead(const TKqlReadTableIndex& read, TExprContext& ctx,
    const TKikimrTableDescription& tableDesc, TIntrusivePtr<TKikimrTableMetadata> indexMeta,
    const TVector<TString>& extraColumns, const std::function<TExprBase(const TExprBase&)>& middleFilter = {})
{
    const bool needDataRead = CheckIndexCovering(read, indexMeta);

    if (read.Range().From().ArgCount() == 0 && read.Range().To().ArgCount() == 0) {
        TString indexName = read.Index().StringValue();
        auto issue = TIssue(ctx.GetPosition(read.Pos()), "Given predicate is not suitable for used index: " + indexName);
        SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_KIKIMR_WRONG_INDEX_USAGE, issue);
        ctx.AddWarning(issue);
    }

    if (!needDataRead) {
        // We can read all data from index table.
        auto ret = Build<TKqlReadTable>(ctx, read.Pos())
            .Table(BuildTableMeta(*indexMeta, read.Pos(), ctx))
            .Range(read.Range())
            .Columns(read.Columns())
            .Settings(read.Settings())
            .Done();

        if (middleFilter) {
            return middleFilter(ret);
        }
        return ret;
    }

    auto keyColumnsList = BuildKeyColumnsList(tableDesc, read.Pos(), ctx);
    auto columns = MergeColumns(keyColumnsList, extraColumns, ctx);

    TExprBase readIndexTable = Build<TKqlReadTable>(ctx, read.Pos())
        .Table(BuildTableMeta(*indexMeta, read.Pos(), ctx))
        .Range(read.Range())
        .Columns(columns)
        .Settings(read.Settings())
        .Done();

    if (middleFilter) {
        readIndexTable = middleFilter(readIndexTable);
    }

    if (extraColumns) {
        TCoArgument arg = Build<TCoArgument>(ctx, read.Pos())
            .Name("Arg")
            .Done();

        TVector<TExprBase> structMembers;
        structMembers.reserve(keyColumnsList.Size());

        for (const auto& c : keyColumnsList) {
            auto member = Build<TCoNameValueTuple>(ctx, read.Pos())
                .Name().Build(c.Value())
                .Value<TCoMember>()
                    .Struct(arg)
                    .Name().Build(c.Value())
                    .Build()
                .Done();

            structMembers.push_back(member);
        }

        readIndexTable= Build<TCoMap>(ctx, read.Pos())
            .Input(readIndexTable)
            .Lambda()
                .Args({arg})
                .Body<TCoAsStruct>()
                    .Add(structMembers)
                    .Build()
                .Build()
            .Done();
    }

    return Build<TKqlLookupTable>(ctx, read.Pos())
        .Table(read.Table())
        .LookupKeys(readIndexTable.Ptr())
        .Columns(read.Columns())
        .Done();
}

} // namespace

TExprBase KqpRewriteIndexRead(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!kqpCtx.IsDataQuery()) {
        return node;
    }

    if (auto maybeIndexRead = node.Maybe<TKqlReadTableIndex>()) {
        auto indexRead = maybeIndexRead.Cast();

        const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, indexRead.Table().Path());
        const auto& [indexMeta, _ ] = tableDesc.Metadata->GetIndexMetadata(TString(indexRead.Index().Value()));

        return DoRewriteIndexRead(indexRead, ctx, tableDesc, indexMeta, {});
    }

    return node;
}

TExprBase KqpRewriteLookupIndex(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!kqpCtx.IsDataQuery()) {
        return node;
    }

    if (auto maybeLookupIndex = node.Maybe<TKqlLookupIndex>()) {
        auto lookupIndex = maybeLookupIndex.Cast();

        const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, lookupIndex.Table().Path());
        const auto& [indexMeta, _ ] = tableDesc.Metadata->GetIndexMetadata(lookupIndex.Index().StringValue());

        const bool needDataRead = CheckIndexCovering(lookupIndex, indexMeta);

        if (!needDataRead) {
            return Build<TKqlLookupTable>(ctx, node.Pos())
                .Table(BuildTableMeta(*indexMeta, node.Pos(), ctx))
                .LookupKeys(lookupIndex.LookupKeys())
                .Columns(lookupIndex.Columns())
                .Done();
        }

        auto keyColumnsList = BuildKeyColumnsList(tableDesc, node.Pos(), ctx);

        TExprBase lookupIndexTable = Build<TKqlLookupTable>(ctx, node.Pos())
            .Table(BuildTableMeta(*indexMeta, node.Pos(), ctx))
            .LookupKeys(lookupIndex.LookupKeys())
            .Columns(keyColumnsList)
            .Done();

        return Build<TKqlLookupTable>(ctx, node.Pos())
            .Table(lookupIndex.Table())
            .LookupKeys(lookupIndexTable.Ptr())
            .Columns(lookupIndex.Columns())
            .Done();
    }

    return node;
}

// The index and main table have same number of rows, so we can push a copy of TCoTopSort or TCoTake
// through TKqlLookupTable.
// The simplest way is to match TopSort or Take over TKqlReadTableIndex.
TExprBase KqpRewriteTopSortOverIndexRead(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!kqpCtx.IsDataQuery()) {
        return node;
    }

    if (!node.Maybe<TCoTopSort>()) {
        return node;
    }

    auto topSort = node.Maybe<TCoTopSort>().Cast();

    if (auto maybeReadTableIndex = topSort.Input().Maybe<TKqlReadTableIndex>()) {
        auto readTableIndex = maybeReadTableIndex.Cast();

        const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, readTableIndex.Table().Path());
        const auto& [indexMeta, _ ] = tableDesc.Metadata->GetIndexMetadata(TString(readTableIndex.Index().Value()));
        const auto& indexDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, indexMeta->Name);

        TVector<TString> sortByColumns;

        if (!CanPushTopSort(topSort, indexDesc, &sortByColumns)) {
            return node;
        }

        auto filter = [&ctx, &node, &topSort](const TExprBase& in) mutable {
            auto newTopSort = Build<TCoTopSort>(ctx, node.Pos())
                .Input(in)
                .KeySelectorLambda(ctx.DeepCopyLambda(topSort.KeySelectorLambda().Ref()))
                .SortDirections(topSort.SortDirections())
                .Count(topSort.Count())
                .Done();
            return TExprBase(newTopSort);
        };

        auto lookup = DoRewriteIndexRead(readTableIndex, ctx, tableDesc, indexMeta, sortByColumns, filter);

        return Build<TCoTopSort>(ctx, node.Pos())
            .Input(lookup)
            .KeySelectorLambda(ctx.DeepCopyLambda(topSort.KeySelectorLambda().Ref()))
            .SortDirections(topSort.SortDirections())
            .Count(topSort.Count())
            .Done();
    }

    return node;
}

TExprBase KqpRewriteTakeOverIndexRead(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!kqpCtx.IsDataQuery()) {
        return node;
    }

    if (!node.Maybe<TCoTake>()) {
        return node;
    }

    auto take = node.Maybe<TCoTake>().Cast();

    if (auto maybeReadTableIndex = take.Input().Maybe<TKqlReadTableIndex>()) {
        auto readTableIndex = maybeReadTableIndex.Cast();

        const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, readTableIndex.Table().Path());
        const auto& [indexMeta, _ ] = tableDesc.Metadata->GetIndexMetadata(TString(readTableIndex.Index().Value()));

        auto filter = [&ctx, &node](const TExprBase& in) mutable {
            // Change input for TCoTake. New input is result of TKqlReadTable.
            return TExprBase(ctx.ChangeChild(*node.Ptr(), 0, in.Ptr()));
        };

        return DoRewriteIndexRead(readTableIndex, ctx, tableDesc, indexMeta, {}, filter);
    }

    return node;
}

} // namespace NKikimr::NKqp::NOpt
