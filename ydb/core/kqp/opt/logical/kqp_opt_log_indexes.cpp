#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/dq/opt/dq_opt_phy.h>
#include <ydb/library/yql/core/yql_opt_utils.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

TCoAtomList BuildKeyColumnsList(const TKikimrTableDescription& table, TPositionHandle pos, TExprContext& ctx) {
    TVector<TExprBase> columnsToSelect;
    columnsToSelect.reserve(table.Metadata->KeyColumnNames.size());
    for (auto key : table.Metadata->KeyColumnNames) {
        auto value = table.Metadata->Columns.at(key);
        auto atom = Build<TCoAtom>(ctx, pos)
            .Value(value.Name)
            .Done();

        columnsToSelect.push_back(atom);
    }

    return Build<TCoAtomList>(ctx, pos)
        .Add(columnsToSelect)
        .Done();
}

TCoAtomList MergeColumns(const NNodes::TCoAtomList& col1, const TVector<TString>& col2, TExprContext& ctx) {
    TVector<TCoAtom> columns;
    THashSet<TString> uniqColumns;
    columns.reserve(col1.Size() + col2.size());

    for (const auto& c : col1) {
        YQL_ENSURE(uniqColumns.emplace(c.StringValue()).second);
        columns.push_back(c);
    }

    for (const auto& c : col2) {
        if (uniqColumns.emplace(c).second) {
            auto atom = Build<TCoAtom>(ctx, col1.Pos())
                .Value(c)
                .Done();
            columns.push_back(atom);
        }
    }

    return Build<TCoAtomList>(ctx, col1.Pos())
        .Add(columns)
        .Done();
}

bool IsKeySelectorPkPrefix(NNodes::TCoLambda keySelector, const TKikimrTableDescription& tableDesc, TVector<TString>* columns) {
    auto checkKey = [keySelector, &tableDesc, columns] (const TExprBase& key, ui32 index) {
        if (!key.Maybe<TCoMember>()) {
            return false;
        }

        auto member = key.Cast<TCoMember>();
        if (member.Struct().Raw() != keySelector.Args().Arg(0).Raw()) {
            return false;
        }

        auto column = member.Name().StringValue();
        auto columnIndex = tableDesc.GetKeyColumnIndex(column);
        if (!columnIndex || *columnIndex != index) {
            return false;
        }

        if (columns) {
            columns->emplace_back(std::move(column));
        }

        return true;
    };

    auto lambdaBody = keySelector.Body();
    if (auto maybeTuple = lambdaBody.Maybe<TExprList>()) {
        auto tuple = maybeTuple.Cast();
        for (size_t i = 0; i < tuple.Size(); ++i) {
            if (!checkKey(tuple.Item(i), i)) {
                return false;
            }
        }
    } else {
        if (!checkKey(lambdaBody, 0)) {
            return false;
        }
    }

    return true;
}

bool CanPushTopSort(const TCoTopBase& node, const TKikimrTableDescription& tableDesc, TVector<TString>* columns) {
    return IsKeySelectorPkPrefix(node.KeySelectorLambda(), tableDesc, columns);
}

struct TReadMatch {
    TMaybeNode<TKqlReadTableIndex> Read;
    TMaybeNode<TKqlReadTableIndexRanges> ReadRanges;

    static TReadMatch Match(TExprBase expr) {
        if (auto read = expr.Maybe<TKqlReadTableIndex>()) {
            return {read.Cast(), {}};
        }
        if (auto read = expr.Maybe<TKqlReadTableIndexRanges>()) {
            return {{}, read.Cast()};
        }
        return {};
    }

    operator bool () const {
        return Read.IsValid() || ReadRanges.IsValid();
    }

    TKqpTable Table() const {
        if (Read) {
            return Read.Cast().Table();
        }
        if (ReadRanges) {
            return ReadRanges.Cast().Table();
        }
        YQL_ENSURE(false, "Invalid ReadTableIndex match");
    }

    TCoAtom Index() const {
        if (Read) {
            return Read.Cast().Index();
        }
        if (ReadRanges) {
            return ReadRanges.Cast().Index();
        }
        YQL_ENSURE(false, "Invalid ReadTableIndex match");
    }

    TCoAtomList Columns() const {
        if (Read) {
            return Read.Cast().Columns();
        }
        if (ReadRanges) {
            return ReadRanges.Cast().Columns();
        }
        YQL_ENSURE(false, "Invalid ReadTableIndex match");
    }

    NYql::TPositionHandle Pos() const {
        if (Read) {
            return Read.Cast().Pos();
        }
        if (ReadRanges) {
            return ReadRanges.Cast().Pos();
        }
        YQL_ENSURE(false, "Invalid ReadTableIndex match");
    }

    TCoNameValueTupleList Settings() const {
        if (Read) {
            return Read.Cast().Settings();
        }
        if (ReadRanges) {
            return ReadRanges.Cast().Settings();
        }
        YQL_ENSURE(false, "Invalid ReadTableIndex match");
    }

    bool FullScan() const {
        if (Read) {
            return Read.Cast().Range().From().ArgCount() == 0 && Read.Cast().Range().To().ArgCount() == 0;
        }
        if (ReadRanges) {
            return TCoVoid::Match(ReadRanges.Cast().Ranges().Raw());
        }
        return false;
    }

    TExprBase BuildRead(TExprContext& ctx, TKqpTable tableMeta, TCoAtomList columns) const {
        if (Read) {
            return Build<TKqlReadTable>(ctx, Pos())
                .Table(tableMeta)
                .Range(Read.Range().Cast())
                .Columns(columns)
                .Settings(Settings())
                .Done();
        }

        if (ReadRanges) {
            return Build<TKqlReadTableRanges>(ctx, Pos())
                .Table(tableMeta)
                .Ranges(ReadRanges.Ranges().Cast())
                .Columns(columns)
                .Settings(Settings())
                .ExplainPrompt(ReadRanges.ExplainPrompt().Cast())
                .Done();
        }

        YQL_ENSURE(false);
    }
};

template<typename TRead>
bool CheckIndexCovering(const TRead& read, const TIntrusivePtr<TKikimrTableMetadata>& indexMeta) {
    for (const auto& col : read.Columns()) {
        if (!indexMeta->Columns.contains(col.StringValue())) {
            return true;
        }
    }
    return false;
}

TExprBase DoRewriteIndexRead(const TReadMatch& read, TExprContext& ctx,
    const TKikimrTableDescription& tableDesc, TIntrusivePtr<TKikimrTableMetadata> indexMeta, bool useStreamLookup,
    const TVector<TString>& extraColumns, const std::function<TExprBase(const TExprBase&)>& middleFilter = {})
{
    const bool needDataRead = CheckIndexCovering(read, indexMeta);

    if (read.FullScan()) {
        TString indexName = read.Index().StringValue();
        auto issue = TIssue(ctx.GetPosition(read.Pos()), "Given predicate is not suitable for used index: " + indexName);
        SetIssueCode(EYqlIssueCode::TIssuesIds_EIssueCode_KIKIMR_WRONG_INDEX_USAGE, issue);
        ctx.AddWarning(issue);
    }

    if (!needDataRead) {
        // We can read all data from index table.
        auto ret = read.BuildRead(ctx, BuildTableMeta(*indexMeta, read.Pos(), ctx), read.Columns());

        if (middleFilter) {
            return middleFilter(ret);
        }
        return ret;
    }

    auto keyColumnsList = BuildKeyColumnsList(tableDesc, read.Pos(), ctx);
    auto columns = MergeColumns(keyColumnsList, extraColumns, ctx);

    TExprBase readIndexTable = read.BuildRead(ctx, BuildTableMeta(*indexMeta, read.Pos(), ctx), columns);

    if (middleFilter) {
        readIndexTable = middleFilter(readIndexTable);
    }

    if (extraColumns) {
        TCoArgument arg = Build<TCoArgument>(ctx, read.Pos())
            .Name("Arg")
            .Done();

        TVector<TExprBase> structMembers;
        structMembers.reserve(keyColumnsList.Size());

        for (const auto& keyColumn : keyColumnsList) {
            auto member = Build<TCoNameValueTuple>(ctx, read.Pos())
                .Name().Build(keyColumn.Value())
                .Value<TCoMember>()
                    .Struct(arg)
                    .Name().Build(keyColumn.Value())
                    .Build()
                .Done();

            structMembers.push_back(member);
        }

        readIndexTable = Build<TCoMap>(ctx, read.Pos())
            .Input(readIndexTable)
            .Lambda()
                .Args({arg})
                .Body<TCoAsStruct>()
                    .Add(structMembers)
                    .Build()
                .Build()
            .Done();
    }

    if (useStreamLookup) {
        return Build<TKqlStreamLookupTable>(ctx, read.Pos())
            .Table(read.Table())
            .LookupKeys(readIndexTable.Ptr())
            .Columns(read.Columns())
            .Done();
    } else {
        return Build<TKqlLookupTable>(ctx, read.Pos())
            .Table(read.Table())
            .LookupKeys(readIndexTable.Ptr())
            .Columns(read.Columns())
            .Done();
    }
}

} // namespace

TExprBase KqpRewriteIndexRead(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (auto indexRead = TReadMatch::Match(node)) {

        const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, indexRead.Table().Path());
        const auto& [indexMeta, _ ] = tableDesc.Metadata->GetIndexMetadata(TString(indexRead.Index().Value()));

        return DoRewriteIndexRead(indexRead, ctx, tableDesc, indexMeta, kqpCtx.IsScanQuery(), {});
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

TExprBase KqpRewriteStreamLookupIndex(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!kqpCtx.IsScanQuery()) {
        return node;
    }

    if (auto maybeStreamLookupIndex = node.Maybe<TKqlStreamLookupIndex>()) {
        auto streamLookupIndex = maybeStreamLookupIndex.Cast();

        const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, streamLookupIndex.Table().Path());
        const auto& [indexMeta, _] = tableDesc.Metadata->GetIndexMetadata(streamLookupIndex.Index().StringValue());

        const bool needDataRead = CheckIndexCovering(streamLookupIndex, indexMeta);
        if (!needDataRead) {
            return Build<TKqlStreamLookupTable>(ctx, node.Pos())
                .Table(BuildTableMeta(*indexMeta, node.Pos(), ctx))
                .LookupKeys(streamLookupIndex.LookupKeys())
                .Columns(streamLookupIndex.Columns())
                .Done();
        }

        auto keyColumnsList = BuildKeyColumnsList(tableDesc, streamLookupIndex.Pos(), ctx);

        TExprBase lookupIndexTable = Build<TKqlStreamLookupTable>(ctx, node.Pos())
            .Table(BuildTableMeta(*indexMeta, node.Pos(), ctx))
            .LookupKeys(streamLookupIndex.LookupKeys())
            .Columns(keyColumnsList)
            .Done();

        return Build<TKqlStreamLookupTable>(ctx, node.Pos())
            .Table(streamLookupIndex.Table())
            .LookupKeys(lookupIndexTable.Ptr())
            .Columns(streamLookupIndex.Columns())
            .Done();
    }

    return node;
}

/// Can push flat map node to read from table using only columns available in table description
bool CanPushFlatMap(const TCoFlatMapBase& flatMap, const TKikimrTableDescription& tableDesc, const TParentsMap& parentsMap, TVector<TString> & extraColumns) {
    auto flatMapLambda = flatMap.Lambda();
    if (!IsFilterFlatMap(flatMapLambda)) {
        return false;
    }

    const auto & flatMapLambdaArgument = flatMapLambda.Args().Arg(0).Ref();
    auto flatMapLambdaConditional = flatMapLambda.Body().Cast<TCoConditionalValueBase>();

    TSet<TString> lambdaSubset;
    if (!HaveFieldsSubset(flatMapLambdaConditional.Predicate().Ptr(), flatMapLambdaArgument, lambdaSubset, parentsMap)) {
        return false;
    }

    for (auto & lambdaColumn : lambdaSubset) {
        auto columnIndex = tableDesc.GetKeyColumnIndex(lambdaColumn);
        if (!columnIndex) {
            return false;
        }
    }

    extraColumns.insert(extraColumns.end(), lambdaSubset.begin(), lambdaSubset.end());
    return true;
}

// The index and main table have same number of rows, so we can push a copy of TCoTopSort or TCoTake
// through TKqlLookupTable.
// The simplest way is to match TopSort or Take over TKqlReadTableIndex.
// Additionally if there is TopSort or Take over filter, and filter depends only on columns available in index,
// we also push copy of filter through TKqlLookupTable.
TExprBase KqpRewriteTopSortOverIndexRead(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
                                        const TParentsMap& parentsMap) {
    if (!node.Maybe<TCoTopBase>()) {
        return node;
    }

    const auto topBase = node.Maybe<TCoTopBase>().Cast();

    auto maybeFlatMap = topBase.Input().Maybe<TCoFlatMap>();
    TExprBase input = maybeFlatMap ? maybeFlatMap.Cast().Input() : topBase.Input();

    auto readTableIndex = TReadMatch::Match(input);
    if (!readTableIndex)
        return node;

    const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, readTableIndex.Table().Path());
    const auto& [indexMeta, _ ] = tableDesc.Metadata->GetIndexMetadata(TString(readTableIndex.Index().Value()));
    const auto& indexDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, indexMeta->Name);

    TVector<TString> extraColumns;

    if (maybeFlatMap && !CanPushFlatMap(maybeFlatMap.Cast(), indexDesc, parentsMap, extraColumns))
        return node;

    if (!CanPushTopSort(topBase, indexDesc, &extraColumns)) {
        return node;
    }

    auto filter = [&](const TExprBase& in) mutable {
        auto sortInput = in;

        if (maybeFlatMap)
        {
            sortInput = Build<TCoFlatMap>(ctx, node.Pos())
                .Input(in)
                .Lambda(ctx.DeepCopyLambda(maybeFlatMap.Lambda().Ref()))
                .Done();
        }

        auto newTop = Build<TCoTopBase>(ctx, node.Pos())
            .CallableName(node.Ref().Content())
            .Input(sortInput)
            .KeySelectorLambda(ctx.DeepCopyLambda(topBase.KeySelectorLambda().Ref()))
            .SortDirections(topBase.SortDirections())
            .Count(topBase.Count())
            .Done();

        return TExprBase(newTop);
    };

    auto lookup = DoRewriteIndexRead(readTableIndex, ctx, tableDesc, indexMeta,
        kqpCtx.IsScanQuery(), extraColumns, filter);

    return Build<TCoTopBase>(ctx, node.Pos())
        .CallableName(node.Ref().Content())
        .Input(lookup)
        .KeySelectorLambda(ctx.DeepCopyLambda(topBase.KeySelectorLambda().Ref()))
        .SortDirections(topBase.SortDirections())
        .Count(topBase.Count())
        .Done();
}

TExprBase KqpRewriteTakeOverIndexRead(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx,
                                    const TParentsMap& parentsMap) {
    if (!node.Maybe<TCoTake>()) {
        return node;
    }

    auto take = node.Maybe<TCoTake>().Cast();

    auto maybeFlatMap = take.Input().Maybe<TCoFlatMap>();
    TExprBase input = maybeFlatMap ? maybeFlatMap.Cast().Input() : take.Input();

    auto readTableIndex = TReadMatch::Match(input);
    if (!readTableIndex)
        return node;

    const auto& tableDesc = GetTableData(*kqpCtx.Tables, kqpCtx.Cluster, readTableIndex.Table().Path());
    const auto& [indexMeta, _ ] = tableDesc.Metadata->GetIndexMetadata(TString(readTableIndex.Index().Value()));
    const auto& indexDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, indexMeta->Name);

    TVector<TString> extraColumns;
    if (maybeFlatMap && !CanPushFlatMap(maybeFlatMap.Cast(), indexDesc, parentsMap, extraColumns))
        return node;

    auto filter = [&](const TExprBase& in) mutable {
        auto takeChild = in;

        if (maybeFlatMap)
        {
            takeChild = Build<TCoFlatMap>(ctx, node.Pos())
                .Input(in)
                .Lambda(ctx.DeepCopyLambda(maybeFlatMap.Lambda().Ref()))
                .Done();
        }

        // Change input for TCoTake. New input is result of TKqlReadTable.
        return TExprBase(ctx.ChangeChild(*node.Ptr(), 0, takeChild.Ptr()));
    };

    return DoRewriteIndexRead(readTableIndex, ctx, tableDesc, indexMeta, kqpCtx.IsScanQuery(), extraColumns, filter);
}

} // namespace NKikimr::NKqp::NOpt
