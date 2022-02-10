#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h> 
#include <ydb/core/kqp/opt/kqp_opt_impl.h> 
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h> 

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/library/yql/providers/common/provider/yql_table_lookup.h>

namespace NKikimr::NKqp::NOpt {

namespace {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

TExprBase BuildEquiRangeLookup(const TKeyRange& keyRange, const TKikimrTableDescription& tableDesc,
    TPositionHandle pos, TExprContext& ctx)
{
    YQL_ENSURE(keyRange.IsEquiRange());

    TVector<TExprBase> structMembers;
    TVector<TCoAtom> skipNullColumns;
    for (ui32 i = 0; i < keyRange.GetNumDefined(); ++i) {
        const auto& columnName = tableDesc.Metadata->KeyColumnNames[i];
        TCoAtom columnNameAtom(ctx.NewAtom(pos, columnName));
        auto value = keyRange.GetFromTuple().GetValue(i).Cast();

        if (TCoNull::Match(value.Raw())) {
            value = Build<TCoNothing>(ctx, pos)
                .OptionalType(NCommon::BuildTypeExpr(pos, *tableDesc.GetColumnType(columnName), ctx))
                .Done();
        } else {
            skipNullColumns.push_back(columnNameAtom);
        }

        auto member = Build<TExprList>(ctx, pos)
            .Add(columnNameAtom)
            .Add(value)
            .Done();

        structMembers.push_back(member);
    }

    auto keysToLookup = Build<TCoAsList>(ctx, pos)
        .Add<TCoAsStruct>()
            .Add(structMembers)
            .Build()
        .Done();

    // Actually residual predicate for the key range already has a check for NULL keys,
    // but it's better to skip redundant lookup. Consider removing check from residual
    // predicate in this case.
    return Build<TCoSkipNullMembers>(ctx, pos)
        .Input(keysToLookup)
        .Members()
            .Add(skipNullColumns)
            .Build()
        .Done();
}

TKqlKeyRange BuildKeyRangeExpr(const TKeyRange& keyRange, const TKikimrTableDescription& tableDesc,
    TPositionHandle pos, TExprContext& ctx)
{
    bool fromInclusive = true;
    bool toInclusive = true;
    TVector<TExprBase> fromValues;
    TVector<TExprBase> toValues;

    for (size_t i = 0; i < keyRange.GetColumnRangesCount(); ++i) {
        const auto& columnName = tableDesc.Metadata->KeyColumnNames[i];
        const auto& range = keyRange.GetColumnRange(i);

        if (range.GetFrom().IsDefined()) {
            fromInclusive = range.GetFrom().IsInclusive();
            if (TCoNull::Match(range.GetFrom().GetValue().Raw())) {
                fromValues.emplace_back(
                    Build<TCoNothing>(ctx, pos)
                        .OptionalType(NCommon::BuildTypeExpr(pos, *tableDesc.GetColumnType(columnName), ctx))
                        .Done());
            } else {
                fromValues.emplace_back(range.GetFrom().GetValue());
            }
        }

        if (range.GetTo().IsDefined()) {
            toInclusive = range.GetTo().IsInclusive();
            if (TCoNull::Match(range.GetTo().GetValue().Raw())) {
                toValues.emplace_back(
                    Build<TCoNothing>(ctx, pos)
                        .OptionalType(NCommon::BuildTypeExpr(pos, *tableDesc.GetColumnType(columnName), ctx))
                        .Done());
            } else {
                toValues.emplace_back(range.GetTo().GetValue());
            }
        }
    }

    auto fromExpr = fromInclusive
        ? Build<TKqlKeyInc>(ctx, pos).Add(fromValues).Done().Cast<TKqlKeyTuple>()
        : Build<TKqlKeyExc>(ctx, pos).Add(fromValues).Done().Cast<TKqlKeyTuple>();

    auto toExpr = toInclusive
        ? Build<TKqlKeyInc>(ctx, pos).Add(toValues).Done().Cast<TKqlKeyTuple>()
        : Build<TKqlKeyExc>(ctx, pos).Add(toValues).Done().Cast<TKqlKeyTuple>();

    return Build<TKqlKeyRange>(ctx, pos)
        .From(fromExpr)
        .To(toExpr)
        .Done();
}

} // namespace

TExprBase KqpPushPredicateToReadTable(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TCoFlatMap>()) {
        return node;
    }
    auto flatmap = node.Cast<TCoFlatMap>();

    if (!IsPredicateFlatMap(flatmap.Lambda().Body().Ref())) {
        return node;
    }

    TMaybeNode<TKqlReadTableBase> readTable;
    TMaybeNode<TCoFilterNullMembers> filterNull;
    TMaybeNode<TCoSkipNullMembers> skipNull;

    TMaybeNode<TCoAtom> indexName;

    if (auto maybeRead = flatmap.Input().Maybe<TKqlReadTable>()) {
        readTable = maybeRead.Cast();
    }

    if (auto maybeRead = flatmap.Input().Maybe<TKqlReadTableIndex>()) {
        readTable = maybeRead.Cast();
        indexName = maybeRead.Cast().Index();
    }

    if (auto maybeRead = flatmap.Input().Maybe<TCoFilterNullMembers>().Input().Maybe<TKqlReadTable>()) {
        readTable = maybeRead.Cast();
        filterNull = flatmap.Input().Cast<TCoFilterNullMembers>();
    }

    if (auto maybeRead = flatmap.Input().Maybe<TCoFilterNullMembers>().Input().Maybe<TKqlReadTableIndex>()) {
        readTable = maybeRead.Cast();
        filterNull = flatmap.Input().Cast<TCoFilterNullMembers>();
        indexName = maybeRead.Cast().Index();
    }

    if (auto maybeRead = flatmap.Input().Maybe<TCoSkipNullMembers>().Input().Maybe<TKqlReadTable>()) {
        readTable = maybeRead.Cast();
        skipNull = flatmap.Input().Cast<TCoSkipNullMembers>();
    }

    if (auto maybeRead = flatmap.Input().Maybe<TCoSkipNullMembers>().Input().Maybe<TKqlReadTableIndex>()) {
        readTable = maybeRead.Cast();
        skipNull = flatmap.Input().Cast<TCoSkipNullMembers>();
        indexName = maybeRead.Cast().Index();
    }

    if (!readTable) {
        return node;
    }

    auto read = readTable.Cast();

    if (read.Range().From().ArgCount() > 0 || read.Range().To().ArgCount() > 0) {
        return node;
    }

    auto& mainTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, read.Table().Path());

    auto& tableDesc = indexName ? kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, mainTableDesc.Metadata->GetIndexMetadata(TString(indexName.Cast())).first->Name) : mainTableDesc;

    YQL_ENSURE(tableDesc.Metadata->Kind != EKikimrTableKind::Olap);

    auto row = flatmap.Lambda().Args().Arg(0);
    auto predicate = TExprBase(flatmap.Lambda().Body().Ref().ChildPtr(0));
    TTableLookup lookup = ExtractTableLookup(row, predicate, tableDesc.Metadata->KeyColumnNames,
        &KiTableLookupGetValue, &KiTableLookupCanCompare, &KiTableLookupCompare, ctx,
        kqpCtx.Config->HasAllowNullCompareInIndex());

    if (lookup.IsFullScan()) {
        return node;
    }

    auto readSettings = TKqpReadTableSettings::Parse(read);

    TVector<TExprBase> fetches;
    fetches.reserve(lookup.GetKeyRanges().size());

    for (auto& keyRange : lookup.GetKeyRanges()) {
        bool useLookup = false;
        if (keyRange.IsEquiRange()) {
            bool isFullKey = keyRange.GetNumDefined() == tableDesc.Metadata->KeyColumnNames.size();

            // NOTE: Use more efficient full key lookup implementation in datashard.
            // Consider using lookup for partial keys as well once better constant folding
            // is available, currently it can introduce redundant compute stage.
            useLookup = kqpCtx.IsDataQuery() && isFullKey;
        }

        TMaybeNode<TExprBase> readInput;
        if (useLookup) {
            auto lookupKeys = BuildEquiRangeLookup(keyRange, tableDesc, read.Pos(), ctx);

            if (indexName) {
                readInput = Build<TKqlLookupIndex>(ctx, read.Pos())
                    .Table(read.Table())
                    .LookupKeys(lookupKeys)
                    .Columns(read.Columns())
                    .Index(indexName.Cast())
                    .Done();
            } else {
                readInput = Build<TKqlLookupTable>(ctx, read.Pos())
                    .Table(read.Table())
                    .LookupKeys(lookupKeys)
                    .Columns(read.Columns())
                    .Done();
            }
        } else {
            auto keyRangeExpr = BuildKeyRangeExpr(keyRange, tableDesc, node.Pos(), ctx);

            TKqpReadTableSettings settings = readSettings;
            for (size_t i = 0; i < keyRange.GetColumnRangesCount(); ++i) {
                const auto& column = tableDesc.Metadata->KeyColumnNames[i];
                auto& range = keyRange.GetColumnRange(i);
                if (range.IsDefined() && !range.IsNull()) {
                    settings.AddSkipNullKey(column);
                }
            }

            if (indexName) {
                readInput = Build<TKqlReadTableIndex>(ctx, read.Pos())
                    .Table(read.Table())
                    .Range(keyRangeExpr)
                    .Columns(read.Columns())
                    .Index(indexName.Cast())
                    .Settings(settings.BuildNode(ctx, read.Pos()))
                    .Done();
            } else {
                readInput = Build<TKqlReadTable>(ctx, read.Pos())
                    .Table(read.Table())
                    .Range(keyRangeExpr)
                    .Columns(read.Columns())
                    .Settings(settings.BuildNode(ctx, read.Pos()))
                    .Done();
            }
        }

        auto input = readInput.Cast();

        auto residualPredicate = keyRange.GetResidualPredicate()
            ? keyRange.GetResidualPredicate().Cast().Ptr()
            : MakeBool<true>(node.Pos(), ctx);

        auto newBody = ctx.ChangeChild(flatmap.Lambda().Body().Ref(), 0, std::move(residualPredicate));

        if (filterNull) {
            input = Build<TCoFilterNullMembers>(ctx, node.Pos())
                .Input(input)
                .Members(filterNull.Cast().Members())
                .Done();
        }

        if (skipNull) {
            input = Build<TCoSkipNullMembers>(ctx, node.Pos())
                .Input(input)
                .Members(skipNull.Cast().Members())
                .Done();
        }

        auto fetch = Build<TCoFlatMap>(ctx, node.Pos())
            .Input(input)
            .Lambda()
                .Args({"item"})
                .Body<TExprApplier>()
                    .Apply(TExprBase(newBody))
                    .With(flatmap.Lambda().Args().Arg(0), "item")
                    .Build()
                .Build()
            .Done();

        fetches.push_back(fetch);
    }

    return Build<TCoExtend>(ctx, node.Pos())
        .Add(fetches)
        .Done();
}

} // namespace NKikimr::NKqp::NOpt

