#include "kqp_opt_log_impl.h"
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

} // namespace

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

bool IsPointPrefix(const TKeyRange& range) {
    size_t prefixLen = 0;
    for (size_t i = 0; i < range.GetColumnRangesCount(); ++i) {
        if (range.GetColumnRange(i).IsPoint() && i == prefixLen) {
            prefixLen += 1;
        }
        if (range.GetColumnRange(i).IsDefined() && i >= prefixLen) {
            return false;
        }
    }
    return prefixLen > 0;
}

TExprBase KqpPushPredicateToReadTable(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TCoFlatMap>()) {
        return node;
    }
    auto flatmap = node.Cast<TCoFlatMap>();

    if (!IsPredicateFlatMap(flatmap.Lambda().Body().Ref())) {
        return node;
    }

    bool onlyPointRanges = false;
    auto readMatch = MatchRead<TKqlReadTableBase>(flatmap.Input());
    TMaybeNode<TCoAtom> indexName;

    //TODO: remove this branch KIKIMR-15255, KIKIMR-15321
    if (!readMatch && (kqpCtx.IsDataQuery() || kqpCtx.IsGenericQuery())) {
        if (auto readRangesMatch = MatchRead<TKqlReadTableRangesBase>(flatmap.Input())) {
            auto read = readRangesMatch->Read.Cast<TKqlReadTableRangesBase>();
            if (TCoVoid::Match(read.Ranges().Raw())) {
                auto key = Build<TKqlKeyInc>(ctx, read.Pos()).Done();
                readMatch = readRangesMatch;
                readMatch->Read =
                    Build<TKqlReadTable>(ctx, read.Pos())
                        .Settings(read.Settings())
                        .Table(read.Table())
                        .Columns(read.Columns())
                        .Range<TKqlKeyRange>()
                            .From(key)
                            .To(key)
                            .Build()
                        .Done();
                onlyPointRanges = true;
                if (auto indexRead = read.Maybe<TKqlReadTableIndexRanges>()) {
                    indexName = indexRead.Index();
                }
            } else {
                return node;
            }
        } else {
            return node;
        }
    }

    if (!readMatch) {
        return node;
    }

    if (readMatch->FlatMap) {
        return node;
    }

    auto read = readMatch->Read.Cast<TKqlReadTableBase>();

    static const std::set<TStringBuf> supportedReads {
        TKqlReadTable::CallableName(),
        TKqlReadTableIndex::CallableName(),
    };

    if (!supportedReads.contains(read.CallableName())) {
        return node;
    }

    if (auto maybeIndexRead = read.Maybe<TKqlReadTableIndex>()) {
        indexName = maybeIndexRead.Cast().Index();
    }

    if (read.Range().From().ArgCount() > 0 || read.Range().To().ArgCount() > 0) {
        return node;
    }

    auto& mainTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, read.Table().Path());

    auto& tableDesc = indexName ? kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, mainTableDesc.Metadata->GetIndexMetadata(TString(indexName.Cast())).first->Name) : mainTableDesc;

    if (tableDesc.Metadata->Kind == EKikimrTableKind::Olap) {
        return node;
    }

    auto row = flatmap.Lambda().Args().Arg(0);
    auto predicate = TExprBase(flatmap.Lambda().Body().Ref().ChildPtr(0));
    TTableLookup lookup = ExtractTableLookup(row, predicate, tableDesc.Metadata->KeyColumnNames,
        &KqpTableLookupGetValue, &KqpTableLookupCanCompare, &KqpTableLookupCompare, ctx, false);

    if (lookup.IsFullScan()) {
        return node;
    }

    auto readSettings = TKqpReadTableSettings::Parse(read);

    TVector<TExprBase> fetches;
    fetches.reserve(lookup.GetKeyRanges().size());

    for (auto& keyRange : lookup.GetKeyRanges()) {
        bool useDataOrGenericQueryLookup = false;
        bool useScanQueryLookup = false;
        if (onlyPointRanges && !IsPointPrefix(keyRange)) {
            return node;
        }
        if (keyRange.IsEquiRange()) {
            bool isFullKey = keyRange.GetNumDefined() == tableDesc.Metadata->KeyColumnNames.size();

            // NOTE: Use more efficient full key lookup implementation in datashard.
            // Consider using lookup for partial keys as well once better constant folding
            // is available, currently it can introduce redundant compute stage.
            useDataOrGenericQueryLookup = (kqpCtx.IsDataQuery() || kqpCtx.IsGenericQuery()) && isFullKey;
            useScanQueryLookup = kqpCtx.IsScanQuery() && isFullKey
                && kqpCtx.Config->EnableKqpScanQueryStreamLookup;
        }

        TMaybeNode<TExprBase> readInput;
        // TODO: Use single implementation for all kinds of queries.
        if (useDataOrGenericQueryLookup) {
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
        } else if (useScanQueryLookup) {
            YQL_ENSURE(kqpCtx.Config->EnableKqpScanQueryStreamLookup);
            auto lookupKeys = BuildEquiRangeLookup(keyRange, tableDesc, read.Pos(), ctx);

            if (indexName) {
                readInput = Build<TKqlStreamLookupIndex>(ctx, read.Pos())
                    .Table(read.Table())
                    .LookupKeys(lookupKeys)
                    .Columns(read.Columns())
                    .Index(indexName.Cast())
                    .Done();
            } else {
                readInput = Build<TKqlStreamLookupTable>(ctx, read.Pos())
                    .Table(read.Table())
                    .LookupKeys(lookupKeys)
                    .Columns(read.Columns())
                    .LookupStrategy().Build(TKqpStreamLookupStrategyName)
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

        input = readMatch->BuildProcessNodes(input, ctx);

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

TMaybeNode<TExprBase> KqpRewriteLiteralLookup(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlLookupTable>()) {
        return {};
    }

    const TKqlLookupTable& lookup = node.Cast<TKqlLookupTable>();

    if (!kqpCtx.Config->EnableKqpDataQuerySourceRead) {
        return {};
    }

    TMaybeNode<TExprBase> lookupKeys = lookup.LookupKeys();
    TMaybeNode<TCoSkipNullMembers> skipNullMembers;
    if (lookupKeys.Maybe<TCoSkipNullMembers>()) {
        skipNullMembers = lookupKeys.Cast<TCoSkipNullMembers>();
        lookupKeys = skipNullMembers.Input();
    }

    TKqpReadTableSettings settings;
    if (skipNullMembers) {
        auto skipNullColumns = skipNullMembers.Cast().Members();

        if (skipNullColumns) {
            for (const auto &column : skipNullColumns.Cast()) {
                settings.AddSkipNullKey(TString(column.Value()));
            }
        }
    }

    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, lookup.Table().Path().Value());
    if (auto lookupKeysFlatMap = lookupKeys.Maybe<TCoFlatMapBase>()) {
        auto flatMapRangeInput = lookupKeysFlatMap.Cast().Input().Maybe<TCoRangeFinalize>();

        // This rule should depend on feature flag for safety
        if (!flatMapRangeInput || !kqpCtx.Config->EnableKqpDataQueryStreamLookup) {
            return {};
        }

        auto lookupKeysType = lookupKeys.Ref().GetTypeAnn();
        YQL_ENSURE(lookupKeysType);
        YQL_ENSURE(lookupKeysType->GetKind() == ETypeAnnotationKind::List);
        auto itemType = lookupKeysType->Cast<TListExprType>()->GetItemType();
        YQL_ENSURE(itemType->GetKind() == ETypeAnnotationKind::Struct);
        auto structType = itemType->Cast<TStructExprType>();

        TVector<TString> usedColumns;
        usedColumns.reserve(structType->GetSize());
        for (const auto& keyColumnName : table.Metadata->KeyColumnNames) {
            if (!structType->FindItem(keyColumnName)) {
                break;
            }
            
            usedColumns.emplace_back(keyColumnName);
        }

        YQL_ENSURE(usedColumns.size() == structType->GetSize());

        TKqpReadTableExplainPrompt prompt;
        prompt.SetUsedKeyColumns(std::move(usedColumns));
        prompt.SetPointPrefixLen(structType->GetSize());


        return Build<TKqlReadTableRanges>(ctx, lookup.Pos())
            .Table(lookup.Table())
            .Ranges(flatMapRangeInput.Cast())
            .Columns(lookup.Columns())
            .Settings(settings.BuildNode(ctx, lookup.Pos()))
            .ExplainPrompt(prompt.BuildNode(ctx, lookup.Pos()))
            .Done();
    }

    auto maybeAsList = lookupKeys.Maybe<TCoAsList>();
    if (!maybeAsList) {
        return {};
    }

    // one point expected
    if (maybeAsList.Cast().ArgCount() != 1) {
        return {};
    }

    auto maybeStruct = maybeAsList.Cast().Arg(0).Maybe<TCoAsStruct>();
    if (!maybeStruct) {
        return node;
    }

    // full pk expected
    if (table.Metadata->KeyColumnNames.size() != maybeStruct.Cast().ArgCount()) {
        return {};
    }

    std::unordered_map<TString, TExprBase> keyColumnsStruct;
    for (const auto& item : maybeStruct.Cast()) {
        const auto& tuple = item.Cast<TCoNameValueTuple>();
        keyColumnsStruct.insert({TString(tuple.Name().Value()),  tuple.Value().Cast()});
    }

    TVector<TExprBase> keyValues;
    keyValues.reserve(maybeStruct.Cast().ArgCount());
    for (const auto& name : table.Metadata->KeyColumnNames) {
        auto it = keyColumnsStruct.find(name);
        YQL_ENSURE(it != keyColumnsStruct.end());
        keyValues.push_back(it->second);
    }

    if (skipNullMembers) {
        auto skipNullColumns = skipNullMembers.Cast().Members();

        if (skipNullColumns) {
            for (const auto &column : skipNullColumns.Cast()) {
                settings.AddSkipNullKey(TString(column.Value()));
            }
        }
    }

    return Build<TKqlReadTable>(ctx, lookup.Pos())
        .Table(lookup.Table())
        .Range<TKqlKeyRange>()
            .From<TKqlKeyInc>()
                .Add(keyValues)
                .Build()
            .To<TKqlKeyInc>()
                .Add(keyValues)
                .Build()
            .Build()
        .Columns(lookup.Columns())
        .Settings(settings.BuildNode(ctx, lookup.Pos()))
        .Done();
}

TExprBase KqpRewriteLookupTable(const TExprBase& node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TKqlLookupTable>()) {
        return node;
    }

    if (auto literal = KqpRewriteLiteralLookup(node, ctx, kqpCtx)) {
        return literal.Cast();
    }

    const TKqlLookupTable& lookup = node.Cast<TKqlLookupTable>();

    if (!kqpCtx.Config->EnableKqpDataQueryStreamLookup) {
        return node;
    }

    return Build<TKqlStreamLookupTable>(ctx, lookup.Pos())
        .Table(lookup.Table())
        .LookupKeys(lookup.LookupKeys())
        .Columns(lookup.Columns())
        .LookupStrategy().Build(TKqpStreamLookupStrategyName)
        .Done();
}

TExprBase KqpDropTakeOverLookupTable(const TExprBase& node, TExprContext&, const TKqpOptimizeContext& kqpCtx) {
    if (!node.Maybe<TCoTake>().Input().Maybe<TKqlLookupTableBase>()) {
        return node;
    }

    auto take = node.Cast<TCoTake>();
    auto lookupTable = take.Input().Cast<TKqlLookupTableBase>();

    if (!take.Count().Maybe<TCoUint64>()) {
        return node;
    }

    const ui64 count = FromString<ui64>(take.Count().Cast<TCoUint64>().Literal().Value());
    YQL_ENSURE(count > 0);

    auto maybeAsList = lookupTable.LookupKeys().Maybe<TCoAsList>();
    if (!maybeAsList) {
        maybeAsList = lookupTable.LookupKeys().Maybe<TCoIterator>().List().Maybe<TCoAsList>();
    }

    if (!maybeAsList) {
        return node;
    }

    if (maybeAsList.Cast().ArgCount() > count) {
        return node;
    }

    const auto tablePath = lookupTable.Table().Path().Value();
    const auto& table = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, tablePath);

    const auto& lookupKeys = GetSeqItemType(*lookupTable.LookupKeys().Ref().GetTypeAnn()).Cast<TStructExprType>()->GetItems();
    if (table.Metadata->KeyColumnNames.size() != lookupKeys.size()) {
        return node;
    }

    return lookupTable;
}

} // namespace NKikimr::NKqp::NOpt

