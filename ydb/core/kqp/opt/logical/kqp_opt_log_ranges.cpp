#include "kqp_opt_log_impl.h"
#include "kqp_opt_log_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/opt/kqp_opt_impl.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider_impl.h>

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

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

