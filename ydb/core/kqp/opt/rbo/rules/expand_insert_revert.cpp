#include "kqp_rules_include.h"

#include <ydb/core/kqp/opt/kqp_opt_impl.h>

namespace NKikimr::NKqp {

namespace {

// One row when the input is empty, no rows otherwise. Counting a constant also
// handles nullable keys: COUNT(key) would miss conflicts on a NULL primary key.
TIntrusivePtr<IOperator> RequireEmpty(TIntrusivePtr<IOperator> input, TPositionHandle pos,
    TExprContext& ctx, TPlanProps& props)
{
    const auto row = props.InfoUnitRegistry.AddGenerated("conflict_row");
    const auto count = props.InfoUnitRegistry.AddGenerated("conflict_count");
    TMapIUs columns;
    columns.Add(row, MakeConstant("Uint64", "1", pos, &ctx));
    auto rows = MakeIntrusive<TOpMap>(std::move(input), pos, std::move(columns));
    TAggregationIUs traits;
    traits.Add(count, TOpAggregationTraits{row, "count"});
    auto aggregate = MakeIntrusive<TOpAggregate>(rows, std::move(traits), TOrderedIUs<>{}, EOpPhase::Undefined, false, pos);
    return MakeIntrusive<TOpFilter>(aggregate, pos,
        MakeBinaryPredicate("==", MakeColumnAccess(count, pos, &ctx, &props), MakeConstant("Uint64", "0", pos, &ctx)));
}

TIntrusivePtr<IOperator> CheckInsertKeys(const TIntrusivePtr<TOpReplicate>& rows,
    const TExprNode::TPtr& table, const TVector<TString>& keyNames,
    const THashMap<TString, TInfoUnitId>& sourceColumns, bool skipNullKeys,
    TPositionHandle pos, TExprContext& ctx, TPlanProps& props)
{
    TIntrusivePtr<IOperator> input = rows;
    TOrderedIUs<> keys;
    TVector<TExpression> nonNull;
    for (const auto& name : keyNames) {
        const auto key = rows->GetRebindings().At(sourceColumns.at(name));
        keys.Append(key);
        if (skipNullKeys) {
            nonNull.push_back(MakeUnaryCallable("Exists", MakeColumnAccess(key, pos, &ctx, &props)));
        }
    }
    if (skipNullKeys) {
        input = MakeIntrusive<TOpFilter>(input, pos, MakeConjunction(nonNull));
    }

    const auto row = props.InfoUnitRegistry.AddGenerated("insert_row");
    const auto count = props.InfoUnitRegistry.AddGenerated("key_count");
    TMapIUs columns;
    columns.Add(row, MakeConstant("Uint64", "1", pos, &ctx));
    auto countedRows = MakeIntrusive<TOpMap>(input, pos, std::move(columns));
    TAggregationIUs traits;
    traits.Add(count, TOpAggregationTraits{row, "count"});
    auto grouped = MakeIntrusive<TOpAggregate>(countedRows, std::move(traits), keys, EOpPhase::Undefined, false, pos);
    auto groups = TReplicate::Create(grouped, pos, props.InfoUnitRegistry);
    auto duplicatesInput = groups->AddOutput();
    auto lookupInput = groups->AddOutput();

    auto duplicates = MakeIntrusive<TOpFilter>(duplicatesInput, pos,
        MakeBinaryPredicate(">", MakeColumnAccess(count, pos, &ctx, &props), MakeConstant("Uint64", "1", pos, &ctx)));
    auto noDuplicates = RequireEmpty(duplicates, pos, ctx, props);

    TOpTableLookup::TLookupKeys lookupKeys;
    for (size_t i = 0; i < keyNames.size(); ++i) {
        lookupKeys.Append(lookupInput->GetRebindings().At(keys.Items()[i]), keyNames[i]);
    }
    const auto found = props.InfoUnitRegistry.Add(TInfoUnit("insert_conflict", keyNames.front()));
    auto lookup = MakeIntrusive<TOpTableLookup>(lookupInput, pos, table, TUnorderedIUs{found}, std::move(lookupKeys));
    lookup->AllowNullKeys = true;
    auto noExistingKeys = RequireEmpty(lookup, pos, ctx, props);
    return MakeIntrusive<TOpJoin>(noDuplicates, noExistingKeys, pos, "Cross", TJoinIUs{});
}

} // anonymous namespace

bool TExpandInsertRevertRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    if (input->Kind != EOperator::TableEffect) {
        return false;
    }
    const auto& effect = CastOperator<TOpTableEffect>(*input);
    return (effect.EffectType == EEffectType::InsertRows || effect.EffectType == EEffectType::InsertRowsIndex)
        && effect.Options.OnConflict == "revert";
}

bool TExpandInsertRevertRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (!QuickMatch(input)) {
        return false;
    }
    auto& effect = CastOperator<TOpTableEffect>(*input);
    const auto pos = effect.Pos;
    const auto& table = ctx.KqpCtx.Tables->ExistingTable(ctx.KqpCtx.Cluster, NNodes::TKqpTable(effect.Table).Path());
    THashMap<TString, TInfoUnitId> columns;
    for (const auto& [id, name] : effect.GetColumns().Items()) {
        columns.emplace(name, id);
    }

    auto rows = TReplicate::Create(effect.GetInput(), pos, props.InfoUnitRegistry);
    TIntrusivePtr<IOperator> checked = rows->AddOutput();
    auto checks = CheckInsertKeys(rows->AddOutput(), effect.Table, table.Metadata->KeyColumnNames,
        columns, false, pos, ctx.ExprCtx, props);
    checked = MakeIntrusive<TOpJoin>(checked, checks, pos, "Cross", TJoinIUs{});

    // Match unique-index semantics: NULL index keys do not conflict, and an
    // omitted index column is NULL in the inserted row.
    for (size_t i = 0; i < table.Metadata->Indexes.size(); ++i) {
        const auto& index = table.Metadata->Indexes[i];
        if (index.Type != TIndexDescription::EType::GlobalSyncUnique
            || (!ctx.KqpCtx.Config->FeatureFlags.GetEnableOnlineAddUniqueIndex()
                && index.State != TIndexDescription::EIndexState::Ready)) {
            continue;
        }
        if (std::any_of(index.KeyColumns.begin(), index.KeyColumns.end(), [&](const auto& name) { return !columns.contains(name); })) {
            continue;
        }
        const auto indexTable = NOpt::BuildTableMeta(*table.Metadata->ImplTables.at(i), pos, ctx.ExprCtx);
        checks = CheckInsertKeys(rows->AddOutput(), indexTable.Ptr(), index.KeyColumns,
            columns, true, pos, ctx.ExprCtx, props);
        checked = MakeIntrusive<TOpJoin>(checked, checks, pos, "Cross", TJoinIUs{});
    }

    // The complete input is now gated on all conflicts, so the ordinary insert
    // sink can retain its constraint checks for races without reverting per row.
    auto options = effect.Options;
    options.OnConflict = "abort";
    input = MakeIntrusive<TOpTableEffect>(std::move(checked), pos, effect.Table, effect.EffectType,
        std::move(options), effect.GetColumns(), effect.GetReturningColumns());
    return true;
}

} // namespace NKikimr::NKqp
