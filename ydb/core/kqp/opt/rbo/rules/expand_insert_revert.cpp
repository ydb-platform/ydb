#include "kqp_rules_include.h"

#include <ydb/core/kqp/opt/kqp_opt_impl.h>

namespace NKikimr::NKqp {

namespace {

// Normalize every conflict branch to a non-null marker and combine them before
// aggregating. Only emptiness matters: UNION ALL preserves that property without
// deduplication, and a marker avoids COUNT(key) missing a NULL primary-key conflict.
// The scalar aggregate emits count=0 even for an empty union. Filtering it gives
// exactly one row on success and no rows on conflict, suitable for one cross-join
// gate regardless of the number of constraints.
TIntrusivePtr<IOperator> RequireNoConflicts(TVector<TIntrusivePtr<IOperator>> conflicts, TPositionHandle pos,
    TExprContext& ctx, TPlanProps& props)
{
    TUnionInputRow markers;
    for (auto& conflict : conflicts) {
        const auto marker = props.InfoUnitRegistry.AddGenerated("conflict_marker");
        TMapIUs columns;
        columns.Add(marker, MakeConstant("Uint64", "1", pos, &ctx));
        conflict = MakeIntrusive<TOpMap>(std::move(conflict), pos, std::move(columns));
        markers.Inputs.push_back(marker);
    }
    const auto row = props.InfoUnitRegistry.AddGenerated("conflict_row");
    const auto count = props.InfoUnitRegistry.AddGenerated("conflict_count");
    TUnionAllIUs columns(TUnionInputPolicy{conflicts.size()});
    columns.Add(row, std::move(markers));
    auto rows = MakeIntrusive<TOpUnionAll>(std::move(conflicts), pos, std::move(columns));
    TAggregationIUs traits;
    traits.Add(count, TOpAggregationTraits{row, "count"});
    auto aggregate = MakeIntrusive<TOpAggregate>(rows, std::move(traits), TOrderedIUs<>{}, EOpPhase::Undefined, false, pos);
    return MakeIntrusive<TOpFilter>(aggregate, pos,
        MakeBinaryPredicate("==", MakeColumnAccess(count, pos, &ctx, &props), MakeConstant("Uint64", "0", pos, &ctx)));
}

// Add two conflict streams for one constraint: duplicate input keys and keys
// already present in storage. Grouping serves both checks: it counts duplicates
// globally and reduces storage lookups to one per distinct key. Replicate ports
// share the grouped input; the primary port keeps its IUs, while the lookup port
// rebinds them so both branches can coexist in the plan.
void CollectInsertKeyConflicts(const TIntrusivePtr<TOpReplicate>& rows,
    const TExprNode::TPtr& table, const TVector<TString>& keyNames,
    const THashMap<TString, TInfoUnitId>& sourceColumns, bool skipNullKeys,
    TPositionHandle pos, TExprContext& ctx, TPlanProps& props,
    TVector<TIntrusivePtr<IOperator>>& conflicts)
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
        MakeBinaryPredicate(">", MakeColumnAccess(count, pos, &ctx, &props),
            MakeConstant("Uint64", "1", pos, &ctx)));
    conflicts.push_back(duplicates);

    TOpTableLookup::TLookupKeys lookupKeys;
    for (size_t i = 0; i < keyNames.size(); ++i) {
        lookupKeys.Append(lookupInput->GetRebindings().At(keys.Items()[i]), keyNames[i]);
    }
    const auto found = props.InfoUnitRegistry.Add(TInfoUnit("insert_conflict", keyNames.front()));
    auto lookup = MakeIntrusive<TOpTableLookup>(lookupInput, pos, table, TUnorderedIUs{found}, std::move(lookupKeys));
    lookup->AllowNullKeys = true;
    conflicts.push_back(lookup);
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
    // Keep sequenced INSERT OR REVERT unsupported until its conflict branches
    // can share the allocation stage in every supported spilling configuration.
    for (const auto& item : IterateSubtree(effect.GetInput().Get())) {
        Y_ENSURE(item.Current->Kind != EOperator::Sequencer,
            "INSERT OR REVERT with sequenced defaults is not supported in New RBO");
    }
    const auto pos = effect.Pos;
    const auto& table = ctx.KqpCtx.Tables->ExistingTable(ctx.KqpCtx.Cluster, NNodes::TKqpTable(effect.Table).Path());
    THashMap<TString, TInfoUnitId> columns;
    for (const auto& [id, name] : effect.GetColumns().Items()) {
        columns.emplace(name, id);
    }

    // INSERT OR REVERT is a successful no-op for the entire statement when any
    // constraint conflicts. Ensure would fail the query, and filtering individual
    // rows would incorrectly insert the nonconflicting subset. Share the original
    // input with all checks, then gate it once on their combined result:
    //   input CROSS JOIN (COUNT(all conflict markers) = 0)
    // The global aggregate must finish before the gate can emit any input row.
    auto rows = TReplicate::Create(effect.GetInput(), pos, props.InfoUnitRegistry);
    auto insertRows = rows->AddOutput();
    TVector<TIntrusivePtr<IOperator>> conflicts;
    CollectInsertKeyConflicts(rows->AddOutput(), effect.Table, table.Metadata->KeyColumnNames,
        columns, false, pos, ctx.ExprCtx, props, conflicts);

    // Match unique-index semantics: NULL index keys do not conflict, and an
    // omitted index column is NULL in the inserted row. Exclude keys with any
    // NULL component from both checks for unique indexes, but keep NULL primary
    // keys: they participate in primary-key uniqueness and storage lookups.
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
        CollectInsertKeyConflicts(rows->AddOutput(), indexTable.Ptr(), index.KeyColumns,
            columns, true, pos, ctx.ExprCtx, props, conflicts);
    }

    auto checks = RequireNoConflicts(std::move(conflicts), pos, ctx.ExprCtx, props);
    auto checked = MakeIntrusive<TOpJoin>(insertRows, checks, pos, "Cross", TJoinIUs{});

    // The complete input is now gated on all conflicts, so the ordinary insert
    // sink can retain its constraint checks for races without reverting per row.
    // Prechecks describe the transaction's read snapshot; they do not replace
    // write-time constraint enforcement. Changing the mode also prevents this
    // rule from expanding the same effect again.
    auto options = effect.Options;
    options.OnConflict = "abort";
    // The gate preserves the input IUs.
    input = MakeIntrusive<TOpTableEffect>(std::move(checked), pos, effect.Table, effect.EffectType,
        std::move(options));
    return true;
}

} // namespace NKikimr::NKqp
