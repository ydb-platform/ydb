#include "kqp_native_frontend.h"
#include "kqp_native_read_preparation.h"
#include "kqp_native_write_preparation.h"
#include "kqp_rbo_utils.h"
#include "kqp_subplan_value.h"
#include "frontend/frontend.h"
#include "frontend/execution.h"
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace NKikimr::NKqp {
namespace {
TString String(std::string_view text) { return TString(text.data(), text.size()); }

TString AggregateName(std::string_view name) {
    // This existing backend trait computes sqrt(M2 / (count - 1)).
    return name == "stddev_samp" ? TString("variance_1_1") : String(name);
}

NYql::TPositionHandle Position(NFrontend::TSourcePosition pos, NYql::TExprContext& ctx) {
    return ctx.AppendPosition(NYql::TPosition(pos.Column, pos.Line));
}

TString JoinName(NFrontend::EJoinKind kind) {
    using K = NFrontend::EJoinKind;
    switch (kind) {
        case K::Inner: return "Inner";
        case K::Cross: return "Cross";
        case K::Left: return "Left";
        case K::Right: return "Right";
        case K::Full: return "Full";
        case K::Exclusion: return "Exclusion";
        case K::LeftSemi: return "LeftSemi";
        case K::RightSemi: return "RightSemi";
        case K::LeftOnly: return "LeftOnly";
        case K::RightOnly: return "RightOnly";
    }
    Y_UNREACHABLE();
}

// ON predicates are evaluated against the inputs before outer-join padding.
// A computed equality whose operands each belong to one input can therefore
// become a key on that input, including for outer, semi and anti joins.
void PrepareJoinKeys(TOpJoin& join, TPlanProps& props) {
    const auto left = join.GetLeftInput()->GetOutputIUs(), right = join.GetRightInput()->GetOutputIUs();
    TMapIUs leftDefinitions, rightDefinitions;
    TVector<TExpression> remaining;
    bool changed = false;
    for (const auto& predicate : join.JoinFilters) {
        if (!predicate.MaybeExprEquiJoinCondition() || ReferencesUnresolvedSubplan(predicate, props)) {
            remaining.push_back(predicate);
            continue;
        }
        TEquiJoinCondition condition(predicate);
        const bool forward = condition.LeftIUs.IsSubsetOf(left) && condition.RightIUs.IsSubsetOf(right);
        const bool reverse = condition.LeftIUs.IsSubsetOf(right) && condition.RightIUs.IsSubsetOf(left);
        if (!forward && !reverse) {
            remaining.push_back(predicate);
            continue;
        }
        TMappedIUs<TExpression> definitions;
        const auto extracted = condition.ExtractExpressions(definitions);
        const TEquiJoinCondition keys(extracted ? *extracted : predicate);
        join.JoinKeys.Add(forward ? keys.GetLeftIU() : keys.GetRightIU(), forward ? keys.GetRightIU() : keys.GetLeftIU());
        for (const auto& [id, value] : definitions.Items()) {
            auto& destination = value.GetInputIUs(false, true).IsSubsetOf(left) ? leftDefinitions : rightDefinitions;
            destination.Add(id, value);
        }
        changed = true;
    }
    if (!changed) return;
    if (join.JoinKind == "Cross") join.JoinKind = "Inner";
    if (!leftDefinitions.Keys().Empty())
        join.SetLeftInput(MakeIntrusive<TOpMap>(join.GetLeftInput(), join.Pos, std::move(leftDefinitions), true));
    if (!rightDefinitions.Keys().Empty())
        join.SetRightInput(MakeIntrusive<TOpMap>(join.GetRightInput(), join.Pos, std::move(rightDefinitions), true));
    join.JoinFilters = std::move(remaining);
}

TIntrusivePtr<IOperator> BuildGroupingSetBranches(const NFrontend::TRelation& relation,
    const std::vector<NFrontend::TColumn>& columns, const TIntrusivePtr<IOperator>& input,
    TPlanProps& props, NYql::TPositionHandle pos) {
    const auto& arena = props.NativeExpressions;
    const auto expression = [&](NFrontend::TExprId value) {
        return TExpression::FromNative(arena, value, &props, pos);
    };
    // Lower once to executable branches. A GroupingSets wrapper
    // requires an untouched aggregate child; expanding DISTINCT
    // below it would change that child's meaning and structure.
    const bool multiple = relation.GroupingSets.size() > 1;
    auto& registry = props.InfoUnitRegistry;
    auto hub = multiple ? TReplicate::Create(input, pos, registry) : TIntrusivePtr<TReplicate>{};
    TVector<TIntrusivePtr<IOperator>> branches;
    TMappedIUs<TUnionInputRow> rows;
    for (auto output : relation.Columns) rows.Add(output);
    for (const auto& set : relation.GroupingSets) {
        auto port = hub ? hub->AddOutput() : nullptr;
        const auto* renames = port && !port->IsPrimary() ? &port->GetRebindings() : nullptr;
        auto rebind = [&](TInfoUnitId source) { return renames ? renames->At(source) : source; };
        const std::unordered_set<NFrontend::TColumnId> present(set.begin(), set.end());
        TOrderedIUs<> branchKeys;
        TMappedIUs<TInfoUnitId> values;
        std::unordered_map<NFrontend::TColumnId, NFrontend::TType> types;
        for (auto key : set) {
            branchKeys.Append(rebind(key)); values.Add(key, rebind(key));
            types.emplace(key, columns[key].Type);
        }
        TAggregationIUs traits;
        for (const auto& aggregate : relation.Aggregates) {
            const auto value = registry.AddCopy(aggregate.Output);
            values.Add(aggregate.Output, value);
            auto type = columns[aggregate.Output].Type;
            if (set.empty() && aggregate.Function != "count") type = type.Optional();
            traits.Add(value, TOpAggregationTraits{rebind(aggregate.Input), AggregateName(aggregate.Function), aggregate.Distinct, false,
                aggregate.Function == "avg" ? std::optional{type} : std::nullopt});
            types.emplace(aggregate.Output, type);
        }
        TIntrusivePtr<IOperator> branch = MakeIntrusive<TOpAggregate>(port ? port : input,
            std::move(traits), std::move(branchKeys), EOpPhase::Undefined, false, pos);
        TMapIUs padding;
        auto add = [&](TInfoUnitId output, NFrontend::TExprId value) {
            const auto column = multiple ? registry.AddCopy(output) : output;
            padding.Add(column, TMapElement(expression(value)));
            rows.At(output).Inputs.push_back(column);
        };
        for (const auto& [output, source] : relation.GroupingColumns) {
            const auto type = columns[output].Type;
            const auto* value = values.Find(source);
            auto scalar = value ? arena->Column(*value, types.at(source), relation.Position)
                : arena->Literal(type, std::monostate{}, relation.Position);
            add(output, arena->Convert(scalar, type, relation.Position));
        }
        for (const auto& [output, key] : relation.GroupingIndicators)
            add(output, arena->Literal({NFrontend::ETypeKind::Uint64}, std::uint64_t{present.contains(key) ? 0u : 1u}, relation.Position));
        if (!padding.Keys().Empty()) branch = MakeIntrusive<TOpMap>(branch, pos, std::move(padding));
        branches.push_back(std::move(branch));
    }
    if (multiple) {
        TUnionAllIUs outputs(TUnionInputPolicy{branches.size()});
        for (auto output : relation.Columns) outputs.Add(output, std::move(rows.At(output)));
        return MakeIntrusive<TOpUnionAll>(std::move(branches), pos, std::move(outputs), false);
    }
    return std::move(branches.front());
}

void ApplyPlannerSettings(const std::vector<NFrontend::TPlannerSetting>& settings, NYql::TKikimrConfiguration* config) {
    using S = NFrontend::EPlannerSetting;
    if (settings.empty()) return;
    if (!config) throw NFrontend::TUnsupportedFeature(NFrontend::EFeature::Backend, settings.front().Position,
        "native planner pragmas require a configuration at the RBO handoff");
    // Validate complex options before changing query configuration. Hint parsing
    // belongs to KQP's optimizer; no YQL configuration/annotation pipeline runs.
    TVector<TOptimizerHints> hints;
    TVector<NYql::NDq::EHashJoinMode> hashModes;
    for (const auto& option : settings) {
        try {
            if (option.Kind == S::OptimizerHints) hints.push_back(TOptimizerHints::Parse(String(std::get<std::string>(option.Value))));
            else if (option.Kind == S::HashJoinMode) hashModes.push_back(FromString<NYql::NDq::EHashJoinMode>(String(std::get<std::string>(option.Value))));
        } catch (const std::exception& error) {
            throw NFrontend::TFrontendError(option.Position, std::string("invalid planner pragma: ") + error.what());
        }
    }
    size_t hint = 0, hashMode = 0;
    for (const auto& option : settings) {
        switch (option.Kind) {
            case S::OptDisableAutoIndexSelection: config->OptDisableAutoIndexSelection = std::get<bool>(option.Value); break;
            case S::EnableAutoIndexSelectionForIndexLookupJoin: config->EnableAutoIndexSelectionForIndexLookupJoin = std::get<bool>(option.Value); break;
            case S::OptDisableTopSort: config->OptDisableTopSort = std::get<bool>(option.Value); break;
            case S::OptEnablePredicateExtract: config->OptEnablePredicateExtract = std::get<bool>(option.Value); break;
            case S::OptEnableInplaceUpdate: config->OptEnableInplaceUpdate = std::get<bool>(option.Value); break;
            case S::OptShuffleElimination: config->OptShuffleElimination = std::get<bool>(option.Value); break;
            case S::OptShuffleEliminationWithMap: config->OptShuffleEliminationWithMap = std::get<bool>(option.Value); break;
            case S::OptShuffleEliminationForAggregation: config->OptShuffleEliminationForAggregation = std::get<bool>(option.Value); break;
            case S::EnableNewRBOPhysicalStagePeephole: config->EnableNewRBOPhysicalStagePeephole = std::get<bool>(option.Value); break;
            case S::WindowFunctionsV2: config->WindowFunctionsV2 = std::get<bool>(option.Value); break;
            case S::OptEnableOlapPushdown: config->OptEnableOlapPushdown = std::get<bool>(option.Value); break;
            case S::OptEnableOlapPushdownAggregate: config->OptEnableOlapPushdownAggregate = std::get<bool>(option.Value); break;
            case S::OptEnableOlapPushdownProjections: config->OptEnableOlapPushdownProjections = std::get<bool>(option.Value); break;
            case S::OptEnableOlapPushdownRegexp: config->OptEnableOlapPushdownRegexp = std::get<bool>(option.Value); break;
            case S::DqHashOperatorsUseBlocks: config->DqHashOperatorsUseBlocks = std::get<bool>(option.Value); break;
            case S::UseBlockHashJoin: config->UseBlockHashJoin = std::get<bool>(option.Value); break;
            case S::DisableBlockExecution: config->DisableBlockExecution = std::get<bool>(option.Value); break;
            case S::OptFallbackToLegacyOptimizer: config->OptFallbackToLegacyOptimizer = std::get<bool>(option.Value); break;
            case S::CostBasedOptimizationLevel: config->CostBasedOptimizationLevel = std::get<std::uint32_t>(option.Value); break;
            case S::DqChannelVersion: config->DqChannelVersion = std::get<std::uint32_t>(option.Value); break;
            case S::CostBasedOptimization: {
                const auto& mode = std::get<std::string>(option.Value);
                config->CostBasedOptimization = String(mode);
                config->CostBasedOptimizationLevel = mode == "on" ? std::numeric_limits<ui32>::max() :
                    mode == "off" ? 0 : config->GetDefaultCostBasedOptimizationLevel();
                break;
            }
            case S::HashJoinMode: config->HashJoinMode = hashModes.at(hashMode++); break;
            case S::OptimizerHints: config->OptimizerHints = std::move(hints.at(hint++)); break;
        }
    }
}
} // namespace

TNativeQuery CompileNativeToRbo(std::string_view sql,
    const TNativeTableResolver& resolve, NYql::TExprContext& ctx,
    const NFrontend::TBindSettings& bindSettings, const NFrontend::TParseSettings& parseSettings, NYql::TKikimrConfiguration* config) {
    std::unordered_map<NFrontend::TTableAddress, TNativeTable, NFrontend::TTableAddressHash> tables;
    NFrontend::TCompilerSettings settings;
    settings.Binding = bindSettings;
    settings.Parsing = parseSettings;
    auto discovery = NFrontend::DiscoverNative(NFrontend::ParseNative(sql, settings), bindSettings);
    for (const auto& request : discovery.Tables()) if (request.Address.Cluster != bindSettings.DefaultCluster)
        throw NFrontend::TUnsupportedFeature(NFrontend::EFeature::Backend, request.Position,
            "native execution requires the configured cluster");
    auto compiled = NFrontend::BindNative(discovery, [&](const NFrontend::TTableAddress& address, NFrontend::TSourcePosition pos) {
        auto it = tables.find(address);
        if (it == tables.end()) {
            auto table = resolve(address, pos);
            if (!table.Table) throw NFrontend::TFrontendError(pos, "table resolver returned no table identity");
            auto schema = table.Schema;
            // The bound relation uses the resolved path, not its source alias.
            tables.emplace(NFrontend::TTableAddress{address.Cluster, schema.Get().Name}, table);
            tables.emplace(address, std::move(table));
            return schema;
        }
        return it->second.Schema;
    });
    return ConvertNativeToRbo(std::move(compiled), [&](const NFrontend::TTableAddress& address, NFrontend::TSourcePosition) {
        return tables.at(address);
    }, ctx, config);
}

TNativeQuery ConvertNativeToRbo(NFrontend::TCompiledProgram compiled,
    const TNativeTableResolver& resolve, NYql::TExprContext& ctx, NYql::TKikimrConfiguration* config) {
    const auto& bound = compiled.Get();
    for (const auto& relation : bound.Relations) {
        if (relation.Kind == NFrontend::ERelation::Join && (relation.LeftAny || relation.RightAny))
            throw NFrontend::TUnsupportedFeature(NFrontend::EFeature::Backend, relation.Position,
                "ANY joins require native duplicate-selection semantics");
        if ((relation.Kind == NFrontend::ERelation::Read || relation.Kind == NFrontend::ERelation::Effect)
            && relation.Table.Cluster != bound.DefaultCluster)
            throw NFrontend::TUnsupportedFeature(NFrontend::EFeature::Backend, relation.Position,
                "native execution requires the configured cluster");
    }
    auto execution = NFrontend::PrepareExecution(std::move(compiled));
    auto& program = execution.Program;
    TNativeQuery query;
    query.Parameters = std::move(program.Parameters);
    ApplyPlannerSettings(program.PlannerSettings, config);
    std::unordered_map<NFrontend::TTableAddress, TNativeTable, NFrontend::TTableAddressHash> tables;
    for (const auto& relation : program.Relations) if ((relation.Kind == NFrontend::ERelation::Read || relation.Kind == NFrontend::ERelation::Effect) && !tables.contains(relation.Table)) {
        auto table = resolve(relation.Table, relation.Position);
        if (!table.Table) throw NFrontend::TFrontendError(relation.Position, "table resolver returned no table identity");
        table.Table.NativeCatalog = table.Schema;
        tables.emplace(relation.Table, std::move(table));
    }
    auto arena = std::make_shared<NFrontend::TExpressionArena>(std::move(program.Expressions));
    // Checked frontend IDs are dense indexes into Columns and Relations.
    // Keep their translation bindings in vectors instead of allocating hash nodes.
    // Convert a source spelling once; TString shares its owned bytes across
    // the source's columns and independently owned result registries.
    std::vector<std::optional<TString>> sourceAliases(program.Columns.size());
    for (const auto& relation : program.Relations) if (relation.Kind == NFrontend::ERelation::Read) {
        const auto alias = String(relation.Alias);
        for (const auto column : relation.Columns) if (!sourceAliases.at(column)) sourceAliases[column] = alias;
    }
    std::unordered_map<NFrontend::TRelId, NFrontend::TColumnId> subplanBindings;
    for (const auto& [binding, subplan] : program.Subplans) subplanBindings.emplace(subplan.Root, binding);
    auto& roots = query.Roots;
    std::unordered_map<NFrontend::TTableAddress, std::unordered_map<std::string, TNativeIndex>, NFrontend::TTableAddressHash> indexes;
    THashMap<TOpRoot*, TVector<TNativeIndexRead>> indexReads;
    for (const auto& executionRoot : execution.Roots) {
        const auto& result = executionRoot.Query;
        TPlanProps props;
        TVector<TNativeIndexRead> requestedIndexes;
        props.NativeExpressions = arena;
        for (NFrontend::TColumnId id = 0; id < program.Columns.size(); ++id) {
            const auto& column = program.Columns[id];
            props.InfoUnitRegistry.Add(TInfoUnit(sourceAliases[id] ? *sourceAliases[id] : String(column.Qualifier), String(column.Name)));
        }
        auto expression = [&](NFrontend::TExprId id) {
            return TExpression::FromNative(arena, id, &props, Position((*arena)[id].Position, ctx));
        };
        std::vector<TIntrusivePtr<IOperator>> converted(program.Relations.size());
        std::unordered_map<NFrontend::TRelId, TIntrusivePtr<TReplicate>> shared;
        std::vector<std::pair<NFrontend::TRelId, bool>> pending{{result.Root, false}};
        std::unordered_set<NFrontend::TExprId> visitedExpressions;
        auto scheduleExpression = [&](NFrontend::TExprId root) {
            if (program.Subplans.empty()) return;
            std::vector<NFrontend::TExprId> expressions{root};
            while (!expressions.empty()) {
                const auto id = expressions.back(); expressions.pop_back();
                if (!visitedExpressions.insert(id).second) continue;
                const auto& node = (*arena)[id];
                expressions.insert(expressions.end(), node.Arguments.begin(), node.Arguments.end());
                if (node.Op == NFrontend::EOpcode::Column) {
                    if (const auto call = program.Subplans.find(node.ColumnId); call != program.Subplans.end())
                        pending.emplace_back(call->second.Root, false);
                }
            }
        };
        while (!pending.empty()) {
            const auto [id, ready] = pending.back(); pending.pop_back();
            if (converted.at(id)) continue;
            const auto& relation = program.Relations.at(id);
            if (!ready) {
                pending.emplace_back(id, true);
                for (auto it = relation.Inputs.rbegin(); it != relation.Inputs.rend(); ++it) pending.emplace_back(*it, false);
                for (const auto& definition : relation.Definitions) scheduleExpression(definition.Expression);
                if (relation.Predicate) scheduleExpression(*relation.Predicate);
                if (relation.Count) scheduleExpression(*relation.Count);
                if (relation.Offset) scheduleExpression(*relation.Offset);
                continue;
            }
            auto pos = Position(relation.Position, ctx);
            TIntrusivePtr<IOperator> op;
            auto input = [&](std::size_t index = 0) { return converted.at(relation.Inputs.at(index)); };
            using R = NFrontend::ERelation;
            switch (relation.Kind) {
                case R::Reuse: {
                    auto& hub = shared[relation.Inputs.front()];
                    if (!hub) hub = TReplicate::Create(input(), pos, props.InfoUnitRegistry);
                    auto port = hub->AddOutput();
                    TMapIUs definitions;
                    for (const auto& definition : relation.Definitions) {
                        const auto& column = (*arena)[definition.Expression];
                        const auto id = port->IsPrimary() ? column.ColumnId : port->GetRebindings().At(column.ColumnId);
                        definitions.Add(definition.Column, MakeColumnAccess(id, column.Type, pos, &props));
                    }
                    op = MakeIntrusive<TOpMap>(port, pos, std::move(definitions));
                    break;
                }
                case R::Unit: op = MakeIntrusive<TOpEmptySource>(pos); break;
                case R::Parameter: {
                    TOpEmptySource::TParameterInput parameter;
                    parameter.Name = "$" + String(relation.ParameterName);
                    for (const auto id : relation.Columns) {
                        const auto& column = program.Columns.at(id);
                        parameter.Fields.Add(id, NFrontend::TCatalogColumn{column.Name, column.Type});
                    }
                    op = MakeIntrusive<TOpEmptySource>(pos, std::move(parameter)); break;
                }
                case R::Read: {
                    const auto& table = tables.at(relation.Table);
                    TUnorderedIUs columns; for (auto column : relation.Columns) columns.Add(column);
                    // Capture the source binding before pruning its columns: row
                    // and join hints use this identity, not the table's path.
                    const auto alias = String(relation.Alias);
                    op = MakeIntrusive<TOpRead>(alias, std::move(columns), table.Storage, table.Table, nullptr, std::nullopt,
                        std::nullopt, std::nullopt, ESortDir::None, TPhysicalOpProps{}, pos);
                    if (relation.Index) {
                        auto& byName = indexes[relation.Table];
                        auto found = byName.find(*relation.Index);
                        if (found == byName.end()) {
                            if (!table.ResolveIndex) throw NFrontend::TFrontendError(relation.Position,
                                "Index not found: " + *relation.Index);
                            found = byName.emplace(*relation.Index, table.ResolveIndex(*relation.Index, relation.Position)).first;
                        }
                        requestedIndexes.push_back({CastOperator<TOpRead>(op), found->second});
                    }
                    break;
                }
                case R::Effect: {
                    const auto& effect = *relation.Effect;
                    const auto& table = tables.at(relation.Table);
                    Y_ENSURE(table.Storage == NYql::EStorageType::RowStorage);
                    TEffectOptions options;
                    options.OnConflict = String(effect.OnConflict);
                    options.IsBatch = effect.Batch;
                    options.Columns.emplace();
                    options.ReturningColumns.emplace();
                    if (!effect.DefaultColumns.empty()) {
                        options.DefaultColumns.emplace();
                        for (const auto& name : effect.DefaultColumns) options.DefaultColumns->push_back(String(name));
                    }
                    for (const auto& [name, value] : effect.GeneratedColumns)
                        options.GeneratedColumns.emplace_back(String(name), value);
                    TOrderedIUs<TString> columns, returning;
                    for (const auto& [id, name] : effect.Columns) {
                        columns.Append(id, String(name)); options.Columns->push_back(String(name));
                    }
                    for (const auto& [id, name] : effect.Returning) {
                        returning.Append(id, String(name)); options.ReturningColumns->push_back(String(name));
                    }
                    using W = NFrontend::EWriteKind;
                    EEffectType kind;
                    switch (effect.Kind) {
                        case W::Insert: kind = effect.HasIndexes ? EEffectType::InsertRowsIndex : EEffectType::InsertRows; break;
                        case W::Replace: options.Settings = TVector<TVector<TString>>{{"Mode", "replace"}}; [[fallthrough]];
                        case W::Upsert: kind = effect.HasIndexes ? EEffectType::UpsertRowsIndex : EEffectType::UpsertRows; break;
                        case W::Update:
                            if (!effect.HasIndexes && !effect.InputRows) {
                                // Predicate updates already read existing rows. Match
                                // BuildUpdateTable's conditional write construction;
                                // UPDATE ON still needs the sink's existence check.
                                kind = EEffectType::UpsertRows;
                                options.Settings = TVector<TVector<TString>>{
                                    {"Mode", config && config->GetEnableIndexStreamWrite() ? "update_conditional" : "upsert"},
                                    {"IsUpdate"}, {"IsConditionalUpdate"}};
                            } else {
                                kind = effect.HasIndexes ? EEffectType::UpdateRowsIndex : EEffectType::UpdateRows;
                            }
                            break;
                        case W::Delete: kind = effect.HasIndexes ? EEffectType::DeleteRowsIndex : EEffectType::DeleteRows; break;
                    }
                    auto prepared = input();
                    if (effect.OnConflict == "revert") {
                        prepared = PrepareInsertRevert(std::move(prepared), columns, table, pos, props);
                        // Prechecks use the transaction's read snapshot. Keep
                        // ordinary insert constraint enforcement for races.
                        options.OnConflict = "abort";
                    }
                    op = MakeIntrusive<TOpTableEffect>(std::move(prepared), pos, table.Table, kind, std::move(options), std::move(columns), std::move(returning));
                    props.WithEffects = true;
                    props.WithReturning = !effect.Returning.empty();
                    break;
                }
                case R::Map: {
                    TMapIUs definitions;
                    for (const auto& definition : relation.Definitions) definitions.Add(definition.Column, TMapElement(expression(definition.Expression)));
                    auto map = MakeIntrusive<TOpMap>(input(), pos, std::move(definitions));
                    // Projected membership needs a value, including SQL UNKNOWN.
                    // Construct that input here; the optimizer's IN/EXISTS rules
                    // retain their original filter-only matching policy.
                    for (const auto call : map->GetSubplanIUs(props.Subplans)) {
                        const auto kind = props.Subplans.At(call).Type;
                        if (kind != ESubplanType::IN_SUBPLAN && kind != ESubplanType::EXISTS) continue;
                        for (const auto& item : IterateSubtreeWithSubplans(map.Get(), props)) {
                            if (!item.Current->NativeSchema) ComputeNativeTypes(*item.Current, props);
                        }
                        const bool nativeEqualNulls = config && config->GetUseBlockHashJoin()
                            && config->GetEnableBlockHashJoinEqualNulls();
                        map = MakeIntrusive<TOpMap>(BuildInExistsValue(map->GetInput(), call,
                            pos, props, nativeEqualNulls), pos, map->GetMapElements());
                    }
                    op = std::move(map);
                    break;
                }
                case R::Capture: {
                    TDependencyIUs captures;
                    for (const auto& capture : relation.Captures) captures.Add(capture.Local,
                        TCapturedIU{capture.Outer, program.Columns.at(capture.Local).Type});
                    op = MakeIntrusive<TOpAddDependencies>(input(), pos, std::move(captures)); break;
                }
                case R::Filter: op = MakeIntrusive<TOpFilter>(input(), pos, expression(*relation.Predicate)); break;
                case R::Join: {
                    TJoinIUs keys;
                    TVector<TExpression> filters;
                    auto left = input(), right = input(1);
                    if (relation.Predicate) {
                        for (auto predicate : expression(*relation.Predicate).SplitConjunct()) {
                            if (predicate.MaybeEquiJoinCondition()) {
                                TEquiJoinCondition equality(predicate);
                                auto a = equality.GetLeftIU(), b = equality.GetRightIU();
                                if (left->GetOutputIUs().Contains(b) && right->GetOutputIUs().Contains(a)) std::swap(a, b);
                                if (left->GetOutputIUs().Contains(a) && right->GetOutputIUs().Contains(b)) {
                                    keys.Add({a, b, false}); continue;
                                }
                            }
                            filters.push_back(std::move(predicate));
                        }
                    }
                    auto join = MakeIntrusive<TOpJoin>(left, right, pos, JoinName(relation.JoinKind), std::move(keys), std::move(filters));
                    PrepareJoinKeys(*join, props);
                    op = std::move(join);
                    break;
                }
                case R::Aggregate: {
                    if (relation.GroupingSets.empty()) {
                        TOrderedIUs<> keys; for (auto key : relation.GroupKeys) keys.Append(key);
                        TAggregationIUs aggregates;
                        for (const auto& aggregate : relation.Aggregates) aggregates.Add(aggregate.Output,
                            TOpAggregationTraits{aggregate.Input, AggregateName(aggregate.Function), aggregate.Distinct, false,
                                aggregate.Function == "avg" ? std::optional{program.Columns[aggregate.Output].Type} : std::nullopt});
                        op = MakeIntrusive<TOpAggregate>(input(), std::move(aggregates), std::move(keys), EOpPhase::Undefined, relation.DistinctAll, pos);
                        break;
                    }
                    op = BuildGroupingSetBranches(relation, program.Columns, input(), props, pos);
                    break;
                }
                case R::Sort: {
                    // RBO's existing SQL entry records NULL as the first base
                    // comparison value even for descending order. Preserve that
                    // construction convention and its read-pushdown decisions.
                    TSortIUs keys; for (const auto& key : relation.SortKeys) keys.Append(key.Column, TSortOrder{!key.Descending, true});
                    op = MakeIntrusive<TOpSort>(input(), pos, std::move(keys), std::nullopt); break;
                }
                case R::Window: {
                    TWindowIUs functions;
                    for (const auto& function : relation.WindowFunctions) {
                        TOrderedIUs<> arguments;
                        if (function.Input) arguments.Append(*function.Input);
                        functions.Add(function.Output, TOpWindowFunc{String(function.Function),
                            function.Input ? EWindowFuncKind::Aggregate : EWindowFuncKind::Native, std::move(arguments)});
                    }
                    TOrderedIUs<> partitions; for (auto key : relation.GroupKeys) partitions.Append(key);
                    TSortIUs order; for (const auto& key : relation.SortKeys) order.Append(key.Column, TSortOrder{!key.Descending, true});
                    auto bound = [](NFrontend::EWindowBound kind) {
                        using B = NFrontend::EWindowBound;
                        switch (kind) {
                            case B::UnboundedPreceding: return EWindowFrameBound::UnboundedPreceding;
                            case B::Preceding: return EWindowFrameBound::Preceding;
                            case B::CurrentRow: return EWindowFrameBound::CurrentRow;
                            case B::Following: return EWindowFrameBound::Following;
                            case B::UnboundedFollowing: return EWindowFrameBound::UnboundedFollowing;
                        }
                        Y_UNREACHABLE();
                    };
                    const auto& frame = relation.WindowFrame;
                    Y_ENSURE(frame.Type == NFrontend::EWindowFrame::Rows || frame.Type == NFrontend::EWindowFrame::Range);
                    op = MakeIntrusive<TOpWindow>(input(), pos, std::move(functions), std::move(partitions), std::move(order),
                        TOpWindowFrame{frame.Type == NFrontend::EWindowFrame::Rows ? EWindowFrameType::Rows : EWindowFrameType::Range,
                            bound(frame.Begin), frame.BeginOffset, bound(frame.End), frame.EndOffset});
                    break;
                }
                case R::Limit: {
                    if (relation.Offset) op = MakeIntrusive<TOpLimit>(input(), pos, expression(*relation.Count), expression(*relation.Offset), EOpPhase::Undefined);
                    else op = MakeIntrusive<TOpLimit>(input(), pos, expression(*relation.Count), EOpPhase::Undefined);
                    break;
                }
                case R::UnionAll: {
                    TVector<TIntrusivePtr<IOperator>> inputs; for (auto child : relation.Inputs) inputs.push_back(converted.at(child));
                    TUnionAllIUs columns(TUnionInputPolicy{inputs.size()});
                    for (std::size_t i = 0; i < relation.Columns.size(); ++i) {
                        TUnionInputRow row; for (const auto& branch : relation.UnionInputs) row.Inputs.push_back(branch[i]);
                        columns.Add(relation.Columns[i], std::move(row));
                    }
                    op = MakeIntrusive<TOpUnionAll>(std::move(inputs), pos, std::move(columns), false); break;
                }
            }
            op->GetOutputIUs();
            converted[id] = op;
            if (const auto call = subplanBindings.find(id); call != subplanBindings.end()) {
                const auto& subplan = program.Subplans.at(call->second);
                const auto kind = subplan.Kind == NFrontend::ESubquery::Exists ? ESubplanType::EXISTS :
                    subplan.Kind == NFrontend::ESubquery::In ? ESubplanType::IN_SUBPLAN : ESubplanType::EXPR;
                TOrderedIUs<> tuple;
                if (subplan.Lookup) tuple.Append(*subplan.Lookup);
                props.Subplans.Add(call->second, std::move(op), kind, std::move(tuple),
                    kind != ESubplanType::EXISTS ? std::optional<TInfoUnitId>{subplan.Result} : std::nullopt);
                props.Subplans.RefreshDependencies(call->second);
            }
        }
        TOrderedIUs<TString> columns;
        for (const auto& column : result.Columns) columns.Append(column.Column, String(column.Name));
        auto root = MakeIntrusive<TOpRoot>(converted.at(result.Root), converted.at(result.Root)->Pos, std::move(columns));
        props.PhysicalTxGroup = executionRoot.Phase;
        if (executionRoot.MaterializedInput) props.MaterializedResult = "$" + String(*executionRoot.MaterializedInput);
        props.EffectPriority = roots.size();
        if (!props.MaterializedResult && (!props.WithEffects || props.WithReturning)) {
            props.ResultIndex = query.Execution.DiscardResults.size();
            query.Execution.DiscardResults.push_back(false);
        }
        root->PlanProps = std::move(props);
        for (const auto& item : IterateSubtreeWithSubplans(root.Get(), root->PlanProps)) item.Current->BindExpressionPlanProps(&root->PlanProps);
        root->RecomputeOutputIUsSubtree(); root->ComputeParents();
        if (!requestedIndexes.empty()) indexReads.emplace(root.Get(), std::move(requestedIndexes));
        roots.push_back(std::move(root));
    }
    THashSet<TString> readTables;
    for (const auto& root : roots) for (const auto& item : IterateSubtreeWithSubplans(root.Get(), root->PlanProps)) {
        if (item.Current->Kind != EOperator::Source) continue;
        const auto& table = CastOperator<TOpRead>(*item.Current).GetTable();
        if (readTables.insert(table.Path).second) {
            auto& operation = query.Execution.TableOperations.emplace_back();
            const auto position = ctx.GetPosition(item.Current->Pos);
            operation.MutablePosition()->SetRow(position.Row);
            operation.MutablePosition()->SetColumn(position.Column);
            operation.SetTable(table.Path);
            operation.SetOperation(static_cast<ui32>(NYql::TYdbOperation::Select));
        }
    }
    // Authorization refers to SQL tables, even when storage reads their private
    // index implementation. Record operations before constructing those reads.
    for (auto& [root, reads] : indexReads) PrepareNativeIndexReads(*root, reads);
    for (const auto& relation : program.Relations) if (relation.Effect) {
        const auto& effect = *relation.Effect;
        using W = NFrontend::EWriteKind;
        using O = NYql::TYdbOperation;
        const auto kind = effect.Kind == W::Insert ? (effect.OnConflict == "revert" ? O::InsertRevert : O::InsertAbort) :
            effect.Kind == W::Replace ? O::Replace : effect.Kind == W::Upsert ? O::Upsert :
            effect.Kind == W::Update ? (effect.InputRows ? O::UpdateOn : O::Update) : (effect.InputRows ? O::DeleteOn : O::Delete);
        auto& operation = query.Execution.TableOperations.emplace_back();
        operation.MutablePosition()->SetRow(relation.Position.Line);
        operation.MutablePosition()->SetColumn(relation.Position.Column);
        operation.SetTable(tables.at(relation.Table).Table.Path);
        operation.SetOperation(static_cast<ui32>(kind));
    }
    // Execution preparation assigns statement and materialization dependencies.
    // Result indices preserve public source order across physical phases.
    std::stable_sort(roots.begin(), roots.end(), [](const auto& a, const auto& b) {
        return a->PlanProps.PhysicalTxGroup < b->PlanProps.PhysicalTxGroup;
    });
    return query;
}

} // namespace NKikimr::NKqp
