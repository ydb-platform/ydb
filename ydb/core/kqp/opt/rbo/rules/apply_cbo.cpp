#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_cbo.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_join_cost_based.h>
#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_make_join_hypergraph.h>
#include <library/cpp/iterator/zip.h>
#include <typeinfo>
#include <bitset>
#include <limits>
#include <optional>

namespace {
using namespace NKikimr;
using namespace NKikimr::NKqp;

struct TShuffleEliminationContext {
    TSimpleSharedPtr<TOrderingsStateMachine> FSM;
    TTableAliasMap TableAliasMap;
};

constexpr size_t MaxShuffleEliminationRelationCount = 256;

TShuffleEliminationContext BuildShuffleEliminationContext(
    TIntrusivePtr<TOpCBOTree>& cboTree,
    const std::shared_ptr<TJoinOptimizerNode>& joinTree,
    TVector<std::shared_ptr<TRelOptimizerNode>>& rels)
{
    TFDStorage fdStorage;
    TTableAliasMap tableAliasMap;
    auto& rootLineage = cboTree->TreeRoot->Props.Metadata->ColumnLineage;

    // -- Build table alias map --------------------------------------
    THashSet<TString> addedAliases;
    for (const auto& [iu, entry] : rootLineage.Mapping) {
        auto alias = entry.GetCannonicalAlias();
        if (addedAliases.insert(alias).second) {
            tableAliasMap.AddMapping(entry.TableName, alias);
        }
    }

    auto resolveColumn = [&](const TInfoUnit& column) -> TInfoUnit {
        auto& mapping = rootLineage.Mapping;
        if (mapping.contains(column)) {
            return mapping.at(column).GetInfoUnit();
        }
        return column;
    };

    // -- Collect interesting orderings & FDs ------------------------
    // The original CBO tree can group several join predicates in one operator,
    // while MakeJoinHypergraph splits them by relation pair and adds transitive
    // closure edges. DPHyp edge ordering indexes must be looked up in an FSM
    // built from that same shape.
    auto hypergraph = MakeJoinHypergraph<std::bitset<256>>(joinTree, {}, false);
    for (const auto& edge : hypergraph.GetEdges()) {
        for (const auto& [lhs, rhs] : Zip(edge.LeftJoinKeys, edge.RightJoinKeys)) {
            fdStorage.AddFD(lhs, rhs, TFunctionalDependency::EEquivalence, false, &tableAliasMap);
        }

        fdStorage.AddInterestingOrdering(edge.LeftJoinKeys, TOrdering::EShuffle, &tableAliasMap);
        fdStorage.AddInterestingOrdering(edge.RightJoinKeys, TOrdering::EShuffle, &tableAliasMap);
    }

    // -- Collect base-table shufflings & sortings -------------------
    // Resolve once, cache for reuse during rel initialization below.
    TVector<TVector<TJoinColumn>> resolvedChildShufflings(cboTree->Children.size());

    for (size_t i = 0; i < cboTree->Children.size(); ++i) {
        const auto& child = cboTree->Children[i];
        if (!child->Props.Metadata.has_value()) {
            continue;
        }
        const auto& metadata = *child->Props.Metadata;

        if (!metadata.ShuffledByColumns.empty()) {
            auto& shuffledBy = resolvedChildShufflings[i];
            shuffledBy.reserve(metadata.ShuffledByColumns.size());
            for (const auto& col : metadata.ShuffledByColumns) {
                auto mapped = resolveColumn(col);
                shuffledBy.emplace_back(mapped.GetAlias(), mapped.GetColumnName());
            }
            fdStorage.AddShuffling(TShuffling(shuffledBy), &tableAliasMap);
        }

        if (!metadata.KeyColumns.empty()) {
            TVector<TJoinColumn> sortedBy;
            sortedBy.reserve(metadata.KeyColumns.size());
            for (const auto& col : metadata.KeyColumns) {
                auto mapped = resolveColumn(col);
                sortedBy.emplace_back(mapped.GetAlias(), mapped.GetColumnName());
            }
            TVector<TOrdering::TItem::EDirection> dirs(
                sortedBy.size(), TOrdering::TItem::EDirection::EAscending);
            fdStorage.AddSorting(TSorting(sortedBy, dirs), &tableAliasMap);
        }
    }

    // -- Build the FSM ----------------------------------------------
    auto fsm = MakeSimpleShared<TOrderingsStateMachine>(
        std::move(fdStorage), TOrdering::EType::EShuffle);

    // -- Seed each rel's LogicalOrderings from cached shufflings ----
    for (size_t i = 0; i < resolvedChildShufflings.size(); ++i) {
        if (resolvedChildShufflings[i].empty()) {
            continue;
        }
        auto orderingIdx = fsm->FDStorage.FindShuffling(
            TShuffling(resolvedChildShufflings[i]), &tableAliasMap);
        if (orderingIdx != std::numeric_limits<std::size_t>::max()) {
            rels[i]->Stats.LogicalOrderings = fsm->CreateState(orderingIdx);
        }
    }

    // -- Log orderings FSM that we created --------------------------
    if (NYql::NLog::YqlLogger().NeedToLog(
            NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        YQL_CLOG(TRACE, CoreDq) << "\nShufflings FSM: " << fsm->ToString();
    }

    return {std::move(fsm), std::move(tableAliasMap)};
}

// To use DP CBO, we need to use the column lineage map and map all variables in join condition to
// original aliases and column names in order to correctly use column statistics and shuffle elimination
//
// However, the same alias can appear multiple times in a query, but might already be out of scope
// So we first collect all join conditions and fetch aliases and mappings only for the columns used in join conditions

std::shared_ptr<TJoinOptimizerNode> ConvertJoinTree(TIntrusivePtr<TOpCBOTree>& cboTree, TVector<std::shared_ptr<TRelOptimizerNode>>& rels) {
    THashSet<TInfoUnit, TInfoUnit::THashFunction> allJoinColumns;
    std::shared_ptr<TJoinOptimizerNode> result;

    auto lineage = cboTree->TreeRoot->Props.Metadata->ColumnLineage;
    int fakeAliasId = 0;

    for (auto op : cboTree->TreeNodes) {
        auto joinOp = CastOperator<TOpJoin>(op);
        for (const auto& [left, right] : joinOp->JoinKeys) {
            allJoinColumns.insert(left);
            allJoinColumns.insert(right);
        }
    }

    THashMap<IOperator*, std::shared_ptr<IBaseOptimizerNode>> nodeMap;

    // Build rels for CBO. Rel contains a set of aliases and statistics object
    for (auto child : cboTree->Children) {

        TVector<TString> childAliases;
        auto childIUs = child->GetOutputIUs();

        for (auto col : allJoinColumns) {
            if (std::find(childIUs.begin(), childIUs.end(), col) != childIUs.end()) {
                if (auto it = lineage.Mapping.find(col); it != lineage.Mapping.end()) {
                    auto alias = it->second.GetCannonicalAlias();
                    if (std::find(childAliases.begin(), childAliases.end(), alias) == childAliases.end()) {
                        childAliases.push_back(alias);
                    }
                }
            }
        }
        // If there is a real cross join in the plan with no conditions just create a fake alias
        if (childAliases.empty()) {
            childAliases.push_back("#fake_alias" + std::to_string(fakeAliasId++));
        }

        TVector<TInfoUnit> mappedKeyColumns;
        for (const auto& col : child->Props.Metadata->KeyColumns) {
            mappedKeyColumns.push_back(cboTree->TreeRoot->Props.Metadata->MapColumn(col));
        }

        auto stats = BuildOptimizerStatistics(child->Props, true, mappedKeyColumns);
        auto relNode = std::make_shared<TRBORelOptimizerNode>(childAliases, stats, child);
        rels.push_back(relNode);
        nodeMap.insert({child.get(), relNode});
    }

    for (auto node : cboTree->TreeNodes) {
        auto join = CastOperator<TOpJoin>(node);
        auto leftNode = nodeMap.at(join->GetLeftInput().get());
        auto rightNode = nodeMap.at(join->GetRightInput().get());
        TVector<TJoinColumn> leftKeys;
        TVector<TJoinColumn> rightKeys;

        for (auto [leftKey, rightKey] : join->JoinKeys) {
            auto mappedLeftKey = cboTree->TreeRoot->Props.Metadata->MapColumn(leftKey);
            auto mappedRightKey = cboTree->TreeRoot->Props.Metadata->MapColumn(rightKey);

            leftKeys.push_back(TJoinColumn(mappedLeftKey.GetAlias(), mappedLeftKey.GetColumnName()));
            rightKeys.push_back(TJoinColumn(mappedRightKey.GetAlias(), mappedRightKey.GetColumnName()));
        }

        result = std::make_shared<TJoinOptimizerNode>(leftNode,
            rightNode,
            leftKeys,
            rightKeys,
            ConvertToJoinKind(join->JoinKind),
            NKikimr::NKqp::EJoinAlgoType::Undefined,
            false,
            false,
            false);

        nodeMap.insert({join.get(), result});
    }

    return result;
}

using TInfoUnitSet = THashSet<TInfoUnit, TInfoUnit::THashFunction>;

TInfoUnitSet BuildOutputIUSet(const TIntrusivePtr<IOperator>& input) {
    TInfoUnitSet result;
    for (const auto& column : input->GetOutputIUs()) {
        result.insert(column);
    }
    return result;
}

TInfoUnit ConvertJoinColumn(const TJoinColumn& column, const TColumnLineage& lineage, const TInfoUnitSet& visibleColumns) {
    const auto original = TInfoUnit(column.RelName, column.AttributeName);
    if (visibleColumns.contains(original)) {
        return original;
    }

    if (const auto it = lineage.ReverseMapping.find(original);
        it != lineage.ReverseMapping.end() && visibleColumns.contains(it->second)) {

        return it->second;
    }

    std::optional<TInfoUnit> resolved;
    for (const auto& [unit, entry] : lineage.Mapping) {
        const bool matchesLineageColumn =
            entry.ColumnName == column.AttributeName &&
            (entry.TableName == column.RelName ||
             entry.GetCannonicalAlias() == column.RelName ||
             entry.GetRawAlias() == column.RelName
            );

        if (!visibleColumns.contains(unit) || !matchesLineageColumn) {
            continue;
        }

        Y_ENSURE(!resolved || *resolved == unit, "Ambiguous CBO column mapping for NEW RBO input");
        resolved = unit;
    }

    Y_ENSURE(resolved, "Could not map CBO column back to NEW RBO input");
    return *resolved;
}

TVector<TInfoUnit> ConvertJoinColumns(const TVector<TJoinColumn>& columns, const TColumnLineage& lineage, const TInfoUnitSet& visibleColumns) {
    TVector<TInfoUnit> result;
    result.reserve(columns.size());

    for (const auto& column : columns) {
        result.push_back(ConvertJoinColumn(column, lineage, visibleColumns));
    }

    return result;
}

TIntrusivePtr<IOperator> ConvertOptimizedTree(std::shared_ptr<IBaseOptimizerNode> tree, const TColumnLineage& lineage, TPositionHandle pos) {
    if (tree->Kind == RelNodeType) {
        auto rel = std::static_pointer_cast<TRBORelOptimizerNode>(tree);
        return rel->Op;
    } else {
        auto join = std::static_pointer_cast<TJoinOptimizerNode>(tree);
        auto leftArg = ConvertOptimizedTree(join->LeftArg, lineage, pos);
        auto rightArg = ConvertOptimizedTree(join->RightArg, lineage, pos);

        const auto leftVisibleColumns = BuildOutputIUSet(leftArg);
        const auto rightVisibleColumns = BuildOutputIUSet(rightArg);

        Y_ENSURE(join->LeftJoinKeys.size() == join->RightJoinKeys.size());

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        for (size_t i=0; i<join->LeftJoinKeys.size(); i++) {
            auto leftKey = ConvertJoinColumn(join->LeftJoinKeys[i], lineage, leftVisibleColumns);
            auto rightKey = ConvertJoinColumn(join->RightJoinKeys[i], lineage, rightVisibleColumns);
            joinKeys.push_back(std::make_pair(leftKey, rightKey));
        }

        auto joinKind = ConvertToJoinString(join->JoinType);

        auto res = MakeIntrusive<TOpJoin>(leftArg, rightArg, pos, joinKind, joinKeys);

        // JoinAlgo is optional, set it only if CBO ran and decided on an algo.
        // Otherwise MaybeSetJoinAlgo can see that it's std::nullopt and set it to the default.
        if (join->JoinAlgo != NKikimr::NKqp::EJoinAlgoType::Undefined) {
            res->Props.JoinAlgo = join->JoinAlgo;
        }

        if (join->JoinAlgo == NKikimr::NKqp::EJoinAlgoType::GraceJoin) {
            res->Props.LeftShuffleBy = ConvertJoinColumns(join->ShuffleLeftSideBy, lineage, leftVisibleColumns);
            res->Props.RightShuffleBy = ConvertJoinColumns(join->ShuffleRightSideBy, lineage, rightVisibleColumns);
        }
        return res;
    }
}
}

namespace NKikimr {
namespace NKqp {


/**
 * Run dynamic programming CBO and convert the resulting tree into operator tree
 *
 * In order to support good CBO with pg syntax, where all the variables in the joins
 * are transformed into Pg types, we remap the synthenic variables back into original ones
 * to run the CBO, and then map them back
 */
TIntrusivePtr<IOperator> TOptimizeCBOTreeRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(props);

    if (input->Kind != EOperator::CBOTree) {
        return input;
    }

    auto& Config = ctx.KqpCtx.Config;
    auto optLevel = Config->CostBasedOptimizationLevel.Get().GetOrElse(Config->GetDefaultCostBasedOptimizationLevel());
    auto useBlockHashJoin = Config->UseBlockHashJoin.Get().GetOrElse(false);

    if (optLevel <= 1) {
        return input;
    }

    auto cboTree = CastOperator<TOpCBOTree>(input);

    // Check that all inputs have statistics
    for (auto c : cboTree->Children) {
        if (!c->Props.Statistics.has_value()) {
            ctx.ExprCtx.AddWarning(
                YqlIssue(ctx.ExprCtx.GetPosition(cboTree->Pos), TIssuesIds::CBO_MISSING_TABLE_STATS,
                "Cost Based Optimizer could not be applied to this query: couldn't load statistics"
            )
        );
            return input;
        }
    }

    TVector<std::shared_ptr<TRelOptimizerNode>> rels;
    auto joinTree = ConvertJoinTree(cboTree, rels);

    bool allRowStorage = std::any_of(
        rels.begin(),
        rels.end(),
        [](std::shared_ptr<TRelOptimizerNode>& r) {return r->Stats.StorageType==EStorageType::RowStorage; });

    if (optLevel == 2 && allRowStorage) {
        return input;
    }

    TCBOSettings settings{
        .CBOTimeout = Config->CBOTimeout.Get().GetOrElse(NKikimr::NKqp::TCBOSettings{}.CBOTimeout),
        .CBOHardTimeout = Config->CBOHardTimeout.Get().GetOrElse(NKikimr::NKqp::TCBOSettings{}.CBOHardTimeout),
        .ShuffleEliminationJoinNumCutoff = Config->ShuffleEliminationJoinNumCutoff.Get().GetOrElse(TDqSettings::TDefault::ShuffleEliminationJoinNumCutoff)
    };

    bool enableShuffleElimination = ctx.KqpCtx.Config->OptShuffleElimination.Get().GetOrElse(ctx.KqpCtx.Config->GetDefaultEnableShuffleElimination());

    const bool canBuildShuffleCtx = rels.size() <= MaxShuffleEliminationRelationCount;
    std::optional<TShuffleEliminationContext> shuffleCtx;
    if (enableShuffleElimination && canBuildShuffleCtx) {
        shuffleCtx.emplace(BuildShuffleEliminationContext(cboTree, joinTree, rels));
    } else if (enableShuffleElimination) {
        YQL_CLOG(TRACE, CoreDq)
            << "Shuffle elimination disabled for CBO tree with " << rels.size()
            << " relations; maximum supported relation count is "
            << MaxShuffleEliminationRelationCount;
    }

    auto providerCtx = TRBOProviderContext(ctx.KqpCtx, optLevel, useBlockHashJoin);
    auto opt = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(
        providerCtx, settings, ctx.ExprCtx,
        enableShuffleElimination && canBuildShuffleCtx,
        shuffleCtx ? shuffleCtx->FSM : nullptr,
        shuffleCtx ? &shuffleCtx->TableAliasMap : nullptr)
    );

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Converted join tree:\n";
        joinTree->Print(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    {
        YQL_PROFILE_SCOPE(TRACE, "CBO");
        joinTree = opt->JoinSearch(joinTree, ctx.KqpCtx.GetOptimizerHints());
    }

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Optimizied join tree:\n";
        joinTree->Print(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    return ConvertOptimizedTree(joinTree, cboTree->Props.Metadata->ColumnLineage, cboTree->Pos);
}

}
}
