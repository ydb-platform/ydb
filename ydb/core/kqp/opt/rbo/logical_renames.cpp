#include "kqp_operator.h"
#include "kqp_rbo_utils.h"

namespace NKikimr {
namespace NKqp {

namespace {

TInfoUnit RenameInfoUnit(const TInfoUnit& iu, const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap) {
    const auto it = renameMap.find(iu);
    return it == renameMap.end() ? iu : it->second;
}

bool RenameInfoUnitInPlace(TInfoUnit& iu, const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap) {
    const auto renamed = RenameInfoUnit(iu, renameMap);
    if (renamed == iu) {
        return false;
    }

    iu = renamed;
    return true;
}

bool RenameInfoUnits(TVector<TInfoUnit>& ius, const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap) {
    bool changed = false;
    for (auto& iu : ius) {
        changed |= RenameInfoUnitInPlace(iu, renameMap);
    }
    return changed;
}

void RenameMapRenameSources(TOpMap& map, const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap) {
    for (auto& el : map.MapElements) {
        if (!el.IsRename()) {
            continue;
        }

        const auto from = el.GetRename();
        const auto it = renameMap.find(from);
        if (it == renameMap.end()) {
            continue;
        }

        auto expr = el.GetExpression();
        el.SetExpression(MakeColumnAccess(it->second, map.Pos, expr.Ctx, expr.PlanProps));
    }
}

void RenameSubplanLocalReferences(
    const TIntrusivePtr<IOperator>& op,
    const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap,
    TExprContext& ctx)
{
    if (op->Kind == EOperator::CBOTree) {
        for (const auto& treeOp : CastOperator<TOpCBOTree>(op)->TreeNodes) {
            RenameSubplanLocalReferences(treeOp, renameMap, ctx);
        }
        return;
    }

    op->RenameProducedIUs(renameMap, ctx);
    op->RenameUsedIUs(renameMap, ctx);

    if (op->Kind == EOperator::Map) {
        RenameMapRenameSources(*CastOperator<TOpMap>(op), renameMap);
    }
}

bool RenameExternalSubplanReferences(
    const TIntrusivePtr<IOperator>& op,
    const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap,
    TExprContext& ctx)
{
    if (!op) {
        return false;
    }

    if (op->Kind == EOperator::AddDependencies) {
        auto addDeps = CastOperator<TOpAddDependencies>(op);
        return RenameInfoUnits(addDeps->Dependencies, renameMap);
    }

    bool hasRenamedExternalChild = false;
    for (const auto& child : op->Children) {
        hasRenamedExternalChild |= RenameExternalSubplanReferences(child, renameMap, ctx);
    }

    if (hasRenamedExternalChild) {
        RenameSubplanLocalReferences(op, renameMap, ctx);
    }

    return hasRenamedExternalChild;
}

} // anonymous namespace

bool TSubplans::RenameReferences(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    if (renameMap.empty() || PlanMap.empty()) {
        return false;
    }

    THashMap<TInfoUnit, TSubplanEntry, TInfoUnit::THashFunction> renamedPlanMap;
    TVector<TInfoUnit> renamedOrderedList;
    renamedOrderedList.reserve(OrderedList.size());
    bool changed = false;

    for (const auto& iu : OrderedList) {
        auto entry = PlanMap.at(iu);
        const auto renamedIU = RenameInfoUnit(iu, renameMap);

        changed |= renamedIU != iu;

        const auto renamedEntryIU = RenameInfoUnit(entry.IU, renameMap);
        changed |= renamedEntryIU != entry.IU;
        entry.IU = renamedEntryIU;

        changed |= RenameInfoUnits(entry.Tuple, renameMap);
        changed |= RenameInfoUnits(entry.DependentIUs, renameMap);
        changed |= RenameExternalSubplanReferences(CastOperator<IOperator>(entry.Plan), renameMap, ctx);

        const auto inserted = renamedPlanMap.emplace(renamedIU, std::move(entry)).second;
        Y_ENSURE(inserted, "Subplan rename produced duplicate binding " << renamedIU.GetFullName());
        renamedOrderedList.push_back(renamedIU);
    }

    PlanMap = std::move(renamedPlanMap);
    OrderedList = std::move(renamedOrderedList);
    return changed;
}

void IOperator::RenameProducedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    Y_UNUSED(renameMap);
    Y_UNUSED(ctx);
}

void IOperator::RenameUsedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    Y_UNUSED(renameMap);
    Y_UNUSED(ctx);
}

void TOpRead::RenameProducedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    Y_UNUSED(ctx);
    RenameInfoUnits(OutputIUs, renameMap);
}

void TOpMap::RenameProducedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    Y_UNUSED(ctx);

    for (auto& el : MapElements) {
        const auto it = renameMap.find(el.GetElementName());
        if (it != renameMap.end()) {
            el.SetElementName(it->second);
        }
    }
}

void TOpMap::RenameUsedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    Y_UNUSED(ctx);

    for (auto& el : MapElements) {
        if (!el.IsRename()) {
            el.SetExpression(el.GetExpression().ApplyRenames(renameMap));
        }
    }
}

void TOpAddDependencies::RenameProducedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    Y_UNUSED(ctx);
    RenameInfoUnits(Dependencies, renameMap);
}

void TOpFilter::RenameUsedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    Y_UNUSED(ctx);
    FilterExpr = FilterExpr.ApplyRenames(renameMap);
}

void TOpJoin::RenameUsedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    Y_UNUSED(ctx);

    for (auto& k : JoinKeys) {
        if (renameMap.contains(k.first)) {
            k.first = renameMap.at(k.first);
        }
        if (renameMap.contains(k.second)) {
            k.second = renameMap.at(k.second);
        }
    }

    for (auto& filter : JoinFilters) {
        filter = filter.ApplyRenames(renameMap);
    }
}

void TOpUnionAll::RenameProducedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    Y_UNUSED(ctx);
    RenameInfoUnits(Columns, renameMap);
}

void TOpLimit::RenameUsedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    Y_UNUSED(ctx);
    LimitCond = LimitCond.ApplyRenames(renameMap);
    if (OffsetCond) {
        OffsetCond = OffsetCond->ApplyRenames(renameMap);
    }
}

void TOpSort::RenameUsedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    Y_UNUSED(ctx);

    for (auto& element : SortElements) {
        const auto it = renameMap.find(element.SortColumn);
        if (it != renameMap.end()) {
            element.SortColumn = it->second;
        }
    }

    if (LimitCond.has_value()) {
        LimitCond = LimitCond->ApplyRenames(renameMap);
    }
}

void TOpAggregate::RenameProducedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    Y_UNUSED(ctx);

    const auto oldKeyColumns = DistinctAll ? KeyColumns : TVector<TInfoUnit>{};
    RenameInfoUnits(KeyColumns, renameMap);
    for (auto& trait : AggregationTraitsList) {
        if (DistinctAll && ContainsInfoUnit(oldKeyColumns, trait.OriginalColName)) {
            RenameInfoUnitInPlace(trait.OriginalColName, renameMap);
        }
        if (renameMap.contains(trait.ResultColName)) {
            trait.ResultColName = renameMap.at(trait.ResultColName);
        }
    }
}

void TOpAggregate::RenameUsedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    Y_UNUSED(ctx);

    if (DistinctAll) {
        return;
    }

    for (auto& trait : AggregationTraitsList) {
        if (renameMap.contains(trait.OriginalColName)) {
            trait.OriginalColName = renameMap.at(trait.OriginalColName);
        }
    }
}

void TOpCBOTree::RenameProducedIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
    for (auto op : TreeNodes) {
        op->RenameProducedIUs(renameMap, ctx);
    }
}

} // namespace NKqp
} // namespace NKikimr
