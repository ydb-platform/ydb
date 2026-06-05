#include "kqp_operator.h"

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
        op->RenameIUs(renameMap, ctx);
    }

    return hasRenamedExternalChild;
}

} // anonymous namespace

bool TSubplans::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx) {
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

void IOperator::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                          const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(renameMap);
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);
}

void TOpRead::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                        const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);

    for (auto& column : OutputIUs) {
        const auto it = renameMap.find(column);
        if (it != renameMap.end()) {
            column = it->second;
        }
    }
}

void TOpMap::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                       const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(ctx);
    TVector<TMapElement> newMapElements;

    for (const auto& el : MapElements) {
        TInfoUnit newIU = el.GetElementName();
        const auto it = renameMap.find(newIU);
        if (it != renameMap.end()) {
            newIU = it->second;
        }

        if (el.IsRename()) {
            auto expr = el.GetExpression();
            auto from = el.GetRename();
            if (renameMap.contains(from) && !stopList.contains(from)) {
                from = renameMap.at(from);
            }
            newMapElements.emplace_back(newIU, MakeColumnAccess(from, Pos, expr.Ctx, expr.PlanProps), true);
        } else {
            auto expr = el.GetExpression();
            auto newBody = expr.ApplyRenames(renameMap);
            newMapElements.emplace_back(newIU, newBody);
        }
    }
    MapElements = std::move(newMapElements);
}

void TOpAddDependencies::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                                   const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);
    RenameInfoUnits(Dependencies, renameMap);
}

void TOpFilter::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                          const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);
    FilterExpr = FilterExpr.ApplyRenames(renameMap);
}

void TOpJoin::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                        const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);

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

void TOpLimit::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                         const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);
    LimitCond = LimitCond.ApplyRenames(renameMap);
    if (OffsetCond) {
        OffsetCond = OffsetCond->ApplyRenames(renameMap);
    }
}

void TOpSort::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                        const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);
    TVector<TSortElement> newSortElements;
    for (const auto& element : SortElements) {
        TInfoUnit newIU(element.SortColumn);

        const auto it = renameMap.find(newIU);
        if (it != renameMap.end()) {
            newIU = it->second;
        }

        auto sortElement = TSortElement(element);
        sortElement.SortColumn = newIU;
        newSortElements.push_back(sortElement);
    }

    if (LimitCond.has_value()) {
        LimitCond = LimitCond->ApplyRenames(renameMap);
    }
    SortElements = std::move(newSortElements);
}

void TOpAggregate::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                             const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    Y_UNUSED(ctx);
    Y_UNUSED(stopList);

    for (auto& column : KeyColumns) {
        const auto it = renameMap.find(column);
        if (it != renameMap.end()) {
            column = it->second;
        }
    }
    for (auto& trait : AggregationTraitsList) {
        if (renameMap.contains(trait.OriginalColName) && !stopList.contains(trait.OriginalColName)) {
            trait.OriginalColName = renameMap.at(trait.OriginalColName);
        }
        if (renameMap.contains(trait.ResultColName)) {
            trait.ResultColName = renameMap.at(trait.ResultColName);
        }
    }
}

void TOpCBOTree::RenameIUs(const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap, TExprContext& ctx,
                           const THashSet<TInfoUnit, TInfoUnit::THashFunction>& stopList) {
    for (auto op : TreeNodes) {
        op->RenameIUs(renameMap, ctx, stopList);
    }
}

} // namespace NKqp
} // namespace NKikimr
