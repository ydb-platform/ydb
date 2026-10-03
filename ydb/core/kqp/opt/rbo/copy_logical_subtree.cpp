#include "kqp_operator.h"

namespace NKikimr::NKqp {
namespace {

TInfoUnitId CopyDefinition(TInfoUnitId id, TInfoUnitRegistry& registry, TSubstitutions& renames) {
    if (const auto* renamed = renames.Find(id)) {
        return *renamed;
    }
    return renames.Add(id, registry.AddCopy(id));
}

// A column label names the physical field: a requested ID with another label
// is turned down, leaving the caller to bind it to the fresh copy.
TUnorderedIUs CopyColumns(const TUnorderedIUs& ids, TInfoUnitRegistry& registry, TSubstitutions& renames) {
    TUnorderedIUs result;
    for (const auto id : ids) {
        auto* renamed = renames.Find(id);
        if (!renamed) {
            result.Add(renames.Add(id, registry.AddCopy(id)));
        } else if (registry.Get(*renamed) == registry.Get(id)) {
            result.Add(*renamed);
        } else {
            *renamed = registry.AddCopy(id);
            result.Add(*renamed);
        }
    }
    return result;
}

template <class T>
T CopyDefinitions(const T& definitions, TInfoUnitRegistry& registry, TSubstitutions& renames) {
    T result(definitions.Policy());
    for (const auto& [id, value] : definitions.Items()) {
        result.Add(CopyDefinition(id, registry, renames), value);
    }
    return result;
}

} // namespace

TIntrusivePtr<IOperator> IOperator::Copy(TInfoUnitRegistry& registry, TSubstitutions& renames) const {
    // Children first, in order, so that IDs are allocated deterministically.
    TVector<TIntrusivePtr<IOperator>> inputs;
    for (const auto* child : GetChildren()) {
        auto input = child->Copy(registry, renames);
        if (!input) {
            return nullptr;
        }
        inputs.push_back(std::move(input));
    }
    // Analyses of the original name its IDs, so the copy starts without them.
    auto copy = CopyImpl(std::move(inputs), registry, renames);
    if (copy) {
        copy->RenameUsedIUs(renames);
    }
    return copy;
}

TIntrusivePtr<IOperator> IOperator::CopyImpl(TVector<TIntrusivePtr<IOperator>>, TInfoUnitRegistry&, TSubstitutions&) const {
    return nullptr;
}

TIntrusivePtr<IOperator> TOpEmptySource::CopyImpl(TVector<TIntrusivePtr<IOperator>>, TInfoUnitRegistry& registry,
    TSubstitutions& renames) const
{
    return MakeIntrusive<TOpEmptySource>(Pos, Input, CopyColumns(Columns, registry, renames));
}

TIntrusivePtr<IOperator> TOpRead::CopyImpl(TVector<TIntrusivePtr<IOperator>>, TInfoUnitRegistry& registry,
    TSubstitutions& renames) const
{
    Y_ENSURE(!OlapFilterLambda && !OriginalPredicate, "Logical copying must precede read pushdown");
    return MakeIntrusive<TOpRead>(Alias, CopyColumns(Columns_, registry, renames), StorageType,
        TableCallable, nullptr, Limit, RangeInfo, std::nullopt, SortDir, TPhysicalOpProps{}, Pos);
}

TIntrusivePtr<IOperator> TOpMap::CopyImpl(TVector<TIntrusivePtr<IOperator>> inputs, TInfoUnitRegistry& registry,
    TSubstitutions& renames) const
{
    return MakeIntrusive<TOpMap>(std::move(inputs[0]), Pos, CopyDefinitions(MapElements, registry, renames));
}

TIntrusivePtr<IOperator> TOpFilter::CopyImpl(TVector<TIntrusivePtr<IOperator>> inputs, TInfoUnitRegistry&,
    TSubstitutions&) const
{
    return MakeIntrusive<TOpFilter>(std::move(inputs[0]), Pos, TPhysicalOpProps{}, FilterExpr, PartiallyPushedDown);
}

TIntrusivePtr<IOperator> TOpJoin::CopyImpl(TVector<TIntrusivePtr<IOperator>> inputs, TInfoUnitRegistry&,
    TSubstitutions&) const
{
    return MakeIntrusive<TOpJoin>(std::move(inputs[0]), std::move(inputs[1]), Pos, JoinKind, JoinKeys, JoinFilters);
}

TIntrusivePtr<IOperator> TOpAggregate::CopyImpl(TVector<TIntrusivePtr<IOperator>> inputs, TInfoUnitRegistry& registry,
    TSubstitutions& renames) const
{
    return MakeIntrusive<TOpAggregate>(std::move(inputs[0]), CopyDefinitions(Aggregations, registry, renames),
        KeyColumns, AggregationPhase, DistinctAll, Pos);
}

TIntrusivePtr<IOperator> TOpUnionAll::CopyImpl(TVector<TIntrusivePtr<IOperator>> inputs, TInfoUnitRegistry& registry,
    TSubstitutions& renames) const
{
    return MakeIntrusive<TOpUnionAll>(std::move(inputs), Pos, CopyDefinitions(Columns, registry, renames), Ordered);
}

TIntrusivePtr<IOperator> TOpWindow::CopyImpl(TVector<TIntrusivePtr<IOperator>> inputs, TInfoUnitRegistry& registry,
    TSubstitutions& renames) const
{
    return MakeIntrusive<TOpWindow>(std::move(inputs[0]), Pos, CopyDefinitions(WindowFuncs, registry, renames),
        PartitionKeys, SortElements, Frame);
}

TIntrusivePtr<IOperator> TOpSort::CopyImpl(TVector<TIntrusivePtr<IOperator>> inputs, TInfoUnitRegistry&,
    TSubstitutions&) const
{
    return MakeIntrusive<TOpSort>(std::move(inputs[0]), Pos, TPhysicalOpProps{}, SortElements, LimitCond, SortPhase);
}

TIntrusivePtr<IOperator> TOpLimit::CopyImpl(TVector<TIntrusivePtr<IOperator>> inputs, TInfoUnitRegistry&,
    TSubstitutions&) const
{
    return MakeIntrusive<TOpLimit>(std::move(inputs[0]), Pos, TPhysicalOpProps{}, LimitCond, OffsetCond, LimitPhase);
}

} // namespace NKikimr::NKqp
