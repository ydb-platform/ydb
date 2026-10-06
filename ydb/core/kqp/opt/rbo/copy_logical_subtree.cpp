#include "copy_logical_subtree.h"

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>
#include <yql/essentials/core/yql_expr_optimize.h>

namespace NKikimr::NKqp {
namespace {

bool IsRepeatable(const TExprNode::TPtr& expression) {
    return !expression || !FindNode(expression, [](const TExprNode::TPtr& node) {
        return NYql::NNodes::TCoNonDeterministicBase::Match(node.Get())
            || node->IsCallable({"Udf", "ScriptUdf", "SqlCall"});
    });
}

bool CanDuplicateAggregation(const TOpAggregate& aggregate) {
    // Deliberately ignore input types and reduction-order differences.
    for (const auto& [output, traits] : aggregate.GetAggregationTraits().Items()) {
        Y_UNUSED(output);
        const auto& function = traits.AggFunction;
        if (function != "count" && function != "distinct" && function != "sum"
            && function != "min" && function != "max") {
            return false;
        }
    }
    return true;
}

bool IsRepeatable(const IOperator& op) {
    for (const auto& expression : op.GetExpressions()) {
        if (!IsRepeatable(expression.get().Node)) {
            return false;
        }
    }
    switch (op.Kind) {
        case EOperator::Replicate:
        case EOperator::AddDependencies:
        case EOperator::Filter:
        case EOperator::Map:
        case EOperator::UnionAll:
        case EOperator::Join:
            return true;
        case EOperator::Source: {
            const auto& read = CastOperator<TOpRead>(op);
            return !read.Limit && (!read.RangeInfo ||
                (IsRepeatable(read.RangeInfo->ComputeNode) && IsRepeatable(read.RangeInfo->Points)));
        }
        case EOperator::EmptySource:
            return IsRepeatable(CastOperator<TOpEmptySource>(op).Input);
        case EOperator::Aggregate:
            return CanDuplicateAggregation(CastOperator<TOpAggregate>(op));
        case EOperator::Sort:
            return !CastOperator<TOpSort>(op).LimitCond;
        // Conservatively reject operators without an explicit repeatability check.
        default:
            return false;
    }
}

// Embedded programs need column-ID and row-schema rebinding in CopyImpl.
bool CanCopyRead(const TOpRead& read) {
    return !read.OlapFilterLambda && !read.OriginalPredicate;
}

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

bool CanDuplicateOperator(const IOperator& op) {
    return (op.Kind != EOperator::Source || CanCopyRead(CastOperator<TOpRead>(op)))
        && IsRepeatable(op);
}

bool CanDuplicateSubtree(const IOperator& root, const TSubplans& subplans, size_t maxOperators) {
    THashSet<const IOperator*> visited;
    TVector<const IOperator*> pending{&root};
    while (!pending.empty()) {
        const auto* op = pending.back();
        pending.pop_back();
        if (!visited.insert(op).second) {
            continue;
        }
        if (visited.size() > maxOperators || !CanDuplicateOperator(*op)) {
            return false;
        }
        for (const auto* child : op->GetChildren()) {
            pending.push_back(child);
        }
        for (const auto call : op->GetSubplanIUs(subplans)) {
            pending.push_back(subplans.At(call).Plan.Get());
        }
    }
    return true;
}

// Each invocation owns a separate graph copy. Definitions are allocated before
// any uses are rebound, including captures of definitions visited later.
class TLogicalCopyContext {
public:
    TLogicalCopyContext(TInfoUnitRegistry& registry, TSubstitutions& renames, TSubplans* subplans = nullptr)
        : Registry(registry)
        , Renames(renames)
        , Subplans(subplans)
    {}

    TIntrusivePtr<IOperator> Run(const IOperator& root) {
        auto result = CopyNode(root);
        if (!result) {
            return nullptr;
        }
        for (const auto* original : Order) {
            auto& copy = *Copies.at(original);
            copy.RenameUsedIUs(Renames);
            if (original->Kind != EOperator::Replicate) {
                for (size_t i = 0; i < original->GetChildCount(); ++i) {
                    copy.SetChild(i, Copies.at(original->GetChild(i).Get()));
                }
            }
        }
        // Publish calls only after every copied operator has its final bindings.
        for (const auto binding : Calls) {
            const auto& original = Subplans->At(binding);
            TOrderedIUs<> tuple;
            for (const auto id : original.Tuple.Items()) {
                tuple.Append(Substitute(id, Renames));
            }
            const auto resultId = original.ResultIU
                ? std::optional<TInfoUnitId>(Substitute(*original.ResultIU, Renames)) : std::nullopt;
            const auto copiedBinding = Renames.At(binding);
            Subplans->Add(copiedBinding, Copies.at(original.Plan.Get()), original.Type, std::move(tuple), resultId);
            Subplans->RefreshDependencies(copiedBinding);
        }
        return result;
    }

private:
    TIntrusivePtr<IOperator> CopyNode(const IOperator& original) {
        if (const auto it = Copies.find(&original); it != Copies.end()) {
            return it->second;
        }
        for (const auto* child : original.GetChildren()) {
            if (!CopyNode(*child)) {
                return nullptr;
            }
        }
        if (Subplans) {
            for (const auto binding : original.GetSubplanIUs(*Subplans)) {
                if (!VisitedCalls.insert(binding).second) {
                    continue;
                }
                const auto copiedBinding = CopyDefinition(binding, Registry, Renames);
                Y_ENSURE(!Subplans->Contains(copiedBinding), "A copied subplan requires a fresh call binding");
                if (!CopyNode(*Subplans->At(binding).Plan)) {
                    return nullptr;
                }
                Calls.push_back(binding);
            }
        }
        auto copy = original.Kind == EOperator::Replicate
            ? CopyPort(CastOperator<TOpReplicate>(original)) : original.CopyImpl(Registry, Renames);
        if (copy) {
            Copies.emplace(&original, copy);
            Order.push_back(&original);
        }
        return copy;
    }

    TIntrusivePtr<IOperator> CopyPort(const TOpReplicate& original) {
        const auto& originalHub = original.GetReplicate();
        auto [it, inserted] = Hubs.try_emplace(&originalHub);
        if (inserted) {
            it->second = TReplicate::Create(Copies.at(original.GetInput().Get()), originalHub.Pos, Registry);
        }
        auto& hub = it->second;
        // Preserve primary/secondary identity even when a secondary port is
        // visited first, or the primary is not part of the copied graph.
        auto copy = TIntrusivePtr<TOpReplicate>(new TOpReplicate(hub, original.GetIndex()));
        hub->NextOutputIndex_ = std::max(hub->NextOutputIndex_, original.GetIndex() + 1);
        if (!original.IsPrimary()) {
            const auto& bindings = const_cast<TOpReplicate&>(original).GetRebindings();
            for (const auto source : original.GetInput()->GetOutputIUs()) {
                copy->Rebindings_.Add(Renames.At(source), CopyDefinition(bindings.At(source), Registry, Renames));
            }
        }
        return copy;
    }

    TInfoUnitRegistry& Registry;
    TSubstitutions& Renames;
    TSubplans* Subplans;
    THashMap<const IOperator*, TIntrusivePtr<IOperator>> Copies;
    THashMap<const TReplicate*, TIntrusivePtr<TReplicate>> Hubs;
    TVector<const IOperator*> Order;
    THashSet<TInfoUnitId> VisitedCalls;
    TVector<TInfoUnitId> Calls;
};

TIntrusivePtr<IOperator> IOperator::Copy(TInfoUnitRegistry& registry, TSubstitutions& renames) const {
    return TLogicalCopyContext(registry, renames).Run(*this);
}

TIntrusivePtr<IOperator> IOperator::Copy(TPlanProps& props, TSubstitutions& renames) const {
    return TLogicalCopyContext(props.InfoUnitRegistry, renames, &props.Subplans).Run(*this);
}

TIntrusivePtr<IOperator> IOperator::CopyWithInputs(TVector<TIntrusivePtr<IOperator>> inputs,
    TInfoUnitRegistry& registry, TSubstitutions& renames) const
{
    Y_ENSURE(inputs.size() == GetChildCount(), "Copied operator input count differs");
    for (const auto& input : inputs) {
        Y_ENSURE(input, "Cannot copy an operator over a null input");
    }
    auto copy = CopyImpl(registry, renames);
    if (copy) {
        copy->RenameUsedIUs(renames);
        for (size_t i = 0; i < inputs.size(); ++i) {
            copy->SetChild(i, std::move(inputs[i]));
        }
    }
    return copy;
}

TIntrusivePtr<IOperator> IOperator::CopyImpl(TInfoUnitRegistry&, TSubstitutions&) const {
    return nullptr;
}

TIntrusivePtr<IOperator> TOpEmptySource::CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const {
    return MakeIntrusive<TOpEmptySource>(Pos, Input, CopyColumns(Columns, registry, renames));
}

TIntrusivePtr<IOperator> TOpRead::CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const {
    if (!CanCopyRead(*this)) {
        return nullptr;
    }
    return MakeIntrusive<TOpRead>(Alias, CopyColumns(Columns_, registry, renames), StorageType,
        TableCallable, nullptr, Limit, RangeInfo, std::nullopt, SortDir, TPhysicalOpProps{}, Pos);
}

TIntrusivePtr<IOperator> TOpMap::CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const {
    return MakeIntrusive<TOpMap>(GetInput(), Pos, CopyDefinitions(MapElements, registry, renames), NeedToPush);
}

TIntrusivePtr<IOperator> TOpAddDependencies::CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const {
    return MakeIntrusive<TOpAddDependencies>(GetInput(), Pos, CopyDefinitions(Dependencies, registry, renames));
}

TIntrusivePtr<IOperator> TOpFilter::CopyImpl(TInfoUnitRegistry&, TSubstitutions&) const {
    return MakeIntrusive<TOpFilter>(GetInput(), Pos, TPhysicalOpProps{}, FilterExpr, PartiallyPushedDown);
}

TIntrusivePtr<IOperator> TOpJoin::CopyImpl(TInfoUnitRegistry&, TSubstitutions&) const {
    return MakeIntrusive<TOpJoin>(GetLeftInput(), GetRightInput(), Pos, JoinKind, JoinKeys, JoinFilters);
}

TIntrusivePtr<IOperator> TOpDependentJoin::CopyImpl(TInfoUnitRegistry&, TSubstitutions&) const {
    return MakeIntrusive<TOpDependentJoin>(GetDomain(), GetInput(), Dependencies, Pos, DomainColumns);
}

TIntrusivePtr<IOperator> TOpAggregate::CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const {
    return MakeIntrusive<TOpAggregate>(GetInput(), CopyDefinitions(Aggregations, registry, renames),
        KeyColumns, AggregationPhase, DistinctAll, Pos);
}

TIntrusivePtr<IOperator> TOpGroupingSets::CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const {
    auto columns = CopyDefinitions(Columns, registry, renames);
    auto indicators = CopyDefinitions(GroupingIndicators, registry, renames);
    return MakeIntrusive<TOpGroupingSets>(CastOperator<TOpAggregate>(GetInput()), GroupingSets,
        std::move(columns), Pos, std::move(indicators));
}

TIntrusivePtr<IOperator> TOpUnionAll::CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const {
    return MakeIntrusive<TOpUnionAll>(GetInputs(), Pos, CopyDefinitions(Columns, registry, renames), Ordered);
}

TIntrusivePtr<IOperator> TOpWindow::CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const {
    return MakeIntrusive<TOpWindow>(GetInput(), Pos, CopyDefinitions(WindowFuncs, registry, renames),
        PartitionKeys, SortElements, Frame);
}

TIntrusivePtr<IOperator> TOpSort::CopyImpl(TInfoUnitRegistry&, TSubstitutions&) const {
    return MakeIntrusive<TOpSort>(GetInput(), Pos, TPhysicalOpProps{}, SortElements, LimitCond, SortPhase);
}

TIntrusivePtr<IOperator> TOpLimit::CopyImpl(TInfoUnitRegistry&, TSubstitutions&) const {
    return MakeIntrusive<TOpLimit>(GetInput(), Pos, TPhysicalOpProps{}, LimitCond, OffsetCond, LimitPhase);
}

TIntrusivePtr<IOperator> TOpTableEffect::CopyImpl(TInfoUnitRegistry& registry, TSubstitutions& renames) const {
    TOrderedIUs<TString> returning;
    for (const auto& [id, name] : ReturningColumns_.Items()) {
        returning.Append(CopyDefinition(id, registry, renames), name);
    }
    return MakeIntrusive<TOpTableEffect>(GetInput(), Pos, Table, EffectType, Options, Columns_, std::move(returning));
}

} // namespace NKikimr::NKqp
