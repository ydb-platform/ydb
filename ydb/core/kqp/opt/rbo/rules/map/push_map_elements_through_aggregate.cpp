#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

// A rename of an aggregate output commutes with the aggregate. Keys keep
// their value below the aggregate, so a key rename crosses it downwards:
//
// A: Map [ x <- key ]      == becomes ==>  Aggregate [ x ]
// B: `- Aggregate [ key ]                   `- Map [ x <- key ]
// C:    `- input                                `- input
//
// When the target name already exists in the aggregate input as a member of
// the key's alias class (same value by construction), the key switches to the
// existing column directly and nothing is pushed below:
//
// A: Map [ x <- key ]          == becomes ==>  Aggregate [ x ]
// B: `- Aggregate [ key ]                       `- input [ x, key := x ]
// C:    `- input [ x, key := x ]
//
// Result traits exist only above the aggregate, so their renames apply in
// place:
//
// A: Map [ x <- sum ]               == becomes ==>  Aggregate [ x: sum(...) ]
// B: `- Aggregate [ sum: sum(...) ]                  `- input
//
// A column-access append whose source is dead above its map is a rename in
// disguise and crosses the same way. Every shape changes the aggregate's
// output names, which all consumers see: each consumer must be a Map carrying
// the same rename, and the rename is consumed from all of them at once. The
// matched map decides usefulness; a sole consumer agrees trivially.
//
// Caveats:
// 1.
// A: Map [ x <- key, y <- key ]  -- a name crosses under one new name only;
// B: `- Aggregate [ key ]           a second rename of the same source would
//                                   lose its source, so both stay put.
// 2.
// A: Map [ x := key, y := key ]  -- with "key" dead above A the first append
// B: `- Aggregate [ key ]           crosses as the rename it is, and the
//                                   second stays above rewritten to "y := x".
// 3.
// A: Map [ sum <- key ]                -- a rename may not target a name that
// B: `- Aggregate [ key, sum: ... ]       another aggregate output keeps.

namespace {

using TRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>;

// How a rename crosses the aggregate.
enum class ECrossing {
    RenameResult,          // "from" is a result trait: rename it in place
    PushKeyRename,         // "from" is a key: push the rename below the aggregate
    SwitchKeyToInputAlias, // "from" is a key and "to" already carries its value at the input
};

struct TCandidate {
    TInfoUnit From;
    TInfoUnit To;
    ECrossing Crossing;
};

// A column-access append whose source is dead above the map is a semantic
// rename in disguise: hiding the dead source changes nothing for consumers.
bool IsRenameEquivalent(const TMapElement& element, TOpMap* map) {
    if (element.IsRename()) {
        return true;
    }
    return element.IsColumnAccess() && !GetLiveOut(map).contains(element.GetColumnAccess());
}

TInfoUnit GetRenameSource(const TMapElement& element) {
    return element.IsRename() ? element.GetRename() : element.GetColumnAccess();
}

// Key columns keep their value below the aggregate: a key rename crosses as a
// compensating rename below it, or — when the target already carries the
// key's value at the aggregate input (one alias class) — the key switches to
// that column and the old one dies. Result renames apply in place.
std::optional<ECrossing> ClassifyCrossing(
    const TOpAggregate& aggregate,
    IOperator* aggregateInput,
    const TVector<TInfoUnit>& aggregateInputIUs,
    const TInfoUnit& from,
    const TInfoUnit& to)
{
    if (!ContainsInfoUnit(aggregate.KeyColumns, from)) {
        const bool isResult = AnyOf(aggregate.AggregationTraitsList, [&](const auto& trait) {
            return trait.ResultColName == from;
        });
        // A column access can also reference a subplan variable rather than
        // an aggregate output; those are not this rule's business.
        return isResult ? std::make_optional(ECrossing::RenameResult) : std::nullopt;
    }

    const auto* aliases = GetAliases(aggregateInput, from);
    if (aliases && AnyOf(*aliases, [&](const auto& alias) { return alias.IU == to; })) {
        return ECrossing::SwitchKeyToInputAlias;
    }

    if (!ContainsInfoUnit(aggregateInputIUs, to)) {
        return ECrossing::PushKeyRename;
    }

    // "to" is occupied at the aggregate input by an unrelated value.
    return std::nullopt;
}

// The consumers agree on a rename when each of them consumes "from" under the
// name "to"; only then does consuming it change nothing above any of them.
bool AllParentsCarryRename(const TVector<TOpMap*>& parents, const TInfoUnit& from, const TInfoUnit& to) {
    return AllOf(parents, [&](TOpMap* parent) {
        const auto* element = parent->FindOutputElement(to);
        return element && IsRenameEquivalent(*element, parent) && GetRenameSource(*element) == from;
    });
}

// A second rename of the same source stays behind and would lose its source
// once the first one crosses; such splits keep everything in place.
bool SomeParentSplitsRename(const TVector<TOpMap*>& parents, const TInfoUnit& from, const TInfoUnit& to) {
    return AnyOf(parents, [&](TOpMap* parent) {
        return AnyOf(parent->GetMapElements(), [&](const TMapElement& element) {
            return element.IsRename() && element.GetRename() == from && element.GetElementName() != to;
        });
    });
}

// The aggregate's output names must stay unique after the renames: drop a
// candidate whose target another output keeps, or an earlier candidate took.
// Dropping one keeps its source name in the output, which can invalidate a
// candidate targeting that name, so iterate to a fixpoint.
void PruneCollidingTargets(const TVector<TInfoUnit>& aggregateOutput, TVector<TCandidate>& candidates) {
    bool changed = true;
    while (changed) {
        changed = false;

        TInfoUnitSet renamedAway;
        for (const auto& candidate : candidates) {
            renamedAway.insert(candidate.From);
        }

        TInfoUnitSet takenTargets;
        TVector<TCandidate> kept;
        kept.reserve(candidates.size());
        for (const auto& candidate : candidates) {
            if (takenTargets.contains(candidate.To) ||
                (ContainsInfoUnit(aggregateOutput, candidate.To) && !renamedAway.contains(candidate.To))) {
                changed = true;
                continue;
            }

            takenTargets.insert(candidate.To);
            kept.push_back(candidate);
        }
        candidates = std::move(kept);
    }
}

} // anonymous namespace

bool TPushMapElementsThroughAggregateRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Map &&
        input->Children.front()->Kind == EOperator::Aggregate;
}

TIntrusivePtr<IOperator>
TPushMapElementsThroughAggregateRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto topMap = CastOperator<TOpMap>(input);
    if (topMap->GetInput()->Kind != EOperator::Aggregate) {
        return input;
    }
    auto aggregate = CastOperator<TOpAggregate>(topMap->GetInput());

    TVector<TOpMap*> parents;
    parents.reserve(aggregate->Parents.size());
    for (const auto& [parent, parentIdx] : aggregate->Parents) {
        if (parent->Kind != EOperator::Map) {
            return input;
        }
        parents.push_back(static_cast<TOpMap*>(parent));
    }

    const auto aggregateInput = aggregate->GetInput();
    const auto aggregateInputIUs = aggregateInput->GetOutputIUs();
    const auto& liveOut = GetLiveOut(topMap.get());
    const auto& forbidden = GetForbidden(topMap.get());

    TVector<TCandidate> candidates;
    TInfoUnitSet claimedSources;
    for (const auto& element : topMap->GetMapElements()) {
        if (!IsRenameEquivalent(element, topMap.get())) {
            continue;
        }

        const auto from = GetRenameSource(element);
        const auto to = element.GetElementName();
        if (from == to || claimedSources.contains(from)) {
            continue;
        }

        // Move only renames someone needs: a live target, or a source name
        // this region must stop exposing. Dead renames are pruning's job.
        if (!liveOut.contains(to) && !forbidden.contains(from)) {
            continue;
        }

        const auto crossing = ClassifyCrossing(*aggregate, aggregateInput.get(), aggregateInputIUs, from, to);
        if (!crossing ||
            !AllParentsCarryRename(parents, from, to) ||
            SomeParentSplitsRename(parents, from, to)) {
            continue;
        }

        claimedSources.insert(from);
        candidates.push_back({from, to, *crossing});
    }

    PruneCollidingTargets(aggregate->GetOutputIUs(), candidates);
    if (candidates.empty()) {
        return input;
    }

    TRenameMap renames;
    TRenameMap keyRenames;
    TInfoUnitSet consumedTargets;
    TVector<TMapElement> pushedElements;
    for (const auto& candidate : candidates) {
        renames.emplace(candidate.From, candidate.To);
        consumedTargets.insert(candidate.To);
        if (candidate.Crossing != ECrossing::RenameResult) {
            keyRenames.emplace(candidate.From, candidate.To);
        }
        if (candidate.Crossing == ECrossing::PushKeyRename) {
            auto pushed = *topMap->FindOutputElement(candidate.To);
            pushed.SetIsRename(true);
            pushedElements.push_back(std::move(pushed));
        }
    }

    if (!pushedElements.empty()) {
        aggregate->SetInput(MakeIntrusive<TOpMap>(aggregateInput, topMap->Pos, pushedElements));
    }
    aggregate->RenameProducedIUs(renames, ctx.ExprCtx);
    aggregate->RenameUsedIUs(keyRenames, ctx.ExprCtx);
    props.Subplans.RenameExternalReferences(renames, ctx.ExprCtx);

    // Consume the renames from every consumer map and rebind what stays above
    // to the new output names.
    for (TOpMap* parent : parents) {
        auto mapElements = parent->GetMapElements();
        EraseIf(mapElements, [&](const TMapElement& element) {
            return consumedTargets.contains(element.GetElementName());
        });
        for (auto& element : mapElements) {
            if (!element.IsRename()) {
                element.SetExpression(element.GetExpression().ApplyRenames(renames));
            }
        }
        parent->SetMapElements(std::move(mapElements));
    }

    if (topMap->GetMapElements().empty()) {
        return aggregate;
    }
    return MakeIntrusive<TOpMap>(aggregate, topMap->Pos, topMap->GetMapElements());
}

} // namespace NKqp
} // namespace NKikimr
