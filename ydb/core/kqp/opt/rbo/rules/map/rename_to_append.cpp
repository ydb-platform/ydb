#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

// Main shape this handles:
// A: Map [ x <- a ]         == becomes ==>  Map [ x := a ]
// B: `- input [a]                           `- input [a]
//
// The rule keeps alias "x" but stops hiding source "a". This simplifies
// propagation of some properties, like ShuffledByColumns or KeyColumns.
//
// Caveats:
// 1. Another rename keeps hiding the source when exposing it is not safe.
// A: Map [ x <- a, y <- a ] == becomes ==>  Map [ x := a, y <- a ]
// B: `- input [a]                           `- input [a]
//
// 2. Source exposure is prevented when it would duplicate an existing output
//    or violate a forbidden-name constraint at Map A.
// A: Map [ x <- a, a := f ] -- not changed
// B: `- input [a]

namespace {

using TRenameSourceCounts = THashMap<TInfoUnit, size_t, TInfoUnit::THashFunction>;

TRenameSourceCounts CountRenameSources(const TOpMap& map) {
    TRenameSourceCounts result;

    for (const auto& element : map.GetMapElements()) {
        if (!element.IsRename()) {
            continue;
        }

        ++result[element.GetRename()];
    }

    return result;
}

} // anonymous namespace

bool TRenameToAppendRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Map;
}

bool TRenameToAppendRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto map = CastOperator<TOpMap>(input);
    auto remainingRenameSourceCounts = CountRenameSources(*map);
    if (remainingRenameSourceCounts.empty()) {
        return false;
    }

    const auto output = MakeInfoUnitSet(map->GetOutputIUs());
    const auto& forbidden = GetForbidden(map.get());
    auto mapElements = map->GetMapElements();
    bool changed = false;

    for (auto& mapElement : mapElements) {
        if (!mapElement.IsRename()) {
            continue;
        }

        const auto source = mapElement.GetRename();
        if (source == mapElement.GetElementName()) {
            // Identity renames are handled by identity-map cleanup.
            continue;
        }

        const bool exposesSource = remainingRenameSourceCounts.at(source) == 1;
        if (exposesSource && (output.contains(source) || forbidden.contains(source))) {
            continue;
        }

        mapElement.SetIsRename(false);
        --remainingRenameSourceCounts[source];
        changed = true;
    }

    if (changed) {
        map->SetMapElements(std::move(mapElements));
    }
    return changed;
}

} // namespace NKqp
} // namespace NKikimr
