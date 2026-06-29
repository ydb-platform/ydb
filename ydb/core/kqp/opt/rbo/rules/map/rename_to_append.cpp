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
// 1. Another rename still hides the source after this rename is converted.
// A: Map [ x <- a, y <- a ] == becomes ==>  Map [ x := a, y <- a ]
// B: `- input [a]                           `- input [a]
//
// 2. Source exposure is prevented when it would duplicate an existing output
//    or violate a forbidden-name constraint at Map A.
// A: Map [ x <- a, a := f ] -- not changed
// B: `- input [a]

namespace {

using TRenameSourceCounts = THashMap<TInfoUnit, size_t, TInfoUnit::THashFunction>;

struct TRenameSourceInfo {
    TVector<TInfoUnit> SourcesByElement;
    TRenameSourceCounts Counts;
};

TRenameSourceInfo BuildRenameSourceInfo(const TOpMap& map) {
    TRenameSourceInfo result;
    result.SourcesByElement.resize(map.MapElements.size());

    for (size_t idx = 0; idx < map.MapElements.size(); ++idx) {
        const auto& element = map.MapElements[idx];
        if (!element.IsRename()) {
            continue;
        }

        const auto source = element.GetRename();
        result.SourcesByElement[idx] = source;
        ++result.Counts[source];
    }

    return result;
}

bool WillExposeRenameSource(const TRenameSourceCounts& renameSourceCounts, const TInfoUnit& source) {
    const auto it = renameSourceCounts.find(source);
    Y_ENSURE(it != renameSourceCounts.end());
    return it->second == 1;
}

bool CanConvertRenameToAppend(
    const TIntrusivePtr<TOpMap>& map,
    const TRenameSourceInfo& renameSourceInfo,
    const TInfoUnitSet& output,
    size_t renameIdx)
{
    const auto& element = map->MapElements[renameIdx];
    const auto& source = renameSourceInfo.SourcesByElement[renameIdx];
    if (source == element.GetElementName()) {
        // Identity renames are handled by identity-map cleanup.
        return false;
    }

    if (!WillExposeRenameSource(renameSourceInfo.Counts, source)) {
        return true;
    }

    return !output.contains(source) && !GetForbidden(map.get()).contains(source);
}

} // anonymous namespace

bool TRenameToAppendRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto map = CastOperator<TOpMap>(input);
    const auto renameSourceInfo = BuildRenameSourceInfo(*map);
    if (renameSourceInfo.Counts.empty()) {
        return false;
    }

    const auto output = MakeInfoUnitSet(map->GetOutputIUs());
    for (size_t idx = 0; idx < map->MapElements.size(); ++idx) {
        if (!map->MapElements[idx].IsRename()) {
            continue;
        }

        if (!CanConvertRenameToAppend(map, renameSourceInfo, output, idx)) {
            continue;
        }

        map->MapElements[idx].SetIsRename(false);
        return true;
    }

    return false;
}

} // namespace NKqp
} // namespace NKikimr
