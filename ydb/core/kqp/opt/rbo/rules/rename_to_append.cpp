#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

bool AddInfoUnit(TInfoUnitSet& target, const TInfoUnit& iu) {
    return target.insert(iu).second;
}

bool HasDuplicateOutputs(const TVector<TInfoUnit>& outputIUs) {
    TInfoUnitSet seen;
    for (const auto& iu : outputIUs) {
        if (!AddInfoUnit(seen, iu)) {
            return true;
        }
    }
    return false;
}

bool SatisfiesNameConstraintsAtOutput(const TIntrusivePtr<IOperator>& op, const TVector<TInfoUnit>& outputIUs, const TPlanProps& props) {
    if (HasDuplicateOutputs(outputIUs)) {
        return false;
    }

    for (const auto& [parent, childIdx] : op->Parents) {
        const auto& forbidden = props.NameConstraints.GetForbiddenOut(parent, childIdx, op.get());
        for (const auto& iu : outputIUs) {
            if (forbidden.contains(iu)) {
                return false;
            }
        }
    }

    return true;
}

bool CanConvertRenameToAppend(const TIntrusivePtr<TOpMap>& map, size_t renameIdx, const TPlanProps& props) {
    auto oldElements = map->MapElements;
    map->MapElements[renameIdx].SetIsRename(false);
    const auto outputAfterConvert = map->GetOutputIUs();
    map->MapElements = std::move(oldElements);

    return SatisfiesNameConstraintsAtOutput(map, outputAfterConvert, props);
}

} // anonymous namespace

bool TRenameToAppendRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto map = CastOperator<TOpMap>(input);
    bool changed = false;

    for (size_t idx = 0; idx < map->MapElements.size(); ++idx) {
        if (!map->MapElements[idx].IsRename()) {
            continue;
        }

        if (!CanConvertRenameToAppend(map, idx, props)) {
            continue;
        }

        map->MapElements[idx].SetIsRename(false);
        changed = true;
    }

    return changed;
}

} // namespace NKqp
} // namespace NKikimr
