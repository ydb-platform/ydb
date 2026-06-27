#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool ConversionExposesRenameSource(const TIntrusivePtr<TOpMap>& map, size_t renameIdx) {
    const auto source = map->MapElements[renameIdx].GetRename();
    if (!ContainsInfoUnit(map->GetInput()->GetOutputIUs(), source)) {
        return false;
    }

    for (size_t idx = 0; idx < map->MapElements.size(); ++idx) {
        if (idx != renameIdx && map->MapElements[idx].IsRename() && map->MapElements[idx].GetRename() == source) {
            return false;
        }
    }

    return true;
}

bool CanConvertRenameToAppend(const TIntrusivePtr<TOpMap>& map, size_t renameIdx) {
    if (!map->IsSingleConsumer()) {
        return false;
    }

    const auto& element = map->MapElements[renameIdx];
    const auto source = element.GetRename();
    if (element.GetRename() == element.GetElementName()) {
        // Identity renames are handled by identity-map cleanup.
        return false;
    }

    if (ConversionExposesRenameSource(map, renameIdx)) {
        return !ContainsInfoUnit(map->GetOutputIUs(), source) && !GetForbidden(map.get()).contains(source);
    }

    return true;
}

} // anonymous namespace

bool TRenameToAppendRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto map = CastOperator<TOpMap>(input);
    for (size_t idx = 0; idx < map->MapElements.size(); ++idx) {
        if (!map->MapElements[idx].IsRename()) {
            continue;
        }

        if (!CanConvertRenameToAppend(map, idx)) {
            continue;
        }

        map->MapElements[idx].SetIsRename(false);
        return true;
    }

    return false;
}

} // namespace NKqp
} // namespace NKikimr
