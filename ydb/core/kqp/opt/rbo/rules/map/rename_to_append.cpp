#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool CanConvertRenameToAppend(const TIntrusivePtr<TOpMap>& map, size_t renameIdx, const TPlanProps& props) {
    auto oldElements = map->MapElements;
    map->MapElements[renameIdx].SetIsRename(false);
    const bool valid = CanExposeToParents(map.get(), props);
    map->MapElements = std::move(oldElements);

    return valid;
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
