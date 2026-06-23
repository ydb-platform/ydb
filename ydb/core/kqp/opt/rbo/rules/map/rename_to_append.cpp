#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>
#include <ydb/core/kqp/opt/rbo/rules/map/projection_pruning_helpers.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool CanConvertRenameToAppend(const TIntrusivePtr<TOpMap>& map, size_t renameIdx, const TPlanProps& props, TVector<TInfoUnit>& output) {
    if (map->MapElements[renameIdx].GetRename() == map->MapElements[renameIdx].GetElementName()) {
        return false;
    }

    auto elements = map->MapElements;
    elements[renameIdx].SetIsRename(false);
    output = BuildMapOutput(map, elements);
    return CanExposeOutput(map, output, props);
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

        TVector<TInfoUnit> output;
        if (!CanConvertRenameToAppend(map, idx, props, output)) {
            continue;
        }

        map->MapElements[idx].SetIsRename(false);
        map->Props.OutputIUs = std::move(output);
        changed = true;
    }

    return changed;
}

} // namespace NKqp
} // namespace NKikimr
