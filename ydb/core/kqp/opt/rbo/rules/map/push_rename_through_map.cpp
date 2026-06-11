#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool IsPassThroughMap(const TIntrusivePtr<TOpMap>& map, const TInfoUnit& from) {
    const auto renameSources = map->GetRenameSources();
    return !map->HasOutputElement(from) && !renameSources.contains(from);
}

} // anonymous namespace

bool TPushRenameThroughPassThroughMapRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto topMap = CastOperator<TOpMap>(input);
    const auto candidate = NMapRules::FindRenameCandidate(topMap, props);
    if (!candidate || !NMapRules::CanStartLocalRenamePush(topMap, *candidate, props)) {
        return false;
    }

    if (topMap->GetInput()->Kind != EOperator::Map) {
        return false;
    }

    auto map = CastOperator<TOpMap>(topMap->GetInput());
    if (!map->IsSingleConsumer() || !IsPassThroughMap(map, candidate->From) ||
        !NMapRules::CanRenameOutput(map, candidate->From, candidate->To, props)) {
        return false;
    }

    const auto oldElements = map->MapElements;

    // A pass-through map can perform this rename itself. Adding the element to the
    // lower map is progress; inserting another single-rename map below it would only
    // commute independent renames back and forth.
    map->MapElements.push_back(NMapRules::MakeRenameElement(*candidate, topMap));
    if (!CanExposeOutput(map, map->GetOutputIUs(), props)) {
        map->MapElements = oldElements;
        return false;
    }

    return NMapRules::FinishRenamePush(input, topMap, *candidate, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr
