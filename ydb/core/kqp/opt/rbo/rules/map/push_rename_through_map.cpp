#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>
#include <ydb/core/kqp/opt/rbo/rules/map/projection_pruning_helpers.h>

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
    const auto candidate = NMapRules::FindRenameCandidate(topMap);
    if (!candidate || !NMapRules::CanStartLocalRenamePush(topMap, *candidate)) {
        return false;
    }

    if (topMap->GetInput()->Kind != EOperator::Map) {
        return false;
    }

    auto map = CastOperator<TOpMap>(topMap->GetInput());
    if (!map->IsSingleConsumer() || !IsPassThroughMap(map, candidate->From) ||
        !NMapRules::CanRenameOutput(map, candidate->From, candidate->To)) {
        return false;
    }

    // A pass-through map can perform this rename itself. Adding the element to the
    // lower map is progress; inserting another single-rename map below it would only
    // commute independent renames back and forth.
    auto elements = map->MapElements;
    elements.push_back(NMapRules::MakeRenameElement(*candidate, topMap));

    auto output = BuildMapOutput(map, elements);
    if (MakeInfoUnitSet(output).size() != output.size()) {
        return false;
    }
    if (!NMapRules::CanFinishRenamePush(topMap, *candidate, output)) {
        return false;
    }

    map->MapElements = std::move(elements);
    map->Props.OutputIUs = output;
    return NMapRules::FinishRenamePush(input, topMap, *candidate, output, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr
