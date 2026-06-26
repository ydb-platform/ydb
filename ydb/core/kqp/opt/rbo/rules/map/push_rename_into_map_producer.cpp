#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>
#include <ydb/core/kqp/opt/rbo/rules/map/map_output_utils.h>

namespace NKikimr {
namespace NKqp {

bool TPushRenameIntoMapProducerRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
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
    auto* outputElement = map->FindOutputElement(candidate->From);
    if (!map->IsSingleConsumer() || !outputElement ||
        !NMapRules::CanRenameOutput(map, candidate->From, candidate->To)) {
        return false;
    }
    if (!outputElement->IsRename() &&
        outputElement->IsColumnAccess() &&
        outputElement->GetColumnAccess() == candidate->To) {
        return false;
    }

    const auto outputElementIdx = outputElement - map->MapElements.data();
    auto elements = map->MapElements;
    elements[outputElementIdx].SetElementName(candidate->To);

    auto output = BuildMapOutput(map, elements);
    if (MakeInfoUnitSet(output).size() != output.size()) {
        return false;
    }
    if (!NMapRules::CanFinishRenamePush(topMap, *candidate, output)) {
        return false;
    }

    map->MapElements = std::move(elements);
    return NMapRules::FinishRenamePush(input, topMap, *candidate, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr
