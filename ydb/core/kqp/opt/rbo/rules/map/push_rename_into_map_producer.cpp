#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>
#include <ydb/core/kqp/opt/rbo/rules/map/projection_pruning_helpers.h>

namespace NKikimr {
namespace NKqp {

bool TPushRenameIntoMapProducerRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
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
    auto* outputElement = map->FindOutputElement(candidate->From);
    if (!map->IsSingleConsumer() || !outputElement ||
        !NMapRules::CanRenameOutput(map, candidate->From, candidate->To, props)) {
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
    if (!NMapRules::CanFinishRenamePush(topMap, *candidate, output, props)) {
        return false;
    }

    map->MapElements = std::move(elements);
    map->Props.OutputIUs = output;
    return NMapRules::FinishRenamePush(input, topMap, *candidate, output, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr
