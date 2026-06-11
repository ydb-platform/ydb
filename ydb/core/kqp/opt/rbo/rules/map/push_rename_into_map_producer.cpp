#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>

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

    const auto oldElements = map->MapElements;
    outputElement->SetElementName(candidate->To);

    if (HasOutputConflicts(map->GetOutputIUs())) {
        map->MapElements = oldElements;
        return false;
    }

    return NMapRules::FinishRenamePush(input, topMap, *candidate, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr
