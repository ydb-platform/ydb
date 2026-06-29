#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>

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
    // Do not turn `from := to` into `to := to` inside the same map. The target
    // name may be hidden there by another semantic rename.
    if (!outputElement->IsRename() &&
        outputElement->IsColumnAccess() &&
        outputElement->GetColumnAccess() == candidate->To) {
        return false;
    }

    outputElement->SetElementName(candidate->To);
    return NMapRules::FinishRenamePush(input, topMap, *candidate, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr
