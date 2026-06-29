#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>

namespace NKikimr {
namespace NKqp {

bool TPushRenameIntoReadRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto topMap = CastOperator<TOpMap>(input);
    const auto candidate = NMapRules::FindRenameCandidate(topMap);
    if (!candidate || !NMapRules::CanStartLocalRenamePush(topMap, *candidate)) {
        return false;
    }

    if (topMap->GetInput()->Kind != EOperator::Source) {
        return false;
    }

    auto read = CastOperator<TOpRead>(topMap->GetInput());
    if (!read->IsSingleConsumer() || !NMapRules::CanRenameOutput(read, candidate->From, candidate->To)) {
        return false;
    }

    for (auto& output : read->OutputIUs) {
        if (output == candidate->From) {
            output = candidate->To;
            break;
        }
    }
    return NMapRules::FinishRenamePush(input, topMap, *candidate, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr
