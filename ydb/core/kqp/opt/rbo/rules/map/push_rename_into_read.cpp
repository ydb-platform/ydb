#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>

namespace NKikimr {
namespace NKqp {

bool TPushRenameIntoReadRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto topMap = CastOperator<TOpMap>(input);
    const auto candidate = NMapRules::FindRenameCandidate(topMap, props);
    if (!candidate || !NMapRules::CanStartLocalRenamePush(topMap, *candidate, props)) {
        return false;
    }

    if (topMap->GetInput()->Kind != EOperator::Source) {
        return false;
    }

    auto read = CastOperator<TOpRead>(topMap->GetInput());
    if (!read->IsSingleConsumer() || !NMapRules::CanRenameOutput(read, candidate->From, candidate->To, props)) {
        return false;
    }

    const auto oldOutput = read->OutputIUs;
    const auto oldCachedOutput = read->Props.OutputIUs;
    read->RenameIUs({{candidate->From, candidate->To}}, ctx.ExprCtx);
    read->Props.OutputIUs = read->OutputIUs;
    if (HasOutputConflicts(read->OutputIUs)) {
        read->OutputIUs = oldOutput;
        read->Props.OutputIUs = oldCachedOutput;
        return false;
    }

    return NMapRules::FinishRenamePush(input, topMap, *candidate, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr
