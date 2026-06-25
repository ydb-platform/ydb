#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>

namespace NKikimr {
namespace NKqp {

namespace {

TVector<TInfoUnit> ReplaceOutputName(TVector<TInfoUnit> output, const TInfoUnit& from, const TInfoUnit& to) {
    for (auto& iu : output) {
        if (iu == from) {
            iu = to;
        }
    }
    return output;
}

} // anonymous namespace

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

    const auto output = ReplaceOutputName(read->OutputIUs, candidate->From, candidate->To);
    if (MakeInfoUnitSet(output).size() != output.size() ||
        !NMapRules::CanFinishRenamePush(topMap, *candidate, output, props)) {
        return false;
    }

    read->OutputIUs = output;
    read->Props.OutputIUs = output;
    return NMapRules::FinishRenamePush(input, topMap, *candidate, output, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr
