#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

using TCandidates = TPlanAliases::TCandidates;

std::optional<TInfoUnit> ChoosePreferredAlias(const TCandidates& candidates, const TInfoUnitSet& liveOut) {
    if (candidates.empty()) {
        return std::nullopt;
    }

    const TAliasCandidate* bestLive = nullptr;
    const TAliasCandidate* bestBase = &candidates.front();
    for (const auto& candidate : candidates) {
        if (liveOut.contains(candidate.IU) &&
            (!bestLive || candidate.Priority > bestLive->Priority ||
                (candidate.Priority == bestLive->Priority && candidate.IU.GetFullName() < bestLive->IU.GetFullName()))) {
            bestLive = &candidate;
        }
        if (candidate.Priority < bestBase->Priority ||
            (candidate.Priority == bestBase->Priority && candidate.IU.GetFullName() < bestBase->IU.GetFullName())) {
            bestBase = &candidate;
        }
    }

    return bestLive ? bestLive->IU : bestBase->IU;
}

} // anonymous namespace

bool TRewriteExpressionsToPreferredAliasesRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Filter) {
        return false;
    }

    auto filter = CastOperator<TOpFilter>(input);
    const auto liveIt = props.LiveOut.find(filter.get());
    if (liveIt == props.LiveOut.end()) {
        return false;
    }

    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> renameMap;
    for (const auto& iu : filter->FilterExpr.GetInputIUs(false, true)) {
        const auto* candidates = props.Aliases.GetAliases(filter->GetInput().get(), iu);
        if (!candidates) {
            continue;
        }

        const auto preferred = ChoosePreferredAlias(*candidates, liveIt->second);
        if (preferred && *preferred != iu && ContainsInfoUnit(filter->GetInput()->GetOutputIUs(), *preferred)) {
            renameMap[iu] = *preferred;
        }
    }

    if (renameMap.empty()) {
        return false;
    }

    filter->RenameIUs(renameMap, ctx.ExprCtx);
    props.Subplans.RenameIUs(renameMap, ctx.ExprCtx);
    return true;
}

} // namespace NKqp
} // namespace NKikimr
