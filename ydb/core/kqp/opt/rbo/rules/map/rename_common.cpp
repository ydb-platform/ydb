#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>

namespace NKikimr {
namespace NKqp {
namespace NMapRules {

namespace {

bool CanRewriteResidualTopMap(const TIntrusivePtr<TOpMap>& topMap, size_t renameIdx, const TInfoUnit& from, const TInfoUnit& to) {
    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        if (idx == renameIdx) {
            continue;
        }

        const auto& element = topMap->MapElements[idx];
        // A residual semantic rename from either name would hide the pushed output.
        if (element.IsRename() && (element.GetRename() == from || element.GetRename() == to)) {
            return false;
        }
    }

    return true;
}

TVector<TMapElement> BuildResidualTopMapElements(const TIntrusivePtr<TOpMap>& topMap, size_t renameIdx, const TInfoUnit& from, const TInfoUnit& to) {
    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> renameMap{{from, to}};

    TVector<TMapElement> residualElements;
    residualElements.reserve(topMap->MapElements.size() - 1);
    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        if (idx == renameIdx) {
            continue;
        }

        auto element = topMap->MapElements[idx];
        if (!element.IsRename()) {
            element.SetExpression(element.GetExpression().ApplyRenames(renameMap));
        }
        residualElements.push_back(std::move(element));
    }

    return residualElements;
}

void RemoveTopRenameAndRewriteResiduals(const TIntrusivePtr<TOpMap>& topMap, size_t renameIdx, const TInfoUnit& from, const TInfoUnit& to) {
    topMap->MapElements = BuildResidualTopMapElements(topMap, renameIdx, from, to);
}

bool RenameNeedsPush(const TIntrusivePtr<TOpMap>& topMap, const TMapElement& element, const TInfoUnitSet& liveOut) {
    return liveOut.contains(element.GetElementName()) ||
        GetForbidden(topMap.get()).contains(element.GetRename());
}

bool TryBuildRenameCandidate(
    const TIntrusivePtr<TOpMap>& topMap,
    size_t idx,
    const TInfoUnitSet& liveOut,
    TRenameCandidate& candidate)
{
    const auto& element = topMap->MapElements[idx];
    candidate.Index = idx;
    candidate.To = element.GetElementName();

    if (element.IsRename()) {
        candidate.From = element.GetRename();
        candidate.FromRenameElement = true;
        return RenameNeedsPush(topMap, element, liveOut);
    }

    if (!element.IsColumnAccess()) {
        return false;
    }

    candidate.From = element.GetColumnAccess();
    return liveOut.contains(candidate.To) && !liveOut.contains(candidate.From);
}

} // anonymous namespace

std::optional<TRenameCandidate> FindRenameCandidate(const TIntrusivePtr<TOpMap>& topMap) {
    const auto& liveOut = GetLiveOut(topMap.get());

    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        TRenameCandidate candidate;
        if (TryBuildRenameCandidate(topMap, idx, liveOut, candidate) && candidate.From != candidate.To) {
            return candidate;
        }
    }

    return std::nullopt;
}

bool CanStartLocalRenamePush(const TIntrusivePtr<TOpMap>& topMap, const TRenameCandidate& candidate) {
    return topMap->IsSingleConsumer() &&
        CanRewriteResidualTopMap(topMap, candidate.Index, candidate.From, candidate.To);
}

TMapElement MakeRenameElement(const TRenameCandidate& candidate, const TIntrusivePtr<TOpMap>& topMap) {
    const auto expr = topMap->MapElements[candidate.Index].GetExpression();
    return TMapElement(candidate.To, candidate.From, topMap->Pos, expr.Ctx, expr.PlanProps, true);
}

bool FinishRenamePush(
    TIntrusivePtr<IOperator>& input,
    const TIntrusivePtr<TOpMap>& topMap,
    const TRenameCandidate& candidate,
    TRBOContext& ctx,
    TPlanProps& props)
{
    RemoveTopRenameAndRewriteResiduals(topMap, candidate.Index, candidate.From, candidate.To);
    props.Subplans.RenameIUs({{candidate.From, candidate.To}}, ctx.ExprCtx);

    if (topMap->MapElements.empty()) {
        input = topMap->GetInput();
    }

    return true;
}

} // namespace NMapRules
} // namespace NKqp
} // namespace NKikimr
