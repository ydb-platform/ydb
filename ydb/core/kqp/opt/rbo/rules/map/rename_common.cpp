#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>

namespace NKikimr {
namespace NKqp {
namespace NMapRules {

bool CanRenameOutput(const TIntrusivePtr<IOperator>& op, const TInfoUnit& from, const TInfoUnit& to, const TPlanProps& props) {
    const auto output = op->GetOutputIUs();
    if (!ContainsInfoUnit(output, from) || ContainsInfoUnit(output, to)) {
        return false;
    }
    return !props.NameConstraints.IsForbiddenAtOutput(op.get(), to);
}

namespace {

TVector<TInfoUnit> ReplaceOutputName(TVector<TInfoUnit> output, const TInfoUnit& from, const TInfoUnit& to) {
    for (auto& iu : output) {
        if (iu == from) {
            iu = to;
        }
    }
    return output;
}

bool CanRewriteResidualTopMap(const TIntrusivePtr<TOpMap>& topMap, size_t renameIdx, const TInfoUnit& from, const TInfoUnit& to) {
    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        if (idx == renameIdx) {
            continue;
        }

        const auto& element = topMap->MapElements[idx];
        if (element.GetElementName() == to) {
            return false;
        }
        if (element.IsRename() && (element.GetRename() == from || element.GetRename() == to)) {
            return false;
        }
    }

    return true;
}

TVector<TInfoUnit> SimulateTopMapOutputAfterPush(const TIntrusivePtr<TOpMap>& topMap, size_t renameIdx, const TInfoUnit& from, const TInfoUnit& to) {
    auto result = ReplaceOutputName(topMap->GetInput()->GetOutputIUs(), from, to);

    TInfoUnitSet renameSources;
    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        if (idx == renameIdx) {
            continue;
        }

        const auto& element = topMap->MapElements[idx];
        if (element.IsRename()) {
            AddInfoUnit(renameSources, element.GetRename());
        }
    }

    if (!renameSources.empty()) {
        TVector<TInfoUnit> kept;
        kept.reserve(result.size());
        for (const auto& iu : result) {
            if (!renameSources.contains(iu)) {
                kept.push_back(iu);
            }
        }
        result = std::move(kept);
    }

    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        if (idx != renameIdx) {
            result.push_back(topMap->MapElements[idx].GetElementName());
        }
    }

    return result;
}

void RenameMapInputsOnly(TOpMap& map, const THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>& renameMap) {
    TVector<TMapElement> renamedElements;
    renamedElements.reserve(map.MapElements.size());

    for (const auto& element : map.MapElements) {
        if (element.IsRename()) {
            auto from = element.GetRename();
            if (const auto it = renameMap.find(from); it != renameMap.end()) {
                from = it->second;
            }
            const auto expr = element.GetExpression();
            renamedElements.emplace_back(element.GetElementName(), from, map.Pos, expr.Ctx, expr.PlanProps, true);
        } else {
            auto renamed = element;
            renamed.SetExpression(element.GetExpression().ApplyRenames(renameMap));
            renamedElements.push_back(renamed);
        }
    }

    map.MapElements = std::move(renamedElements);
}

void RemoveTopRenameAndRewriteResiduals(const TIntrusivePtr<TOpMap>& topMap, size_t renameIdx, const TInfoUnit& from, const TInfoUnit& to) {
    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> renameMap{{from, to}};

    TVector<TMapElement> residualElements;
    residualElements.reserve(topMap->MapElements.size() - 1);
    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        if (idx != renameIdx) {
            residualElements.push_back(topMap->MapElements[idx]);
        }
    }

    topMap->MapElements = std::move(residualElements);
    RenameMapInputsOnly(*topMap, renameMap);
}

bool RenameNeedsPush(const TIntrusivePtr<TOpMap>& topMap, const TMapElement& element, const TInfoUnitSet& liveOut, const TPlanProps& props) {
    return liveOut.contains(element.GetElementName()) ||
        props.NameConstraints.IsForbiddenAtOutput(topMap.get(), element.GetRename());
}

bool TryBuildRenameCandidate(
    const TIntrusivePtr<TOpMap>& topMap,
    size_t idx,
    const TInfoUnitSet& liveOut,
    const TPlanProps& props,
    TRenameCandidate& candidate)
{
    const auto& element = topMap->MapElements[idx];
    candidate.Index = idx;
    candidate.To = element.GetElementName();

    if (element.IsRename()) {
        candidate.From = element.GetRename();
        candidate.FromRenameElement = true;
        return RenameNeedsPush(topMap, element, liveOut, props);
    }

    if (!element.IsColumnAccess()) {
        return false;
    }

    candidate.From = element.GetColumnAccess();
    return liveOut.contains(candidate.To) && !liveOut.contains(candidate.From);
}

} // anonymous namespace

std::optional<TRenameCandidate> FindRenameCandidate(const TIntrusivePtr<TOpMap>& topMap, const TPlanProps& props) {
    const auto liveIt = props.LiveOut.find(topMap.get());
    const auto& liveOut = liveIt == props.LiveOut.end() ? EmptyInfoUnitSet() : liveIt->second;

    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        TRenameCandidate candidate;
        if (TryBuildRenameCandidate(topMap, idx, liveOut, props, candidate) && candidate.From != candidate.To) {
            return candidate;
        }
    }

    return std::nullopt;
}

bool CanStartLocalRenamePush(const TIntrusivePtr<TOpMap>& topMap, const TRenameCandidate& candidate, const TPlanProps& props) {
    return topMap->IsSingleConsumer() &&
        CanRewriteResidualTopMap(topMap, candidate.Index, candidate.From, candidate.To) &&
        CanExposeOutput(topMap, SimulateTopMapOutputAfterPush(topMap, candidate.Index, candidate.From, candidate.To), props);
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
        if (CanReplaceInParents(topMap, topMap->GetInput(), props)) {
            input = topMap->GetInput();
        }
    }

    return true;
}

} // namespace NMapRules
} // namespace NKqp
} // namespace NKikimr
