#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>
#include <ydb/core/kqp/opt/rbo/rules/map/map_output_utils.h>

namespace NKikimr {
namespace NKqp {
namespace NMapRules {

bool CanRenameOutput(const TIntrusivePtr<IOperator>& op, const TInfoUnit& from, const TInfoUnit& to) {
    const auto output = op->GetOutputIUs();
    return ContainsInfoUnit(output, from) && !ContainsInfoUnit(output, to);
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

TVector<TMapElement> BuildResidualTopMapElements(const TIntrusivePtr<TOpMap>& topMap, size_t renameIdx, const TInfoUnit& from, const TInfoUnit& to) {
    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> renameMap{{from, to}};

    TVector<TMapElement> residualElements;
    residualElements.reserve(topMap->MapElements.size() - 1);
    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        if (idx != renameIdx) {
            residualElements.push_back(topMap->MapElements[idx]);
        }
    }

    for (auto& element : residualElements) {
        if (element.IsRename()) {
            auto from = element.GetRename();
            if (const auto it = renameMap.find(from); it != renameMap.end()) {
                from = it->second;
            }
            const auto expr = element.GetExpression();
            element = TMapElement(element.GetElementName(), from, topMap->Pos, expr.Ctx, expr.PlanProps, true);
        } else {
            element.SetExpression(element.GetExpression().ApplyRenames(renameMap));
        }
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
    if (!topMap->IsSingleConsumer() ||
        !CanRewriteResidualTopMap(topMap, candidate.Index, candidate.From, candidate.To)) {
        return false;
    }

    const auto output = SimulateTopMapOutputAfterPush(topMap, candidate.Index, candidate.From, candidate.To);
    return MakeInfoUnitSet(output).size() == output.size() &&
        IUSetIntersect(output, GetForbidden(topMap.get())).empty();
}

TMapElement MakeRenameElement(const TRenameCandidate& candidate, const TIntrusivePtr<TOpMap>& topMap) {
    const auto expr = topMap->MapElements[candidate.Index].GetExpression();
    return TMapElement(candidate.To, candidate.From, topMap->Pos, expr.Ctx, expr.PlanProps, true);
}

bool CanFinishRenamePush(
    const TIntrusivePtr<TOpMap>& topMap,
    const TRenameCandidate& candidate,
    const TVector<TInfoUnit>& pushedInputOutput)
{
    const auto residualElements = BuildResidualTopMapElements(topMap, candidate.Index, candidate.From, candidate.To);
    if (residualElements.empty()) {
        return MakeInfoUnitSet(pushedInputOutput).size() == pushedInputOutput.size() &&
            IUSetIntersect(pushedInputOutput, GetForbidden(topMap.get())).empty();
    }

    const auto output = BuildMapOutput(pushedInputOutput, residualElements);
    return MakeInfoUnitSet(output).size() == output.size() &&
        IUSetIntersect(output, GetForbidden(topMap.get())).empty();
}

bool FinishRenamePush(
    TIntrusivePtr<IOperator>& input,
    const TIntrusivePtr<TOpMap>& topMap,
    const TRenameCandidate& candidate,
    const TVector<TInfoUnit>& pushedInputOutput,
    TRBOContext& ctx,
    TPlanProps& props)
{
    RemoveTopRenameAndRewriteResiduals(topMap, candidate.Index, candidate.From, candidate.To);
    props.Subplans.RenameIUs({{candidate.From, candidate.To}}, ctx.ExprCtx);

    if (topMap->MapElements.empty()) {
        input = topMap->GetInput();
    } else {
        topMap->Props.OutputIUs = BuildMapOutput(pushedInputOutput, topMap->MapElements);
    }

    return true;
}

} // namespace NMapRules
} // namespace NKqp
} // namespace NKikimr
