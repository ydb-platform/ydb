#include "kqp_rules_include.h"

#include <algorithm>

namespace NKikimr {
namespace NKqp {

namespace {

bool AddInfoUnitLocal(TInfoUnitSet& target, const TInfoUnit& iu) {
    return target.insert(iu).second;
}

const TInfoUnitSet& EmptyInfoUnitSet() {
    static const TInfoUnitSet empty;
    return empty;
}

bool HasDuplicateOutputs(const TVector<TInfoUnit>& outputIUs) {
    TInfoUnitSet seen;
    for (const auto& iu : outputIUs) {
        if (!AddInfoUnitLocal(seen, iu)) {
            return true;
        }
    }
    return false;
}

bool SatisfiesNameConstraintsAtOutput(const TIntrusivePtr<IOperator>& op, const TVector<TInfoUnit>& outputIUs, const TPlanProps& props) {
    if (HasDuplicateOutputs(outputIUs)) {
        return false;
    }

    for (const auto& [parent, childIdx] : op->Parents) {
        const auto& forbidden = props.NameConstraints.GetForbiddenOut(parent, childIdx, op.get());
        for (const auto& iu : outputIUs) {
            if (forbidden.contains(iu)) {
                return false;
            }
        }
    }

    return true;
}

bool ContainsInfoUnit(const TVector<TInfoUnit>& ius, const TInfoUnit& iu) {
    return std::find(ius.begin(), ius.end(), iu) != ius.end();
}

bool ProducesMapElement(const TIntrusivePtr<TOpMap>& map, const TInfoUnit& iu) {
    return std::any_of(map->MapElements.begin(), map->MapElements.end(), [&iu](const TMapElement& element) {
        return element.GetElementName() == iu;
    });
}

bool HasRenameSource(const TIntrusivePtr<TOpMap>& map, const TInfoUnit& iu) {
    return std::any_of(map->MapElements.begin(), map->MapElements.end(), [&iu](const TMapElement& element) {
        return element.IsRename() && element.GetRename() == iu;
    });
}

bool HasDependency(const TIntrusivePtr<TOpAddDependencies>& deps, const TInfoUnit& iu) {
    return ContainsInfoUnit(deps->Dependencies, iu);
}

bool CanRenameOutput(const TIntrusivePtr<IOperator>& op, const TInfoUnit& from, const TInfoUnit& to, const TPlanProps& props) {
    const auto output = op->GetOutputIUs();
    if (!ContainsInfoUnit(output, from) || ContainsInfoUnit(output, to)) {
        return false;
    }
    return !props.NameConstraints.IsForbiddenAtOutput(op.get(), to);
}

bool IsTransparentUnary(const TIntrusivePtr<IOperator>& op, const TInfoUnit& from) {
    switch (op->Kind) {
        case EOperator::Filter:
        case EOperator::Limit:
        case EOperator::Sort:
            return true;
        case EOperator::AddDependencies:
            return !HasDependency(CastOperator<TOpAddDependencies>(op), from);
        default:
            return false;
    }
}

bool IsPassThroughMap(const TIntrusivePtr<TOpMap>& map, const TInfoUnit& from) {
    return !ProducesMapElement(map, from) && !HasRenameSource(map, from);
}

TIntrusivePtr<IOperator> SelectJoinInputForRename(const TIntrusivePtr<TOpJoin>& join, const TInfoUnit& from) {
    const bool leftHas = ContainsInfoUnit(join->GetLeftInput()->GetOutputIUs(), from);
    const bool rightHas = ContainsInfoUnit(join->GetRightInput()->GetOutputIUs(), from);
    if (leftHas == rightHas) {
        return nullptr;
    }

    return leftHas ? join->GetLeftInput() : join->GetRightInput();
}

struct TRenamePath {
    TIntrusivePtr<IOperator> Producer;
    TVector<TIntrusivePtr<IOperator>> TransparentOps;
};

bool BuildRenamePath(const TIntrusivePtr<TOpMap>& topMap, const TInfoUnit& from, const TInfoUnit& to, const TPlanProps& props, TRenamePath& path) {
    auto current = topMap->GetInput();

    while (current) {
        if (!current->IsSingleConsumer() || !CanRenameOutput(current, from, to, props)) {
            return false;
        }

        if (current->Kind == EOperator::Source) {
            path.Producer = current;
            return true;
        }

        if (current->Kind == EOperator::Map) {
            auto map = CastOperator<TOpMap>(current);
            if (ProducesMapElement(map, from)) {
                path.Producer = current;
                return true;
            }
            if (!IsPassThroughMap(map, from)) {
                return false;
            }
            path.TransparentOps.push_back(current);
            current = map->GetInput();
            continue;
        }

        if (current->Kind == EOperator::Join) {
            auto join = CastOperator<TOpJoin>(current);
            auto next = SelectJoinInputForRename(join, from);
            if (!next) {
                return false;
            }

            path.TransparentOps.push_back(current);
            current = next;
            continue;
        }

        if (!IsTransparentUnary(current, from)) {
            return false;
        }

        path.TransparentOps.push_back(current);
        current = CastOperator<IUnaryOperator>(current)->GetInput();
    }

    return false;
}

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
            AddInfoUnitLocal(renameSources, element.GetRename());
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

void RenameProducerOutput(const TIntrusivePtr<IOperator>& producer, const TInfoUnit& from, const TInfoUnit& to, TExprContext& ctx) {
    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> renameMap{{from, to}};

    if (producer->Kind == EOperator::Source) {
        producer->RenameIUs(renameMap, ctx);
        return;
    }

    auto producerMap = CastOperator<TOpMap>(producer);
    for (auto& element : producerMap->MapElements) {
        if (element.GetElementName() == from) {
            element.SetElementName(to);
            return;
        }
    }

    Y_ENSURE(false, "Rename producer does not produce the expected output");
}

void RewriteTransparentOps(const TVector<TIntrusivePtr<IOperator>>& ops, const TInfoUnit& from, const TInfoUnit& to, TExprContext& ctx) {
    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> renameMap{{from, to}};
    for (auto it = ops.rbegin(); it != ops.rend(); ++it) {
        (*it)->RenameIUs(renameMap, ctx);
    }
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
    const TMapElement& element,
    const TInfoUnitSet& liveOut,
    const TPlanProps& props,
    TInfoUnit& from,
    TInfoUnit& to)
{
    to = element.GetElementName();
    if (element.IsRename()) {
        from = element.GetRename();
        return RenameNeedsPush(topMap, element, liveOut, props);
    }

    if (!element.IsColumnAccess()) {
        return false;
    }

    from = element.GetColumnAccess();
    return liveOut.contains(to) && !liveOut.contains(from);
}

bool TryPushRename(const TIntrusivePtr<TOpMap>& topMap, size_t renameIdx, const TInfoUnit& from, const TInfoUnit& to, TRBOContext& ctx, TPlanProps& props) {
    if (from == to || !topMap->IsSingleConsumer() || !CanRewriteResidualTopMap(topMap, renameIdx, from, to)) {
        return false;
    }

    TRenamePath path;
    if (!BuildRenamePath(topMap, from, to, props, path)) {
        return false;
    }

    const auto topOutputAfterPush = SimulateTopMapOutputAfterPush(topMap, renameIdx, from, to);
    if (!SatisfiesNameConstraintsAtOutput(topMap, topOutputAfterPush, props)) {
        return false;
    }

    RenameProducerOutput(path.Producer, from, to, ctx.ExprCtx);
    RewriteTransparentOps(path.TransparentOps, from, to, ctx.ExprCtx);
    RemoveTopRenameAndRewriteResiduals(topMap, renameIdx, from, to);

    return true;
}

} // anonymous namespace

bool TPushRenameRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto topMap = CastOperator<TOpMap>(input);
    const auto liveIt = props.LiveOut.find(topMap.get());
    const auto& liveOut = liveIt == props.LiveOut.end() ? EmptyInfoUnitSet() : liveIt->second;

    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        const auto& element = topMap->MapElements[idx];
        TInfoUnit from;
        TInfoUnit to;
        if (!TryBuildRenameCandidate(topMap, element, liveOut, props, from, to)) {
            continue;
        }

        if (!TryPushRename(topMap, idx, from, to, ctx, props)) {
            continue;
        }

        if (topMap->MapElements.empty() && SatisfiesNameConstraintsAtOutput(topMap, topMap->GetInput()->GetOutputIUs(), props)) {
            input = topMap->GetInput();
        }
        return true;
    }

    return false;
}

} // namespace NKqp
} // namespace NKikimr
