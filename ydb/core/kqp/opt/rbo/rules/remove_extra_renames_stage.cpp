#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <yql/essentials/utils/log/log.h>

#include <algorithm>

namespace NKikimr {
namespace NKqp {

namespace {

bool IsPureRenameMap(const TIntrusivePtr<TOpMap>& map) {
    return std::all_of(map->MapElements.begin(), map->MapElements.end(), [](const TMapElement& element) {
        return element.IsRename();
    });
}

bool HasDuplicateOutputs(const TIntrusivePtr<TOpMap>& map) {
    THashSet<TInfoUnit, TInfoUnit::THashFunction> seen;
    for (const auto& iu : map->GetOutputIUs()) {
        if (seen.contains(iu)) {
            return true;
        }
        seen.insert(iu);
    }
    return false;
}

bool MergePureRenameIntoInputMap(const TIntrusivePtr<TOpMap>& topMap) {
    if (!IsPureRenameMap(topMap) || topMap->GetInput()->Kind != EOperator::Map || !topMap->GetInput()->IsSingleConsumer()) {
        return false;
    }

    auto bottomMap = CastOperator<TOpMap>(topMap->GetInput());
    THashMap<TInfoUnit, TVector<TInfoUnit>, TInfoUnit::THashFunction> topRenamesBySource;
    for (const auto& topElement : topMap->MapElements) {
        topRenamesBySource[topElement.GetRename()].push_back(topElement.GetElementName());
    }

    TVector<TMapElement> mergedElements;
    THashSet<TInfoUnit, TInfoUnit::THashFunction> rewiredProducedColumns;

    for (const auto& bottomElement : bottomMap->MapElements) {
        const auto produced = bottomElement.GetElementName();
        const auto topIt = topRenamesBySource.find(produced);
        if (topIt == topRenamesBySource.end()) {
            mergedElements.push_back(bottomElement);
            continue;
        }

        rewiredProducedColumns.insert(produced);
        for (const auto& to : topIt->second) {
            auto rewired = bottomElement;
            rewired.SetElementName(to);
            mergedElements.push_back(rewired);
        }
    }

    for (const auto& topElement : topMap->MapElements) {
        const auto from = topElement.GetRename();
        if (!rewiredProducedColumns.contains(from)) {
            mergedElements.emplace_back(topElement.GetElementName(), from, topMap->Pos, topElement.GetExpression().Ctx, topElement.GetExpression().PlanProps);
        }
    }

    auto oldElements = std::move(bottomMap->MapElements);
    bottomMap->MapElements = std::move(mergedElements);
    if (HasDuplicateOutputs(bottomMap)) {
        bottomMap->MapElements = std::move(oldElements);
        return false;
    }

    topMap->MapElements.clear();
    return true;
}

} // anonymous namespace

TRenameStage::TRenameStage()
    : IRBOStage("Remove redundant maps") {
    Props = ERuleProperties::RequireParents;
}

void TRenameStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    Y_UNUSED(ctx);

    for (auto it : root) {
        if (it.Current->Kind != EOperator::Map) {
            continue;
        }

        auto topMap = CastOperator<TOpMap>(it.Current);
        if (!MergePureRenameIntoInputMap(topMap)) {
            continue;
        }

        auto replacement = topMap->GetInput();
        if (it.Current->Parents.empty()) {
            if (it.SubplanIU) {
                root.PlanProps.Subplans.Replace(*it.SubplanIU, replacement);
            } else {
                root.SetInput(replacement);
            }
        } else {
            for (auto& [parent, childIdx] : it.Current->Parents) {
                parent->Children[childIdx] = replacement;
            }
        }
    }
}

}
}
