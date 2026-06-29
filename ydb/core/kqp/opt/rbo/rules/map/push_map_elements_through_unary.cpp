#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>

namespace NKikimr {
namespace NKqp {

// Main shape this handles:
// A: Map [ a := b, x <- c ] == becomes ==>  Unary
// B: `- Unary                                  `- Map [ a := b, x <- c ]
// C:    `- input                                  `- input
//
// Where: Unary = either Filter, Limit or Sort

// Caveats:
// 1.
// A: Map [ a := f(b) ]      -- expression appends move only through filters
// B: `- Sort                   when push-under-filter is enabled; alias appends
// C:    `- input               and semantic renames may move through Limit and Sort.
//
// 2.
// A: Map [ a := b ]         -- move prevented if Unary B has multiple consumers;
// B: `- Unary B                pushing below B would require cloning Unary B to
// C:    `- input               avoid changing Parent2's input.
// D: Parent2
// E: `- Unary B
//
// 3.
// A: Map [ x <- a ]         -- semantic rename move prevented if Map A has
// B: `- Unary                  multiple consumers; changing the name below B
// C:    `- input               would affect every consumer of A.

namespace {

using TRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>;

bool CanPushThroughUnary(const TIntrusivePtr<IOperator>& op, bool pushUnderFilter, bool isExpressionAppend = false) {
    Y_ENSURE(op);

    switch (op->Kind) {
        case EOperator::Filter:
            return pushUnderFilter;
        case EOperator::Limit:
        case EOperator::Sort:
            return !isExpressionAppend;
        default:
            return false;
    }
}

void FindRenamesToPush(
    const TIntrusivePtr<TOpMap>& topMap,
    THashSet<size_t>& pushAsRename,
    TRenameMap& renameMap)
{
    if (!topMap->IsSingleConsumer()) {
        return;
    }

    TInfoUnitSet renameSources;
    TInfoUnitSet renameTargets;
    auto hasRenameOverlap = [&renameSources, &renameTargets](const TInfoUnit& from, const TInfoUnit& to) {
        return renameSources.contains(from) ||
            renameTargets.contains(to) ||
            renameSources.contains(to) ||
            renameTargets.contains(from);
    };

    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        const auto& mapElement = topMap->MapElements[idx];
        if (!mapElement.IsRename()) {
            continue;
        }

        const auto from = mapElement.GetRename();
        const auto to = mapElement.GetElementName();
        if (from == to ||
            hasRenameOverlap(from, to)) {
            continue;
        }

        pushAsRename.insert(idx);
        renameSources.insert(from);
        renameTargets.insert(to);
        renameMap.emplace(from, to);
    }

    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        if (pushAsRename.contains(idx)) {
            continue;
        }
        const auto& mapElement = topMap->MapElements[idx];
        if (mapElement.IsRename() &&
            (renameSources.contains(mapElement.GetRename()) || renameTargets.contains(mapElement.GetRename()))) {
            pushAsRename.clear();
            renameMap.clear();
            return;
        }
    }
}

} // anonymous namespace

TIntrusivePtr<IOperator>
TPushAppendThroughUnaryRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto topMap = CastOperator<TOpMap>(input);
    if (!CanPushThroughUnary(topMap->GetInput(), PushUnderFilter)) {
        return input;
    }

    auto unary = CastOperator<IUnaryOperator>(topMap->GetInput());
    if (!unary->IsSingleConsumer()) {
        return input;
    }

    const auto unaryInput = unary->GetInput();
    THashSet<size_t> pushAsRename;
    TRenameMap renameMap;
    FindRenamesToPush(topMap, pushAsRename, renameMap);

    TVector<TMapElement> pushedElements;
    TVector<TMapElement> topElements;

    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        const auto& mapElement = topMap->MapElements[idx];
        if (pushAsRename.contains(idx)) {
            const auto expr = mapElement.GetExpression();
            pushedElements.emplace_back(mapElement.GetElementName(), mapElement.GetRename(), topMap->Pos, expr.Ctx, expr.PlanProps, true);
            continue;
        }

        if (topMap->IsExtractableAppend(mapElement) &&
            (mapElement.IsColumnAccess() || unary->Kind == EOperator::Filter)) {
            pushedElements.push_back(mapElement);
            continue;
        }

        topElements.push_back(mapElement);
        if (!renameMap.empty() && !topElements.back().IsRename()) {
            topElements.back().SetExpression(topElements.back().GetExpression().ApplyRenames(renameMap));
        }
    }

    if (pushedElements.empty()) {
        return input;
    }

    auto pushedMap = MakeIntrusive<TOpMap>(unaryInput, topMap->Pos, pushedElements);
    unary->SetInput(pushedMap);

    if (!renameMap.empty()) {
        unary->RenameIUs(renameMap, ctx.ExprCtx);
        props.Subplans.RenameIUs(renameMap, ctx.ExprCtx);
    }

    if (topElements.empty()) {
        return unary;
    }

    return MakeIntrusive<TOpMap>(unary, topMap->Pos, topElements, topMap->Ordered);
}

} // namespace NKqp
} // namespace NKikimr
