#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

#include <optional>

namespace NKikimr {
namespace NKqp {

// Main shapes this handles:
// A: Map [ a := b, x <- c ] == becomes ==>  Unary
// B: `- Unary                                  `- Map [ a := b, x <- c ]
// C:    `- input                                  `- input
//
// A: Map [ x := l, y <- r ]  == becomes ==>  Join
// B: `- Join                                  |- Map [ x := l ]
// C:    |- left                               |  `- left
// D:    `- right                              `- Map [ y <- r ]
// E:                                              `- right
//
// Where: Unary = either Filter, Limit or Sort. Join inputs are selected by the
// child output that the map element depends on.
//
// Caveats:
// 1.
// A: Map [ a := f(b) ]      -- expression appends move through Filter, move
// B: `- Sort                   through Limit/Sort only when this rule is
// C:    `- input               configured to push expressions, and move below
//                              joins only to a side whose rows are preserved.
//
// 2.
// A: Map [ a := b ]         -- move prevented if operator B has multiple
// B: `- Operator B             consumers; pushing below B would require cloning
// C:    `- input               B to avoid changing Parent2's input.
// D: Parent2
// E: `- Operator B
//
// 3.
// A: Map [ a := f(b), x <- a ] -- rename x <- a stays above B if a := f(b)
// B: `- Operator B               stays above it; otherwise x would rename the
// C:    `- input                 input name that the top expression still needs.

namespace {

using TRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>;

bool CanPushThroughInputOperator(const IOperator& op) {
    switch (op.Kind) {
        case EOperator::Filter:
        case EOperator::Limit:
        case EOperator::Sort:
        case EOperator::Join:
            return true;
        default:
            return false;
    }
}

bool IsLeftPreserved(const TString& joinKind) {
    return joinKind == "Inner" || joinKind == "Cross" || joinKind == "Left" || joinKind == "LeftOnly" || joinKind == "LeftSemi";
}

bool IsRightPreserved(const TString& joinKind) {
    return joinKind == "Inner" || joinKind == "Cross";
}

bool IsJoinChildPreserved(const TOpJoin& join, ui32 childIdx) {
    Y_ENSURE(childIdx < join.Children.size());
    return childIdx == 0
        ? IsLeftPreserved(join.JoinKind)
        : IsRightPreserved(join.JoinKind);
}

std::optional<ui32> SelectAvailableChild(const IOperator& op, const TMapElement& mapElement) {
    std::optional<ui32> result;
    for (ui32 childIdx = 0; childIdx < op.Children.size(); ++childIdx) {
        if (!mapElement.DependsOnlyOn(op.Children[childIdx]->GetOutputIUs())) {
            continue;
        }
        if (result) {
            return std::nullopt;
        }
        result = childIdx;
    }
    return result;
}

std::optional<ui32> SelectExpressionChild(
    const IOperator& op,
    const TMapElement& mapElement,
    bool pushExpressions)
{
    if (op.Kind != EOperator::Join) {
        if (op.Kind != EOperator::Filter && !pushExpressions) {
            return std::nullopt;
        }
        return SelectAvailableChild(op, mapElement);
    }

    const auto& join = static_cast<const TOpJoin&>(op);
    if (auto childIdx = SelectAvailableChild(op, mapElement)) {
        if (IsJoinChildPreserved(join, *childIdx)) {
            return childIdx;
        }
        return std::nullopt;
    }

    if (!mapElement.GetExpression().GetInputIUs(false, true).empty()) {
        return std::nullopt;
    }

    for (ui32 childIdx = 0; childIdx < op.Children.size(); ++childIdx) {
        if (IsJoinChildPreserved(join, childIdx)) {
            return childIdx;
        }
    }
    return std::nullopt;
}

std::optional<ui32> SelectAppendChild(
    const IOperator& op,
    const TMapElement& mapElement,
    bool pushExpressions)
{
    if (mapElement.IsColumnAccess()) {
        return SelectAvailableChild(op, mapElement);
    }
    return SelectExpressionChild(op, mapElement, pushExpressions);
}

} // anonymous namespace

TIntrusivePtr<IOperator>
TPushMapElementsThroughInputRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto topMap = CastOperator<TOpMap>(input);
    auto op = topMap->GetInput();
    if (!CanPushThroughInputOperator(*op) || !op->IsSingleConsumer()) {
        return input;
    }

    TVector<TVector<TMapElement>> pushedElements(op->Children.size());
    TVector<TMapElement> topElements;
    TRenameMap renameMap;
    THashMap<TInfoUnit, ui32, TInfoUnit::THashFunction> pushedOutputChild;
    TInfoUnitSet keptOutputs;

    for (const auto& mapElement : topMap->MapElements) {
        if (mapElement.IsRename()) {
            continue;
        }

        if (const auto childIdx = SelectAppendChild(*op, mapElement, PushExpressions)) {
            pushedOutputChild.emplace(mapElement.GetElementName(), *childIdx);
        } else {
            keptOutputs.insert(mapElement.GetElementName());
        }
    }

    for (const auto& mapElement : topMap->MapElements) {
        std::optional<ui32> childIdx;

        if (mapElement.IsRename()) {
            const auto source = mapElement.GetRename();
            if (keptOutputs.contains(source)) {
                topElements.push_back(mapElement);
                continue;
            }

            if (const auto pushed = pushedOutputChild.find(source); pushed != pushedOutputChild.end()) {
                childIdx = pushed->second;
            } else {
                childIdx = SelectAvailableChild(*op, mapElement);
            }
        } else if (const auto pushed = pushedOutputChild.find(mapElement.GetElementName()); pushed != pushedOutputChild.end()) {
            childIdx = pushed->second;
        }

        if (!childIdx) {
            topElements.push_back(mapElement);
            continue;
        }

        pushedElements[*childIdx].push_back(mapElement);
        if (mapElement.IsRename() && mapElement.GetRename() != mapElement.GetElementName()) {
            renameMap.emplace(mapElement.GetRename(), mapElement.GetElementName());
        }
    }

    bool pushed = false;
    for (const auto& elements : pushedElements) {
        pushed |= !elements.empty();
    }
    if (!pushed) {
        return input;
    }

    if (!renameMap.empty()) {
        for (auto& mapElement : topElements) {
            if (!mapElement.IsRename()) {
                mapElement.SetExpression(mapElement.GetExpression().ApplyRenames(renameMap));
            }
        }
    }

    for (ui32 childIdx = 0; childIdx < pushedElements.size(); ++childIdx) {
        if (!pushedElements[childIdx].empty()) {
            auto originalChild = op->Children[childIdx];
            op->Children[childIdx] = MakeIntrusive<TOpMap>(originalChild, topMap->Pos, pushedElements[childIdx]);
        }
    }

    if (!renameMap.empty()) {
        op->RenameUsedIUs(renameMap, ctx.ExprCtx);
        props.Subplans.RenameReferences(renameMap, ctx.ExprCtx);
    }

    if (topElements.empty()) {
        return op;
    }

    return MakeIntrusive<TOpMap>(op, topMap->Pos, topElements, topMap->Ordered);
}

} // namespace NKqp
} // namespace NKikimr
