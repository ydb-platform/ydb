#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

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

bool CanPushAppendToChild(
    const IOperator& op,
    ui32 childIdx,
    const TMapElement& mapElement,
    bool pushExpressions)
{
    const bool dependsOnlyOnChild = mapElement.DependsOnlyOn(op.Children[childIdx]->GetOutputIUs());

    if (mapElement.IsColumnAccess()) {
        return dependsOnlyOnChild;
    }

    if (op.Kind != EOperator::Join) {
        if (op.Kind != EOperator::Filter && !pushExpressions) {
            return false;
        }
        return dependsOnlyOnChild;
    }

    const auto& join = static_cast<const TOpJoin&>(op);
    if (!IsJoinChildPreserved(join, childIdx)) {
        return false;
    }

    if (dependsOnlyOnChild) {
        return true;
    }

    return mapElement.GetExpression().GetInputIUs(false, true).empty();
}

} // anonymous namespace

bool TPushMapElementsThroughInputRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Map;
}

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
    TVector<TInfoUnitSet> pushedOutputs(op->Children.size());
    TVector<TMapElement> topElements;
    TVector<bool> pushed(topMap->GetMapElements().size(), false);
    TRenameMap renameMap;
    TInfoUnitSet keptOutputs;
    TInfoUnitSet pushableOutputs;

    for (const auto& mapElement : topMap->GetMapElements()) {
        if (mapElement.IsRename()) {
            continue;
        }

        bool canPush = false;
        for (ui32 childIdx = 0; childIdx < op->Children.size(); ++childIdx) {
            if (CanPushAppendToChild(*op, childIdx, mapElement, PushExpressions)) {
                pushedOutputs[childIdx].insert(mapElement.GetElementName());
                canPush = true;
            }
        }

        if (!canPush) {
            keptOutputs.insert(mapElement.GetElementName());
        } else {
            pushableOutputs.insert(mapElement.GetElementName());
        }
    }

    bool hasPushed = false;
    for (ui32 childIdx = 0; childIdx < op->Children.size(); ++childIdx) {
        for (size_t idx = 0; idx < topMap->GetMapElements().size(); ++idx) {
            const auto& mapElement = topMap->GetMapElements()[idx];
            if (pushed[idx]) {
                continue;
            }

            if (mapElement.IsRename()) {
                const auto source = mapElement.GetRename();
                if (keptOutputs.contains(source)) {
                    continue;
                }

                if (pushableOutputs.contains(source)) {
                    if (!pushedOutputs[childIdx].contains(source)) {
                        continue;
                    }
                } else {
                    if (!mapElement.DependsOnlyOn(op->Children[childIdx]->GetOutputIUs())) {
                        continue;
                    }
                }
            } else {
                if (!pushedOutputs[childIdx].contains(mapElement.GetElementName())) {
                    continue;
                }
            }

            pushed[idx] = true;
            hasPushed = true;
            pushedElements[childIdx].push_back(mapElement);
            if (mapElement.IsRename() && mapElement.GetRename() != mapElement.GetElementName()) {
                renameMap.emplace(mapElement.GetRename(), mapElement.GetElementName());
            }
        }
    }

    if (!hasPushed) {
        return input;
    }

    for (size_t idx = 0; idx < topMap->GetMapElements().size(); ++idx) {
        if (!pushed[idx]) {
            topElements.push_back(topMap->GetMapElements()[idx]);
        }
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
        props.Subplans.RenameExternalReferences(renameMap, ctx.ExprCtx);
    }

    if (topElements.empty()) {
        return op;
    }

    return MakeIntrusive<TOpMap>(op, topMap->Pos, topElements, topMap->IsOrdered());
}

} // namespace NKqp
} // namespace NKikimr
