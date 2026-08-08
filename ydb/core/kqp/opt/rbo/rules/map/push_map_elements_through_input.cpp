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
// A: Map [ a := f(b) ]      -- computed expressions stay above every input
// B: `- Operator B             boundary. Filter, Limit, TopSort, and Join can
// C:    `- input               all discard rows, so moving a partial expression
//                              below them can make an otherwise lazy error
//                              observable. This rule only moves direct column
//                              accesses and semantic renames.
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

bool CanPushAppendToChild(
    const IOperator& op,
    ui32 childIdx,
    const TMapElement& mapElement)
{
    return mapElement.IsColumnAccess() &&
        mapElement.DependsOnlyOn(op.Children[childIdx]->GetOutputIUs());
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
    TVector<bool> pushed(topMap->MapElements.size(), false);
    TRenameMap renameMap;
    TInfoUnitSet keptOutputs;
    TInfoUnitSet pushableOutputs;

    for (const auto& mapElement : topMap->MapElements) {
        if (mapElement.IsRename()) {
            continue;
        }

        bool canPush = false;
        for (ui32 childIdx = 0; childIdx < op->Children.size(); ++childIdx) {
            if (CanPushAppendToChild(*op, childIdx, mapElement)) {
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
        for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
            const auto& mapElement = topMap->MapElements[idx];
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

    for (size_t idx = 0; idx < topMap->MapElements.size(); ++idx) {
        if (!pushed[idx]) {
            topElements.push_back(topMap->MapElements[idx]);
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
        props.Subplans.RenameReferences(renameMap, ctx.ExprCtx);
    }

    if (topElements.empty()) {
        return op;
    }

    return MakeIntrusive<TOpMap>(op, topMap->Pos, topElements, topMap->Ordered);
}

} // namespace NKqp
} // namespace NKikimr
