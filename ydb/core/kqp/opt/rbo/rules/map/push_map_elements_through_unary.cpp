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
// A: Map [ a := f(b) ]      -- expression appends move through Filter, and move
// B: `- Sort                   through Limit/Sort only when this rule is
// C:    `- input               configured to push expressions.
//
// 2.
// A: Map [ a := b ]         -- move prevented if Unary B has multiple consumers;
// B: `- Unary B                pushing below B would require cloning Unary B to
// C:    `- input               avoid changing Parent2's input.
// D: Parent2
// E: `- Unary B
//
// 3.
// A: Map [ a := f(b), x <- a ] -- rename x <- a stays above Unary B if a := f(b)
// B: `- Unary                    stays above it; otherwise x would rename the
// C:    `- input                 input name that the top expression still needs.

namespace {

using TRenameMap = THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction>;

bool CanPushThroughUnary(const TIntrusivePtr<IOperator>& op) {
    Y_ENSURE(op);

    switch (op->Kind) {
        case EOperator::Filter:
        case EOperator::Limit:
        case EOperator::Sort:
            return true;
        default:
            return false;
    }
}

} // anonymous namespace

TIntrusivePtr<IOperator>
TPushMapElementsThroughUnaryRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto topMap = CastOperator<TOpMap>(input);
    if (!CanPushThroughUnary(topMap->GetInput())) {
        return input;
    }

    auto unary = CastOperator<IUnaryOperator>(topMap->GetInput());
    if (!unary->IsSingleConsumer()) {
        return input;
    }

    const auto unaryInput = unary->GetInput();
    const bool pushExpressions = PushExpressions || unary->Kind == EOperator::Filter;

    TInfoUnitSet keptExpressionOutputs;
    for (const auto& mapElement : topMap->MapElements) {
        if (!pushExpressions && !mapElement.IsRename() && !mapElement.IsColumnAccess()) {
            keptExpressionOutputs.insert(mapElement.GetElementName());
        }
    }

    TVector<TMapElement> pushedElements;
    TVector<TMapElement> topElements;
    TRenameMap renameMap;

    for (const auto& mapElement : topMap->MapElements) {
        if (mapElement.IsRename()) {
            if (keptExpressionOutputs.contains(mapElement.GetRename())) {
                topElements.push_back(mapElement);
            } else {
                pushedElements.push_back(mapElement);
                if (mapElement.GetRename() != mapElement.GetElementName()) {
                    renameMap.emplace(mapElement.GetRename(), mapElement.GetElementName());
                }
            }
            continue;
        }

        if (mapElement.IsColumnAccess() || pushExpressions) {
            pushedElements.push_back(mapElement);
            continue;
        }

        topElements.push_back(mapElement);
    }

    if (!renameMap.empty()) {
        for (auto& mapElement : topElements) {
            if (!mapElement.IsRename()) {
                mapElement.SetExpression(mapElement.GetExpression().ApplyRenames(renameMap));
            }
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
