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
// B: `- Sort                   or with shadowing renames; alias appends and
// C:    `- input               semantic renames may move through Limit and Sort.
//
// 2.
// A: Map [ a := b ]         -- move prevented if Unary B has multiple consumers;
// B: `- Unary B                pushing below B would require cloning Unary B to
// C:    `- input               avoid changing Parent2's input.
// D: Parent2
// E: `- Unary B
//
// 3.
// A: Map [ a := b, x <- a ] -- append and shadowing rename move together so
// B: `- Unary                  the source name exposed below Unary B is still
// C:    `- input               hidden above it.

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
TPushAppendThroughUnaryRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
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

    TInfoUnitSet movedRenameSources;
    TRenameMap renameMap;
    for (const auto& mapElement : topMap->MapElements) {
        if (mapElement.IsRename() && mapElement.GetRename() != mapElement.GetElementName()) {
            movedRenameSources.insert(mapElement.GetRename());
            renameMap.emplace(mapElement.GetRename(), mapElement.GetElementName());
        }
    }

    TVector<TMapElement> pushedElements;
    TVector<TMapElement> topElements;

    for (const auto& mapElement : topMap->MapElements) {
        if (mapElement.IsRename()) {
            if (mapElement.GetRename() != mapElement.GetElementName()) {
                pushedElements.push_back(mapElement);
                continue;
            }

            topElements.push_back(mapElement);
            continue;
        }

        if (mapElement.IsColumnAccess() ||
            unary->Kind == EOperator::Filter ||
            movedRenameSources.contains(mapElement.GetElementName())) {
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
