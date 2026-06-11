#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool CanPushAliasAppendThroughUnary(EOperator kind, bool pushUnderFilter) {
    switch (kind) {
        case EOperator::Filter:
            return pushUnderFilter;
        case EOperator::Limit:
        case EOperator::Sort:
        case EOperator::AddDependencies:
            return true;
        default:
            return false;
    }
}

} // anonymous namespace

TIntrusivePtr<IOperator>
TPushAppendThroughUnaryRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Map) {
        return input;
    }

    auto topMap = CastOperator<TOpMap>(input);
    const auto unaryKind = topMap->GetInput()->Kind;
    const bool canPushAlias = CanPushAliasAppendThroughUnary(unaryKind, PushUnderFilter);
    const bool canPushExpression = PushUnderFilter && unaryKind == EOperator::Filter;

    if (!canPushAlias && !canPushExpression) {
        return input;
    }

    auto unary = CastOperator<IUnaryOperator>(topMap->GetInput());
    if (!unary->IsSingleConsumer()) {
        return input;
    }

    const auto unaryInput = unary->GetInput();
    const auto inputIUs = unaryInput->GetOutputIUs();

    TVector<TMapElement> pushedElements;
    TVector<TMapElement> topElements;

    // Greedily keep candidates that do not introduce conflicts at the lower boundary.
    auto canAcceptPushedElement = [&](const TMapElement& mapElement) {
        pushedElements.push_back(mapElement);

        auto pushedMap = MakeIntrusive<TOpMap>(unaryInput, topMap->Pos, pushedElements);
        bool valid = !HasOutputConflicts(pushedMap->GetOutputIUs());
        if (valid) {
            unary->SetInput(pushedMap);
            valid = !HasOutputConflicts(unary->GetOutputIUs());
            unary->SetInput(unaryInput);
        }

        if (!valid) {
            pushedElements.pop_back();
        }
        return valid;
    };

    for (const auto& mapElement : topMap->MapElements) {
        const bool isExtractableAppend = topMap->IsExtractableAppend(mapElement);
        const bool canMove = isExtractableAppend &&
            ((canPushAlias && mapElement.IsColumnAccess()) ||
             (canPushExpression && !mapElement.IsColumnAccess()));

        if (canMove && mapElement.DependsOnlyOn(inputIUs) && canAcceptPushedElement(mapElement)) {
            continue;
        }
        topElements.push_back(mapElement);
    }

    if (pushedElements.empty()) {
        return input;
    }

    auto pushedMap = MakeIntrusive<TOpMap>(unaryInput, topMap->Pos, pushedElements);
    unary->SetInput(pushedMap);

    if (topElements.empty()) {
        if (!CanReplaceInParents(topMap, unary, props)) {
            unary->SetInput(unaryInput);
            return input;
        }
        return unary;
    }

    auto newTopMap = MakeIntrusive<TOpMap>(unary, topMap->Pos, topElements, topMap->Ordered);
    if (!CanExposeOutput(topMap, newTopMap->GetOutputIUs(), props)) {
        unary->SetInput(unaryInput);
        return input;
    }

    return newTopMap;
}

} // namespace NKqp
} // namespace NKikimr
