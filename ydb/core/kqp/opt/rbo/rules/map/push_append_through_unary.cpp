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
    Y_UNUSED(props);

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

    for (const auto& mapElement : topMap->MapElements) {
        const bool isExtractableAppend = topMap->IsExtractableAppend(mapElement);
        const bool canMove = isExtractableAppend &&
            ((canPushAlias && mapElement.IsColumnAccess()) ||
             (canPushExpression && !mapElement.IsColumnAccess()));

        if (canMove && mapElement.DependsOnlyOn(inputIUs)) {
            pushedElements.push_back(mapElement);
            continue;
        }
        topElements.push_back(mapElement);
    }

    if (pushedElements.empty()) {
        return input;
    }

    auto pushedMap = MakeIntrusive<TOpMap>(unaryInput, topMap->Pos, pushedElements);

    if (topElements.empty()) {
        unary->SetInput(pushedMap);
        return unary;
    }

    unary->SetInput(pushedMap);
    return MakeIntrusive<TOpMap>(unary, topMap->Pos, topElements, topMap->Ordered);
}

} // namespace NKqp
} // namespace NKikimr
