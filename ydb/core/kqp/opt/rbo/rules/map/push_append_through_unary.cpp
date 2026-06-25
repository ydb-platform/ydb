#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>
#include <ydb/core/kqp/opt/rbo/rules/map/projection_pruning_helpers.h>

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

TVector<TInfoUnit> BuildUnaryOutput(const TIntrusivePtr<IUnaryOperator>& unary, const TVector<TInfoUnit>& inputOutput) {
    if (unary->Kind != EOperator::AddDependencies) {
        return inputOutput;
    }

    auto output = inputOutput;
    const auto addDependencies = CastOperator<TOpAddDependencies>(unary);
    output.insert(output.end(), addDependencies->Dependencies.begin(), addDependencies->Dependencies.end());
    return output;
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
    TVector<TInfoUnit> pushedOutput = inputIUs;
    TVector<TInfoUnit> unaryOutput = BuildUnaryOutput(unary, pushedOutput);

    TVector<TMapElement> pushedElements;
    TVector<TMapElement> topElements;

    // Greedily keep candidates that do not introduce conflicts at the lower boundary.
    auto canAcceptPushedElement = [&](const TMapElement& mapElement) {
        auto candidateElements = pushedElements;
        candidateElements.push_back(mapElement);

        auto candidatePushedOutput = BuildMapOutput(inputIUs, candidateElements);
        bool valid = MakeInfoUnitSet(candidatePushedOutput).size() == candidatePushedOutput.size();
        const auto candidateUnaryOutput = BuildUnaryOutput(unary, candidatePushedOutput);
        valid = valid && MakeInfoUnitSet(candidateUnaryOutput).size() == candidateUnaryOutput.size();

        if (valid) {
            pushedElements = std::move(candidateElements);
            pushedOutput = std::move(candidatePushedOutput);
            unaryOutput = std::move(candidateUnaryOutput);
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

    if (topElements.empty()) {
        if (!IUSetIntersect(unaryOutput, GetForbidden(props, topMap.get())).empty()) {
            return input;
        }
        pushedMap->Props.OutputIUs = pushedOutput;
        unary->SetInput(pushedMap);
        unary->Props.OutputIUs = unaryOutput;
        return unary;
    }

    const auto newTopOutput = BuildMapOutput(unaryOutput, topElements);
    if (MakeInfoUnitSet(newTopOutput).size() != newTopOutput.size() ||
        !IUSetIntersect(newTopOutput, GetForbidden(props, topMap.get())).empty()) {
        return input;
    }

    pushedMap->Props.OutputIUs = pushedOutput;
    unary->SetInput(pushedMap);
    unary->Props.OutputIUs = unaryOutput;
    auto newTopMap = MakeIntrusive<TOpMap>(unary, topMap->Pos, topElements, topMap->Ordered);
    newTopMap->Props.OutputIUs = newTopOutput;
    return newTopMap;
}

} // namespace NKqp
} // namespace NKikimr
