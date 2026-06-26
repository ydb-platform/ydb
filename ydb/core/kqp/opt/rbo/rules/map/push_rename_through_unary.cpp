#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>
#include <ydb/core/kqp/opt/rbo/rules/map/map_output_utils.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool HasDependency(const TIntrusivePtr<TOpAddDependencies>& deps, const TInfoUnit& iu) {
    return ContainsInfoUnit(deps->Dependencies, iu);
}

bool IsTransparentUnary(const TIntrusivePtr<IOperator>& op, const TInfoUnit& from) {
    switch (op->Kind) {
        case EOperator::Filter:
        case EOperator::Limit:
        case EOperator::Sort:
            return true;
        case EOperator::AddDependencies:
            return !HasDependency(CastOperator<TOpAddDependencies>(op), from);
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

bool TPushRenameThroughTransparentUnaryRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto topMap = CastOperator<TOpMap>(input);
    const auto candidate = NMapRules::FindRenameCandidate(topMap);
    if (!candidate || !NMapRules::CanStartLocalRenamePush(topMap, *candidate)) {
        return false;
    }

    if (topMap->GetInput()->Kind != EOperator::Filter &&
        topMap->GetInput()->Kind != EOperator::Limit &&
        topMap->GetInput()->Kind != EOperator::Sort &&
        topMap->GetInput()->Kind != EOperator::AddDependencies) {
        return false;
    }

    // Append aliases are handled by append-push rules. In the logical stage those
    // rules intentionally do not push appends under filters, because filter pushdown
    // would otherwise move the same map back above the filter.
    if (!PushAppendAliasesUnderFilter && topMap->GetInput()->Kind == EOperator::Filter && !candidate->FromRenameElement) {
        return false;
    }

    auto unary = CastOperator<IUnaryOperator>(topMap->GetInput());
    if (!unary->IsSingleConsumer() || !IsTransparentUnary(unary, candidate->From) ||
        !NMapRules::CanRenameOutput(unary, candidate->From, candidate->To)) {
        return false;
    }

    const auto oldInput = unary->GetInput();
    const TVector<TMapElement> pushedElements{NMapRules::MakeRenameElement(*candidate, topMap)};
    const auto pushedOutput = BuildMapOutput(oldInput->GetOutputIUs(), pushedElements);
    if (MakeInfoUnitSet(pushedOutput).size() != pushedOutput.size()) {
        return false;
    }

    const auto output = BuildUnaryOutput(unary, pushedOutput);
    if (MakeInfoUnitSet(output).size() != output.size() ||
        !NMapRules::CanFinishRenamePush(topMap, *candidate, output)) {
        return false;
    }

    auto pushedMap = MakeIntrusive<TOpMap>(oldInput, topMap->Pos, pushedElements);
    pushedMap->Props.OutputIUs = pushedOutput;
    unary->SetInput(pushedMap);
    unary->RenameIUs({{candidate->From, candidate->To}}, ctx.ExprCtx);
    unary->Props.OutputIUs = output;
    return NMapRules::FinishRenamePush(input, topMap, *candidate, output, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr
