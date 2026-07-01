#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>

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

} // anonymous namespace

bool TPushRenameThroughTransparentUnaryRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto topMap = CastOperator<TOpMap>(input);
    const auto candidate = NMapRules::FindRenameCandidate(topMap, props);
    if (!candidate || !NMapRules::CanStartLocalRenamePush(topMap, *candidate, props)) {
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
        !NMapRules::CanRenameOutput(unary, candidate->From, candidate->To, props)) {
        return false;
    }

    const auto oldInput = unary->GetInput();
    auto pushedMap = MakeIntrusive<TOpMap>(oldInput, topMap->Pos, TVector<TMapElement>{NMapRules::MakeRenameElement(*candidate, topMap)});
    if (HasOutputConflicts(pushedMap->GetOutputIUs())) {
        return false;
    }

    unary->SetInput(pushedMap);
    unary->RenameIUs({{candidate->From, candidate->To}}, ctx.ExprCtx);
    if (HasOutputConflicts(unary->GetOutputIUs())) {
        unary->RenameIUs({{candidate->To, candidate->From}}, ctx.ExprCtx);
        unary->SetInput(oldInput);
        return false;
    }

    return NMapRules::FinishRenamePush(input, topMap, *candidate, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr
