#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>

namespace NKikimr {
namespace NKqp {

namespace {

TIntrusivePtr<IOperator> SelectJoinInputForRename(const TIntrusivePtr<TOpJoin>& join, const TInfoUnit& from) {
    const bool leftHas = ContainsInfoUnit(join->GetLeftInput()->GetOutputIUs(), from);
    const bool rightHas = ContainsInfoUnit(join->GetRightInput()->GetOutputIUs(), from);
    if (leftHas == rightHas) {
        return nullptr;
    }

    return leftHas ? join->GetLeftInput() : join->GetRightInput();
}

} // anonymous namespace

bool TPushRenameThroughJoinSideRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto topMap = CastOperator<TOpMap>(input);
    const auto candidate = NMapRules::FindRenameCandidate(topMap, props);
    if (!candidate || !NMapRules::CanStartLocalRenamePush(topMap, *candidate, props)) {
        return false;
    }

    if (topMap->GetInput()->Kind != EOperator::Join) {
        return false;
    }

    auto join = CastOperator<TOpJoin>(topMap->GetInput());
    if (!join->IsSingleConsumer() || !NMapRules::CanRenameOutput(join, candidate->From, candidate->To, props)) {
        return false;
    }

    const auto selectedInput = SelectJoinInputForRename(join, candidate->From);
    if (!selectedInput || !selectedInput->IsSingleConsumer() ||
        !NMapRules::CanRenameOutput(selectedInput, candidate->From, candidate->To, props)) {
        return false;
    }

    const bool pushLeft = selectedInput == join->GetLeftInput();
    const auto oldLeftInput = join->GetLeftInput();
    const auto oldRightInput = join->GetRightInput();
    const auto oldKeys = join->JoinKeys;
    const auto oldFilters = join->JoinFilters;

    auto pushedMap = MakeIntrusive<TOpMap>(selectedInput, topMap->Pos, TVector<TMapElement>{NMapRules::MakeRenameElement(*candidate, topMap)});
    if (HasOutputConflicts(pushedMap->GetOutputIUs())) {
        return false;
    }

    if (pushLeft) {
        join->SetLeftInput(pushedMap);
    } else {
        join->SetRightInput(pushedMap);
    }
    join->RenameIUs({{candidate->From, candidate->To}}, ctx.ExprCtx);
    if (HasOutputConflicts(join->GetOutputIUs())) {
        join->SetLeftInput(oldLeftInput);
        join->SetRightInput(oldRightInput);
        join->JoinKeys = oldKeys;
        join->JoinFilters = oldFilters;
        return false;
    }

    return NMapRules::FinishRenamePush(input, topMap, *candidate, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr
