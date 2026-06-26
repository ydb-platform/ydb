#include <ydb/core/kqp/opt/rbo/rules/map/rename_common.h>
#include <ydb/core/kqp/opt/rbo/rules/map/map_output_utils.h>

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

TVector<TInfoUnit> BuildJoinOutput(const TString& joinKind, TVector<TInfoUnit> leftOutput, TVector<TInfoUnit> rightOutput) {
    if (joinKind == "LeftOnly" || joinKind == "LeftSemi") {
        rightOutput.clear();
    }
    if (joinKind == "RightOnly" || joinKind == "RightSemi") {
        leftOutput.clear();
    }

    leftOutput.insert(leftOutput.end(), rightOutput.begin(), rightOutput.end());
    return leftOutput;
}

} // anonymous namespace

bool TPushRenameThroughJoinSideRule::MatchAndApply(TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    if (input->Kind != EOperator::Map) {
        return false;
    }

    auto topMap = CastOperator<TOpMap>(input);
    const auto candidate = NMapRules::FindRenameCandidate(topMap);
    if (!candidate || !NMapRules::CanStartLocalRenamePush(topMap, *candidate)) {
        return false;
    }

    if (topMap->GetInput()->Kind != EOperator::Join) {
        return false;
    }

    auto join = CastOperator<TOpJoin>(topMap->GetInput());
    if (!join->IsSingleConsumer() || !NMapRules::CanRenameOutput(join, candidate->From, candidate->To)) {
        return false;
    }

    const auto selectedInput = SelectJoinInputForRename(join, candidate->From);
    if (!selectedInput || !selectedInput->IsSingleConsumer() ||
        !NMapRules::CanRenameOutput(selectedInput, candidate->From, candidate->To)) {
        return false;
    }

    const bool pushLeft = selectedInput == join->GetLeftInput();
    const TVector<TMapElement> pushedElements{NMapRules::MakeRenameElement(*candidate, topMap)};
    const auto pushedOutput = BuildMapOutput(selectedInput->GetOutputIUs(), pushedElements);
    if (MakeInfoUnitSet(pushedOutput).size() != pushedOutput.size()) {
        return false;
    }

    const auto output = BuildJoinOutput(
        join->JoinKind,
        pushLeft ? pushedOutput : join->GetLeftInput()->GetOutputIUs(),
        pushLeft ? join->GetRightInput()->GetOutputIUs() : pushedOutput);
    if (MakeInfoUnitSet(output).size() != output.size() ||
        !NMapRules::CanFinishRenamePush(topMap, *candidate, output)) {
        return false;
    }

    auto pushedMap = MakeIntrusive<TOpMap>(selectedInput, topMap->Pos, pushedElements);
    pushedMap->Props.OutputIUs = pushedOutput;
    if (pushLeft) {
        join->SetLeftInput(pushedMap);
    } else {
        join->SetRightInput(pushedMap);
    }
    join->RenameIUs({{candidate->From, candidate->To}}, ctx.ExprCtx);
    join->Props.OutputIUs = output;
    return NMapRules::FinishRenamePush(input, topMap, *candidate, output, ctx, props);
}

} // namespace NKqp
} // namespace NKikimr
