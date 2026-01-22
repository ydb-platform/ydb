#include "kqp_rbo_physical_join_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

TString TPhysicalJoinBuilder::GetValidJoinKind(const TString& joinKind) {
    const auto joinKindLowered = to_lower(joinKind);
    if (joinKindLowered == "left") {
        return "Left";
    } else if (joinKindLowered == "inner") {
        return "Inner";
    } else if (joinKindLowered == "cross") {
        return "Cross";
    }
    return joinKind;
}

TExprNode::TPtr TPhysicalJoinBuilder::BuildCrossJoin(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput) {
    TCoArgument leftArg{Ctx.NewArgument(Pos, "_kqp_left")};
    TCoArgument rightArg{Ctx.NewArgument(Pos, "_kqp_right")};

    TVector<TExprNode::TPtr> keys;
    for (const auto& iu : Join->GetLeftInput()->GetOutputIUs()) {
        YQL_CLOG(TRACE, CoreDq) << "Converting Cross Join, left key: " << iu.GetFullName();

        // clang-format off
        auto keyPtr = Build<TCoNameValueTuple>(Ctx, Pos)
            .Name().Build(iu.GetFullName())
            .Value<TCoMember>()
                .Struct(leftArg)
                .Name().Build(iu.GetFullName())
            .Build()
            .Done().Ptr();
        // clang-format on
        keys.push_back(keyPtr);
    }

    for (const auto& iu : Join->GetRightInput()->GetOutputIUs()) {
        YQL_CLOG(TRACE, CoreDq) << "Converting Cross Join, right key: " << iu.GetFullName();

        // clang-format off
        auto keyPtr = Build<TCoNameValueTuple>(Ctx, Pos)
            .Name().Build(iu.GetFullName())
            .Value<TCoMember>()
                .Struct(rightArg)
                .Name().Build(iu.GetFullName())
            .Build()
            .Done().Ptr();
        // clang-format on
        keys.push_back(keyPtr);
    }

    // clang-format off
    // We have to `Condense` right input as single-element stream of lists (single list of all elements from the right),
    // because stream supports single iteration only
    //auto itemArg = Build<TCoArgument>(Ctx, Pos).Name("item").Done();
    auto rightAsStreamOfLists = Build<TCoCondense1>(Ctx, Pos)
        .Input<TCoToFlow>()
            .Input(rightInput)
            .Build()
        .InitHandler()
            .Args({"itemArg"})
            .Body<TCoAsList>()
                .Add("itemArg")
                .Build()
            .Build()
        .SwitchHandler()
            .Args({"item", "state"})
            .Body<TCoBool>()
                .Literal().Build("false")
                .Build()
            .Build()
        .UpdateHandler()
            .Args({"item", "state"})
            .Body<TCoAppend>()
                .List("state")
                .Item("item")
            .Build()
        .Build()
    .Done();

    auto flatMap = Build<TCoFlatMap>(Ctx, Pos)
        .Input(rightAsStreamOfLists)
        .Lambda()
            .Args({"rightAsList"})
            .Body<TCoFlatMap>()
                .Input(leftInput)
                .Lambda()
                    .Args({leftArg})
                    .Body<TCoMap>()
                        // here we have `List`, so we can iterate over it many times (for every `leftArg`)
                        .Input("rightAsList")
                        .Lambda()
                            .Args({rightArg})
                            .Body<TCoAsStruct>()
                                .Add(keys)
                            .Build()
                        .Build()
                    .Build()
                .Build()
            .Build()
        .Build()
    .Done().Ptr();

    return Build<TCoFromFlow>(Ctx, Pos)
        .Input(flatMap)
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TPhysicalJoinBuilder::BuildGraceJoinCore(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput) {
    TVector<TCoAtom> leftColumnIdxs;
    TVector<TCoAtom> rightColumnIdxs;
    TVector<TCoAtom> leftRenames, rightRenames;
    TVector<TCoAtom> leftKeyColumnNames;
    TVector<TCoAtom> rightKeyColumnNames;

    auto leftIUs = Join->GetLeftInput()->GetOutputIUs();
    auto rightIUs = Join->GetRightInput()->GetOutputIUs();
    const bool leftSideOnly = (Join->JoinKind == "LeftSemi" || Join->JoinKind == "LeftOnly");

    if (leftSideOnly) {
        rightIUs.clear();
        for (const auto& [left, right] : Join->JoinKeys) {
            rightIUs.push_back(right);
        }
    }

    const auto leftTupleSize = leftIUs.size();
    for (size_t i = 0; i < leftIUs.size(); i++) {
        leftRenames.push_back(Build<TCoAtom>(Ctx, Pos).Value(i).Done());
        leftRenames.push_back(Build<TCoAtom>(Ctx, Pos).Value(i).Done());
    }

    for (size_t i = 0; i < rightIUs.size(); i++) {
        rightRenames.push_back(Build<TCoAtom>(Ctx, Pos).Value(i).Done());
        rightRenames.push_back(Build<TCoAtom>(Ctx, Pos).Value(i + leftTupleSize).Done());
    }

    for (const auto& joinKey : Join->JoinKeys) {
        const auto leftIdx = std::distance(leftIUs.begin(), std::find(leftIUs.begin(), leftIUs.end(), joinKey.first));
        const auto rightIdx = std::distance(rightIUs.begin(), std::find(rightIUs.begin(), rightIUs.end(), joinKey.second));

        leftColumnIdxs.push_back(Build<TCoAtom>(Ctx, Pos).Value(leftIdx).Done());
        rightColumnIdxs.push_back(Build<TCoAtom>(Ctx, Pos).Value(rightIdx).Done());

        leftKeyColumnNames.push_back(Build<TCoAtom>(Ctx, Pos).Value(joinKey.first.GetFullName()).Done());
        rightKeyColumnNames.push_back(Build<TCoAtom>(Ctx, Pos).Value(joinKey.second.GetFullName()).Done());
    }

    // Convert to wide flow
    // clang-format off
    leftInput = Build<TCoToFlow>(Ctx, Pos)
        .Input(leftInput)
    .Done().Ptr();
    // clang-forat on

    leftInput = NPhysicalConvertionUtils::BuildExpandMapForNarrowInput(leftInput, Join->GetLeftInput()->GetOutputIUs(), Ctx);

    // clang-format off
    rightInput = Build<TCoToFlow>(Ctx, Pos)
        .Input(rightInput)
    .Done().Ptr();
    // clang-format on

    rightInput = NPhysicalConvertionUtils::BuildExpandMapForNarrowInput(rightInput, rightIUs, Ctx);

    // clang-format off
    auto graceJoin = Build<TCoGraceJoinCore>(Ctx, Pos)
        .LeftInput(leftInput)
        .RightInput(rightInput)
        .JoinKind<TCoAtom>()
            .Value(GetValidJoinKind(Join->JoinKind))
        .Build()
        .LeftKeysColumns<TCoAtomList>()
            .Add(leftColumnIdxs)
        .Build()
        .RightKeysColumns<TCoAtomList>()
            .Add(rightColumnIdxs)
        .Build()
        .LeftRenames()
            .Add(leftRenames)
        .Build()
        .RightRenames()
            .Add(rightRenames)
        .Build()
        .LeftKeysColumnNames<TCoAtomList>()
            .Add(leftKeyColumnNames)
        .Build()
        .RightKeysColumnNames<TCoAtomList>()
            .Add(rightKeyColumnNames)
        .Build()
        .Flags().Build()
    .Done().Ptr();

    // Convert back to narrow stream
    return Build<TCoFromFlow>(Ctx, Pos)
        .Input(NPhysicalConvertionUtils::BuildNarrowMapForWideInput(graceJoin, Join->GetOutputIUs(), Ctx))
    .Done().Ptr();
    // clang-format on
}

TExprNode::TPtr TPhysicalJoinBuilder::BuildPhysicalJoin(TExprNode::TPtr leftInput, TExprNode::TPtr rightInput) {
    const auto joinKind = to_lower(Join->JoinKind);
    if (joinKind == "cross") {
        return BuildCrossJoin(leftInput, rightInput);
    }

    Y_ENSURE(joinKind == "inner" || joinKind == "left" || joinKind == "leftonly" || joinKind == "leftsemi");
    return BuildGraceJoinCore(leftInput, rightInput);
}
