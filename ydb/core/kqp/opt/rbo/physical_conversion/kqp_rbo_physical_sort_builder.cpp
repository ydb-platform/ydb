#include "kqp_rbo_physical_sort_builder.h"
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

std::pair<TExprNode::TPtr, TVector<TExprNode::TPtr>> TPhysicalSortBuilder::BuildSortKeySelector(const TSortIUs& sortElements) {
    auto arg = Build<TCoArgument>(Ctx, Pos).Name("arg").Done().Ptr();
    TVector<TExprNode::TPtr> directions;
    TVector<TExprNode::TPtr> members;

    for (const auto& [id, order] : sortElements.Items()) {
        // clang-format off
        members.push_back(Build<TCoMember>(Ctx, Pos)
            .Struct(arg)
            .Name().Build(Names.Get(id))
        .Done().Ptr());
        // clang-format on

        directions.push_back(Build<TCoBool>(Ctx, Pos).Literal().Build(order.Ascending ? "true" : "false").Done().Ptr());
    }

    TExprNode::TPtr selector;
    if (sortElements.Items().size() == 1) {
        // clang-format off
        selector = Build<TCoLambda>(Ctx, Pos)
            .Args({arg})
            .Body(members[0])
            .Done().Ptr();
        // clang-format on
    } else {
        // clang-format off
        selector = Build<TCoLambda>(Ctx, Pos)
            .Args({arg})
            .Body<TExprList>().Add(members).Build()
            .Done().Ptr();
        // clang-format on
    }

    return std::make_pair(selector, directions);
}

TExprNode::TPtr TPhysicalSortBuilder::BuildSort(TExprNode::TPtr input, TOrderEnforcer& enforcer) {
    if (enforcer.Action != EOrderEnforcerAction::REQUIRE) {
        return input;
    }

    auto [selector, dirs] = BuildSortKeySelector(enforcer.SortElements);

    TExprNode::TPtr dirList;
    if (dirs.size() == 1) {
        dirList = dirs[0];
    } else {
        dirList = Build<TExprList>(Ctx, Pos).Add(dirs).Done().Ptr();
    }

    // clang-format off
    return Build<TCoSort>(Ctx, Pos)
        .Input(input)
        .SortDirections(dirList)
        .KeySelectorLambda(selector)
    .Done().Ptr();
    // clang-format on
}

TVector<TExprNode::TPtr> TPhysicalSortBuilder::BuildSortKeysForWideSort(const TVector<TInfoUnitId>& inputs, const TSortIUs& sortElements) {
    // We have to map wide input with sort elements to find a right index.
    TMappedIUs<ui32> indices;
    for (ui32 i = 0; i < inputs.size(); ++i) {
        indices.Add(inputs[i], i);
    }

    TVector<TExprNode::TPtr> sortKeys;
    for (const auto& [id, order] : sortElements.Items()) {
        const auto* index = indices.Find(id);
        Y_ENSURE(index, "Cannot find a sort element in wide input.");
        const auto wideIndex = ToString(*index);
        // clang-format off
        auto sortKey = Ctx.Builder(Pos)
            .List()
                .Atom(0, wideIndex)
                .Callable(1, "Bool")
                    .Atom(0, order.Ascending ? "true" : "false")
                .Seal()
            .Seal()
        .Build();
        // clang-format off
        sortKeys.push_back(sortKey);
    }
    return sortKeys;
}

TExprNode::TPtr TPhysicalSortBuilder::BuildPhysicalOp(TExprNode::TPtr input) {
    const auto inputs = NPhysicalConvertionUtils::GetLiveInputIUs(Sort, 0);
    const auto& sortElements = Sort.GetSortElements();
    // clang-format off
    input = Build<TCoToFlow>(Ctx, Pos)
        .Input(input)
    .Done().Ptr();
    // clang-format on

    // Expand narrow input.
    input = NPhysicalConvertionUtils::BuildExpandMapForNarrowInput(input, inputs, Ctx, Names);

    if (Sort.LimitCond.has_value()) {
        // clang-format off
        input = Build<TCoWideTopSort>(Ctx, Pos)
            .Input(input)
            .Count(Sort.LimitCond->GetExpressionBody())
            .Keys<TCoSortKeys>()
                .Add(BuildSortKeysForWideSort(inputs, sortElements))
            .Build()
        .Done().Ptr();
        // clang-format on
    } else {
        // clang-format off
        input = Build<TCoWideSort>(Ctx, Pos)
            .Input(input)
            .Keys<TCoSortKeys>()
                .Add(BuildSortKeysForWideSort(inputs, sortElements))
            .Build()
        .Done().Ptr();
        // clang-format on
    }

    // Merge-connection keys are already included in LiveOut.
    input = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(
        input,
        inputs,
        NPhysicalConvertionUtils::BuildNameSet(NPhysicalConvertionUtils::GetLiveOutputIUs(Sort), Names),
        Ctx, Names);

    // clang-format off
    input = Build<TCoFromFlow>(Ctx, Pos)
        .Input(input)
    .Done().Ptr();
    // clang-format on

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical sort] " << KqpExprToPrettyString(TExprBase(input), Ctx);
    return input;
}
