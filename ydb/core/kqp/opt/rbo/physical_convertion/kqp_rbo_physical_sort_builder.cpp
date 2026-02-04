#include "kqp_rbo_physical_sort_builder.h"
using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

std::pair<TExprNode::TPtr, TVector<TExprNode::TPtr>> TPhysicalSortBuilder::BuildSortKeySelector(const TVector<TSortElement>& sortElements) {
    auto arg = Build<TCoArgument>(Ctx, Pos).Name("arg").Done().Ptr();
    TVector<TExprNode::TPtr> directions;
    TVector<TExprNode::TPtr> members;

    for (const auto& element : sortElements) {
        // clang-format off
        members.push_back(Build<TCoMember>(Ctx, Pos)
            .Struct(arg)
            .Name().Build(element.SortColumn.GetFullName())
        .Done().Ptr());
        // clang-format on

        directions.push_back(Build<TCoBool>(Ctx, Pos).Literal().Build(element.Ascending ? "true" : "false").Done().Ptr());
    }

    TExprNode::TPtr selector;
    if (sortElements.size() == 1) {
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

TVector<TExprNode::TPtr> TPhysicalSortBuilder::BuildSortKeysForWideSort(const TVector<TInfoUnit>& inputs, const TVector<TSortElement>& sortElements) {
    // We have to map wide input with sort elements to find a right index.
    THashMap<TString, ui32> indices;
    for (ui32 i = 0; i < inputs.size(); ++i) {
        indices.emplace(inputs[i].GetFullName(), i);
    }

    TVector<TExprNode::TPtr> sortKeys;
    for (ui32 i = 0; i < sortElements.size(); ++i) {
        const auto& sortElement = sortElements[i];
        auto it = indices.find(sortElement.SortColumn.GetFullName());
        Y_ENSURE(it != indices.end(), "Cannot find a sort element in wide input.");
        const auto wideIndex = ToString(it->second);
        // clang-format off
        auto sortKey = Ctx.Builder(Pos)
            .List()
                .Atom(0, wideIndex)
                .Callable(1, "Bool")
                    .Atom(0, sortElement.Ascending ? "true" : "false")
                .Seal()
            .Seal()
        .Build();
        // clang-format off
        sortKeys.push_back(sortKey);
    }
    return sortKeys;
}

TExprNode::TPtr TPhysicalSortBuilder::BuildPhysicalOp(TExprNode::TPtr input) {
    const auto inputs = Sort->GetInput()->GetOutputIUs();
    const auto& sortElements = Sort->SortElements;
    // clang-format off
    input = Build<TCoToFlow>(Ctx, Pos)
        .Input(input)
    .Done().Ptr();
    // clang-format on

    // Expand narrow input.
    input = NPhysicalConvertionUtils::BuildExpandMapForNarrowInput(input, inputs, Ctx);

    if (Sort->LimitCond.has_value()) {
        // clang-format off
        input = Build<TCoWideTopSort>(Ctx, Pos)
            .Input(input)
            .Count(Sort->LimitCond->Node)
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

    // Fuse wide input.
    input = NPhysicalConvertionUtils::BuildNarrowMapForWideInput(input, inputs, Ctx);

    // clang-format off
    input = Build<TCoFromFlow>(Ctx, Pos)
        .Input(input)
    .Done().Ptr();
    // clang-format on

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical sort] " << KqpExprToPrettyString(TExprBase(input), Ctx);
    return input;
}
