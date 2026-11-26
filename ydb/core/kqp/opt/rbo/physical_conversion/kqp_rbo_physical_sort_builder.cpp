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

TExprNode::TPtr TPhysicalSortBuilder::BuildNarrowTopSort(TExprNode::TPtr wideInput, const TVector<TInfoUnitId>& inputs, const TSortIUs& sortElements,
                                                         TExprNode::TPtr count) {
    TMappedIUs<ui32> indices;
    TExprNode::TListType wideArgs;
    wideArgs.reserve(inputs.size());
    for (ui32 i = 0; i < inputs.size(); ++i) {
        indices.Add(inputs[i], i);
        wideArgs.push_back(Ctx.NewArgument(Pos, "column" + ToString(i)));
    }

    // Peephole swaps TopSort with a NarrowMap to AsStruct, which makes the sort wide and then lets it take blocks.
    // A map to a tuple keeps the sort narrow.
    auto tuple = Ctx.NewList(Pos, TExprNode::TListType(wideArgs));
    auto narrowLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, std::move(wideArgs)), std::move(tuple));
    auto narrowInput = Ctx.NewCallable(Pos, "NarrowMap", {std::move(wideInput), std::move(narrowLambda)});

    const auto buildNth = [&](const TExprNode::TPtr& row, ui32 index) {
        return Ctx.NewCallable(Pos, "Nth", {row, Ctx.NewAtom(Pos, index)});
    };

    auto keyRow = Ctx.NewArgument(Pos, "row");
    TExprNode::TListType keys;
    TExprNode::TListType directions;
    for (const auto& [id, order] : sortElements.Items()) {
        const auto* index = indices.Find(id);
        Y_ENSURE(index, "Cannot find a sort element in wide input.");
        keys.push_back(buildNth(keyRow, *index));
        directions.push_back(Build<TCoBool>(Ctx, Pos).Literal().Build(order.Ascending ? "true" : "false").Done().Ptr());
    }
    auto keyBody = keys.size() == 1 ? keys[0] : Ctx.NewList(Pos, std::move(keys));
    auto directionsNode = directions.size() == 1 ? directions[0] : Ctx.NewList(Pos, std::move(directions));
    auto keySelector = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {keyRow}), std::move(keyBody));
    auto topSort = Ctx.NewCallable(Pos, "TopSort", {std::move(narrowInput), std::move(count), std::move(directionsNode), std::move(keySelector)});

    auto outputRow = Ctx.NewArgument(Pos, "row");
    TExprNode::TListType outputs;
    outputs.reserve(inputs.size());
    for (ui32 i = 0; i < inputs.size(); ++i) {
        outputs.push_back(buildNth(outputRow, i));
    }
    auto expandLambda = Ctx.NewLambda(Pos, Ctx.NewArguments(Pos, {outputRow}), std::move(outputs));
    return Ctx.NewCallable(Pos, "ExpandMap", {std::move(topSort), std::move(expandLambda)});
}

NPhysicalConvertionUtils::TStageBody TPhysicalSortBuilder::BuildPhysicalOp(const NPhysicalConvertionUtils::TStageBody& input) {
    const auto inputs = NPhysicalConvertionUtils::GetLiveInputIUs(Sort, 0);
    const auto& sortElements = Sort.GetSortElements();

    auto wideInput = input.AsWide(inputs, Ctx, Names);

    TExprNode::TPtr count = Sort.LimitCond.has_value() ? Sort.LimitCond->GetExpressionBody() : nullptr;
    ui64 literalCount = 0;
    if (count && TCoUint64::Match(count.Get()) && TryFromString(TCoUint64(count).Literal().Value(), literalCount) &&
        literalCount == std::numeric_limits<ui64>::max()) {
        // The maximum limit keeps every row, so there is nothing for a top-N to do.
        count = nullptr;
    }

    TExprNode::TPtr output;
    if (!count) {
        // clang-format off
        output = Build<TCoWideSort>(Ctx, Pos)
            .Input(wideInput)
            .Keys<TCoSortKeys>()
                .Add(BuildSortKeysForWideSort(inputs, sortElements))
            .Build()
        .Done().Ptr();
        // clang-format on
    } else if (!Types.UseBlocks && Types.BlockEngineMode == NYql::EBlockEngineMode::Auto) {
        // The block top-N kernel reserves memory for twice its count up front, whatever its input size, so a large
        // limit fails on any table. The narrow top-N uses memory only for the rows it receives. In the block engine's
        // auto mode, the wide one would turn into the block kernel over a block input.
        output = BuildNarrowTopSort(wideInput, inputs, sortElements, count);
    } else {
        // clang-format off
        output = Build<TCoWideTopSort>(Ctx, Pos)
            .Input(wideInput)
            .Count(count)
            .Keys<TCoSortKeys>()
                .Add(BuildSortKeysForWideSort(inputs, sortElements))
            .Build()
        .Done().Ptr();
        // clang-format on
    }

    YQL_CLOG(TRACE, CoreDq) << "[NEW RBO Physical sort] " << KqpExprToPrettyString(TExprBase(output), Ctx);

    // WideSort passes every input slot through, so drop the ones that are dead past this
    // operator. Merge-connection keys are already included in LiveOut.
    return NPhysicalConvertionUtils::BuildLiveWideBody(output, NPhysicalConvertionUtils::GetFullNames(inputs, Names),
        NPhysicalConvertionUtils::BuildNameSet(NPhysicalConvertionUtils::GetLiveOutputIUs(Sort), Names), Ctx);
}
