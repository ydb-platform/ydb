#include "kqp_rules_include.h"

#include <ydb/core/kqp/opt/rbo/map_renames.h>

namespace NKikimr {
namespace NKqp {

namespace {

enum class EScalarEmptyInputRepair {
    None,
    Count,
};

size_t CountInfoUnit(
    const TVector<TInfoUnit>& ius,
    const TInfoUnit& needle)
{
    return std::count(ius.begin(), ius.end(), needle);
}

std::optional<TInfoUnit> ExactMemberSource(const TMapElement& element) {
    const auto lambda = element.GetExpression().GetLambda();
    if (!lambda || lambda->ChildrenSize() != 2 ||
        !lambda->Child(0)->IsArguments() ||
        lambda->Child(0)->ChildrenSize() != 1 ||
        !lambda->Child(0)->Child(0)->IsArgument())
    {
        return std::nullopt;
    }

    const auto* argument = lambda->Child(0)->Child(0);
    const auto* body = lambda->Child(1);
    if (!body->IsCallable("Member") || body->ChildrenSize() != 2 ||
        body->Child(0) != argument || !body->Child(1)->IsAtom())
    {
        return std::nullopt;
    }

    return TInfoUnit(TString(body->Child(1)->Content()));
}

TIntrusivePtr<TOpAggregate> FindOnlyMarkedAggregate(
    const TIntrusivePtr<IOperator>& root)
{
    TIntrusivePtr<TOpAggregate> marked;
    TVector<TIntrusivePtr<IOperator>> pending{root};
    THashSet<const IOperator*> visited;
    while (!pending.empty()) {
        auto current = pending.back();
        pending.pop_back();
        if (!visited.insert(current.Get()).second) {
            continue;
        }
        if (current->Kind == EOperator::Aggregate) {
            auto aggregate = CastOperator<TOpAggregate>(current);
            if (aggregate->WasKeylessBeforeCorrelation) {
                Y_ENSURE(
                    !marked,
                    "Nested originally-keyless correlated scalar aggregates "
                    "require general empty-row reconstruction");
                marked = aggregate;
            }
        }
        for (const auto& child : current->GetChildren()) {
            pending.push_back(child);
        }
    }
    return marked;
}

EScalarEmptyInputRepair ClassifyScalarEmptyInputRepair(
    const TIntrusivePtr<IOperator>& root,
    TInfoUnit resultIU,
    const TTypeAnnotationNode* resultType)
{
    const auto marked = FindOnlyMarkedAggregate(root);
    if (!marked) {
        return EScalarEmptyInputRepair::None;
    }

    auto current = root;
    while (current != marked) {
        Y_ENSURE(
            current->Kind == EOperator::Map,
            "Originally-keyless correlated scalar aggregate result must have "
            "only direct Map aliases");
        auto map = CastOperator<TOpMap>(current);
        Y_ENSURE(
            CountInfoUnit(map->GetOutputIUs(), resultIU) == 1,
            "Correlated scalar result IU is absent or ambiguous in Map output");

        const TMapElement* producer = nullptr;
        for (const auto& element : map->MapElements) {
            if (element.GetElementName() != resultIU) {
                continue;
            }
            Y_ENSURE(
                !producer,
                "Correlated scalar result IU has multiple Map producers");
            producer = &element;
        }

        const auto input = map->GetInput();
        if (producer) {
            const auto source = ExactMemberSource(*producer);
            Y_ENSURE(
                source,
                "Computed correlated scalar aggregate results require general "
                "empty-row reconstruction");
            Y_ENSURE(
                CountInfoUnit(input->GetOutputIUs(), *source) == 1,
                "Correlated scalar Map alias source is absent or ambiguous");
            const auto* outputType = map->GetIUType(resultIU);
            const auto* sourceType = input->GetIUType(*source);
            Y_ENSURE(
                outputType && sourceType &&
                    IsSameAnnotation(*outputType, *sourceType),
                "Correlated scalar Map alias changes the selected result type");
            resultIU = *source;
        } else {
            Y_ENSURE(
                CountInfoUnit(input->GetOutputIUs(), resultIU) == 1,
                "Correlated scalar pass-through IU is absent or ambiguous");
        }
        current = input;
    }

    Y_ENSURE(
        marked->GetAggregationPhase() == EOpPhase::Undefined &&
            !marked->IsDistinctAll(),
        "Originally-keyless correlated scalar repair requires one logical "
        "aggregate");

    const TOpAggregationTraits* selectedTrait = nullptr;
    for (const auto& trait : marked->AggregationTraitsList) {
        if (trait.ResultColName != resultIU) {
            continue;
        }
        Y_ENSURE(
            !selectedTrait,
            "Correlated scalar aggregate result IU is ambiguous");
        selectedTrait = &trait;
    }
    Y_ENSURE(
        selectedTrait,
        "Correlated scalar result is not a direct aggregate trait");
    Y_ENSURE(
        !selectedTrait->Distinct && !selectedTrait->Unwrap &&
            CountInfoUnit(marked->GetOutputIUs(), resultIU) == 1,
        "Correlated scalar aggregate result trait is not a unique direct value");
    if (selectedTrait->AggFunction != "count") {
        return EScalarEmptyInputRepair::None;
    }

    Y_ENSURE(
        resultType && !resultType->IsOptionalOrNull() &&
            resultType->GetKind() == ETypeAnnotationKind::Data &&
            resultType->Cast<TDataExprType>()->GetSlot() ==
                NUdf::EDataSlot::Uint64,
        "Direct correlated COUNT result must be non-null Uint64");
    return EScalarEmptyInputRepair::Count;
}

TExpression MakeOptionalCountRepair(
    const TInfoUnit& countIU,
    TPositionHandle pos,
    TExprContext& exprCtx,
    TPlanProps& props)
{
    const auto count = MakeColumnAccess(countIU, pos, &exprCtx, &props);
    const auto zero = exprCtx.NewCallable(
        pos,
        "Uint64",
        {exprCtx.NewAtom(pos, "0")});
    const auto coalesced = exprCtx.NewCallable(
        pos,
        "Coalesce",
        {count.GetExpressionBody(), zero});
    return TExpression(
        exprCtx.NewCallable(pos, "Just", {coalesced}),
        &exprCtx,
        &props);
}

} // namespace

// Rewrite a single scalar subplan into a cross-join for uncorrelated queries
// or into a left join for correlated (assuming at most one tuple in the output of each subquery)
// FIXME: Need to do correct general case decorellation in the future

bool TInlineScalarSubplanRule::MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    auto subplanIUs = input->GetSubplanIUs(props);
    TVector<TInfoUnit> scalarIUs;
    for (const auto& iu : subplanIUs) {
        auto subplanEntry = props.Subplans.PlanMap.at(iu);
        if (subplanEntry.Type == ESubplanType::EXPR) {
            scalarIUs.push_back(iu);
            break;
        }
    }

    if (scalarIUs.empty()) {
        return false;
    }

    auto scalarIU = scalarIUs[0];
    auto subplanEntry = props.Subplans.PlanMap.at(scalarIU);
    auto subplan = CastOperator<IOperator>(subplanEntry.Plan);
    auto subplanResIU = GetSubplanResultIUs(subplan)[0];
    auto subplanResType = subplan->GetIUType(subplanResIU);
    const bool makeResultOptional = !subplanResType->IsOptionalOrNull();
    const auto* scalarResultType = makeResultOptional
        ? ctx.ExprCtx.MakeType<TOptionalExprType>(subplanResType)
        : subplanResType;

    Y_ENSURE(MatchOperator<IUnaryOperator>(input));
    auto unaryOp = CastOperator<IUnaryOperator>(input);

    auto child = unaryOp->GetInput();

    // Check whether this is a correlated subplan with filter pushed up
    // FIXME: if the filter got stuck we will crash later in the optimizer
    if (subplan->Kind == EOperator::Filter && CastOperator<TOpFilter>(subplan)->GetInput()->Kind == EOperator::AddDependencies) {
        auto subplanFilter = CastOperator<TOpFilter>(subplan);
        auto addDeps = CastOperator<TOpAddDependencies>(subplanFilter->GetInput());
        auto uncorrSubplan = addDeps->GetInput();
        const auto emptyInputRepair = ClassifyScalarEmptyInputRepair(
            uncorrSubplan,
            subplanResIU,
            subplanResType);

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        TVector<TExpression> joinFilters;
        NMapRenames::TRenameMap subplanOutputRenames;

        auto leftIUs = child->GetOutputIUs();
        auto rightIUs = uncorrSubplan->GetOutputIUs();
        THashSet<TInfoUnit, TInfoUnit::THashFunction> usedIUs;
        NMapRenames::AddUsedIUs(usedIUs, leftIUs);
        NMapRenames::AddUsedIUs(usedIUs, rightIUs);
        NMapRenames::AddUsedIUs(usedIUs, subplanIUs);

        for (const auto& iu : rightIUs) {
            if (ContainsInfoUnit(leftIUs, iu) && !subplanOutputRenames.contains(iu)) {
                subplanOutputRenames.emplace(iu, NMapRenames::MakeUniqueInternalIU(props.InternalVarIdx, usedIUs));
            }
        }

        auto conjuncts = subplanFilter->FilterExpr.SplitConjunct();

        for (const auto & conj : conjuncts) {
            if (!conj.MaybeEquiJoinCondition()) {
                joinFilters.push_back(conj);
                continue;
            }

            TEquiJoinCondition jc(conj);
            TInfoUnit leftKey = jc.GetLeftIU();
            TInfoUnit rightKey = jc.GetRightIU();

            if (std::find(addDeps->Dependencies.begin(), addDeps->Dependencies.end(), rightKey) != addDeps->Dependencies.end()) {
                std::swap(leftKey, rightKey);
            } else if (std::find(addDeps->Dependencies.begin(), addDeps->Dependencies.end(), leftKey) == addDeps->Dependencies.end()) {
                Y_ENSURE(false, "Correlated filter missing join condition");
            }

            if (ContainsInfoUnit(leftIUs, rightKey)) {
                const auto renameIt = subplanOutputRenames.find(rightKey);
                if (renameIt != subplanOutputRenames.end()) {
                    rightKey = renameIt->second;
                } else {
                    auto newKey = NMapRenames::MakeUniqueInternalIU(props.InternalVarIdx, usedIUs);
                    subplanOutputRenames.emplace(rightKey, newKey);
                    rightKey = newKey;
                }
            }

            joinKeys.push_back(std::make_pair(leftKey, rightKey));
        }

        auto joinedSubplanResIU = subplanResIU;
        if (const auto renameIt = subplanOutputRenames.find(joinedSubplanResIU); renameIt != subplanOutputRenames.end()) {
            joinedSubplanResIU = renameIt->second;
        }

        auto leftJoin = NMapRenames::MakeJoinWithRightRenames(
            child, uncorrSubplan, subplan->Pos, "Left", joinKeys, joinFilters, subplanOutputRenames, ctx.ExprCtx, props);

        TIntrusivePtr<IOperator> repairedInput = leftJoin;
        auto repairedSubplanResIU = joinedSubplanResIU;
        if (emptyInputRepair == EScalarEmptyInputRepair::Count) {
            repairedSubplanResIU =
                NMapRenames::MakeUniqueInternalIU(props.InternalVarIdx, usedIUs);
            TVector<TMapElement> repairElements;
            repairElements.emplace_back(
                repairedSubplanResIU,
                MakeOptionalCountRepair(
                    joinedSubplanResIU,
                    subplan->Pos,
                    ctx.ExprCtx,
                    props));
            repairedInput = MakeIntrusive<TOpMap>(
                leftJoin,
                subplan->Pos,
                repairElements);
        }

        if (input->Kind == EOperator::Filter) {
            auto outerFilter = CastOperator<TOpFilter>(input);
            outerFilter->FilterExpr = outerFilter->FilterExpr.ApplyRenames(
                {{scalarIU, repairedSubplanResIU}});
            outerFilter->SetInput(repairedInput);
        } else {
            TVector<TMapElement> renameElements;
            renameElements.emplace_back(
                scalarIU,
                repairedSubplanResIU,
                subplan->Pos,
                &ctx.ExprCtx,
                &props);
            auto rename = MakeIntrusive<TOpMap>(
                repairedInput,
                subplan->Pos,
                renameElements);
            unaryOp->SetInput(rename);
        }
    }

    // If its a correlated subplan where filter pull up didn't succeed, throw an exception
    else if (subplanEntry.DependentIUs.size()) {
        Y_ENSURE(false, "Decorrelation via filter pull up didn't succeed");
    }

    // Otherwise we assume an uncorrelated supbplan
    // Here we don't assume at most one tuple from the subplan
    else {
        auto emptySource = MakeIntrusive<TOpEmptySource>(subplan->Pos);

        TVector<TMapElement> mapElements;
        mapElements.emplace_back(scalarIU, MakeNothing(subplan->Pos, scalarResultType, &ctx.ExprCtx));
        auto map = MakeIntrusive<TOpMap>(emptySource, subplan->Pos, mapElements);

        auto scalarBound = MakeIntrusive<TOpLimit>(
            subplan,
            subplan->Pos,
            MakeConstant("Uint64", "2", subplan->Pos, &ctx.ExprCtx),
            EOpPhase::Undefined);

        // Bound the scalar side before Cross materializes it, then gate its
        // cardinality observation with one outer row.
        auto outerGate = MakeIntrusive<TOpLimit>(
            child,
            subplan->Pos,
            MakeConstant("Uint64", "1", subplan->Pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;

        const auto outerIUs = child->GetOutputIUs();
        const auto scalarIUs = subplan->GetOutputIUs();
        TInfoUnitSet usedIUs;
        NMapRenames::AddUsedIUs(usedIUs, outerIUs);
        NMapRenames::AddUsedIUs(usedIUs, scalarIUs);
        NMapRenames::TRenameMap scalarRenames;
        for (const auto& iu : scalarIUs) {
            if (ContainsInfoUnit(outerIUs, iu)) {
                scalarRenames.emplace(
                    iu,
                    NMapRenames::MakeUniqueInternalIU(props.InternalVarIdx, usedIUs));
            }
        }
        auto gatedSubplanResIU = subplanResIU;
        if (const auto it = scalarRenames.find(subplanResIU); it != scalarRenames.end()) {
            gatedSubplanResIU = it->second;
        }

        auto demandedScalar = NMapRenames::MakeJoinWithRightRenames(
            outerGate,
            scalarBound,
            subplan->Pos,
            "Cross",
            joinKeys,
            {},
            scalarRenames,
            ctx.ExprCtx,
            props);
        demandedScalar->PreserveInputOrder = true;
        auto cardinalityCheck = MakeIntrusive<TOpLimit>(
            demandedScalar,
            subplan->Pos,
            MakeConstant("Uint64", "2", subplan->Pos, &ctx.ExprCtx),
            EOpPhase::Undefined);
        cardinalityCheck->Props.EnsureAtMostOne = true;

        TVector<TMapElement> renameElements;
        if (makeResultOptional) {
            auto value = MakeColumnAccess(gatedSubplanResIU, subplan->Pos, &ctx.ExprCtx, &props);
            auto optionalValue = ctx.ExprCtx.NewCallable(
                subplan->Pos,
                "Just",
                {value.GetExpressionBody()});
            renameElements.emplace_back(
                scalarIU,
                TExpression(optionalValue, &ctx.ExprCtx, &props));
        } else {
            renameElements.emplace_back(scalarIU, gatedSubplanResIU, subplan->Pos, &ctx.ExprCtx, &props);
        }
        auto rename = MakeIntrusive<TOpMap>(cardinalityCheck, subplan->Pos, renameElements);

        auto unionAll = MakeIntrusive<TOpUnionAll>(
            rename,
            map,
            subplan->Pos,
            TVector<TInfoUnit>{scalarIU},
            true
        );

        auto limit = MakeIntrusive<TOpLimit>(unionAll, subplan->Pos, MakeConstant("Uint64", "1", subplan->Pos, &ctx.ExprCtx), EOpPhase::Undefined);
    
        auto cross = MakeIntrusive<TOpJoin>(child, limit, subplan->Pos, "Cross", joinKeys);
        cross->PreserveInputOrder = true;
        unaryOp->SetInput(cross);
    }

    props.Subplans.Remove(scalarIU);

    return true;
}
}
}
