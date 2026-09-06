#include "kqp_rules_include.h"

#include <ydb/core/kqp/opt/rbo/map_renames.h>

#include <variant>

namespace NKikimr {
namespace NKqp {

namespace {

enum class EScalarEmptyInputRepair {
    None,
    Count,
};

// Inlining introduces a column that did not exist when join namespaces were
// resolved. A shared consumer can expose it through both join inputs. Rename
// only these new, dead scratch columns on the right edge; never merge values
// or rename a live key, predicate, or public result.
void ProtectInlinedScalarOutputs(IOperator& consumer, const TInfoUnit& scalar,
    TRBOContext& ctx, TPlanProps& props)
{
    THashSet<IOperator*> seen;
    TVector<IOperator*> ancestors;
    const auto visit = [&](const auto& self, IOperator* op) -> void {
        if (!seen.insert(op).second) {
            return;
        }
        for (const auto& [parent, _] : op->Parents) {
            self(self, parent);
        }
        ancestors.push_back(op);
    };
    visit(visit, &consumer);
    TInfoUnitSet introduced{scalar};
    TInfoUnitSet used;
    for (auto* op : ancestors) {
        AddInfoUnits(used, op->GetOutputIUs());
        op->Props.OutputIUs.reset();
    }
    // Reverse the parent DFS: repair lower joins before their consumers.
    for (auto it = ancestors.rbegin(); it != ancestors.rend(); ++it) {
        auto* op = *it;
        if (op->Kind != EOperator::Join) {
            continue;
        }
        auto& join = *static_cast<TOpJoin*>(op);
        if (!JoinOutputsLeft(join.JoinKind) || !JoinOutputsRight(join.JoinKind)) {
            continue;
        }
        const auto left = MakeInfoUnitSet(join.GetLeftInput()->GetOutputIUs());
        const auto right = join.GetRightInput()->GetOutputIUs();
        const auto referenced = MakeInfoUnitSet(join.GetUsedIUs(props));
        TVector<TMapElement> renames;
        for (const auto& iu : right) {
            if (!introduced.contains(iu) || !left.contains(iu)) {
                continue;
            }
            Y_ENSURE(!GetLiveOut(&join).contains(iu) && !referenced.contains(iu),
                "Scalar inlining would expose an ambiguous live column " << iu.GetFullName());
            const auto fresh = NMapRenames::MakeUniqueInternalIU(props.InternalVarIdx, used);
            introduced.insert(fresh);
            renames.emplace_back(fresh, iu, join.Pos, &ctx.ExprCtx, &props);
        }
        if (!renames.empty()) {
            join.SetRightInput(MakeIntrusive<TOpMap>(join.GetRightInput(), join.Pos, renames));
        }
    }
}

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

const TTypeAnnotationNode* ExactOptionalDecimalItem(
    const TTypeAnnotationNode* type)
{
    if (!type || type->GetKind() != ETypeAnnotationKind::Optional) {
        return nullptr;
    }

    const auto* item = type->Cast<TOptionalExprType>()->GetItemType();
    if (!item || item->GetKind() != ETypeAnnotationKind::Data) {
        return nullptr;
    }

    const auto* data = dynamic_cast<const TDataExprParamsType*>(item);
    return data && data->GetSlot() == NUdf::EDataSlot::Decimal
        ? item
        : nullptr;
}

bool IsExactMatchingDecimalLiteral(
    const TExprNode& node,
    const TTypeAnnotationNode& decimalType)
{
    if (!node.IsCallable("Decimal") || node.ChildrenSize() != 3 ||
        !node.Child(0)->IsAtom() || !node.Child(1)->IsAtom() ||
        !node.Child(2)->IsAtom() || !node.GetTypeAnn() ||
        node.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data)
    {
        return false;
    }

    const auto* literalType =
        dynamic_cast<const TDataExprParamsType*>(node.GetTypeAnn().Get());
    return literalType &&
        literalType->GetSlot() == NUdf::EDataSlot::Decimal &&
        literalType->GetParamOne() == node.Child(1)->Content() &&
        literalType->GetParamTwo() == node.Child(2)->Content() &&
        IsSameAnnotation(*literalType, decimalType);
}

bool IsExactTypeDescriptorAnnotation(
    const TExprNode& descriptor,
    const TTypeAnnotationNode& describedType)
{
    const auto annotation = descriptor.GetTypeAnn();
    return annotation &&
        annotation->GetKind() == ETypeAnnotationKind::Type &&
        IsSameAnnotation(
            *annotation->Cast<TTypeExprType>()->GetType(),
            describedType);
}

bool IsExactMatchingStringDecimalSafeCast(
    const TExprNode& node,
    const TTypeAnnotationNode& optionalDecimalType,
    const TTypeAnnotationNode& decimalType)
{
    if (!node.IsCallable("SafeCast") || node.ChildrenSize() != 2 ||
        !node.GetTypeAnn() ||
        !IsSameAnnotation(*node.GetTypeAnn(), optionalDecimalType))
    {
        return false;
    }

    const auto& source = *node.Child(0);
    const bool isString = source.IsCallable("String");
    const bool isUtf8 = source.IsCallable("Utf8");
    if ((!isString && !isUtf8) || source.ChildrenSize() != 1 ||
        !source.Child(0)->IsAtom() || !source.GetTypeAnn() ||
        source.GetTypeAnn()->GetKind() != ETypeAnnotationKind::Data)
    {
        return false;
    }
    const auto* sourceType =
        source.GetTypeAnn()->Cast<TDataExprType>();
    if (dynamic_cast<const TDataExprParamsType*>(sourceType) ||
        sourceType->GetSlot() != (isString
            ? NUdf::EDataSlot::String
            : NUdf::EDataSlot::Utf8))
    {
        return false;
    }

    const auto& target = *node.Child(1);
    if (!target.IsCallable("OptionalType") ||
        target.ChildrenSize() != 1 ||
        !IsExactTypeDescriptorAnnotation(target, optionalDecimalType))
    {
        return false;
    }
    const auto& item = *target.Child(0);
    const auto* decimal =
        dynamic_cast<const TDataExprParamsType*>(&decimalType);
    if (!decimal || !item.IsCallable("DataType") ||
        item.ChildrenSize() != 3 || !item.Child(0)->IsAtom() ||
        !item.Child(1)->IsAtom() || !item.Child(2)->IsAtom() ||
        item.Child(0)->Content() != "Decimal" ||
        item.Child(1)->Content() != decimal->GetParamOne() ||
        item.Child(2)->Content() != decimal->GetParamTwo() ||
        !IsExactTypeDescriptorAnnotation(item, decimalType))
    {
        return false;
    }

    constexpr NUdf::TCastResultOptions ExpectedCast =
        static_cast<NUdf::TCastResultOptions>(
            NUdf::ECastOptions::MayFail |
            NUdf::ECastOptions::MayLoseData);
    if (CastResult<false>(source.GetTypeAnn(), &decimalType) != ExpectedCast) {
        return false;
    }
    return true;
}

bool IsExactMatchingDecimalFactor(
    const TExprNode& node,
    const TTypeAnnotationNode& optionalDecimalType,
    const TTypeAnnotationNode& decimalType)
{
    return IsExactMatchingDecimalLiteral(node, decimalType) ||
        IsExactMatchingStringDecimalSafeCast(
            node,
            optionalDecimalType,
            decimalType);
}

std::optional<TInfoUnit> ExactNullableDecimalMulSource(
    const TMapElement& element,
    const TTypeAnnotationNode* outputType)
{
    if (element.GetExpression().HasWindowSemantics()) {
        return std::nullopt;
    }

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
    const auto* decimalType = ExactOptionalDecimalItem(outputType);
    if (!decimalType || !body->IsCallable("DecimalMul") ||
        body->ChildrenSize() != 2 || !body->GetTypeAnn() ||
        !IsSameAnnotation(*body->GetTypeAnn(), *outputType))
    {
        return std::nullopt;
    }

    const TExprNode* member = nullptr;
    const TExprNode* factor = nullptr;
    for (ui32 index = 0; index < body->ChildrenSize(); ++index) {
        const auto* child = body->Child(index);
        if (child->IsCallable("Member")) {
            if (member) {
                return std::nullopt;
            }
            member = child;
        } else if (child->IsCallable({"Decimal", "SafeCast"})) {
            if (factor) {
                return std::nullopt;
            }
            factor = child;
        } else {
            return std::nullopt;
        }
    }

    if (!member || !factor || member->ChildrenSize() != 2 ||
        member->Child(0) != argument || !member->Child(1)->IsAtom() ||
        !member->GetTypeAnn() ||
        !IsSameAnnotation(*member->GetTypeAnn(), *outputType) ||
        !IsExactMatchingDecimalFactor(
            *factor,
            *outputType,
            *decimalType))
    {
        return std::nullopt;
    }

    return TInfoUnit(TString(member->Child(1)->Content()));
}

bool IsExactTypePreservingMemberAlias(
    const TMapElement& element,
    TOpMap& map,
    const TIntrusivePtr<IOperator>& input)
{
    if (element.GetExpression().HasWindowSemantics()) {
        return false;
    }

    const auto source = ExactMemberSource(element);
    if (!source ||
        CountInfoUnit(map.GetOutputIUs(), element.GetElementName()) != 1 ||
        CountInfoUnit(input->GetOutputIUs(), *source) != 1)
    {
        return false;
    }

    const auto* outputType = map.GetIUType(element.GetElementName());
    const auto* sourceType = input->GetIUType(*source);
    return outputType && sourceType &&
        IsSameAnnotation(*outputType, *sourceType);
}

bool IsExactComputedResultPathMap(
    TOpMap& map,
    const TMapElement* computed,
    const TIntrusivePtr<IOperator>& input)
{
    for (const auto& element : map.MapElements) {
        if (&element != computed &&
            !IsExactTypePreservingMemberAlias(element, map, input))
        {
            return false;
        }
    }

    // Map elements describe produced values; every other output is an
    // implicit pass-through and must retain its unique input type exactly.
    for (const auto& output : map.GetOutputIUs()) {
        const bool explicitlyProduced = std::any_of(
            map.MapElements.begin(),
            map.MapElements.end(),
            [&](const TMapElement& element) {
                return element.GetElementName() == output;
            });
        if (explicitlyProduced) {
            continue;
        }

        if (CountInfoUnit(map.GetOutputIUs(), output) != 1 ||
            CountInfoUnit(input->GetOutputIUs(), output) != 1)
        {
            return false;
        }
        const auto* outputType = map.GetIUType(output);
        const auto* inputType = input->GetIUType(output);
        if (!outputType || !inputType ||
            !IsSameAnnotation(*outputType, *inputType))
        {
            return false;
        }
    }
    return true;
}

bool HasExactAlignedCorrelationDependencies(
    const TVector<TInfoUnit>& registered,
    TOpAddDependencies& dependencies,
    IOperator& outer,
    const TExpression& filter)
{
    const auto& added = dependencies.Dependencies;
    if (registered.empty() || registered.size() != added.size() ||
        added.size() != dependencies.Types.size() ||
        !outer.Type || !dependencies.Type)
    {
        return false;
    }

    for (size_t index = 0; index < added.size(); ++index) {
        const auto& iu = added[index];
        if (CountInfoUnit(registered, iu) != 1 ||
            CountInfoUnit(added, iu) != 1 ||
            CountInfoUnit(outer.GetOutputIUs(), iu) != 1 ||
            CountInfoUnit(dependencies.GetOutputIUs(), iu) != 1)
        {
            return false;
        }
        const auto* outerType = outer.GetIUType(iu);
        const auto* addedType = dependencies.GetIUType(iu);
        const auto* declaredType = dependencies.Types[index];
        if (!outerType || !addedType || !declaredType ||
            !IsSameAnnotation(*outerType, *declaredType) ||
            !IsSameAnnotation(*addedType, *declaredType))
        {
            return false;
        }
    }

    // Pull-up must preserve every dependency as an ordinary SQL equality.
    // Multiple correlation keys change the grouping key, not NULL matching.
    TInfoUnitSet covered;
    for (const auto& condition : filter.SplitConjunct()) {
        if (!condition.MaybeEquiJoinCondition()) {
            return false;
        }
        const TEquiJoinCondition equality(condition);
        auto outerKey = equality.GetLeftIU();
        auto innerKey = equality.GetRightIU();
        if (ContainsInfoUnit(added, innerKey)) {
            std::swap(outerKey, innerKey);
        }
        if (!ContainsInfoUnit(added, outerKey) ||
            ContainsInfoUnit(added, innerKey) ||
            CountInfoUnit(dependencies.GetInput()->GetOutputIUs(), innerKey) != 1)
        {
            return false;
        }
        covered.insert(outerKey);
    }
    return covered.size() == added.size();
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

// A traced path says where the selected value comes from, not yet whether
// replacing the originally-keyless aggregate's empty row is sound.
struct TScalarMapPath {
    TInfoUnit AggregateResultIU;
    TVector<TIntrusivePtr<TOpMap>> Maps;
    const TMapElement* Computation = nullptr;
};

TScalarMapPath TraceScalarResultMaps(
    const TIntrusivePtr<IOperator>& root,
    const TIntrusivePtr<TOpAggregate>& marked,
    TInfoUnit resultIU)
{
    TScalarMapPath path;
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
            auto source = ExactMemberSource(*producer);
            if (!source) {
                Y_ENSURE(
                    !path.Computation,
                    "Correlated scalar aggregate result permits only one "
                    "computed DecimalMul");
                source = ExactNullableDecimalMulSource(
                    *producer,
                    map->GetIUType(resultIU));
                Y_ENSURE(
                    source,
                    "Computed correlated scalar aggregate result must be "
                    "exactly one nullable DecimalMul of a direct member and "
                    "matching constant Decimal factor");
                path.Computation = producer;
            }
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
        path.Maps.push_back(map);
        current = input;
    }

    path.AggregateResultIU = resultIU;
    return path;
}

struct TUnmarkedScalarResult {
};

struct TDirectScalarAggregateResult {
    TString AggFunction;
};

// Constructed only after the exact AVG-or-SUM/strict-factor/Map/dependency premises
// below have been checked. This is optimizer applicability, not verifier input.
struct TExactScaledNullableAggregateResult {
};

using TScalarResultPath = std::variant<
    TUnmarkedScalarResult,
    TDirectScalarAggregateResult,
    TExactScaledNullableAggregateResult>;

TScalarResultPath AnalyzeScalarResultPath(
    const TIntrusivePtr<IOperator>& root,
    TInfoUnit resultIU,
    bool hasExactAlignedCorrelationDependency)
{
    const auto marked = FindOnlyMarkedAggregate(root);
    if (!marked) {
        return TUnmarkedScalarResult{};
    }

    const auto path = TraceScalarResultMaps(root, marked, resultIU);
    resultIU = path.AggregateResultIU;
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
    if (path.Computation) {
        for (const auto& map : path.Maps) {
            Y_ENSURE(
                IsExactComputedResultPathMap(
                    *map, path.Computation, map->GetInput()),
                "Computed correlated scalar DecimalMul path Maps must contain "
                "only exact type-preserving member aliases and pass-throughs");
        }
        Y_ENSURE(
            marked->AggregationTraitsList.size() == 1 &&
                (selectedTrait->AggFunction == "avg" ||
                 selectedTrait->AggFunction == "sum"),
            "Computed correlated scalar DecimalMul requires one unique direct "
            "AVG or SUM trait");
        Y_ENSURE(
            hasExactAlignedCorrelationDependency,
            "Computed correlated scalar DecimalMul requires matching unique "
            "typed registered and AddDependencies equality correlation IUs");

        // AVG(empty) and SUM(empty) are NULL. This exact DecimalMul is strict;
        // its factor is a row-independent Decimal literal or String/Utf8-literal
        // SafeCast (including a constant NULL when that cast fails).  The
        // synthetic NULL introduced by the Left join therefore already has
        // the scalar expression's empty-input value; unlike COUNT, it needs
        // no post-join repair. The computed right Map stays after aggregation:
        // no multiplication is distributed and no SUM order/type is changed.
        return TExactScaledNullableAggregateResult{};
    }
    return TDirectScalarAggregateResult{selectedTrait->AggFunction};
}

EScalarEmptyInputRepair DecideScalarEmptyInputRepair(
    const TScalarResultPath& path,
    const TTypeAnnotationNode* resultType)
{
    const auto* direct = std::get_if<TDirectScalarAggregateResult>(&path);
    if (!direct || direct->AggFunction != "count") {
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
        const bool hasExactAlignedCorrelationDependency =
            HasExactAlignedCorrelationDependencies(
                subplanEntry.DependentIUs,
                *addDeps,
                *child,
                subplanFilter->FilterExpr);
        const auto resultPath = AnalyzeScalarResultPath(
            uncorrSubplan,
            subplanResIU,
            hasExactAlignedCorrelationDependency);
        const auto emptyInputRepair = DecideScalarEmptyInputRepair(
            resultPath,
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

    ProtectInlinedScalarOutputs(*input, scalarIU, ctx, props);
    props.Subplans.Remove(scalarIU);

    return true;
}
}
}
