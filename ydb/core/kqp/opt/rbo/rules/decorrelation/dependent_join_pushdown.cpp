#include "dependent_join_pushdown.h"

#include "../kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

// Some helpers.
bool ColumnsIntersect(const TUnorderedIUs& left, const TUnorderedIUs& right) {
    for (const auto iu : left) {
        if (right.Contains(iu)) {
            return true;
        }
    }
    return false;
}

// QuickMatch of a rule with the pattern DependentJoin <- kind.
bool IsDependentJoinOver(const TIntrusivePtr<IOperator>& input, EOperator kind) {
    return input->Kind == EOperator::DependentJoin && CastOperator<TOpDependentJoin>(*input).GetInput()->Kind == kind;
}

bool HasOperatorBelow(const TIntrusivePtr<IOperator>& op, EOperator kind) {
    if (op->Kind == kind) {
        return true;
    }
    for (ui32 i = 0; i < op->GetChildCount(); ++i) {
        if (HasOperatorBelow(op->GetChild(i), kind)) {
            return true;
        }
    }
    return false;
}

TIntrusivePtr<TOpDependentJoin> PushInto(const TIntrusivePtr<TOpDependentJoin>& dependentJoin, const TIntrusivePtr<IOperator>& newInput) {
    return MakeIntrusive<TOpDependentJoin>(dependentJoin->GetDomain(), newInput, dependentJoin->Dependencies, dependentJoin->Pos, dependentJoin->DomainColumns);
}

TIntrusivePtr<IOperator> MakeCrossJoinWithDomain(const TIntrusivePtr<TOpDependentJoin>& dependentJoin, const TIntrusivePtr<IOperator>& input) {
    return MakeIntrusive<TOpJoin>(dependentJoin->GetDomain(), input, dependentJoin->Pos, "Cross", TJoinIUs{});
}

TOrderedIUs<> MissingDomainColumns(const TUnorderedIUs& dependencies, const TUnorderedIUs& present) {
    TOrderedIUs<> result;
    for (const auto iu : dependencies) {
        if (!present.Contains(iu)) {
            result.Append(iu);
        }
    }
    return result;
}

bool NeedsNullSafeEncoding(const TIntrusivePtr<IOperator>& leftInput, TInfoUnitId leftKey,
                           const TIntrusivePtr<IOperator>& rightInput, TInfoUnitId rightKey, TExprContext& ctx) {
    // Only ok if the keys are domain keys, which always carry the same values.
    const auto* leftInputType = leftInput->Type;
    const auto* rightInputType = rightInput->Type;

    if (leftInputType && rightInputType) {
        return IsNullableIU(leftInput, leftKey, ctx) || IsNullableIU(rightInput, rightKey, ctx);
    }
    if (leftInputType) {
        return IsNullableIU(leftInput, leftKey, ctx);
    }
    if (rightInputType) {
        return IsNullableIU(rightInput, rightKey, ctx);
    }

    // If types are unknown it's safe to keep null, because for non optional column there are no nulls.
    return true;
}

// Only Replicate ports share an operator, each port under its own IDs. Adds a
// port reading the operator in `slot`; `columns` maps the slot's IDs to the port's.
TIntrusivePtr<IOperator> ShareInput(TIntrusivePtr<IOperator>& slot, TPositionHandle pos, TPlanProps& props, TSubstitutions& columns) {
    if (slot->Kind != EOperator::Replicate) {
        auto hub = TReplicate::Create(std::move(slot), pos, props.InfoUnitRegistry);
        slot = hub->AddOutput();
    }
    auto& port = CastOperator<TOpReplicate>(*slot);
    auto copy = port.GetReplicate().AddOutput();
    for (const auto source : port.GetReplicate().GetInput()->GetOutputIUs()) {
        columns.Add(port.IsPrimary() ? source : port.GetRebindings().At(source), copy->GetRebindings().At(source));
    }
    return copy;
}

// Like PushInto for one more consumer of the domain: the pushed join reads a
// copy of the domain, which carries the same parameters in its own columns.
TIntrusivePtr<TOpDependentJoin> PushIntoCopy(const TIntrusivePtr<IOperator>& pushed, const TIntrusivePtr<IOperator>& newInput, TPlanProps& props) {
    auto dependentJoin = CastOperator<TOpDependentJoin>(pushed);
    TSubstitutions columns;
    auto domain = ShareInput(dependentJoin->GetDomain(), dependentJoin->Pos, props, columns);
    TSubstitutions domainColumns;
    for (const auto iu : dependentJoin->Dependencies) {
        domainColumns.Add(iu, columns.At(dependentJoin->GetDomainColumn(iu)));
    }
    return MakeIntrusive<TOpDependentJoin>(domain, newInput, dependentJoin->Dependencies, dependentJoin->Pos, std::move(domainColumns));
}

bool IsDeterministic(const TExpression& expression) {
    static const THashSet<TStringBuf> nondeterministic = {
        "Random", "RandomNumber", "RandomUuid", "Now",
        "CurrentUtcDate", "CurrentUtcDatetime", "CurrentUtcTimestamp",
        "CurrentTzDate", "CurrentTzDatetime", "CurrentTzTimestamp",
        // Nothing says which UDFs are deterministic.
        "Udf", "ScriptUdf", "SqlCall",
    };
    return !FindNode(expression.GetLambda(), [](const TExprNode::TPtr& node) {
        return node->IsCallable() && nondeterministic.contains(node->Content());
    });
}

bool IsDeterministicAggregation(const TString& function) {
    static const THashSet<TStringBuf> deterministic = {"count", "sum", "min", "max", "avg", "distinct", "variance_1_1"};
    return deterministic.contains(function);
}

// A Replicate gives every consumer the same rows, but a copy is evaluated on its own.
// So only an operator that returns the same rows for the same input can be copied.
bool CanCopyForConsumer(const IOperator& op) {
    for (const auto& expression : op.GetExpressions()) {
        if (!IsDeterministic(expression)) {
            return false;
        }
    }

    switch (op.Kind) {
        case EOperator::Replicate:
        case EOperator::AddDependencies:
        case EOperator::Filter:
        case EOperator::Map:
        case EOperator::UnionAll:
        case EOperator::Join:
            return true;
        case EOperator::Aggregate:
            return std::ranges::all_of(CastOperator<TOpAggregate>(op).GetAggregationTraits().Items() | std::views::values,
                                       [](const TOpAggregationTraits& traits) { return IsDeterministicAggregation(traits.AggFunction); });
        // Without a total order a limit can take other rows in every copy.
        case EOperator::Sort:
            return !CastOperator<TOpSort>(op).LimitCond;
        default:
            return false;
    }
}

TSortIUs SubstituteSortKeys(const TSortIUs& keys, const TSubstitutions& substitutions) {
    TSortIUs result;
    for (const auto& [iu, order] : keys.Items()) {
        result.Append(Substitute(iu, substitutions), order);
    }
    return result;
}

// A copy of the port's producer for this consumer only: the top operator with
// fresh definitions, reading the producer's inputs through new ports.
// `outputs` maps the port's columns to the copy's.
TIntrusivePtr<IOperator> CopyForConsumer(TOpReplicate& port, TSubstitutions& outputs, TPlanProps& props) {
    auto& producer = port.GetReplicate().GetInput();
    const auto pos = producer->Pos;
    // Producer columns -> copy columns: shared inputs first, then definitions.
    TSubstitutions copied;
    const auto define = [&](TInfoUnitId iu) {
        const auto copy = props.InfoUnitRegistry.AddCopy(iu);
        copied.Add(iu, copy);
        return copy;
    };
    TVector<TIntrusivePtr<IOperator>> inputs;
    if (producer->Kind != EOperator::Replicate) {
        for (ui32 i = 0; i < producer->GetChildCount(); ++i) {
            inputs.push_back(ShareInput(producer->MutableChild(i), pos, props, copied));
        }
    }

    TIntrusivePtr<IOperator> copy;
    switch (producer->Kind) {
        case EOperator::Replicate:
            copy = ShareInput(producer, pos, props, copied);
            break;
        case EOperator::AddDependencies: {
            TDependencyIUs dependencies;
            for (const auto& [iu, capture] : CastOperator<TOpAddDependencies>(producer)->GetDependencies().Items()) {
                dependencies.Add(define(iu), capture);
            }
            copy = MakeIntrusive<TOpAddDependencies>(inputs[0], pos, std::move(dependencies));
            break;
        }
        case EOperator::Filter:
            copy = MakeIntrusive<TOpFilter>(inputs[0], pos, CastOperator<TOpFilter>(producer)->GetFilterExpression().ApplyRenames(copied));
            break;
        case EOperator::Map: {
            TMapIUs elements;
            for (const auto& [iu, element] : CastOperator<TOpMap>(producer)->GetMapElements().Items()) {
                auto expression = element.GetExpression().ApplyRenames(copied);
                elements.Add(define(iu), std::move(expression));
            }
            copy = MakeIntrusive<TOpMap>(inputs[0], pos, std::move(elements));
            break;
        }
        case EOperator::Aggregate: {
            auto aggregate = CastOperator<TOpAggregate>(producer);
            TOrderedIUs<> keys;
            for (const auto iu : aggregate->GetKeyColumns().Items()) {
                keys.Append(Substitute(iu, copied));
            }
            TAggregationIUs traits;
            for (const auto& [iu, trait] : aggregate->GetAggregationTraits().Items()) {
                auto rebound = trait;
                rebound.Input = Substitute(trait.Input, copied);
                traits.Add(define(iu), std::move(rebound));
            }
            copy = MakeIntrusive<TOpAggregate>(inputs[0], std::move(traits), std::move(keys), aggregate->GetAggregationPhase(),
                aggregate->IsDistinctAll(), pos);
            break;
        }
        case EOperator::UnionAll: {
            auto unionAll = CastOperator<TOpUnionAll>(producer);
            TUnionAllIUs columns(TUnionInputPolicy{unionAll->GetChildCount()});
            for (const auto& [iu, row] : unionAll->GetColumns().Items()) {
                auto rebound = row;
                for (auto& input : rebound.Inputs) {
                    input = Substitute(input, copied);
                }
                columns.Add(define(iu), std::move(rebound));
            }
            copy = MakeIntrusive<TOpUnionAll>(inputs, pos, std::move(columns), unionAll->Ordered);
            break;
        }
        case EOperator::Join: {
            auto join = CastOperator<TOpJoin>(producer);
            TJoinIUs joinKeys;
            for (const auto& [left, right, equalNulls] : join->JoinKeys.Items()) {
                joinKeys.Add({Substitute(left, copied), Substitute(right, copied), equalNulls});
            }
            TVector<TExpression> joinFilters;
            for (const auto& filter : join->JoinFilters) {
                joinFilters.push_back(filter.ApplyRenames(copied));
            }
            copy = MakeIntrusive<TOpJoin>(inputs[0], inputs[1], pos, join->JoinKind, std::move(joinKeys), joinFilters);
            break;
        }
        case EOperator::Sort:
            copy = MakeIntrusive<TOpSort>(inputs[0], pos, SubstituteSortKeys(CastOperator<TOpSort>(producer)->GetSortElements(), copied));
            break;
        default:
            Y_ENSURE(false, "Cannot copy " << producer->GetExplainName() << " for a correlated consumer");
    }

    for (const auto source : producer->GetOutputIUs()) {
        outputs.Add(port.IsPrimary() ? source : port.GetRebindings().At(source), Substitute(source, copied));
    }
    return copy;
}

// Here is a special case for count(*). count(*) with empty keys returns 0 on empty input, but with group by keys we can lost those values.
// So, we make left join to restore columns and apply coalesce (column, 0).
TIntrusivePtr<IOperator> RestoreEmptyGroupCounts(const TIntrusivePtr<TOpDependentJoin>& dependentJoin, const TIntrusivePtr<TOpAggregate>& aggregate,
                                                 const TUnorderedIUs& countResults, TRBOContext& ctx, TPlanProps& props) {
    const auto& dependencies = dependentJoin->Dependencies;
    const auto pos = aggregate->Pos;

    // The pushed dependent join keeps the domain; the left side reads a copy.
    TSubstitutions leftRenames;
    auto pushed = CastOperator<TOpDependentJoin>(aggregate->GetInput());
    TIntrusivePtr<IOperator> leftInput = ShareInput(pushed->GetDomain(), pos, props, leftRenames);
    TIntrusivePtr<IOperator> rightInput = aggregate;

    TJoinIUs joinKeys;
    for (const auto iu : dependencies) {
        const auto column = dependentJoin->GetDomainColumn(iu);
        joinKeys.Add(leftRenames.At(column), column);
    }
    joinKeys = MakeNullSafeJoinKeys(leftInput, rightInput, joinKeys, pos, ctx, props);

    TSubstitutions renamedCounts;
    for (const auto iu : countResults) {
        renamedCounts.Add(iu, props.InfoUnitRegistry.AddCopy(iu));
    }

    auto join = MakeIntrusive<TOpJoin>(leftInput, rightInput, pos, "Left", joinKeys);

    TMapIUs resultElements;
    for (const auto& [resultIU, renamedIU] : renamedCounts.Items()) {
        resultElements.Add(
            renamedIU, MakeBinaryPredicate("Coalesce", MakeColumnAccess(resultIU, pos, &ctx.ExprCtx, &props), MakeConstant("Uint64", "0", pos, &ctx.ExprCtx)));
    }

    // Consumers read the domain from the left side and the restored counts.
    for (const auto& [resultIU, renamedIU] : renamedCounts.Items()) {
        leftRenames.Add(resultIU, renamedIU);
    }
    RebindConsumers(*dependentJoin, leftRenames, props.Subplans);

    return MakeIntrusive<TOpMap>(join, pos, resultElements);
}

// The keys of the sort that orders the rows a limit takes, under the limit's IDs. Maps, filters and
// Replicate ports keep the order and the sort keys; a port exposes them under its own IDs.
TSortIUs FindLimitOrdering(const TIntrusivePtr<IOperator>& input) {
    TVector<TOpReplicate*> ports;
    auto* op = input.Get();
    while (op->Kind == EOperator::Map || op->Kind == EOperator::Filter || op->Kind == EOperator::Replicate) {
        if (op->Kind == EOperator::Replicate && !CastOperator<TOpReplicate>(*op).IsPrimary()) {
            ports.push_back(&CastOperator<TOpReplicate>(*op));
        }
        op = op->GetChild(0).Get();
    }
    if (op->Kind != EOperator::Sort) {
        return {};
    }

    auto keys = CastOperator<TOpSort>(*op).GetSortElements();
    for (auto it = ports.rbegin(); it != ports.rend(); ++it) {
        keys = SubstituteSortKeys(keys, (*it)->GetRebindings());
    }
    return keys;
}

// Neumann's limit unnesting: a limit applies to each domain value, so the rows are numbered per domain value.
// D ⋈ limit(k, o, sort(T)) = filter(o < rn <= o + k, window(rn: row_number() over (partition by D order by sort), D ⋈ T)).
TIntrusivePtr<IOperator> LimitPerDomainValue(const TIntrusivePtr<TOpDependentJoin>& dependentJoin, const TIntrusivePtr<IOperator>& input,
                                             const TSortIUs& sortKeys, const TExpression& limit, const std::optional<TExpression>& offset,
                                             TPositionHandle pos, TRBOContext& ctx, TPlanProps& props) {
    const auto rowNumber = props.InfoUnitRegistry.AddGenerated("row_number");
    TWindowIUs functions;
    functions.Add(rowNumber, TOpWindowFunc{"rownumber", EWindowFuncKind::Native, {}});
    const auto domainColumns = dependentJoin->GetDomainColumns();
    auto window = MakeIntrusive<TOpWindow>(PushInto(dependentJoin, input), pos, std::move(functions),
                                           TOrderedIUs<>(domainColumns.begin(), domainColumns.end()), sortKeys, TOpWindowFrame{});

    auto position = MakeColumnAccess(rowNumber, pos, &ctx.ExprCtx, &props);
    TVector<TExpression> conjuncts;
    if (offset) {
        conjuncts.push_back(MakeBinaryPredicate(">", position, *offset));
        // Subtract instead of adding the offset to the limit, which can overflow.
        position = MakeBinaryPredicate("-", position, *offset);
    }
    conjuncts.push_back(MakeBinaryPredicate("<=", position, limit));
    return MakeIntrusive<TOpFilter>(window, pos, MakeConjunction(conjuncts));
}
} // anonymous namespace

// Domain projection is a distinct on free variables.
TIntrusivePtr<TOpAggregate> MakeDomainProjection(const TIntrusivePtr<IOperator>& input, const TUnorderedIUs& columns, TPositionHandle pos) {
    Y_ENSURE(!columns.Empty(), "Domain of a dependent join cannot be empty");

    // Grouping keys keep their IDs, unlike DISTINCT results.
    return MakeIntrusive<TOpAggregate>(input, TAggregationIUs{}, TOrderedIUs<>(columns.begin(), columns.end()), EOpPhase::Undefined, /*distinctAll=*/false, pos);
}

bool IsNullableIU(const TIntrusivePtr<IOperator>& input, TInfoUnitId iu, TExprContext& ctx) {
    if (!input->Type) {
        return true;
    }
    const auto* columnType = input->GetIUType(iu, ctx);
    return !columnType || columnType->IsOptionalOrNull();
}

TJoinIUs MakeNullSafeJoinKeys(TIntrusivePtr<IOperator>& leftInput, TIntrusivePtr<IOperator>& rightInput,
                              const TJoinIUs& joinKeys, TPositionHandle pos, TRBOContext& ctx,
                              TPlanProps& props) {
    TJoinIUs result;
    const bool nativeEqualNulls =
        ctx.KqpCtx.Config->GetUseBlockHashJoin() && ctx.KqpCtx.Config->GetEnableBlockHashJoinEqualNulls();

    TMapIUs leftElements;
    TMapIUs rightElements;

    auto encode = [&](TInfoUnitId iu, TMapIUs& elements) {
        auto encodedIU = props.InfoUnitRegistry.AddGenerated("null_safe_key");
        // Emulates null == null.
        elements.Add(encodedIU, MakeUnaryCallable("StablePickle", MakeColumnAccess(iu, pos, &ctx.ExprCtx, &props)));
        return encodedIU;
    };

    for (const auto& [leftKey, rightKey, equalNulls] : joinKeys.Items()) {
        if (!NeedsNullSafeEncoding(leftInput, leftKey, rightInput, rightKey, ctx.ExprCtx)) {
            result.Add({leftKey, rightKey, equalNulls});
            continue;
        }

        if (nativeEqualNulls) {
            result.Add({leftKey, rightKey, /*equalNulls=*/true});
        } else {
            result.Add(encode(leftKey, leftElements), encode(rightKey, rightElements));
        }
    }

    if (!leftElements.Keys().Empty()) {
        leftInput = MakeIntrusive<TOpMap>(leftInput, pos, leftElements);
    }
    if (!rightElements.Keys().Empty()) {
        rightInput = MakeIntrusive<TOpMap>(rightInput, pos, rightElements);
    }

    return result;
}

bool HasFreeCorrelation(const TIntrusivePtr<IOperator>& op, const TUnorderedIUs& correlatedColumns) {
    if (op->Kind == EOperator::AddDependencies) {
        if (ColumnsIntersect(CastOperator<TOpAddDependencies>(op)->GetDependencies().MappedIUs(), correlatedColumns)) {
            return true;
        }
    }

    for (ui32 i = 0; i < op->GetChildCount(); ++i) {
        if (HasFreeCorrelation(op->GetChild(i), correlatedColumns)) {
            return true;
        }
    }

    return false;
}

bool TRewriteDependentJoinToCrossJoinRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return IsDependentJoinOver(input, EOperator::AddDependencies);
}

TIntrusivePtr<IOperator> TRewriteDependentJoinToCrossJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);

    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto depJoinInput = dependentJoin->GetInput();
    // Check that input is dependencies op.
    if (depJoinInput->Kind != EOperator::AddDependencies) {
        return input;
    }

    auto addDependencies = CastOperator<TOpAddDependencies>(depJoinInput);
    if (!addDependencies->GetDependencies().MappedIUs().IsSubsetOf(dependentJoin->Dependencies)) {
        return input;
    }

    if (HasFreeCorrelation(addDependencies->GetInput(), dependentJoin->Dependencies)) {
        return input;
    }

    // Consumers read the captured columns from the domain.
    TSubstitutions captures;
    for (const auto& [iu, capture] : addDependencies->GetDependencies().Items()) {
        captures.Add(iu, dependentJoin->GetDomainColumn(capture.Outer));
    }
    RebindConsumers(*dependentJoin, captures, props.Subplans);

    return MakeCrossJoinWithDomain(dependentJoin, addDependencies->GetInput());
}

bool TRewriteDependentJoinToCrossJoinNoFreeVarsRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::DependentJoin;
}

// In some cases we can have a situation, when we push dependent join through op, but it does not have a free variables.
// For example for union all we push dependent join for each branch.
TIntrusivePtr<IOperator> TRewriteDependentJoinToCrossJoinNoFreeVarsRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx,
                                                                                             TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    if (HasFreeCorrelation(dependentJoin->GetInput(), dependentJoin->Dependencies)) {
        return input;
    }

    return MakeCrossJoinWithDomain(dependentJoin, dependentJoin->GetInput());
}

bool TEliminateDependentJoinDomainRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return IsDependentJoinOver(input, EOperator::Filter);
}

// We can eliminate dependent join by rewriting it into map -> filter.
TIntrusivePtr<IOperator> TEliminateDependentJoinDomainRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();

    // Dependent join <- Filter <- AddDep.
    if (body->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(body);
    if (filter->GetInput()->Kind != EOperator::AddDependencies) {
        return input;
    }

    auto addDependencies = CastOperator<TOpAddDependencies>(filter->GetInput());
    auto correlatedInput = addDependencies->GetInput();
    const auto& captures = addDependencies->GetDependencies();
    if (!captures.MappedIUs().IsSubsetOf(dependentJoin->Dependencies)) {
        return input;
    }
    if (HasFreeCorrelation(correlatedInput, dependentJoin->Dependencies)) {
        return input;
    }
    const auto innerIUs = correlatedInput->GetOutputIUs();

    THashMap<TInfoUnitId, TInfoUnitId> bindings;
    TVector<TExpression> restConjuncts;
    // Collect eq predicates from each conj.
    for (const auto& conj : filter->GetFilterExpression().SplitConjunct()) {
        std::optional<std::pair<TInfoUnitId, TInfoUnitId>> binding;

        if (conj.MaybeEquiJoinCondition()) {
            TEquiJoinCondition condition(conj);
            const auto left = condition.GetLeftIU();
            const auto right = condition.GetRightIU();

            if (captures.Keys().Contains(left) && innerIUs.Contains(right)) {
                binding = std::make_pair(left, right);
            } else if (captures.Keys().Contains(right) && innerIUs.Contains(left)) {
                binding = std::make_pair(right, left);
            }
        }

        if (binding && bindings.emplace(binding->first, binding->second).second) {
            continue;
        }
        // Keep rest.
        restConjuncts.push_back(conj);
    }

    // Consumers read each bound parameter from its capture instead of the domain.
    TSubstitutions boundColumns;
    for (const auto& [iu, capture] : captures.Items()) {
        if (!bindings.contains(iu)) {
            return input;
        }
        if (innerIUs.Contains(iu) || boundColumns.Keys().Contains(dependentJoin->GetDomainColumn(capture.Outer))) {
            return input;
        }
        boundColumns.Add(dependentJoin->GetDomainColumn(capture.Outer), iu);
    }

    // Here we want to put them into map.
    TMapIUs bindingElements;
    for (const auto iu : captures.Keys()) {
        const auto& source = bindings.at(iu);
        bindingElements.Add(iu, MakeColumnAccess(source, filter->Pos, &ctx.ExprCtx, &props));

        // Special case for optional column.
        if (IsNullableIU(correlatedInput, source, ctx.ExprCtx)) {
            restConjuncts.push_back(MakeUnaryCallable("Exists", MakeColumnAccess(source, filter->Pos, &ctx.ExprCtx, &props)));
        }
    }

    TIntrusivePtr<IOperator> newBody = MakeIntrusive<TOpMap>(correlatedInput, filter->Pos, bindingElements);
    // Keep rest conj in filter, if all domain columns binded into eq prdicates, we can eliminate a filter.
    if (!restConjuncts.empty()) {
        newBody = MakeIntrusive<TOpFilter>(newBody, filter->Pos, MakeConjunction(restConjuncts, props.PgSyntax));
    }

    // If nothing remaining we can return map/filter.
    auto remainingDependencies = MissingDomainColumns(dependentJoin->Dependencies, captures.MappedIUs());
    if (remainingDependencies.Items().empty()) {
        RebindConsumers(*dependentJoin, boundColumns, props.Subplans);
        return newBody;
    }

    auto domain = dependentJoin->GetDomain();
    if (domain->Kind != EOperator::Aggregate || !CastOperator<TOpAggregate>(domain)->GetAggregationTraits().Keys().Empty()) {
        return input;
    }

    // Reduce domain.
    auto smallerDomain = MakeDomainProjection(CastOperator<TOpAggregate>(domain)->GetInput(), remainingDependencies.Unordered(), domain->Pos);
    RebindConsumers(*dependentJoin, boundColumns, props.Subplans);
    return MakeIntrusive<TOpDependentJoin>(smallerDomain, newBody, remainingDependencies.Unordered(), dependentJoin->Pos);
}

bool TPushDependentJoinThroughFilterRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return IsDependentJoinOver(input, EOperator::Filter);
}

TIntrusivePtr<IOperator> TPushDependentJoinThroughFilterRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx,
                                                                                 TPlanProps& props) {
    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();
    if (body->Kind != EOperator::Filter) {
        return input;
    }

    auto filter = CastOperator<TOpFilter>(body);
    auto newInput = PushInto(dependentJoin, filter->GetInput());
    return MakeIntrusive<TOpFilter>(newInput, filter->Pos, TExpression(filter->GetFilterExpression().GetLambda(), &ctx.ExprCtx, &props));
}

bool TPushDependentJoinThroughMapRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return IsDependentJoinOver(input, EOperator::Map);
}

TIntrusivePtr<IOperator> TPushDependentJoinThroughMapRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();
    if (body->Kind != EOperator::Map) {
        return input;
    }
    auto map = CastOperator<TOpMap>(body);

    for (const auto iu : map->GetMapElements().Keys()) {
        if (dependentJoin->Dependencies.Contains(iu)) {
            return input;
        }
    }

    auto newInput = PushInto(dependentJoin, map->GetInput());
    return MakeIntrusive<TOpMap>(newInput, map->Pos, map->GetMapElements());
}

bool TPushDependentJoinThroughAggregateRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return IsDependentJoinOver(input, EOperator::Aggregate);
}

TIntrusivePtr<IOperator> TPushDependentJoinThroughAggregateRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx,
                                                                                     TPlanProps& props) {
    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();
    if (body->Kind != EOperator::Aggregate) {
        return input;
    }

    auto aggregate = CastOperator<TOpAggregate>(body);
    Y_ENSURE(aggregate->GetAggregationPhase() == EOpPhase::Undefined);

    auto newKeyColumns = MissingDomainColumns(dependentJoin->GetDomainColumns(), aggregate->GetKeyColumns().Unordered());
    for (const auto iu : aggregate->GetKeyColumns().Items()) {
        newKeyColumns.Append(iu);
    }

    auto newTraits = aggregate->GetAggregationTraits();
    if (aggregate->IsDistinctAll()) {
        // DISTINCT ALL outputs only its results: consumers read the domain from new results.
        TSubstitutions domainResults;
        const auto missingDomainColumns = MissingDomainColumns(dependentJoin->GetDomainColumns(), newTraits.Keys());
        for (const auto iu : missingDomainColumns.Items()) {
            domainResults.Add(iu, props.InfoUnitRegistry.AddCopy(iu));
            newTraits.Add(domainResults.At(iu), TOpAggregationTraits{iu, "distinct"});
        }
        RebindConsumers(*dependentJoin, domainResults, props.Subplans);
    }

    auto newInput = PushInto(dependentJoin, aggregate->GetInput());
    auto newAggregate =
        MakeIntrusive<TOpAggregate>(newInput, newTraits, newKeyColumns, aggregate->GetAggregationPhase(), aggregate->IsDistinctAll(), aggregate->Pos);

    if (!aggregate->GetKeyColumns().Items().empty() || aggregate->IsDistinctAll()) {
        return newAggregate;
    }

    // Special case for count.
    TUnorderedIUs countResults;
    for (const auto& [iu, traits] : newTraits.Items()) {
        if (traits.AggFunction == "count") {
            countResults.Add(iu);
        }
    }

    if (countResults.Empty()) {
        return newAggregate;
    }

    return RestoreEmptyGroupCounts(dependentJoin, newAggregate, countResults, ctx, props);
}

bool TPushDependentJoinThroughUnionAllRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return IsDependentJoinOver(input, EOperator::UnionAll);
}

TIntrusivePtr<IOperator> TPushDependentJoinThroughUnionAllRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx,
                                                                                   TPlanProps& props) {
    Y_UNUSED(ctx);

    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();
    if (body->Kind != EOperator::UnionAll) {
        return input;
    }
    auto unionAll = CastOperator<TOpUnionAll>(body);

    if (ColumnsIntersect(dependentJoin->GetDomainColumns(), unionAll->GetOutputIUs())) {
        return input;
    }

    // Push on each side.
    TVector<TIntrusivePtr<IOperator>> newInputs;
    newInputs.reserve(unionAll->GetChildCount());
    for (const auto& child : unionAll->GetInputs()) {
        newInputs.push_back(newInputs.empty() ? PushInto(dependentJoin, child) : PushIntoCopy(newInputs.front(), child, props));
    }

    // Each branch carries the domain in its own columns: the union outputs new ones.
    auto newColumns = unionAll->GetColumns();
    TSubstitutions unionColumns;
    for (const auto iu : dependentJoin->Dependencies) {
        TUnionInputRow row;
        for (const auto& newInput : newInputs) {
            row.Inputs.push_back(CastOperator<TOpDependentJoin>(newInput)->GetDomainColumn(iu));
        }
        const auto column = dependentJoin->GetDomainColumn(iu);
        unionColumns.Add(column, props.InfoUnitRegistry.AddCopy(column));
        newColumns.Add(unionColumns.At(column), std::move(row));
    }
    RebindConsumers(*dependentJoin, unionColumns, props.Subplans);

    return MakeIntrusive<TOpUnionAll>(newInputs, unionAll->Pos, newColumns, unionAll->Ordered);
}

bool TPushDependentJoinThroughJoinRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return IsDependentJoinOver(input, EOperator::Join);
}

TIntrusivePtr<IOperator> TPushDependentJoinThroughJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();
    if (body->Kind != EOperator::Join) {
        return input;
    }
    auto join = CastOperator<TOpJoin>(body);
    const auto& dependencies = dependentJoin->Dependencies;

    const bool leftCorrelated = HasFreeCorrelation(join->GetLeftInput(), dependencies);
    const bool rightCorrelated = HasFreeCorrelation(join->GetRightInput(), dependencies);

    if (!leftCorrelated && !rightCorrelated) {
        return input;
    }

    const auto joinKind = GetValidJoinKind(join->JoinKind);
    const bool innerLike = joinKind == "Inner" || joinKind == "Cross";
    const bool leftPreserving = innerLike || joinKind == "Left" || joinKind == "LeftSemi" || joinKind == "LeftOnly";

    // Here we want to push the dependent join on the side where we have a free variables.
    if (leftCorrelated && !rightCorrelated && leftPreserving) {
        auto newLeft = PushInto(dependentJoin, join->GetLeftInput());
        return MakeIntrusive<TOpJoin>(newLeft, join->GetRightInput(), join->Pos, join->JoinKind, join->JoinKeys, join->JoinFilters);
    }

    // Inner or cross.
    if (!leftCorrelated && rightCorrelated && innerLike) {
        auto newRight = PushInto(dependentJoin, join->GetRightInput());
        return MakeIntrusive<TOpJoin>(join->GetLeftInput(), newRight, join->Pos, join->JoinKind, join->JoinKeys, join->JoinFilters);
    }

    // All right joins must be rewritten before to left joins.
    if (!leftPreserving) {
        return input;
    }

    // Otherwise push a dependet join on both sides.
    TIntrusivePtr<IOperator> newLeft = PushInto(dependentJoin, join->GetLeftInput());
    TIntrusivePtr<IOperator> newRight = PushIntoCopy(newLeft, join->GetRightInput(), props);

    // Add a domain keys if both side a correlated.
    TJoinIUs domainKeys;
    for (const auto iu : dependencies) {
        domainKeys.Add(CastOperator<TOpDependentJoin>(newLeft)->GetDomainColumn(iu), CastOperator<TOpDependentJoin>(newRight)->GetDomainColumn(iu));
    }
    domainKeys = MakeNullSafeJoinKeys(newLeft, newRight, domainKeys, join->Pos, ctx, props);

    auto joinKeys = join->JoinKeys;
    for (const auto& key : domainKeys.Items()) {
        joinKeys.Add(key);
    }

    // rewrite cross to inner because it has a join keys based on domain.
    const TString newJoinKind = joinKind == "Cross" ? "Inner" : joinKind;

    return MakeIntrusive<TOpJoin>(newLeft, newRight, join->Pos, newJoinKind, joinKeys, join->JoinFilters);
}

bool TPushDependentJoinThroughReplicateRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return IsDependentJoinOver(input, EOperator::Replicate);
}

// Only Replicate ports share an operator. Push through a port as through any
// shared operator: this consumer gets its own copy of the producer.
TIntrusivePtr<IOperator> TPushDependentJoinThroughReplicateRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx,
                                                                                    TPlanProps& props) {
    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();
    if (body->Kind != EOperator::Replicate || !HasFreeCorrelation(body, dependentJoin->Dependencies)) {
        return input;
    }

    auto port = CastOperator<TOpReplicate>(body);
    if (TOpReplicate::TryCollapse(body, ctx.ExprCtx, props)) {
        return PushInto(dependentJoin, body);
    }
    if (!CanCopyForConsumer(*port->GetReplicate().GetInput())) {
        return input;
    }

    TSubstitutions outputs;
    auto copy = CopyForConsumer(*port, outputs, props);
    RebindConsumers(*dependentJoin, outputs, props.Subplans);
    return PushInto(dependentJoin, copy);
}

bool TPushDependentJoinThroughSortRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return IsDependentJoinOver(input, EOperator::Sort);
}

TIntrusivePtr<IOperator> TPushDependentJoinThroughSortRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx,
                                                                                TPlanProps& props) {
    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();
    if (body->Kind != EOperator::Sort) {
        return input;
    }

    auto sort = CastOperator<TOpSort>(body);
    // The output of a dependent join has no order, a limit above already took the sort keys.
    if (!sort->LimitCond) {
        return PushInto(dependentJoin, sort->GetInput());
    }
    if (!sort->LimitCond->GetRawInputIUs().Empty()) {
        return input;
    }
    return LimitPerDomainValue(dependentJoin, sort->GetInput(), sort->GetSortElements(), *sort->LimitCond, std::nullopt, sort->Pos, ctx, props);
}

bool TPushDependentJoinThroughLimitRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return IsDependentJoinOver(input, EOperator::Limit);
}

TIntrusivePtr<IOperator> TPushDependentJoinThroughLimitRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx,
                                                                                 TPlanProps& props) {
    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();
    if (body->Kind != EOperator::Limit) {
        return input;
    }

    auto limit = CastOperator<TOpLimit>(body);
    // Row numbers are compared with the same bounds for every domain value.
    if (!limit->GetUniqueRawInputIUs().Empty()) {
        return input;
    }

    // Without a sort a limit takes any rows.
    return LimitPerDomainValue(dependentJoin, limit->GetInput(), FindLimitOrdering(limit->GetInput()), limit->GetLimitCond(),
                               limit->GetOffsetCond(), limit->Pos, ctx, props);
}

bool TDependentJoinNotSupportedRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::DependentJoin;
}

// We should fail if we cannot push dependent join or rewrite it or eliminate it.
TIntrusivePtr<IOperator> TDependentJoinNotSupportedRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps& props) {
    Y_UNUSED(ctx);
    Y_UNUSED(props);

    auto dependentJoin = CastOperator<TOpDependentJoin>(input);
    auto body = dependentJoin->GetInput();

    // Nested one.
    if (HasOperatorBelow(body, EOperator::DependentJoin)) {
        return input;
    }

    Y_ENSURE(false, "Cannot decorrelate the subquery, correlation cannot be pushed through " << body->GetExplainName());
    return input;
}

TSubplanDomain MakeSubplanDomain(TIntrusivePtr<IOperator>& caller, const TUnorderedIUs& parameters,
    TPositionHandle pos, TPlanProps& props)
{
    Y_ENSURE(!parameters.Empty() && parameters.IsSubsetOf(caller->GetOutputIUs()),
        "Correlation parameters must be produced by the caller");
    auto hub = TReplicate::Create(std::move(caller), pos, props.InfoUnitRegistry);
    caller = hub->AddOutput();
    auto port = hub->AddOutput();
    TPairedIUs keys;
    for (const auto id : parameters) {
        keys.Add(id, *port->GetRebindings().Find(id));
    }
    auto domain = MakeDomainProjection(port, keys.Right(), pos);
    return {std::move(domain), std::move(keys)};
}

TIntrusivePtr<IOperator> TSubplanDomain::Bind(TIntrusivePtr<IOperator> body, TPositionHandle pos) && {
    const TSubstitutions substitutions(Keys.Items().begin(), Keys.Items().end());
    for (const auto& item : IterateSubtree(body.get())) {
        if (item.Current->Kind == EOperator::AddDependencies) {
            CastOperator<TOpAddDependencies>(*item.Current).RebindCaptures(substitutions);
        }
    }
    return MakeIntrusive<TOpDependentJoin>(std::move(Input), std::move(body), Keys.Right(), pos);
}

} // namespace NKqp
} // namespace NKikimr
