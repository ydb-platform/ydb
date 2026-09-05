// Included exactly once inside semantic_snapshot.cpp's anonymous namespace.
// Requires the source-window grammar and the shared output/type helpers.

bool IsQ51WindowCandidate(const TExpression& expression) {
    if (!expression.Node || !expression.Node->IsLambda()) {
        return false;
    }
    const auto body = expression.GetExpressionBody();
    if (!body->IsCallable("YqlAggWin")) {
        return false;
    }

    // Preserve the whole-partition family's routing, including malformed
    // factory mutations. The exact Q51 decoder still checks every premise.
    const bool exactAvg = body->ChildrenSize() == 5 &&
        body->Child(0)->IsCallable("YqlWinFactory") &&
        body->Child(0)->ChildrenSize() == 1 &&
        body->Child(0)->Child(0)->IsAtom("avg");
    if (exactAvg) {
        return false;
    }
    const auto& metadata = expression.GetWindowMetadata();
    return !(metadata && metadata->Definition &&
        (!metadata->Definition->IsCallable("YqlWindow") ||
         metadata->Definition->ChildrenSize() != 5 ||
         !metadata->Definition->Child(3)->IsList() ||
         metadata->Definition->Child(3)->ChildrenSize() == 0));
}

class TQ51WindowAdmission;

// Only a completed admission can construct this result. Serialization has no
// access to candidates, partial topology facts, or mutable source expressions.
// This is private C++ evidence; the Python decoder independently rechecks the
// normalized window contract and receives no trusted admission flag.
class TAuditedQ51WindowPlan {
public:
    const NJson::TJsonValue* FindExpression(const TMapElement& element) const {
        return Expressions.FindPtr(&element);
    }

private:
    friend class TQ51WindowAdmission;

    explicit TAuditedQ51WindowPlan(
        THashMap<const TMapElement*, NJson::TJsonValue> expressions)
        : Expressions(std::move(expressions))
    {
    }

    THashMap<const TMapElement*, NJson::TJsonValue> Expressions;
};

class TQ51WindowAdmission {
public:
    static TAuditedQ51WindowPlan Audit(
        const TIntrusivePtr<IOperator>& mainRoot,
        const TVector<TIntrusivePtr<IOperator>>& subplanRoots)
    {
        TQ51WindowAdmission admission;
        // Match normal export's shared-DAG, subplans-first postorder. Q51
        // ultimately excludes subplans, so its only admissible input names
        // are physical outputs, never virtual scalar bindings.
        THashSet<const IOperator*> visited;
        const auto collect = [&](IOperator& op) {
            if (op.GetKind() == EOperator::Map) {
                admission.CollectProjection(static_cast<TOpMap&>(op));
            }
        };
        for (const auto& subplan : subplanRoots) {
            VisitOperators(subplan, visited, collect);
        }
        VisitOperators(mainRoot, visited, collect);
        if (!admission.PreparedWindows.empty()) {
            VisitOperators(
                mainRoot,
                admission.MainNodes,
                [&](IOperator& op) {
                    for (const auto& child : op.GetChildren()) {
                        admission.Parents[child.Get()].push_back(&op);
                    }
                });
        }
        admission.ValidateTopology(!subplanRoots.empty());

        THashMap<const TMapElement*, NJson::TJsonValue> expressions;
        for (auto& [element, window] : admission.PreparedWindows) {
            expressions.emplace(element, std::move(window.Expression));
        }
        return TAuditedQ51WindowPlan(std::move(expressions));
    }

private:
    struct TQ51ProjectionWindow {
        TString Input;
        TString PartitionColumn;
        TString OrderColumn;
        TString WindowName;
        EQ51WindowFunction Function;
        ui32 SourceOrdinal = 0;
        ui32 ExecutionOrder = 0;
    };

    void CollectProjection(TOpMap& map) {
        TVector<std::pair<const TMapElement*, TQ51Window>> windows;
        std::optional<THashSet<TString>> inputNames;
        for (const auto& element : map.MapElements) {
            const auto& expression = element.GetExpression();
            if (!IsQ51WindowCandidate(expression)) {
                // A non-Q51 window is still checked by its own exporter. Its
                // presence alone excludes a mixed Q51 plan.
                const auto body = expression.Node && expression.Node->IsLambda()
                    ? expression.GetExpressionBody()
                    : TExprNode::TPtr{};
                HasOtherWindowFamily |=
                    (body && body->IsCallable("YqlWin")) ||
                    (!element.IsRename() && expression.GetWindowMetadata());
                continue;
            }
            if (!inputNames) {
                inputNames.emplace(OutputNames(*map.GetInput()));
            }
            windows.emplace_back(
                &element,
                ExportQ51Window(expression, *inputNames));
        }
        if (windows.empty()) {
            return;
        }
        if (windows.size() > 2) {
            Unsupported(
                "q51 ROWS Project supports at most two direct window leaves");
        }
        std::sort(
            windows.begin(),
            windows.end(),
            [](const auto& left, const auto& right) {
                return left.second.SourceOrdinal < right.second.SourceOrdinal;
            });
        for (size_t index = 1; index < windows.size(); ++index) {
            if (windows[index - 1].second.SourceOrdinal ==
                windows[index].second.SourceOrdinal)
            {
                Unsupported(
                    "q51 ROWS Project source ordinals must be distinct");
            }
        }

        auto& projection = Projections[&map];
        projection.reserve(windows.size());
        for (size_t index = 0; index < windows.size(); ++index) {
            auto& [element, window] = windows[index];
            const TString output = element->GetElementName().GetFullName();
            const TExactType expectedDecimal{"Decimal(35,2)", true};
            const TExactType expectedPartition{
                "Int64",
                window.Function == EQ51WindowFunction::Max,
            };
            if (output.empty() ||
                !SameType(ExactType(OutputType(map, output)), expectedDecimal) ||
                !SameType(
                    ExactType(OutputType(*map.GetInput(), window.Input)),
                    expectedDecimal) ||
                !SameType(
                    ExactType(OutputType(
                        *map.GetInput(),
                        window.PartitionColumn)),
                    expectedPartition) ||
                !SameType(
                    ExactType(OutputType(
                        *map.GetInput(),
                        window.OrderColumn)),
                    TExactType{"Date", true}))
            {
                Unsupported(
                    "q51 ROWS Project input/output types disagree with its exact window definition");
            }

            window.Expression["execution_order"] =
                static_cast<ui64>(index);
            AuditExactScalarExpression(window.Expression);
            projection.push_back({
                .Input = window.Input,
                .PartitionColumn = window.PartitionColumn,
                .OrderColumn = window.OrderColumn,
                .WindowName = window.WindowName,
                .Function = window.Function,
                .SourceOrdinal = window.SourceOrdinal,
                .ExecutionOrder = static_cast<ui32>(index),
            });
            if (!PreparedWindows.emplace(
                    element,
                    std::move(window)).second)
            {
                Unsupported("q51 ROWS expression was prepared twice");
            }
        }
    }

    static void CertifySumProjection(
        TOpMap& map,
        const TQ51ProjectionWindow& window,
        const THashMap<const IOperator*, TVector<IOperator*>>& parents)
    {
        if (window.Function != EQ51WindowFunction::Sum ||
            map.GetInput()->GetKind() != EOperator::Aggregate)
        {
            Unsupported(
                "q51 running SUM Project must directly consume an Aggregate");
        }
        auto& aggregate = static_cast<TOpAggregate&>(*map.GetInput());
        RequireOnlyMainConsumer(
            parents,
            aggregate,
            map,
            "q51 running SUM Aggregate");
        const auto phase = aggregate.GetAggregationPhase();
        const auto keys = aggregate.GetKeyColumns();
        const auto traits = aggregate.GetAggregationTraits();
        if ((phase != EOpPhase::Undefined && phase != EOpPhase::Final) ||
            aggregate.IsDistinctAll() || keys.size() != 2 ||
            keys[0].GetFullName() != window.PartitionColumn ||
            keys[1].GetFullName() != window.OrderColumn ||
            traits.size() != 1)
        {
            Unsupported(
                "q51 running SUM requires one exact two-key logical or final Aggregate");
        }
        const auto& trait = traits.front();
        if (trait.ResultColName.GetFullName() != window.Input ||
            trait.AggFunction != "sum" || trait.Distinct || trait.Unwrap ||
            !SameType(
                ExactType(OutputType(aggregate, window.PartitionColumn)),
                TExactType{"Int64", false}) ||
            !SameType(
                ExactType(OutputType(aggregate, window.OrderColumn)),
                TExactType{"Date", true}) ||
            !SameType(
                ExactType(OutputType(aggregate, window.Input)),
                TExactType{"Decimal(35,2)", true}))
        {
            Unsupported(
                "q51 running SUM must consume the exact direct Aggregate SUM output");
        }

        TOpAggregate* sourceAggregate = &aggregate;
        TString sourceInput = trait.OriginalColName.GetFullName();
        if (phase == EOpPhase::Final) {
            if (aggregate.GetInput()->GetKind() != EOperator::Aggregate) {
                Unsupported(
                    "final q51 running SUM must directly consume its intermediate Aggregate");
            }
            auto& intermediate =
                static_cast<TOpAggregate&>(*aggregate.GetInput());
            RequireOnlyMainConsumer(
                parents,
                intermediate,
                aggregate,
                "q51 running SUM intermediate Aggregate");
            const auto intermediateTraits =
                intermediate.GetAggregationTraits();
            const TString state = trait.OriginalColName.GetFullName();
            const size_t finalUses = std::count_if(
                traits.begin(),
                traits.end(),
                [&](const TOpAggregationTraits& candidate) {
                    return candidate.OriginalColName.GetFullName() == state;
                });
            if (state.empty() ||
                intermediate.GetAggregationPhase() !=
                    EOpPhase::Intermediate ||
                intermediate.IsDistinctAll() ||
                intermediate.GetKeyColumns() != keys ||
                intermediateTraits.size() != 1 || finalUses != 1 ||
                intermediateTraits.front().ResultColName.GetFullName() !=
                    state ||
                intermediateTraits.front().AggFunction != "sum" ||
                intermediateTraits.front().Distinct ||
                intermediateTraits.front().Unwrap ||
                !SameType(
                    ExactType(OutputType(intermediate, state)),
                    TExactType{"Decimal(35,2)", true}) ||
                !SameType(
                    ExactType(OutputType(
                        intermediate,
                        window.PartitionColumn)),
                    TExactType{"Int64", false}) ||
                !SameType(
                    ExactType(OutputType(
                        intermediate,
                        window.OrderColumn)),
                    TExactType{"Date", true}))
            {
                Unsupported(
                    "final q51 running SUM requires one private matching intermediate SUM state");
            }
            sourceAggregate = &intermediate;
            sourceInput =
                intermediateTraits.front().OriginalColName.GetFullName();
        }

        if (sourceInput.empty() ||
            !SameType(
                ExactType(OutputType(
                    *sourceAggregate->GetInput(),
                    sourceInput)),
                TExactType{"Decimal(7,2)", true}) ||
            !SameType(
                ExactType(OutputType(
                    *sourceAggregate->GetInput(),
                    window.PartitionColumn)),
                TExactType{"Int64", false}) ||
            !SameType(
                ExactType(OutputType(
                    *sourceAggregate->GetInput(),
                    window.OrderColumn)),
                TExactType{"Date", true}))
        {
            Unsupported(
                "q51 running SUM source must preserve its exact item/date/Decimal(7,2) inputs");
        }
    }

    void ValidateTopology(bool hasSubplans) {
        if (Projections.empty()) {
            return;
        }
        if (Projections.size() != 3 || hasSubplans) {
            Unsupported(
                "q51 requires exactly three main ROWS-window Projects and no subplans");
        }
        if (HasOtherWindowFamily) {
            Unsupported(
                "q51 ROWS windows may not be mixed with other window families");
        }

        THashSet<ui32> ordinals;
        THashSet<TString> names;
        THashSet<TOpAggregate*> sumAggregates;
        size_t windowCount = 0;
        size_t sumCount = 0;
        size_t maxCount = 0;
        size_t sumProjects = 0;
        size_t maxProjects = 0;
        for (const auto& [project, windows] : Projections) {
            if (!MainNodes.contains(project) || windows.empty() ||
                windows.size() > 2)
            {
                Unsupported(
                    "q51 window must belong to a bounded main-plan Project");
            }
            if (const auto* consumers = Parents.FindPtr(project);
                consumers && consumers->size() > 1)
            {
                Unsupported("q51 ROWS-window Project may not fan out");
            }

            size_t computedElements = 0;
            for (const auto& element : project->MapElements) {
                if (!element.IsColumnAccess()) {
                    ++computedElements;
                    if (!PreparedWindows.contains(&element)) {
                        Unsupported(
                            "q51 ROWS-window Project may compute only its direct window leaves");
                    }
                }
            }
            if (computedElements != windows.size()) {
                Unsupported(
                    "q51 ROWS-window Project computed-leaf count is inconsistent");
            }

            const bool sumProject = windows.size() == 1 &&
                windows.front().Function == EQ51WindowFunction::Sum;
            const bool maxProject = windows.size() == 2 &&
                windows[0].Function == EQ51WindowFunction::Max &&
                windows[1].Function == EQ51WindowFunction::Max;
            if (!sumProject && !maxProject) {
                Unsupported(
                    "q51 requires singleton SUM Projects and one two-MAX Project");
            }

            if (sumProject) {
                const auto& window = windows.front();
                if (window.SourceOrdinal > 1 || window.ExecutionOrder != 0) {
                    Unsupported(
                        "q51 SUM windows 0 and 1 must each have local execution order zero");
                }
                CertifySumProjection(*project, window, Parents);
                auto* aggregate =
                    static_cast<TOpAggregate*>(project->GetInput().Get());
                if (!sumAggregates.insert(aggregate).second) {
                    Unsupported(
                        "q51 SUM branches must use distinct private Aggregates");
                }
                ++sumProjects;
            } else {
                for (size_t index = 0; index < windows.size(); ++index) {
                    if (windows[index].SourceOrdinal != index + 2 ||
                        windows[index].ExecutionOrder != index)
                    {
                        Unsupported(
                            "q51 MAX windows 2 and 3 must have local execution orders zero and one");
                    }
                }
                ++maxProjects;
            }

            for (const auto& window : windows) {
                const bool expectedSum = window.SourceOrdinal <= 1;
                if ((window.Function == EQ51WindowFunction::Sum) !=
                        expectedSum ||
                    !ordinals.insert(window.SourceOrdinal).second ||
                    !names.insert(window.WindowName).second)
                {
                    Unsupported(
                        "q51 window functions, names, and source ordinals must be globally canonical and unique");
                }
                sumCount += window.Function == EQ51WindowFunction::Sum;
                maxCount += window.Function == EQ51WindowFunction::Max;
                ++windowCount;
            }
        }

        if (windowCount != 4 || sumCount != 2 || maxCount != 2 ||
            sumProjects != 2 || maxProjects != 1 ||
            ordinals.size() != 4 || names.size() != 4)
        {
            Unsupported(
                "q51 requires exactly two SUM and two MAX leaves in three Projects");
        }
        for (ui32 ordinal = 0; ordinal <= MaxQ51WindowOrdinal; ++ordinal) {
            const TString name = TStringBuilder()
                << GlobalRankWindowNamePrefix << ordinal;
            if (!ordinals.contains(ordinal) || !names.contains(name)) {
                Unsupported(
                    "q51 requires the exact four canonical anonymous windows");
            }
        }
    }

private:
    THashSet<const IOperator*> MainNodes;
    THashMap<const IOperator*, TVector<IOperator*>> Parents;
    THashMap<const TMapElement*, TQ51Window> PreparedWindows;
    THashMap<TOpMap*, TVector<TQ51ProjectionWindow>> Projections;
    bool HasOtherWindowFamily = false;
};
