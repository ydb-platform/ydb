// Included exactly once after TPlanExporter is complete inside
// semantic_snapshot.cpp's anonymous namespace.

    void TPlanExporter::PrepareQ51WindowProjection(
        TOpMap& map,
        const THashSet<TString>& inputNames)
    {
        TVector<std::pair<const TMapElement*, TQ51Window>> windows;
        for (const auto& element : map.MapElements) {
            const auto& expression = element.GetExpression();
            if (!expression.Node || !expression.Node->IsLambda()) {
                continue;
            }
            const auto body = expression.GetExpressionBody();
            if (!body->IsCallable("YqlAggWin")) {
                continue;
            }

            // Keep the older whole-partition family on its existing audit
            // path, including malformed factory mutations.  The q51 family
            // is distinguished by a non-avg factory and ordered metadata;
            // ExportQ51Window independently closes the exact order grammar.
            const bool exactAvg = body->ChildrenSize() == 5 &&
                body->Child(0)->IsCallable("YqlWinFactory") &&
                body->Child(0)->ChildrenSize() == 1 &&
                body->Child(0)->Child(0)->IsAtom("avg");
            if (exactAvg) {
                continue;
            }
            const auto& metadata = expression.GetWindowMetadata();
            if (metadata && metadata->Definition &&
                (!metadata->Definition->IsCallable("YqlWindow") ||
                 metadata->Definition->ChildrenSize() != 5 ||
                 !metadata->Definition->Child(3)->IsList() ||
                 metadata->Definition->Child(3)->ChildrenSize() == 0))
            {
                continue;
            }
            windows.emplace_back(
                &element,
                ExportQ51Window(expression, inputNames));
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

        auto& projection = Q51ProjectionWindows[&map];
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
                .Output = output,
                .Input = window.Input,
                .PartitionColumn = window.PartitionColumn,
                .OrderColumn = window.OrderColumn,
                .WindowName = window.WindowName,
                .Function = window.Function,
                .SourceOrdinal = window.SourceOrdinal,
                .ExecutionOrder = static_cast<ui32>(index),
            });
            if (!PreparedQ51Windows.emplace(
                    element,
                    std::move(window)).second)
            {
                Unsupported("q51 ROWS expression was prepared twice");
            }
        }
    }

    void TPlanExporter::CertifyQ51SumProjection(
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

    void TPlanExporter::ValidateQ51WindowProjectionTopology() {
        if (Q51ProjectionWindows.empty()) {
            return;
        }
        if (Q51ProjectionWindows.size() != 3 || !Subplans.empty()) {
            Unsupported(
                "q51 requires exactly three main ROWS-window Projects and no subplans");
        }
        if (!WindowProjectionOutputs.empty() ||
            !GlobalRankProjectionWindows.empty())
        {
            Unsupported(
                "q51 ROWS windows may not be mixed with other window families");
        }

        THashSet<const IOperator*> mainNodes;
        THashMap<const IOperator*, TVector<IOperator*>> parents;
        VisitOperators(
            Root.GetInput(),
            mainNodes,
            [&](IOperator& op) {
                for (const auto& child : op.GetChildren()) {
                    parents[child.Get()].push_back(&op);
                }
            });

        THashSet<ui32> ordinals;
        THashSet<TString> names;
        THashSet<TOpAggregate*> sumAggregates;
        size_t windowCount = 0;
        size_t sumCount = 0;
        size_t maxCount = 0;
        size_t sumProjects = 0;
        size_t maxProjects = 0;
        for (const auto& [project, windows] : Q51ProjectionWindows) {
            if (!mainNodes.contains(project) || windows.empty() ||
                windows.size() > 2)
            {
                Unsupported(
                    "q51 window must belong to a bounded main-plan Project");
            }
            if (const auto* consumers = parents.FindPtr(project);
                consumers && consumers->size() > 1)
            {
                Unsupported("q51 ROWS-window Project may not fan out");
            }

            size_t computedElements = 0;
            for (const auto& element : project->MapElements) {
                if (!element.IsColumnAccess()) {
                    ++computedElements;
                    if (!PreparedQ51Windows.contains(&element)) {
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
                CertifyQ51SumProjection(*project, window, parents);
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

    void TPlanExporter::PrepareGlobalRankProjection(
        TOpMap& map,
        const THashSet<TString>& inputNames)
    {
        TVector<std::pair<const TMapElement*, TGlobalRankWindow>> ranks;
        for (const auto& element : map.MapElements) {
            const auto& expression = element.GetExpression();
            if (!expression.Node || !expression.Node->IsLambda() ||
                !expression.GetExpressionBody()->IsCallable("YqlWin"))
            {
                continue;
            }
            ranks.emplace_back(
                &element,
                ExportGlobalRankWindow(expression, inputNames));
        }
        if (ranks.empty()) {
            return;
        }
        if (ranks.size() != 2) {
            Unsupported(
                "Global rank Project requires exactly two direct rank leaves");
        }
        std::sort(
            ranks.begin(),
            ranks.end(),
            [](const auto& left, const auto& right) {
                return left.second.SourceOrdinal < right.second.SourceOrdinal;
            });
        if (ranks[0].second.SourceOrdinal % 2 != 0 ||
            ranks[1].second.SourceOrdinal !=
                ranks[0].second.SourceOrdinal + 1)
        {
            Unsupported(
                "Global rank Project window names must be one canonical "
                "consecutive even/odd source pair");
        }

        auto& projection = GlobalRankProjectionWindows[&map];
        projection.reserve(ranks.size());
        for (size_t index = 0; index < ranks.size(); ++index) {
            auto& [element, rank] = ranks[index];
            const TString output = element->GetElementName().GetFullName();
            if (output.empty() ||
                !IsExactDataAnnotation(
                    OutputType(map, output),
                    NUdf::EDataSlot::Uint64,
                    false))
            {
                Unsupported(
                    "Global rank Project output must be exact non-null Uint64");
            }
            rank.Expression["execution_order"] =
                static_cast<ui64>(index);
            AuditExactScalarExpression(rank.Expression);
            projection.push_back({
                .Output = output,
                .WindowName = rank.WindowName,
                .OrderColumn = rank.OrderColumn,
                .SourceOrdinal = rank.SourceOrdinal,
                .ExecutionOrder = static_cast<ui32>(index),
            });
            if (!PreparedGlobalRankWindows.emplace(
                    element,
                    std::move(rank)).second)
            {
                Unsupported("Global rank expression was prepared twice");
            }
        }
    }

    void TPlanExporter::CertifyWholePartitionWindowProjection(
        TOpMap& map,
        const TWholePartitionWindow& window)
    {
        const TStringBuf label = WindowLabel(window.Function);
        if (map.GetInput()->GetKind() != EOperator::Aggregate) {
            Unsupported(TStringBuilder()
                << label
                << " Project must directly consume an Aggregate");
        }
        auto& aggregate =
            static_cast<TOpAggregate&>(*map.GetInput());
        if ((aggregate.GetAggregationPhase() != EOpPhase::Undefined &&
             aggregate.GetAggregationPhase() != EOpPhase::Final) ||
            aggregate.IsDistinctAll() ||
            aggregate.GetKeyColumns().empty())
        {
            Unsupported(TStringBuilder()
                << label
                << " requires one grouped logical or final Aggregate");
        }

        const auto& aggregateKeys = aggregate.GetKeyColumns();
        for (const auto& partition : window.PartitionBy) {
            const auto partitionCount = std::count_if(
                aggregateKeys.begin(),
                aggregateKeys.end(),
                [&](const TInfoUnit& key) {
                    return key.GetFullName() == partition.Name;
                });
            if (partitionCount != 1) {
                Unsupported(TStringBuilder()
                    << label
                    << " partition must be one direct Aggregate key");
            }
            if (window.Function == EWholePartitionWindowFunction::Avg &&
                (partition.AggregateKeyIndex >= aggregateKeys.size() ||
                 aggregateKeys[partition.AggregateKeyIndex].GetFullName() !=
                    partition.Name))
            {
                Unsupported(
                    "Window avg partition index/name must match the ordered "
                    "Aggregate keys");
            }

            const auto partitionType =
                ExactType(OutputType(aggregate, partition.Name));
            if (partitionType.Name != partition.Type ||
                !partitionType.Nullable)
            {
                Unsupported(TStringBuilder()
                    << label
                    << " Aggregate partition type disagrees with the "
                       "audited expression");
            }
        }

        const auto traits = aggregate.GetAggregationTraits();
        const auto inputCount = std::count_if(
            traits.begin(),
            traits.end(),
            [&](const TOpAggregationTraits& trait) {
                return trait.ResultColName.GetFullName() == window.Input;
            });
        if (inputCount != 1) {
            Unsupported(TStringBuilder()
                << label
                << " input must be one direct Aggregate output");
        }
        const auto& trait = *std::find_if(
            traits.begin(),
            traits.end(),
            [&](const TOpAggregationTraits& candidate) {
                return candidate.ResultColName.GetFullName() == window.Input;
            });
        if (trait.AggFunction != "sum" || trait.Distinct || trait.Unwrap) {
            Unsupported(TStringBuilder()
                << label
                << " input must be one plain Aggregate sum output");
        }

        if (aggregate.GetAggregationPhase() == EOpPhase::Final) {
            if (aggregate.GetInput()->GetKind() != EOperator::Aggregate) {
                Unsupported(TStringBuilder()
                    << "Final " << label
                    << " Aggregate must directly consume its intermediate "
                       "Aggregate");
            }
            auto& intermediate =
                static_cast<TOpAggregate&>(*aggregate.GetInput());
            if (intermediate.GetAggregationPhase() != EOpPhase::Intermediate ||
                intermediate.IsDistinctAll() ||
                intermediate.GetKeyColumns() != aggregate.GetKeyColumns())
            {
                Unsupported(TStringBuilder()
                    << "Final " << label
                    << " Aggregate must preserve one matching intermediate "
                       "grouped SUM");
            }
            const TString state = trait.OriginalColName.GetFullName();
            const auto intermediateTraits =
                intermediate.GetAggregationTraits();
            const auto sourceCount = std::count_if(
                intermediateTraits.begin(),
                intermediateTraits.end(),
                [&](const TOpAggregationTraits& candidate) {
                    return candidate.ResultColName.GetFullName() == state &&
                        candidate.AggFunction == "sum" &&
                        !candidate.Distinct && !candidate.Unwrap;
                });
            const auto finalUseCount = std::count_if(
                traits.begin(),
                traits.end(),
                [&](const TOpAggregationTraits& candidate) {
                    return candidate.OriginalColName.GetFullName() == state;
                });
            const auto stateType = ExactType(OutputType(intermediate, state));
            if (sourceCount != 1 || finalUseCount != 1 ||
                stateType.Name != "Decimal(35,2)" || !stateType.Nullable)
            {
                Unsupported(TStringBuilder()
                    << "Final " << label
                    << " must consume exactly one matching "
                       "Optional<Decimal(35,2)> intermediate SUM state");
            }
        }

        const auto inputType =
            ExactType(OutputType(aggregate, window.Input));
        if (inputType.Name != "Decimal(35,2)" || !inputType.Nullable)
        {
            Unsupported(TStringBuilder()
                << label
                << " Aggregate input type disagrees with the audited "
                   "expression");
        }
    }

    void TPlanExporter::ValidateWholePartitionWindowProjectionTopology() {
        size_t markedCount = 0;
        for (const auto& [_, outputs] : WindowProjectionOutputs) {
            markedCount += outputs.size();
        }
        if (markedCount == 0) {
            return;
        }
        if (markedCount != 1 || !Subplans.empty()) {
            Unsupported(
                "Whole-partition window requires exactly one main "
                "projection and no subplans");
        }

        THashSet<const IOperator*> mainNodes;
        THashMap<const IOperator*, TVector<IOperator*>> parents;
        VisitOperators(
            Root.GetInput(),
            mainNodes,
            [&](IOperator& op) {
                for (const auto& child : op.GetChildren()) {
                    parents[child.Get()].push_back(&op);
                }
            });

        const auto& [producer, outputs] =
            *WindowProjectionOutputs.begin();
        if (!mainNodes.contains(producer) || outputs.size() != 1 ||
            producer->GetInput()->GetKind() != EOperator::Aggregate)
        {
            Unsupported(
                "Whole-partition window must be a private main-plan Project");
        }
        auto* aggregate = producer->GetInput().Get();
        const auto* aggregateConsumers = parents.FindPtr(aggregate);
        if (!aggregateConsumers || aggregateConsumers->size() != 1 ||
            aggregateConsumers->front() != producer)
        {
            Unsupported(
                "Whole-partition window Aggregate must have exactly one "
                "direct Project consumer");
        }
        auto* aggregateOp = static_cast<TOpAggregate*>(aggregate);
        if (aggregateOp->GetAggregationPhase() == EOpPhase::Final)
        {
            const auto* intermediate = aggregateOp->GetInput().Get();
            const auto* intermediateConsumers = parents.FindPtr(intermediate);
            if (!intermediateConsumers ||
                intermediateConsumers->size() != 1 ||
                intermediateConsumers->front() != aggregate)
            {
                Unsupported(
                    "Whole-partition window intermediate Aggregate must "
                    "have exactly one direct final Aggregate consumer");
            }
        }
        if (const auto* consumers = parents.FindPtr(producer);
            consumers && consumers->size() > 1)
        {
            Unsupported("Whole-partition window Project may not fan out");
        }
    }

    void TPlanExporter::RequireOnlyMainConsumer(
        const THashMap<const IOperator*, TVector<IOperator*>>& parents,
        IOperator& producer,
        IOperator& consumer,
        TStringBuf label)
    {
        const auto* consumers = parents.FindPtr(&producer);
        if (!consumers || consumers->size() != 1 ||
            consumers->front() != &consumer)
        {
            Unsupported(TStringBuilder()
                << label << " must have exactly one audited consumer");
        }
    }

    TPlanExporter::TQ49RatioReference TPlanExporter::TraceGlobalRankOrderToRatio(
        TOpMap& rankProject,
        TString orderColumn,
        const THashMap<const IOperator*, TVector<IOperator*>>& parents)
    {
        IOperator* consumer = &rankProject;
        IOperator* current = rankProject.GetInput().Get();
        for (size_t depth = 0; depth < 32; ++depth) {
            if (!current || current->GetKind() != EOperator::Map) {
                Unsupported(
                    "Global rank order key does not resolve to its ratio Project");
            }
            auto& map = static_cast<TOpMap&>(*current);
            RequireOnlyMainConsumer(
                parents,
                map,
                *consumer,
                "Global rank ratio/rename corridor");

            const auto* element =
                map.FindOutputElement(TInfoUnit(orderColumn));
            if (element && !element->IsColumnAccess()) {
                const auto body = element->GetExpression().GetExpressionBody();
                if (!body->IsCallable("DecimalDiv")) {
                    Unsupported(
                        "Global rank order key must be produced by one direct DecimalDiv");
                }
                return {&map, element};
            }

            for (const auto& candidate : map.MapElements) {
                if (!candidate.IsColumnAccess()) {
                    Unsupported(
                        "Global rank alias corridor may contain only direct columns");
                }
            }

            TString inputColumn = orderColumn;
            if (element) {
                inputColumn = element->GetColumnAccess().GetFullName();
            } else if (!OutputNames(*map.GetInput()).contains(orderColumn)) {
                Unsupported(
                    "Global rank alias corridor loses its order column");
            }
            if (inputColumn.empty() ||
                !SameType(
                    ExactType(OutputType(map, orderColumn)),
                    ExactType(OutputType(*map.GetInput(), inputColumn))))
            {
                Unsupported(
                    "Global rank alias corridor changes its order-column type");
            }
            orderColumn = std::move(inputColumn);
            consumer = &map;
            current = map.GetInput().Get();
        }
        Unsupported("Global rank alias corridor exceeds its audit depth");
    }

    TPlanExporter::TQ49RatioSources TPlanExporter::AuditQ49RatioExpression(
        TOpMap& map,
        const TMapElement& element)
    {
        const auto* rowArgument = AuditWholePartitionWindowLambda(
            element.GetExpression(),
            "Global rank ratio");
        const auto& body = *element.GetExpression().GetExpressionBody();
        CheckExactWindowSafetyTree(body);
        if (!body.IsCallable("DecimalDiv") || body.ChildrenSize() != 2) {
            Unsupported(
                "Global rank key must be one direct DecimalDiv expression");
        }
        const auto signature = CheckDecimalArithmeticCallable(body);
        if (signature.ResultType != "Decimal(15,4)" ||
            signature.ResultNullable)
        {
            Unsupported(
                "Global rank ratio must return non-null Decimal(15,4)");
        }
        const TString output = element.GetElementName().GetFullName();
        if (!SameType(
                ExactType(OutputType(map, output)),
                TExactType{"Decimal(15,4)", false}))
        {
            Unsupported(
                "Global rank ratio Project output type disagrees with its expression");
        }

        TQ49RatioSources result;
        for (size_t index = 0; index < result.Columns.size(); ++index) {
            const auto& castNode = *body.Child(index);
            const auto cast = CheckExactDecimalSafeCastCallable(castNode);
            if (cast.ResultType != "Decimal(15,4)" || cast.Nullable ||
                (cast.SourceType != "Int64" &&
                 cast.SourceType != "Decimal(35,2)"))
            {
                Unsupported(
                    "Global rank ratio requires exact non-null Int64 or "
                    "Decimal(35,2) casts to Decimal(15,4)");
            }
            if (index == 0) {
                result.Type = cast.SourceType;
            } else if (result.Type != cast.SourceType) {
                Unsupported(
                    "Global rank ratio numerator and denominator source families disagree");
            }

            const auto& member = *castNode.Child(0);
            bool nullable = false;
            if (!member.IsCallable("Member") || member.ChildrenSize() != 2 ||
                member.Child(0) != rowArgument ||
                !member.Child(1)->IsAtom() ||
                member.Child(1)->Content().empty() ||
                ScalarTypeName(member, &nullable) != cast.SourceType ||
                nullable)
            {
                Unsupported(
                    "Global rank ratio cast source must be one direct non-null Member");
            }
            const TString column(member.Child(1)->Content());
            if (!OutputNames(*map.GetInput()).contains(column) ||
                !SameType(
                    ExactType(OutputType(*map.GetInput(), column)),
                    TExactType{cast.SourceType, false}))
            {
                Unsupported(
                    "Global rank ratio cast source is unavailable or has the wrong type");
            }
            result.Columns[index] = column;
        }
        if (result.Columns[0] == result.Columns[1]) {
            Unsupported(
                "Global rank ratio must use distinct numerator and denominator sums");
        }
        return result;
    }

    void TPlanExporter::AuditQ49Aggregate(
        TOpAggregate& aggregate,
        const THashMap<TString, TString>& expectedOutputs,
        const THashMap<const IOperator*, TVector<IOperator*>>& parents,
        TOpMap& ratioProject)
    {
        RequireOnlyMainConsumer(
            parents,
            aggregate,
            ratioProject,
            "Global rank Aggregate");
        const auto phase = aggregate.GetAggregationPhase();
        const auto& keys = aggregate.GetKeyColumns();
        const auto traits = aggregate.GetAggregationTraits();
        if ((phase != EOpPhase::Undefined && phase != EOpPhase::Final) ||
            aggregate.IsDistinctAll() || keys.size() != 1 ||
            traits.size() != 4 || expectedOutputs.size() != 4)
        {
            Unsupported(
                "Global rank requires one four-SUM grouped logical or final Aggregate");
        }
        const TString key = keys.front().GetFullName();
        if (key.empty() ||
            !SameType(
                ExactType(OutputType(aggregate, key)),
                TExactType{"Int64", false}))
        {
            Unsupported(
                "Global rank Aggregate requires one non-null Int64 key");
        }

        THashSet<TString> traitOutputs;
        for (const auto& trait : traits) {
            const TString output = trait.ResultColName.GetFullName();
            const auto* expectedType = expectedOutputs.FindPtr(output);
            if (!expectedType || !traitOutputs.insert(output).second ||
                trait.AggFunction != "sum" || trait.Distinct || trait.Unwrap ||
                !SameType(
                    ExactType(OutputType(aggregate, output)),
                    TExactType{*expectedType, false}))
            {
                Unsupported(
                    "Global rank Aggregate outputs must be the four distinct plain sums");
            }
        }

        TOpAggregate* sourceAggregate = &aggregate;
        THashMap<TString, TString> finalStates;
        if (phase == EOpPhase::Final) {
            if (aggregate.GetInput()->GetKind() != EOperator::Aggregate) {
                Unsupported(
                    "Final global rank Aggregate must directly consume its intermediate Aggregate");
            }
            auto& intermediate =
                static_cast<TOpAggregate&>(*aggregate.GetInput());
            RequireOnlyMainConsumer(
                parents,
                intermediate,
                aggregate,
                "Global rank intermediate Aggregate");
            const auto intermediateTraits =
                intermediate.GetAggregationTraits();
            if (intermediate.GetAggregationPhase() != EOpPhase::Intermediate ||
                intermediate.IsDistinctAll() ||
                intermediate.GetKeyColumns() != keys ||
                intermediateTraits.size() != 4)
            {
                Unsupported(
                    "Global rank final Aggregate requires one matching four-SUM intermediate");
            }
            for (const auto& trait : traits) {
                const TString output = trait.ResultColName.GetFullName();
                const TString state = trait.OriginalColName.GetFullName();
                if (state.empty() || !finalStates.emplace(state, output).second) {
                    Unsupported(
                        "Global rank final Aggregate SUM states must be unique");
                }
            }
            for (const auto& trait : intermediateTraits) {
                const TString state = trait.ResultColName.GetFullName();
                const auto* finalOutput = finalStates.FindPtr(state);
                const auto* expectedType = finalOutput
                    ? expectedOutputs.FindPtr(*finalOutput)
                    : nullptr;
                if (!expectedType || trait.AggFunction != "sum" ||
                    trait.Distinct || trait.Unwrap ||
                    !SameType(
                        ExactType(OutputType(intermediate, state)),
                        TExactType{*expectedType, false}))
                {
                    Unsupported(
                        "Global rank intermediate Aggregate states must match the four final sums");
                }
            }
            sourceAggregate = &intermediate;
        }

        const auto sourceTraits = sourceAggregate->GetAggregationTraits();
        THashSet<TString> sourceInputs;
        for (const auto& trait : sourceTraits) {
            const TString stateOrOutput = trait.ResultColName.GetFullName();
            const TString* finalOutput = phase == EOpPhase::Final
                ? finalStates.FindPtr(stateOrOutput)
                : &stateOrOutput;
            const auto* expectedType = finalOutput
                ? expectedOutputs.FindPtr(*finalOutput)
                : nullptr;
            const TString input = trait.OriginalColName.GetFullName();
            const TString expectedInputType = expectedType &&
                    *expectedType == "Int64"
                ? TString("Int64")
                : TString("Decimal(7,2)");
            if (!expectedType || input.empty() ||
                !sourceInputs.insert(input).second ||
                !SameType(
                    ExactType(OutputType(*sourceAggregate->GetInput(), input)),
                    TExactType{expectedInputType, false}))
            {
                Unsupported(
                    "Global rank Aggregate inputs must be two non-null Int64 "
                    "and two non-null Decimal(7,2) values");
            }
        }
        if (!SameType(
                ExactType(OutputType(*sourceAggregate->GetInput(), key)),
                TExactType{"Int64", false}))
        {
            Unsupported(
                "Global rank Aggregate input key must remain non-null Int64");
        }
    }

    void TPlanExporter::ValidateGlobalRankProjectionTopology() {
        if (GlobalRankProjectionWindows.empty()) {
            return;
        }
        if (GlobalRankProjectionWindows.size() != 3 || !Subplans.empty()) {
            Unsupported(
                "Global rank requires exactly three main q49 Projects and no subplans");
        }

        THashSet<const IOperator*> mainNodes;
        THashMap<const IOperator*, TVector<IOperator*>> parents;
        VisitOperators(
            Root.GetInput(),
            mainNodes,
            [&](IOperator& op) {
                for (const auto& child : op.GetChildren()) {
                    parents[child.Get()].push_back(&op);
                }
            });

        THashSet<ui32> ordinals;
        THashSet<TString> names;
        THashSet<TOpMap*> ratioProjects;
        THashSet<TOpAggregate*> aggregates;
        size_t rankCount = 0;
        for (const auto& [rankProject, ranks] :
             GlobalRankProjectionWindows)
        {
            if (!mainNodes.contains(rankProject) || ranks.size() != 2) {
                Unsupported(
                    "Global rank Project must be a two-leaf main-plan Project");
            }
            if (const auto* consumers = parents.FindPtr(rankProject);
                consumers && consumers->size() > 1)
            {
                Unsupported("Global rank Project may not fan out");
            }
            size_t computedElements = 0;
            for (const auto& element : rankProject->MapElements) {
                if (!element.IsColumnAccess()) {
                    ++computedElements;
                    if (!PreparedGlobalRankWindows.contains(&element)) {
                        Unsupported(
                            "Global rank Project may compute only its two direct ranks");
                    }
                }
            }
            if (computedElements != 2) {
                Unsupported(
                    "Global rank Project must compute exactly two direct ranks");
            }

            std::array<TQ49RatioReference, 2> references;
            for (const auto& rank : ranks) {
                if (!ordinals.insert(rank.SourceOrdinal).second ||
                    !names.insert(rank.WindowName).second ||
                    rank.ExecutionOrder > 1)
                {
                    Unsupported(
                        "Global rank names, source ordinals, and execution order must be unique");
                }
                references[rank.ExecutionOrder] =
                    TraceGlobalRankOrderToRatio(
                        *rankProject,
                        rank.OrderColumn,
                        parents);
                ++rankCount;
            }
            if (!references[0].Project || !references[1].Project ||
                references[0].Project != references[1].Project ||
                references[0].Element == references[1].Element)
            {
                Unsupported(
                    "Global rank pair must consume two distinct ratios from one Project");
            }
            auto& ratioProject = *references[0].Project;
            size_t ratioCount = 0;
            for (const auto& element : ratioProject.MapElements) {
                if (!element.IsColumnAccess()) {
                    ++ratioCount;
                    if (&element != references[0].Element &&
                        &element != references[1].Element)
                    {
                        Unsupported(
                            "Global rank ratio Project may compute only its two ratios");
                    }
                }
            }
            if (ratioCount != 2 ||
                ratioProject.GetInput()->GetKind() != EOperator::Aggregate ||
                !ratioProjects.insert(&ratioProject).second)
            {
                Unsupported(
                    "Global rank pair requires one private two-ratio Project over Aggregate");
            }

            const auto first = AuditQ49RatioExpression(
                ratioProject,
                *references[0].Element);
            const auto second = AuditQ49RatioExpression(
                ratioProject,
                *references[1].Element);
            if (first.Type != "Int64" ||
                second.Type != "Decimal(35,2)")
            {
                Unsupported(
                    "Global rank source order must be return ratio then currency ratio");
            }
            THashMap<TString, TString> expectedOutputs;
            for (const auto& source : first.Columns) {
                expectedOutputs.emplace(source, first.Type);
            }
            for (const auto& source : second.Columns) {
                expectedOutputs.emplace(source, second.Type);
            }
            if (expectedOutputs.size() != 4) {
                Unsupported(
                    "Global rank ratios must consume four distinct Aggregate sums");
            }

            auto& aggregate =
                static_cast<TOpAggregate&>(*ratioProject.GetInput());
            if (!aggregates.insert(&aggregate).second) {
                Unsupported(
                    "Global rank branches must use distinct grouped Aggregates");
            }
            AuditQ49Aggregate(
                aggregate,
                expectedOutputs,
                parents,
                ratioProject);
        }

        if (rankCount != 6 || names.size() != 6 || ordinals.size() != 6) {
            Unsupported("Global rank requires the exact six q49 definitions");
        }
        for (ui32 ordinal = 0;
             ordinal <= MaxGlobalRankWindowOrdinal;
             ++ordinal)
        {
            if (!ordinals.contains(ordinal) ||
                !names.contains(TStringBuilder()
                    << GlobalRankWindowNamePrefix << ordinal))
            {
                Unsupported(
                    "Global rank definitions must be the exact canonical q49 set");
            }
        }
    }
