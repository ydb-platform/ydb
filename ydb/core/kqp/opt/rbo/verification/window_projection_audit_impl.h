// Included exactly once after TPlanExporter is complete inside
// semantic_snapshot.cpp's anonymous namespace.

    void TPlanExporter::PrepareGlobalRankProjection(
        TOpMap& map, const THashSet<TString>& inputNames)
    {
        TVector<std::pair<const TMapElement*, TGlobalRankWindow>> ranks;
        THashMap<TString, NJson::TJsonValue> definitions;
        for (const auto& element : map.MapElements) {
            const auto& expression = element.GetExpression();
            if (!expression.Node || !expression.Node->IsLambda() ||
                !expression.GetExpressionBody()->IsCallable("YqlWin"))
            {
                continue;
            }
            auto rank = ExportGlobalRankWindow(expression, inputNames, *OutputStructType(*map.GetInput()));
            const auto [previous, inserted] = definitions.emplace(rank.WindowName, rank.Expression);
            if (!inserted && previous->second != rank.Expression) {
                Unsupported("One Rank window name must describe one definition");
            }
            ranks.emplace_back(&element, std::move(rank));
        }
        if (ranks.empty()) {
            return;
        }
        if (ranks.size() > 2) {
            Unsupported("Rank Project exceeds its two-leaf audit bound");
        }
        auto& projection = GlobalRankProjectionWindows[&map];
        for (size_t index = 0; index < ranks.size(); ++index) {
            auto& [element, rank] = ranks[index];
            const TString output = element->GetElementName().GetFullName();
            if (output.empty() || !IsExactDataAnnotation(
                OutputType(map, output), NUdf::EDataSlot::Uint64, false))
            {
                Unsupported("Rank Project output must be exact non-null Uint64");
            }
            rank.Expression["execution_order"] = static_cast<ui64>(index);
            AuditExactScalarExpression(rank.Expression);
            projection.push_back({.Output = output, .WindowName = rank.WindowName,
                .OrderColumns = rank.OrderColumns, .ExecutionOrder = static_cast<ui32>(index)});
            if (!PreparedGlobalRankWindows.emplace(element, std::move(rank)).second) {
                Unsupported("Rank expression was prepared twice");
            }
        }
    }

    void TPlanExporter::CertifyWholePartitionWindowProjection(
        TOpMap& map,
        const TWholePartitionWindow& window)
    {
        const TStringBuf label = WindowLabel(window.Function);
        if (window.Function == EWholePartitionWindowFunction::Sum && ++WholePartitionSumCount > 1) {
            Unsupported("Exactly one whole-partition SUM expression is modeled");
        }
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

    void TPlanExporter::ValidateWindowSubplanSeparation(const IOperator& project, bool allowClosedInRank) const {
        // Ordinary subplans below the window have already contributed to its
        // shared source outcome. A window may not itself invoke one. Rank may
        // belong to one closed leaf IN relation, with the same family semantics.
        THashSet<const IOperator*> main;
        VisitOperators(Root.GetInput(), main, [](IOperator&) {});
        size_t owners = 0;
        for (const auto& subplan : Subplans) {
            if (std::find(subplan.Consumers.begin(), subplan.Consumers.end(), &project) != subplan.Consumers.end()) {
                Unsupported("Window Project must be separate from subplan evaluation");
            }
            THashSet<const IOperator*> visited;
            VisitOperators(subplan.ExportedRoot, visited, [](IOperator&) {});
            if (!visited.contains(&project)) {
                continue;
            }
            if (!allowClosedInRank || main.contains(&project) || ++owners != 1 ||
                !std::holds_alternative<TInSubplanDetails>(subplan.Details) ||
                std::any_of(subplan.Consumers.begin(), subplan.Consumers.end(),
                    [&](const IOperator* consumer) { return !main.contains(consumer); }))
            {
                Unsupported("Window Project must be separate from subplan evaluation");
            }
            for (const auto& nested : Subplans) {
                if (std::any_of(nested.Consumers.begin(), nested.Consumers.end(),
                    [&](const IOperator* consumer) { return visited.contains(consumer); }))
                {
                    Unsupported("Window Project must be separate from subplan evaluation");
                }
            }
        }
        if (!owners && !main.contains(&project)) {
            Unsupported("Window Project must belong to the main result plan or one closed leaf IN relation");
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
        if (markedCount > 3) {
            Unsupported("Whole-partition window exceeds its three-leaf snapshot audit bound");
        }
        THashSet<const IOperator*> mainNodes;
        THashMap<const IOperator*, TVector<IOperator*>> parents;
        VisitOperators(Root.GetInput(), mainNodes, [&](IOperator& op) {
            for (const auto& child : op.GetChildren()) {
                parents[child.Get()].push_back(&op);
            }
        });
        for (const auto& [producer, outputs] : WindowProjectionOutputs) {
            ValidateWindowSubplanSeparation(*producer);
            if (!mainNodes.contains(producer) || outputs.size() != 1 ||
                producer->GetInput()->GetKind() != EOperator::Aggregate)
            {
                Unsupported("Whole-partition window requires exactly one leaf per private main-plan Project");
            }
            auto* aggregate = producer->GetInput().Get();
            const auto* aggregateConsumers = parents.FindPtr(aggregate);
            if (!aggregateConsumers || aggregateConsumers->size() != 1 ||
                aggregateConsumers->front() != producer)
            {
                Unsupported("Whole-partition window Aggregate must have exactly one direct Project consumer");
            }
            auto* aggregateOp = static_cast<TOpAggregate*>(aggregate);
            if (aggregateOp->GetAggregationPhase() == EOpPhase::Final) {
                const auto* intermediate = aggregateOp->GetInput().Get();
                const auto* intermediateConsumers = parents.FindPtr(intermediate);
                if (!intermediateConsumers || intermediateConsumers->size() != 1 ||
                    intermediateConsumers->front() != aggregate)
                {
                    Unsupported("Whole-partition window intermediate Aggregate must have exactly one direct final Aggregate consumer");
                }
            }
            if (const auto* consumers = parents.FindPtr(producer); consumers && consumers->size() > 1) {
                Unsupported("Whole-partition window Project may not fan out");
            }
        }
    }

    void TPlanExporter::ValidateGlobalRankProjectionTopology() {
        if (GlobalRankProjectionWindows.empty()) {
            return;
        }
        THashSet<const IOperator*> reachableNodes;
        THashMap<const IOperator*, TVector<IOperator*>> parents;
        VisitOperators(Root.GetInput(), reachableNodes, [&](IOperator& op) {
            for (const auto& child : op.GetChildren()) {
                parents[child.Get()].push_back(&op);
            }
        });
        for (const auto& subplan : Subplans) {
            VisitOperators(subplan.ExportedRoot, reachableNodes, [&](IOperator& op) {
                for (const auto& child : op.GetChildren()) {
                    parents[child.Get()].push_back(&op);
                }
            });
        }
        size_t count = 0;
        for (const auto& [project, ranks] : GlobalRankProjectionWindows) {
            ValidateWindowSubplanSeparation(*project, true);
            if (!reachableNodes.contains(project) || ranks.empty() || ranks.size() > 2) {
                Unsupported("Rank requires one bounded private Project");
            }
            if (const auto* consumers = parents.FindPtr(project); consumers && consumers->size() > 1) {
                Unsupported("Rank Project may not fan out");
            }
            count += ranks.size();
        }
        if (count > 6) {
            Unsupported("Rank exceeds its six-leaf snapshot audit bound");
        }
    }
