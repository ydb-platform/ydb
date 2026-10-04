#include "kqp_opt_peephole_rules.h"

#include <ydb/core/kqp/common/kqp_yql.h>
#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

#include <yql/essentials/ast/yql_constraint.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_type_helpers.h>
#include <yql/essentials/core/sql_types/block.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>
#include <yql/essentials/utils/log/log_level.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/fwd.h>
#include <util/generic/hash_set.h>
#include <util/digest/multi.h>
#include <util/string/builder.h>
#include <util/system/types.h>

#include <functional>
#include <string_view>
#include <unordered_set>
#include <utility>

namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

class TStreamingFlowBuilder {
    //// Common optimization helpers

    // Represents one output port for stream callable / argument
    // There may be multiple ports for one callable in case of variants stream type annotation
    struct TSource {
        const TExprNode* Node = nullptr;
        ui32 Output = 0;

        bool operator==(const TSource&) const = default;

        bool IsAggregation() const {
            return Node && TKqpStreamingAggregation::Match(Node);
        }

        struct THash {
            size_t operator()(const TSource& source) const {
                return MultiHash(source.Node, source.Output);
            }
        };
    };

    // Represents one column of single output port of callable / argument
    struct TColumnOrigin {
        TSource Source;
        // Source column name or wide index, if empty - whole source value
        TStringBuf Column;

        bool operator==(const TColumnOrigin&) const = default;

        struct THash {
            size_t operator()(const TColumnOrigin& origin) const {
                return MultiHash(TSource::THash{}(origin.Source), origin.Column);
            }
        };

        // It is reasonable to track multiple origins (e.g. if we handle IF(condition, row.a, row.b), they may squash further)
        using TSet = THashSet<TColumnOrigin, THash>;
    };

    // Mapping: (current output port column name or whole output if key empty -> set of possible sources with column names)
    class TColumnOriginMapping : public THashMap<TStringBuf, TColumnOrigin::TSet> {
    public:
        using THashMap::THashMap;

        // Create initial iid mapping
        static TColumnOriginMapping Initial(const TSource& source, const TTypeAnnotationNode* type, TExprContext& ctx) {
            TColumnOriginMapping columnMapping;
            type = RemoveOptionalType(type);

            if (type->GetKind() == ETypeAnnotationKind::Struct) {
                const auto* structType = type->Cast<TStructExprType>();
                columnMapping.reserve(structType->GetSize());
                for (const auto* item : structType->GetItems()) {
                    const auto& name = item->GetName();
                    columnMapping[name].insert({source, name});
                }
            } else if (type->GetKind() == ETypeAnnotationKind::Multi) {
                const auto multiSize = type->Cast<TMultiExprType>()->GetSize();
                columnMapping.reserve(multiSize);
                for (size_t i = 0; i < multiSize; ++i) {
                    const auto& name = ctx.GetIndexAsString(i);
                    columnMapping[name].insert({source, name});
                }
            } else {
                // Whole output value represent whole source
                columnMapping[TStringBuf()].insert({source, TStringBuf()});
            }

            return columnMapping;
        }

        TColumnOriginMapping SelectColumn(const TStringBuf& name) const {
            const auto it = find(name);
            return it == end() ? TColumnOriginMapping{} : TColumnOriginMapping{{TStringBuf(), it->second}};
        }

        void CommonColumns(const TColumnOriginMapping& other) {
            EraseNodesIf(*this, [&other](auto& item) {
                const auto it = other.find(item.first);
                if (it == other.end()) {
                    return true;
                }

                item.second.insert(it->second.begin(), it->second.end());
                return false;
            });
        }
    };

    // Operator / lambda static streaming input -> output dependency (without streams union tracking)
    struct TOperatorSummary {
        // One operator / lambda output variant with corresponding source of it (if source is unique)
        struct TOutput { 
            TSource Source;
            TColumnOriginMapping ColumnMapping;
        };

        using TOutputs = TVector<TOutput>;

        // Deferred error builder for rules that can not be propagated through some input variants
        struct TInputRejection {
            using TCallback = std::function<void()>;

            TCallback Report;
            const TExprNode* DiscardingLambda = nullptr; // In case when current summary corresponding to lambda
        };

        // Description of one operator / lambda invocation, map: (operator / lambda input variant -> output of one of caller operators)
        class TBindings : public THashMap<TSource, TOutput, TSource::THash> {
        public:
            using THashMap::THashMap;

            // Build lambda invocation over streaming row
            static TBindings BindLambdaOverRow(const TExprNode& lambda, const TColumnOriginMapping& columnMapping, const bool wide, TExprContext& ctx) {
                TBindings bindings;

                for (size_t i = 0; i < lambda.Head().ChildrenSize(); ++i) {
                    bindings.emplace(
                        TSource{lambda.Head().Child(i)},
                        TOutput{.Source = {}, .ColumnMapping = wide ? columnMapping.SelectColumn(ctx.GetIndexAsString(i)) : columnMapping}
                    );
                }

                return bindings;
            }

            TSource BindSource(const TSource& source) const {
                const auto it = find(source);
                return it == end() ? source : it->second.Source;
            }

            // Apply operator columns mapping to its invocation context defined by this bindings
            TColumnOriginMapping BindOperatorColumns(const TColumnOriginMapping& columnMapping) const {
                TColumnOriginMapping result;

                for (const auto& [name, origins] : columnMapping) {
                    TColumnOrigin::TSet bound;
                    bool known = true;

                    for (const auto& origin : origins) {
                        const auto input = find(origin.Source);
                        if (input == end()) {
                            bound.insert(origin);
                        } else if (const auto column = input->second.ColumnMapping.find(origin.Column); column != input->second.ColumnMapping.end()) {
                            bound.insert(column->second.begin(), column->second.end());
                        } else {
                            known = false;
                            break;
                        }
                    }

                    if (known) {
                        result.emplace(name, std::move(bound));
                    }
                }

                return result;
            }
        };

        TOutputs Outputs;
        // List of inputs that cannot accept streaming aggregation
        THashMap<TSource, TInputRejection, TSource::THash> InputsRejectingAggregation;

        void RejectAggregationInput(const TSource& source, TOperatorSummary::TInputRejection::TCallback aggregationError) {
            if (source.Node && aggregationError) {
                InputsRejectingAggregation.try_emplace(source, TOperatorSummary::TInputRejection{std::move(aggregationError), nullptr});
            }
        }

        void MergeAggregationInputRejections(const TOperatorSummary& from) {
            for (const auto& [source, aggregationRejection] : from.InputsRejectingAggregation) {
                InputsRejectingAggregation.try_emplace(source, aggregationRejection);
            }
        }
    };

    // Information about one variant output branch
    struct TVariantOutputInfo {
        // Does this variant branch may be produced
        bool Seen = false;
        // Is this variant branch guaranteed to produce at least one output row for each input row
        bool NonEmpty = false;
        // Mapping of preserved columns for this variant output relative to input type
        TColumnOriginMapping ColumnMapping;
    };

    class TVariantOutputs : public TVector<TVariantOutputInfo> {
    public:
        using TVector::TVector;

        // Merge output variants info based on policy:
        // alternative=true - only one of values *this / other comes to result
        // alternative=false - both values comes to result
        void MergeVariantColumns(const TVariantOutputs& other, const bool alternative) {
            Y_VALIDATE(size() == other.size(), "Variant output count mismatch");

            for (ui32 i = 0; i < size(); ++i) {
                auto& output = at(i);
                const auto& next = other[i];

                output.NonEmpty = alternative ? output.NonEmpty && next.NonEmpty : output.NonEmpty || next.NonEmpty;

                if (next.Seen) {
                    if (std::exchange(output.Seen, true)) {
                        output.ColumnMapping.CommonColumns(next.ColumnMapping);
                    } else {
                        output.ColumnMapping = next.ColumnMapping;
                    }
                }
            }
        }
    };

    //// DQ stages graph representation

    class TStagesGraph {
    public:
        struct TTableWrite {
            TKqpTableSinkSettings Settings;
            ui32 OutputIndex = 0;
        };

        struct TAggregationBinding {
            TKqpStreamingAggregation Node;
            // The origin stage distinguishes instances of a shared aggregation program.
            TDqPhyStage Stage;
            // Each candidate is: (table path, aggregation columns mapping to table columns)
            TVector<TExprNode::TPtr> StateTableCandidates;
            TExprNode::TPtr OutputStateTable;
        };

        struct TStageInput {
            // Connection or source input
            const TExprNode* Node = nullptr;
            // Connection source stage
            TMaybeNode<TDqPhyStage> Producer;
            // Connection source stage output index
            ui32 OutputIndex = 0;
        };

        struct TStageOutput {
            bool HasAggregation = false;
            // Mapping of output columns to origin aggregation columns (may be on another stage)
            THashMap<TStringBuf, TStringBuf> AggregationColumnMapping;
        };

        struct TStageInfo {
            TVector<TStageInput> Inputs;
            TVector<TStageOutput> Outputs;
            TVector<TTableWrite> TableWrites;
            std::optional<TAggregationBinding> Aggregation;
        };

        void Build(const TKqpPhysicalTx& tx) {
            const auto stagesCount = tx.Stages().Size();
            Stages.reserve(stagesCount);
            Order.reserve(stagesCount);
            VisitExpr(tx.Ptr(), [](const TExprNode::TPtr& node) { return !node->IsLambda(); }, [&](const TExprNode::TPtr& node) {
                if (const auto maybeEffect = TMaybeNode<TKqlTableEffect>(node)) {
                    ++TableWritesCount[maybeEffect.Cast().Table().Path().Value()];
                }

                const auto maybeStage = TMaybeNode<TDqPhyStage>(node);
                if (!maybeStage) {
                    return true;
                }

                const auto stage = maybeStage.Cast();
                auto& info = Stages[node.Get()];
                info.Outputs.resize(OutputCount(*node));

                const auto inputs = stage.Inputs();
                info.Inputs.reserve(inputs.Size());
                for (const auto& input : inputs) {
                    info.Inputs.emplace_back(MakeInput(input.Ref()));
                }

                if (const auto maybeOutputs = stage.Outputs()) {
                    for (const auto& output : maybeOutputs.Cast()) {
                        const auto maybeSink = output.Maybe<TDqSink>();
                        const auto maybeTransform = output.Maybe<TDqTransform>();
                        const auto maybeSettings = maybeSink ? maybeSink.Cast().Settings().Maybe<TKqpTableSinkSettings>()
                            : maybeTransform ? maybeTransform.Cast().Settings().Maybe<TKqpTableSinkSettings>() : TMaybeNode<TKqpTableSinkSettings>();
                        if (maybeSettings) {
                            const auto settings = maybeSettings.Cast();
                            info.TableWrites.emplace_back(settings, FromString<ui32>(output.Index().Value()));
                            ++TableWritesCount[settings.Table().Path().Value()];
                        }
                    }
                }

                Order.emplace_back(stage);
                return true;
            });
        }

        const TVector<TDqPhyStage>& GetStageOrder() const {
            return Order;
        }

        TStageInfo& GetStage(const TDqPhyStage& stage) {
            return Stages.at(stage.Raw());
        }

        const TStageInfo& GetStage(const TDqPhyStage& stage) const {
            return Stages.at(stage.Raw());
        }

        std::optional<TAggregationBinding>& GetAggregationBinding(const TDqPhyStage& stage) {
            return GetStage(stage).Aggregation;
        }

        ui32 GetTableWriteCount(const TStringBuf& path) const {
            return TableWritesCount.at(path);
        }

        // Returns original DQ stage that contains streaming aggregation from which this input depends
        TMaybeNode<TDqPhyStage> GetInputAggregationStage(const TStageInput& input) const {
            if (!input.Producer) {
                return {};
            }

            const auto& producer = GetStage(input.Producer.Cast());
            return producer.Aggregation && producer.Outputs.at(input.OutputIndex).HasAggregation ? producer.Aggregation->Stage : TMaybeNode<TDqPhyStage>{};
        }

        // Returns column mapping for this input to corresponding streaming aggregation columns
        TOperatorSummary::TOutputs ResolveInput(const TStageInput& input) const {
            const auto& maybeAggregationStage = GetInputAggregationStage(input);
            if (!maybeAggregationStage) {
                return TOperatorSummary::TOutputs(input.Producer ? 1 : OutputCount(*input.Node));
            }

            const auto& aggregationStage = GetStage(maybeAggregationStage.Cast());
            Y_VALIDATE(aggregationStage.Aggregation, "Aggregation stage must have aggregation");
            TOperatorSummary::TOutput output{.Source = {aggregationStage.Aggregation->Node.Raw()}, .ColumnMapping = {}};

            const auto& columnMapping = GetStage(input.Producer.Cast()).Outputs.at(input.OutputIndex).AggregationColumnMapping;
            output.ColumnMapping.reserve(columnMapping.size());
            for (const auto& [column, origin] : columnMapping) {
                output.ColumnMapping[column].insert({output.Source, origin});
            }

            return {std::move(output)};
        }

        TOperatorSummary::TOutputs ResolveInput(const TExprNode& node) const {
            return ResolveInput(MakeInput(node));
        }

        void SetOutputs(const TDqPhyStage& stage, const TOperatorSummary::TOutputs& outputs) {
            auto& info = GetStage(stage);
            info.Outputs.clear();

            info.Outputs.reserve(outputs.size());
            for (const auto& output : outputs) {
                auto& result = info.Outputs.emplace_back();
                result.HasAggregation = output.Source.IsAggregation();
                if (!result.HasAggregation) {
                    continue;
                }

                for (const auto& [column, origins] : output.ColumnMapping) {
                    if (origins.size() == 1 && origins.begin()->Source == output.Source) {
                        result.AggregationColumnMapping.emplace(column, origins.begin()->Column);
                    }
                }
            }
        }

    private:
        static TStageInput MakeInput(const TExprNode& node) {
            const auto maybeConnection = TMaybeNode<TDqConnection>(&node);
            const auto maybeOutput = maybeConnection ? maybeConnection.Cast().Output().Maybe<TDqOutput>() : TMaybeNode<TDqOutput>(&node);
            if (!maybeOutput) {
                return {.Node = &node};
            }

            const auto output = maybeOutput.Cast();
            return {
                .Node = &node,
                .Producer = TMaybeNode<TDqPhyStage>(output.Stage().Raw()),
                .OutputIndex = FromString<ui32>(output.Index().Value())
            };
        }

        TNodeMap<TStageInfo> Stages;
        TVector<TDqPhyStage> Order;
        THashMap<TStringBuf, ui32> TableWritesCount;
    };

    // Per node information independent from lambda / callable usage context
    struct TNodeInfo {
        bool IsStreaming = false;
        bool IsResultBinding = false;
        TMaybeNode<TKqpStreamingAggregation> Aggregation;
        bool MultipleAggregations = false;
        ui32 OutputCount = 1;
    };

    // Callables which passes streaming constraints but incompatible with checkpoints
    inline static const std::unordered_set<std::string_view> UnsupportedCheckpointsCallables = {
        TCoSkip::CallableName(), TCoTake::CallableName(), TCoLimit::CallableName(),
        "TakeWhile"sv, "SkipWhile"sv, "TakeWhileInclusive"sv, "SkipWhileInclusive"sv,
        "WideTakeWhile"sv, "WideSkipWhile"sv, "WideTakeWhileInclusive"sv, "WideSkipWhileInclusive"sv,
        TCoPruneAdjacentKeys::CallableName(), TCoPruneKeys::CallableName(),
        TCoMapNext::CallableName(), TCoChain1Map::CallableName(), "WideChain1Map"sv
    };

    // Callables for which will be unconditionally allocated checkpoint storage slot
    inline static const std::unordered_set<std::string_view> CheckpointCallables = {
        TCoMultiHoppingCore::CallableName(), "TimeOrderRecover"sv, "MatchRecognizeCore"sv
    };

public:
    TStreamingFlowBuilder(const TStringBuf& cluster, const TKikimrConfiguration& config, const TKikimrTablesData& tables, TExprContext& ctx)
        : Cluster(cluster)
        , Tables(tables)
        , Config(config)
        , CollectColumnMappings(!Config.UseInMemoryStreamingAggregation.Get().GetOrElse(false))
        , Ctx(ctx)
    {}

    // Fast single pass check to find streaming nodes and general constraints validations of found one
    bool ValidateStreamingConstraints(const ui64 txIdx, const TKqpPhysicalTx& tx, THashSet<std::pair<ui64, ui64>>& streamingTxResults) {
        // Validate that streaming result bindings are not materializing into tx precomputes
        for (const auto& binding : tx.ParamBindings()) {
            if (const auto maybeTxBinding = binding.Binding().Maybe<TKqpTxResultBinding>()) {
                const auto txBinding = maybeTxBinding.Cast();
                if (streamingTxResults.contains(std::make_pair(FromString<ui64>(txBinding.TxIndex().Value()), FromString<ui64>(txBinding.ResultIndex().Value())))) {
                    Ctx.AddError(TIssue(Ctx.GetPosition(binding.Pos()), TStringBuilder() << "Streaming result binding " << binding.Name().Value() << " is materializing into tx precompute for transaction " << txIdx));
                    return false;
                }
            }
        }

        HasStreamingNodes = ExprHasStreamingNodes(tx.Ref());
        if (!HasStreamingNodes) {
            return true;
        }

        const auto& results = tx.Results();
        if (!ValidateStreamingConstraintsImpl(tx.Stages().Ref()) || !ValidateStreamingConstraintsImpl(results.Ref())) {
            return false;
        }

        for (size_t i = 0; i < results.Size(); ++i) {
            const auto it = NodesInfo.find(results.Item(i).Raw());
            Y_VALIDATE(it != NodesInfo.end(), "Result " << i << " of tx " << txIdx << " is not visited during streaming constraints validation");

            auto& info = it->second;
            info.IsResultBinding = true;
            if (info.IsStreaming) {
                streamingTxResults.emplace(txIdx, i);
            }
        }

        return true;
    }

    bool ValidateCheckpointsUsage() const {
        if (!Config.OptValidateStreamingCheckpoints.Get().GetOrElse(true) || Config.DisableCheckpoints.Get().GetOrElse(false)) {
            return true;
        }

        Y_VALIDATE(!NodesInfo.empty(), "NodesInfo is empty");

        for (const auto& [node, info] : NodesInfo) {
            if (!node->IsCallable()) {
                continue;
            }

            const auto& name = node->Content();

            if (!node->GetConstraint<TStreamingConstraintNode>()) {
                // Sanity check, that all checkpointed callables are used in streaming context
                if (CheckpointCallables.contains(name)) {
                    YQL_CLOG(WARN, ProviderKqp) << "Found checkpointed callable in non streaming context: " << KqpExprToPrettyString(*node, Ctx);
                    Ctx.AddError(TIssue(Ctx.GetPosition(node->Pos()), TStringBuilder() << "Callable with checkpoints: '" << name << "' cannot be used outside streaming context"));
                    return false;
                }

                continue;
            }

            if (!UnsupportedCheckpointsCallables.contains(name)) {
                continue;
            }

            if (TCoTake::Match(node) || TCoLimit::Match(node)) {
                Ctx.AddError(TIssue(Ctx.GetPosition(node->Pos()), "Checkpoints are not supported for LIMIT operator, query may produce unstable results"));
            } else if (TCoSkip::Match(node)) {
                Ctx.AddError(TIssue(Ctx.GetPosition(node->Pos()), "Checkpoints are not supported for OFFSET operator, query may produce unstable results"));
            } else {
                Ctx.AddError(TIssue(Ctx.GetPosition(node->Pos()), TStringBuilder() << "Unsupported callable for streaming processing with checkpoints: '" << name << "'"));
            }

            YQL_CLOG(WARN, ProviderKqp) << "Found streaming processing node incompatible with checkpoints: " << KqpExprToPrettyString(*node, Ctx);
            return false;
        }

        return true;
    }

    bool ValidateStreamingAggregation(const TKqpPhysicalTx& tx) {
        Y_VALIDATE(!NodesInfo.empty(), "NodesInfo is empty");
        Graph.Build(tx);

        // Fill graph aggregation infos and propagate column renames

        for (const auto& stage : Graph.GetStageOrder()) {
            auto& info = Graph.GetStage(stage);
            const auto& program = stage.Program().Ref();
            const auto& programInfo = NodesInfo.at(&program);
            const auto conflictingOrigins = [this, stage] {
                NodeError(stage.Ref(), "A physical stage may process results from at most one streaming aggregation origin")();
                return false;
            };
            if (programInfo.MultipleAggregations) {
                return conflictingOrigins();
            }

            if (const auto& maybeAggregation = programInfo.Aggregation) {
                info.Aggregation.emplace(TStagesGraph::TAggregationBinding{
                    .Node = maybeAggregation.Cast(),
                    .Stage = stage,
                });
            }

            Y_VALIDATE(info.Inputs.size() == program.Head().ChildrenSize(), "Stage input/argument count mismatch");
            TOperatorSummary::TBindings bindings;

            // Find treaming aggregation in input stages
            for (ui32 i = 0; i < info.Inputs.size(); ++i) {
                if (const auto maybeOrigin = Graph.GetInputAggregationStage(info.Inputs[i])) {
                    const auto origin = maybeOrigin.Cast();
                    if (info.Aggregation && info.Aggregation->Stage.Raw() != origin.Raw()) {
                        return conflictingOrigins();
                    }

                    if (!info.Aggregation) {
                        info.Aggregation.emplace(TStagesGraph::TAggregationBinding{
                            .Node = Graph.GetAggregationBinding(origin).value().Node,
                            .Stage = origin,
                        });
                    }
                }

                auto outputs = Graph.ResolveInput(info.Inputs[i]);
                if (!ValidateAggregationDqConnection(*info.Inputs[i].Node, outputs)) {
                    return false;
                }

                for (ui32 port = 0; port < outputs.size(); ++port) {
                    bindings.emplace(TSource{program.Head().Child(i), port}, std::move(outputs[port]));
                }
            }

            if (!info.Aggregation) {
                continue;
            }

            const auto summary = ApplyLambda(stage.Ref(), program, bindings);
            for (const auto& [source, aggregationRejection] : summary.InputsRejectingAggregation) {
                if (source.IsAggregation()) {
                    Y_VALIDATE(aggregationRejection.Report, "Unbound aggregation validation error");
                    aggregationRejection.Report();
                    return false;
                }
            }

            Graph.SetOutputs(stage, summary.Outputs);

            if (const auto maybeOutputs = stage.Outputs()) {
                for (const auto& output : maybeOutputs.Cast()) {
                    if (info.Outputs.at(FromString<ui32>(output.Index().Value())).HasAggregation && !ValidateAggregationTableOutput(stage, output)) {
                        return false;
                    }
                }
            }
        }

        // Check that aggregation is not written into result / precompute

        for (const auto& result : tx.Results()) {
            auto outputs = Graph.ResolveInput(result.Ref());
            if (!ValidateAggregationDqConnection(result.Ref(), outputs)) {
                return false;
            }

            if (NodesInfo.at(result.Raw()).IsResultBinding && AnyOf(outputs, [](const auto& output) { return output.Source.IsAggregation(); })) {
                NodeError(result.Ref(), "Streaming aggregation output must be written to a table, materialization into query results or precompute is not supported")();
                return false;
            }
        }

        return true;
    }

    bool TieStreamingAggregationWithOutputTable(const TKqpPhysicalTx& tx, TExprNode::TPtr& output) {
        output = tx.Ptr();

        if (Config.UseInMemoryStreamingAggregation.Get().GetOrElse(false)) {
            return true;
        }

        // Collect possible state table candidates for each aggregation

        for (const auto& stage : Graph.GetStageOrder()) {
            const auto& info = Graph.GetStage(stage);
            if (!info.Aggregation) {
                continue;
            }

            auto& aggregation = *Graph.GetAggregationBinding(info.Aggregation->Stage);
            const auto& agg = aggregation.Node;
            if (const auto explicitTable = GetSetting(agg.Settings().Ref(), "state_table_path"); explicitTable && !explicitTable->Tail().Content().empty()) {
                continue;
            }

            if (aggregation.Stage.Raw() == stage.Raw() && !ValidateAggregationOutputState(agg)) {
                return false;
            }

            const auto keys = agg.Keys();
            const auto handlers = agg.Handlers();

            for (const auto& write : info.TableWrites) {
                const auto& sink = write.Settings;
                if (sink.IsIndexImplTable().Value() == "true") {
                    continue;
                }

                const auto& output = info.Outputs.at(write.OutputIndex);
                if (!output.HasAggregation) {
                    continue;
                }

                const auto& primaryKey = Tables.ExistingTable(Cluster, sink.Table().Path().Value()).Metadata->KeyColumnNames;
                const THashSet<TString> primaryKeySet = THashSet<TString>(primaryKey.begin(), primaryKey.end());

                const auto& aggregationColumnMapping = output.AggregationColumnMapping;
                THashMap<TStringBuf, TStringBuf> aggregationToTableColumns;
                for (const auto& [column, origin] : aggregationColumnMapping) {
                    const auto [it, inserted] = aggregationToTableColumns.try_emplace(origin, column);
                    if (inserted) {
                        continue;
                    }

                    const bool isPkColumnNow = primaryKeySet.contains(column);
                    const bool isPkColumnBefore = primaryKeySet.contains(it->second);
                    if ((isPkColumnNow && !isPkColumnBefore) || (isPkColumnNow == isPkColumnBefore && column < it->second)) {
                        aggregationToTableColumns[origin] = column;
                    }
                }

                bool complete = true;

                TExprNodeList pairs;
                pairs.reserve(keys.Size() + handlers.Size());
                for (const auto& key : keys) {
                    const auto it = aggregationToTableColumns.find(key.Value());
                    if (it == aggregationToTableColumns.end()) {
                        complete = false;
                        break;
                    }

                    pairs.emplace_back(Ctx.NewList(agg.Pos(), {key.Ptr(), Ctx.NewAtom(agg.Pos(), it->second)}));
                }

                for (const auto& handler : handlers) {
                    const auto it = aggregationToTableColumns.find(handler.Ref().Head().Content());
                    if (it == aggregationToTableColumns.end()) {
                        complete = false;
                        break;
                    }

                    pairs.emplace_back(Ctx.NewList(agg.Pos(), {handler.Ref().HeadPtr(), Ctx.NewAtom(agg.Pos(), it->second)}));
                }

                if (complete) {
                    aggregation.StateTableCandidates.emplace_back(Ctx.NewList(agg.Pos(), {sink.Table().Path().Ptr(), Ctx.NewList(agg.Pos(), std::move(pairs))}));
                }
            }
        }

        // Select one of candidates tables for each aggregation as state store

        for (const auto& stage : Graph.GetStageOrder()) {
            auto& metadata = Graph.GetAggregationBinding(stage);
            if (!metadata || metadata->Stage.Raw() != stage.Raw()) {
                continue;
            }

            const auto& aggregation = metadata->Node;
            const auto existing = GetSetting(aggregation.Settings().Ref(), "output_state_table");

            if (const auto explicitTable = GetSetting(aggregation.Settings().Ref(), "state_table_path"); explicitTable && !explicitTable->Tail().Content().empty()) {
                if (existing) {
                    Ctx.AddError(TIssue(Ctx.GetPosition(aggregation.Pos()), "Explicit and output state tables cannot be used together"));
                    return false;
                }

                continue;
            }

            auto& candidates = metadata->StateTableCandidates;
            Sort(candidates, [](const auto& lhs, const auto& rhs) { return lhs->Head().Content() < rhs->Head().Content(); });

            TExprNode::TPtr selected;
            bool conflictingWrite = false;
            for (const auto& candidate : candidates) {
                if (Graph.GetTableWriteCount(candidate->Head().Content()) != 1) {
                    conflictingWrite = true;
                    continue;
                }

                if (existing) {
                    const TExprNode* lhs = &existing->Tail();
                    const TExprNode* rhs = candidate.Get();
                    if (!CompareExprTrees(lhs, rhs)) {
                        continue;
                    }
                }

                selected = candidate;
                break;
            }

            if (!selected) {
                const TString message = existing
                    ? "Assigned streaming aggregation output state table is no longer eligible"
                    : conflictingWrite
                        ? "Streaming aggregation output state table has other writes in the same transaction"
                        : "Streaming aggregation requires an output state table preserving every grouping key and aggregated value "
                          "with only renames or optionality changes";
                Ctx.AddError(TIssue(Ctx.GetPosition(aggregation.Pos()), message));
                return false;
            }

            if (!existing) {
                metadata->OutputStateTable = selected;
            }
        }

        // Replace streaming aggregations with table states

        TNodeOnNodeOwnedMap stageReplacements;

        for (const auto& stage : Graph.GetStageOrder()) {
            const auto& info = Graph.GetStage(stage);
            const auto originalProgram = stage.Program().Ptr();
            auto program = originalProgram;

            const auto& binding = info.Aggregation;
            if (binding && binding->OutputStateTable) {
                const auto& aggregation = binding->Node;
                auto updated = Ctx.ChangeChild(aggregation.Ref(), TKqpStreamingAggregation::idx_Settings, AddSetting(aggregation.Settings().Ref(), aggregation.Pos(), "output_state_table", binding->OutputStateTable, Ctx));
                program = Ctx.ReplaceNode(std::move(program), aggregation.Ref(), std::move(updated));
            }

            const auto originalInputs = stage.Inputs().Ptr();
            auto inputs = Ctx.ReplaceNodes(TExprNode::TPtr(originalInputs), stageReplacements);
            if (program != originalProgram || inputs != originalInputs) {
                auto children = stage.Ref().ChildrenList();
                children[TDqPhyStage::idx_Program] = std::move(program);
                children[TDqPhyStage::idx_Inputs] = std::move(inputs);
                stageReplacements.emplace(stage.Raw(), Ctx.ChangeChildren(stage.Ref(), std::move(children)));
            } else {
                // Mark node as unchanged
                stageReplacements.emplace(stage.Raw(), TExprNode::TPtr{});
            }
        }

        if (!stageReplacements.empty()) {
            output = Ctx.ReplaceNodes(tx.Ptr(), stageReplacements);
        }

        return true;
    }

private:
    //// Common helpers

    static bool IsMapNode(const TExprNode& node) {
        return TCoMap::Match(&node) || TCoOrderedMap::Match(&node) || TCoMultiMap::Match(&node) || node.IsCallable("OrderedMultiMap"sv)
            || node.IsCallable("ExpandMap"sv) || TCoNarrowMap::Match(&node) || TCoNarrowMultiMap::Match(&node) || TCoWideMap::Match(&node);
    }

    // Treat sequence of Variant<Tuple<...>> as DQ multi output stream
    static ui32 OutputCount(const TExprNode& node) {
        const auto maybeStage = TMaybeNode<TDqPhyStage>(&node);
        const auto type = maybeStage ? maybeStage.Cast().Program().Body().Ref().GetTypeAnn() : node.GetTypeAnn();
        if (type && IsIn({ETypeAnnotationKind::Flow, ETypeAnnotationKind::Stream, ETypeAnnotationKind::List}, type->GetKind())) {
            if (const auto* item = GetSeqItemType(type); item->GetKind() == ETypeAnnotationKind::Variant) {
                if (const auto* underlying = item->Cast<TVariantExprType>()->GetUnderlyingType(); underlying->GetKind() == ETypeAnnotationKind::Tuple) {
                    return underlying->Cast<TTupleExprType>()->GetSize();
                }
            }
        }

        return 1;
    }

    static const TTypeAnnotationNode* OutputItemType(const TExprNode& node, const ui32 index) {
        const auto* item = GetSeqItemType(node.GetTypeAnn());
        Y_VALIDATE(item, "Expected sequence type annotation");
        if (item->GetKind() == ETypeAnnotationKind::Variant) {
            if (const auto* underlying = item->Cast<TVariantExprType>()->GetUnderlyingType(); underlying->GetKind() == ETypeAnnotationKind::Tuple) {
                return underlying->Cast<TTupleExprType>()->GetItems().at(index);
            } 
        }

        return item;
    }

    static const TDistinctConstraintNode* OutputDistinct(const TExprNode& node, const ui32 index) {
        if (const auto* const multi = node.GetConstraint<TMultiConstraintNode>()) {
            const auto* const output = multi->GetItem(index);
            return output ? output->GetConstraint<TDistinctConstraintNode>() : nullptr;
        }

        return index == 0 ? node.GetConstraint<TDistinctConstraintNode>() : nullptr;
    }

    // Deferred error with node binding
    TOperatorSummary::TInputRejection::TCallback NodeError(const TExprNode& node, const TStringBuf& message) const {
        return [this, &node, message] {
            Ctx.AddError(TIssue(Ctx.GetPosition(node.Pos()), message));
        };
    }

    //// Streaming constraints validation

    static bool ExprHasStreamingNodes(const TExprNode& root) {
        bool found = false;
        VisitExpr(root, [&found](const TExprNode& node) {
            return found ? false : !(found = node.GetConstraint<TStreamingConstraintNode>());
        });
        return found;
    }

    // General constraints validation and context-independent node facts.
    bool ValidateStreamingConstraintsImpl(const TExprNode& root) {
        bool valid = true;
        VisitExpr(root, [&](const TExprNode& node) {
            return valid && !NodesInfo.contains(&node);
        }, [&](const TExprNode& node) {
            if (!valid || NodesInfo.contains(&node)) {
                return true;
            }

            const bool hasStreamingConstraint = node.GetConstraint<TStreamingConstraintNode>();
            auto& info = NodesInfo[&node];
            info.IsStreaming = hasStreamingConstraint;
            info.OutputCount = OutputCount(node);
            info.Aggregation = TMaybeNode<TKqpStreamingAggregation>(&node);
            ui32 streamingChildren = 0;

            for (const auto& child : node.Children()) {
                streamingChildren += !!child->GetConstraint<TStreamingConstraintNode>();

                const auto& childInfo = NodesInfo.at(child.Get());
                info.IsStreaming |= childInfo.IsStreaming;
                info.MultipleAggregations |= childInfo.MultipleAggregations;

                if (childInfo.Aggregation) {
                    info.MultipleAggregations |= info.Aggregation && info.Aggregation.Raw() != childInfo.Aggregation.Raw();
                    info.Aggregation = childInfo.Aggregation;
                }
            }

            if (!info.IsStreaming || !node.IsCallable()) {
                return true;
            }

            if (!hasStreamingConstraint) {
                YQL_CLOG(WARN, ProviderKqp) << "Found invalid streaming processing node: " << KqpExprToPrettyString(node, Ctx);
                Ctx.AddError(TIssue(Ctx.GetPosition(node.Pos()), TStringBuilder() << "Unsupported callable for streaming processing: '" << node.Content() << "'"));
                valid = false;
            } else if ((TCoFlatMapBase::Match(&node) || TCoNarrowFlatMap::Match(&node) || TCoFlatMapToEquiJoinBase::Match(&node)) && streamingChildren > 1) {
                YQL_CLOG(WARN, ProviderKqp) << "Found flatten map over streaming input with streaming results: " << KqpExprToPrettyString(node, Ctx);
                Ctx.AddError(TIssue(Ctx.GetPosition(node.Pos()), TStringBuilder() << "Flatten map over streaming input with streaming results is not supported, found node: '" << node.Content() << "'"));
                valid = false;
            }

            return true;
        });
        return valid;
    }

    //// Streaming aggregation validation

    TOperatorSummary::TInputRejection::TCallback ValidateAggregationProcessorCallable(const TExprNode& node, const ui32 output) const {
        // Well known nodes setups that cannot handle treaming aggregation result stream

        if (TCoExtend::Match(&node)) {
            return NodeError(node, "Union of streaming aggregation results with another data is not supported, please write aggregated result into intermediate table");
        }

        if (TCoTake::Match(&node) || TCoLimit::Match(&node)) {
            return NodeError(node, "LIMIT operator is not supported over streaming aggregation results");
        }

        if (TCoSkip::Match(&node)) {
            return NodeError(node, "OFFSET operator is not supported over streaming aggregation results");
        }

        if (TCoMultiHoppingCore::Match(&node)) {
            return NodeError(node, "Aggregation by hopping windows is not supported over streaming aggregation results");
        }

        if (TCoFilterBase::Match(&node)) {
            return NodeError(node, "Filtering over streaming aggregation results is not supported");
        }

        if (IsMapNode(node) && NodesInfo.at(&node.Head()).OutputCount != 1) {
            return NodeError(node, "Mapping over multiple input variants is not supported for streaming aggregation results");
        }

        if (const auto maybeJoin = TMaybeNode<TCoMapJoinCore>(&node); maybeJoin && maybeJoin.Cast().JoinKind().Value() != "Left"sv) {
            return NodeError(node, "Streaming aggregation results can be joined only with mode LEFT ANY by aggregation keys fields");
        }

        if (const auto maybeLookup = TMaybeNode<TKqpCnStreamLookup>(&node)) {
            const auto strategy = GetSetting(maybeLookup.Cast().Settings().Ref(), TKqpStreamLookupSettings::StrategySettingName);
            if (!strategy || !strategy->Tail().IsAtom(TKqpStreamLookupSettings::LookupJoinStrategyName)) {
                return NodeError(node, "Streaming aggregation results can be joined only with mode LEFT ANY by aggregation keys fields");
            }
        }

        // Validate that processor preserve distinct constraint (originally generated fro aggregation callable)

        if (!OutputDistinct(node, output)) {
            return [this, &node, output] {
                YQL_CLOG(WARN, ProviderKqp) << "Distinct constraint for streaming aggregation was lost on node: " << KqpExprToPrettyString(node, Ctx);
                TString message;

                if (node.GetConstraint<TMultiConstraintNode>()) {
                    message = TStringBuilder() << "Distinct constraint for aggregation key was lost on handler #" << output << " of node: '" << node.Content() << "'";
                } else if (TCoMapJoinCore::Match(&node) || TDqCnStreamLookup::Match(&node)) {
                    message = "Streaming aggregation results can be joined only with mode LEFT ANY by aggregation keys fields, distinct constraint for aggregation key was lost";
                } else if (TCoMapBase::Match(&node)) {
                    message = TStringBuilder() << "Distinct constraint for aggregation key was lost on rows processing with callable: '" << node.Content() << "'";
                } else if (TCoFlatMapBase::Match(&node)) {
                    message = TStringBuilder() << "Distinct constraint for aggregation key was lost on rows flattening with callable: '" << node.Content() << "'";
                } else {
                    message = TStringBuilder() << "Unsupported callable for processing streaming aggregation results: '" << node.Content() << "', distinct constraint for aggregation key was lost";
                }

                Ctx.AddError(TIssue(Ctx.GetPosition(node.Pos()), message));
            };
        }

        if (TCoToFlow::Match(&node) || TCoFromFlow::Match(&node) || TCoWideToBlocks::Match(&node) || TCoWideFromBlocks::Match(&node)
            || TDqConnection::Match(&node) || TDqOutput::Match(&node)
            || TCoMapJoinCore::Match(&node) || TCoSwitch::Match(&node)) {
            return {};
        }

        if (IsMapNode(node)) {
            const TTypeAnnotationNode* const inputType = node.Head().GetTypeAnn();
            Y_VALIDATE(inputType, "Missing node type annotation");
            if (IsWideSequenceBlockType(*inputType)) {
                return NodeError(node, "Wide block items processing is not supported for streaming aggregation results");
            }

            const auto* const itemType = GetSeqItemType(inputType);
            Y_VALIDATE(itemType, "Missing sequence item type annotation");
            if (itemType->GetKind() == ETypeAnnotationKind::Struct && itemType->Cast<TStructExprType>()->FindItem(BlockLengthColumnName).Defined()) {
                return NodeError(node, "Block items processing is not supported for streaming aggregation results");
            }

            return {};
        }

        if (TCoFlatMapBase::Match(&node) || TCoNarrowFlatMap::Match(&node)) {
            return node.Head().GetConstraint<TStreamingConstraintNode>()
                ? NodeError(node, "Flattening streaming aggregation results is not supported, please write result into intermediate table")
                : TOperatorSummary::TInputRejection::TCallback{};
        }

        return [this, &node] {
            Ctx.AddError(TIssue(Ctx.GetPosition(node.Pos()), TStringBuilder() << "Unsupported callable for processing streaming aggregation results: '" << node.Content() << "'"));
        };
    }

    bool ValidateAggregationDqConnection(const TExprNode& node, TOperatorSummary::TOutputs& outputs) const {
        if (outputs.empty() || !outputs.front().Source.IsAggregation()) {
            return true;
        }

        if (TDqConnection::Match(&node)) {
            if (const auto aggregationError = ValidateAggregationProcessorCallable(node.Head(), /* output */ 0)) {
                aggregationError();
                return false;
            }
        }

        if (const auto aggregationError = ValidateAggregationProcessorCallable(node, /* output */ 0)) {
            aggregationError();
            return false;
        }

        if (const auto maybeLookup = TMaybeNode<TDqCnStreamLookup>(&node)) {
            if (const auto label = maybeLookup.Cast().LeftLabel().Value(); !label.empty()) {
                auto& outputColumnMapping = outputs.front().ColumnMapping;
                auto aggregationColumnMapping = std::move(outputColumnMapping);
                outputColumnMapping = {};

                for (auto& [name, origins] : aggregationColumnMapping) {
                    outputColumnMapping.emplace(Ctx.AppendString(TStringBuilder() << label << '.' << name), std::move(origins));
                }
            }
        }

        return true;
    }

    bool ValidateAggregationTableOutput(const TDqPhyStage& stage, const TDqOutputAnnotationBase& output) const {
        const ui32 index = FromString<ui32>(output.Index().Value());
        const auto maybeSink = output.Maybe<TDqSink>();
        const auto maybeSettings = maybeSink ? maybeSink.Cast().Settings().Maybe<TKqpTableSinkSettings>() : TMaybeNode<TKqpTableSinkSettings>();
        if (!maybeSettings) {
            NodeError(output.Ref(), "Streaming aggregation output must be written to a table")();
            return false;
        }

        const auto settings = maybeSettings.Cast();
        if (!settings.Mode().Ref().IsAtom({"", "upsert", "replace"})) {
            NodeError(settings.Mode().Ref(), "Streaming aggregation results can only be written with UPSERT or REPLACE")();
            return false;
        }

        const auto* const distinct = OutputDistinct(stage.Program().Body().Ref(), index);
        if (!distinct) {
            NodeError(output.Ref(), "Distinct constraint for aggregation key was lost on table output")();
            return false;
        }

        const auto& table = Tables.ExistingTable(Cluster, settings.Table().Path().Value());
        Y_VALIDATE(table.Metadata, "Missing existing table metadata");
        const THashSet<TString> keyColumns(table.Metadata->KeyColumnNames.begin(), table.Metadata->KeyColumnNames.end());

        for (const auto& key : distinct->GetContent()) {
            if (key.size() != keyColumns.size()) {
                continue;
            }

            THashSet<std::string_view> matched;
            bool complete = true;

            for (const auto& component : key) {
                bool found = false;

                for (const auto& alternative : component) {
                    if (alternative.size() == 1 && keyColumns.contains(alternative.front()) && matched.insert(alternative.front()).second) {
                        found = true;
                        break;
                    }
                }

                complete &= found;
            }

            if (complete) {
                return true;
            }
        }

        Ctx.AddError(TIssue(Ctx.GetPosition(output.Pos()), TStringBuilder() << "Streaming aggregation key must exactly match the primary key of table '" << settings.Table().Path().Value() << "'"));
        return false;
    }

    //// Streaming aggregation tie with output table

    bool ValidateAggregationOutputState(const TKqpStreamingAggregation& aggregation) const {
        for (const auto& handler : aggregation.Handlers()) {
            if (!handler.Ref().Head().IsAtom()) {
                NodeError(handler.Ref().Head(), "Streaming aggregation output state tables require one column per handler; tuple splitting is not supported")();
                return false;
            }

            const auto traits = handler.Trait().Cast<TCoAggregationTraits>();
            const TExprNode* finish = traits.FinishHandler().Raw();
            const TExprNode* save = traits.SaveHandler().Raw();
            if (!IsIdentityLambda(*finish) && !CompareExprTrees(finish, save)) {
                NodeError(*finish, "Streaming aggregation output state tables require an identity finalizer or a finalizer equal to serialization")();
                return false;
            }
        }

        return true;
    }

    // Trace column mappings for single output callable till argument nodes
    const TColumnOriginMapping& CollectOperatorsColumnsMapping(const TExprNode& node) {
        const auto [mappingIt, inserted] = OperatorsColumnsMapping.try_emplace(&node);
        if (!inserted) {
            return mappingIt->second;
        }

        auto& columnMapping = mappingIt->second;

        if (node.IsArgument()) {
            columnMapping = TColumnOriginMapping::Initial(TSource{&node}, node.GetTypeAnn(), Ctx);
        } else if (TCoMember::Match(&node) || TCoNth::Match(&node)) {
            columnMapping = CollectOperatorsColumnsMapping(node.Head()).SelectColumn(node.Tail().Content());
        } else if (TCoJust::Match(&node) || TCoUnwrap::Match(&node) || TCoToOptional::Match(&node) || TCoEnsure::Match(&node) || TCoToFlow::Match(&node) || TCoFromFlow::Match(&node) || TCoToStream::Match(&node) || TCoIterator::Match(&node)) {
            columnMapping = CollectOperatorsColumnsMapping(node.Head());
        } else if (node.IsCallable("Cast"sv) || TCoStrictCast::Match(&node) || TCoSafeCast::Match(&node)) {
            const auto* sourceType = RemoveOptionalType(node.Head().GetTypeAnn());
            const auto* resultType = RemoveOptionalType(node.GetTypeAnn());
            if (sourceType && resultType && IsSameAnnotation(*resultType, *sourceType)) {
                columnMapping = CollectOperatorsColumnsMapping(node.Head());
            }
        } else if (TCoAsStruct::Match(&node)) {
            for (const auto& member : node.Children()) {
                const auto& value = CollectOperatorsColumnsMapping(member->Tail());
                if (const auto it = value.find(TStringBuf()); it != value.end()) {
                    columnMapping.emplace(member->Head().Content(), it->second);
                }
            }
        } else if (TCoIf::Match(&node) || TCoIfStrict::Match(&node)) {
            columnMapping = CollectOperatorsColumnsMapping(node.Tail());
            columnMapping.CommonColumns(CollectOperatorsColumnsMapping(*node.Child(1)));
        } else if (TCoAsList::Match(&node) || TCoList::Match(&node)) {
            if (const size_t first = TCoList::Match(&node) ? 1 : 0; first < node.ChildrenSize()) {
                columnMapping = CollectOperatorsColumnsMapping(*node.Child(first));

                for (size_t i = first + 1; i < node.ChildrenSize(); ++i) {
                    columnMapping.CommonColumns(CollectOperatorsColumnsMapping(*node.Child(i)));
                }
            }
        } else if (TCoMap::Match(&node) || TCoOrderedMap::Match(&node) || TCoFlatMap::Match(&node) || TCoOrderedFlatMap::Match(&node)) {
            const TTypeAnnotationNode* inputType = node.Head().GetTypeAnn();
            if (inputType && inputType->GetKind() == ETypeAnnotationKind::Optional) {
                const auto& lambda = node.Tail();
                const auto& inputMapping = CollectOperatorsColumnsMapping(node.Head());
                const auto& lambdaRename = CollectOperatorsColumnsMapping(lambda.Tail());
                columnMapping = TOperatorSummary::TBindings::BindLambdaOverRow(lambda, inputMapping, /* wide */ false, Ctx).BindOperatorColumns(lambdaRename);
            }
        } else if (TCoIfPresent::Match(&node) && node.ChildrenSize() == 3 && TCoNothing::Match(&node.Tail())) {
            const auto& lambda = *node.Child(1);
            const auto& inputMapping = CollectOperatorsColumnsMapping(node.Head());
            const auto& lambdaRename = CollectOperatorsColumnsMapping(lambda.Tail());
            columnMapping = TOperatorSummary::TBindings::BindLambdaOverRow(lambda, inputMapping, /* wide */ false, Ctx).BindOperatorColumns(lambdaRename);
        }

        return columnMapping;
    }

    // Trace column mapping for multi output callable
    const TVariantOutputs& CollectVariantOperatorsColumnsMapping(const TExprNode& node, const ui32 count) {
        const auto [it, inserted] = VariantOperatorsColumnsMapping.try_emplace(&node);
        if (!inserted) {
            Y_VALIDATE(it->second.size() == count, "Cached variant outputs count mismatch");
            return it->second;
        }

        auto& outputs = it->second;
        outputs.resize(count);

        if (TCoVariant::Match(&node)) {
            const ui32 index = FromString<ui32>(node.Child(1)->Content());
            outputs.at(index) = {.Seen = true, .NonEmpty = true, .ColumnMapping = CollectColumnMappings ? CollectOperatorsColumnsMapping(node.Head()) : TColumnOriginMapping{}};
        } else if (TCoJust::Match(&node) || TCoToFlow::Match(&node) || TCoFromFlow::Match(&node) || TCoToStream::Match(&node) || TCoIterator::Match(&node) || TCoEnsure::Match(&node)) {
            outputs = CollectVariantOperatorsColumnsMapping(node.Head(), count);
        } else if (TCoIf::Match(&node) || TCoIfStrict::Match(&node)) {
            outputs = CollectVariantOperatorsColumnsMapping(node.Tail(), count);
            outputs.MergeVariantColumns(CollectVariantOperatorsColumnsMapping(*node.Child(1), count), /* alternative */ true);
        } else if (TCoAsList::Match(&node) || TCoList::Match(&node) || TCoExtend::Match(&node) || TCoOrderedExtend::Match(&node)) {
            for (size_t i = TCoList::Match(&node) ? 1 : 0; i < node.ChildrenSize(); ++i) {
                outputs.MergeVariantColumns(CollectVariantOperatorsColumnsMapping(*node.Child(i), count), /* alternative */ false);
            }
        } else if (!TCoNothing::Match(&node)) {
            for (auto& output : outputs) {
                output.Seen = true;
            }
        }

        return outputs;
    }

    void ApplyMapToOutputs(const TExprNode& node, const TOperatorSummary::TOutput& input, TOperatorSummary& summary) {
        auto& outputs = summary.Outputs;
        Y_VALIDATE(outputs.size() >= 1, "Unexpected outputs size");

        const auto& lambda = node.Tail();
        const bool wideOutput = node.IsCallable("ExpandMap"sv) || TCoWideMap::Match(&node);
        TVariantOutputs variants;
        if (!wideOutput && outputs.size() > 1) {
            variants.resize(outputs.size());
            for (size_t i = 1; i < lambda.ChildrenSize(); ++i) {
                variants.MergeVariantColumns(CollectVariantOperatorsColumnsMapping(*lambda.Child(i), outputs.size()), /* alternative */ false);
            }

            if (AnyOf(variants, [](const auto& variant) { return variant.Seen && !variant.NonEmpty; })) {
                summary.RejectAggregationInput(input.Source, NodeError(node, "Filtering over streaming aggregation results is not supported"));
                return;
            }
        }

        if (!CollectColumnMappings) {
            return;
        }

        const bool wide = TCoWideMap::Match(&node) || TCoNarrowMap::Match(&node) || TCoNarrowMultiMap::Match(&node);
        const auto bindings = TOperatorSummary::TBindings::BindLambdaOverRow(lambda, input.ColumnMapping, wide, Ctx);

        if (wideOutput) {
            for (size_t i = 1; i < lambda.ChildrenSize(); ++i) {
                const auto value = bindings.BindOperatorColumns(CollectOperatorsColumnsMapping(*lambda.Child(i)));
                if (const auto it = value.find(TStringBuf()); it != value.end()) {
                    outputs.front().ColumnMapping.emplace(Ctx.GetIndexAsString(i - 1), it->second);
                }
            }
        } else if (outputs.size() > 1) {
            for (size_t i = 0; i < outputs.size(); ++i) {
                outputs[i].ColumnMapping = bindings.BindOperatorColumns(variants[i].ColumnMapping);
            }
        } else {
            auto columnMapping = CollectOperatorsColumnsMapping(*lambda.Child(1));

            for (ui32 i = 2; i < lambda.ChildrenSize(); ++i) {
                columnMapping.CommonColumns(CollectOperatorsColumnsMapping(*lambda.Child(i)));
            }

            outputs.front().ColumnMapping = bindings.BindOperatorColumns(columnMapping);
        }
    }

    const TOperatorSummary& SummarizeLambda(const TExprNode& lambda) {
        const auto [it, inserted] = OperatorSummaries.try_emplace(&lambda);
        if (!inserted) {
            return it->second;
        }

        auto& summary = it->second;
        summary = SummarizeNode(lambda.Tail());

        THashSet<TSource, TSource::THash> returned;
        returned.reserve(summary.Outputs.size());
        for (const auto& output : summary.Outputs) {
            returned.emplace(output.Source);
        }

        for (const auto& argument : lambda.Head().Children()) {
            const auto& argumentInfo = NodesInfo.at(argument.Get());
            if (!argumentInfo.IsStreaming) {
                continue;
            }

            for (ui32 index = 0; index < argumentInfo.OutputCount; ++index) {
                const TSource source{argument.Get(), index};
                if (!returned.contains(source)) {
                    summary.InputsRejectingAggregation.try_emplace(source, TOperatorSummary::TInputRejection{{}, &lambda});
                }
            }
        }

        return summary;
    }

    TOperatorSummary ApplyLambda(const TExprNode& caller, const TExprNode& lambda, const TOperatorSummary::TBindings& bindings) {
        const auto& summary = SummarizeLambda(lambda);
        TOperatorSummary result;
        result.Outputs.reserve(summary.Outputs.size());
        for (const auto& output : summary.Outputs) {
            result.Outputs.emplace_back(bindings.BindSource(output.Source), bindings.BindOperatorColumns(output.ColumnMapping));
        }

        for (const auto& [source, aggregationRejection] : summary.InputsRejectingAggregation) {
            const auto bound = bindings.BindSource(source);
            if (!bound.Node) {
                continue;
            }

            auto rejection = aggregationRejection;
            if (const auto* const lambda = std::exchange(rejection.DiscardingLambda, nullptr)) {
                rejection.Report = [this, &caller, lambda] {
                    YQL_CLOG(WARN, ProviderKqp) << "Found lambda discarding streaming aggregation output: " << KqpExprToPrettyString(caller, Ctx);
                    Ctx.AddError(TIssue(Ctx.GetPosition(lambda->Pos()), TDqPhyStage::Match(&caller)
                        ? "Stage program discards the streaming aggregation output"
                        : TCoSwitch::Match(&caller)
                            ? "Switch handler discards the streaming aggregation output"
                            : "Lambda discards the streaming aggregation output"));
                };
            }

            result.InputsRejectingAggregation.try_emplace(bound, rejection);
        }

        return result;
    }

    const TOperatorSummary& SummarizeNode(const TExprNode& node) {
        if (node.IsLambda()) {
            return SummarizeLambda(node);
        }

        const auto [it, inserted] = OperatorSummaries.try_emplace(&node);
        if (!inserted) {
            return it->second;
        }

        const auto& info = NodesInfo.at(&node);
        auto& summary = it->second;
        summary.Outputs.resize(info.OutputCount);

        if (!info.IsStreaming) {
            return summary;
        }

        if (node.IsArgument()) {
            for (ui32 index = 0; index < summary.Outputs.size(); ++index) {
                auto& output = summary.Outputs[index];
                output.Source = {&node, index};

                if (CollectColumnMappings) {
                    output.ColumnMapping = TColumnOriginMapping::Initial(output.Source, OutputItemType(node, index), Ctx);
                }
            }

            return summary;
        }

        if (TKqpStreamingAggregation::Match(&node)) {
            Y_VALIDATE(summary.Outputs.size() == 1, "Streaming aggregation must have exactly one output");

            const auto& input = SummarizeNode(node.Head());
            summary.MergeAggregationInputRejections(input);
            auto& output = summary.Outputs.front();
            output.Source = {&node};

            if (CollectColumnMappings) {
                output.ColumnMapping = TColumnOriginMapping::Initial(output.Source, OutputItemType(node, /* index */ 0), Ctx);
            }

            if (!node.GetConstraint<TDistinctConstraintNode>()) {
                summary.RejectAggregationInput(output.Source, NodeError(node, "Please consume all aggregation keys and write into table, non-distinct streaming aggregation results processing is not supported"));
            }

            return summary;
        }

        if (TCoSwitch::Match(&node)) {
            const auto& input = SummarizeNode(node.Head());
            summary.MergeAggregationInputRejections(input);
            TVector<bool> covered(input.Outputs.size(), false);
            summary.Outputs.clear();

            for (ui32 i = 2; i + 1 < node.ChildrenSize(); i += 2) {
                const auto& lambda = *node.Child(i + 1);
                TOperatorSummary::TBindings bindings;
                THashSet<TSource, TSource::THash> selectedSources;
                ui32 port = 0;

                for (const auto& index : node.Child(i)->Children()) {
                    const auto selected = FromString<ui32>(index->Content());
                    covered.at(selected) = true;
                    bindings.emplace(TSource{&lambda.Head().Head(), port++}, input.Outputs.at(selected));
                    selectedSources.insert(input.Outputs.at(selected).Source);
                }

                auto handler = ApplyLambda(node, lambda, bindings);
                const auto introducedAggregation = [this, &node] {
                    YQL_CLOG(WARN, ProviderKqp) << "Found streaming aggregation inside switch handler: " << KqpExprToPrettyString(node, Ctx);
                    Ctx.AddError(TIssue(Ctx.GetPosition(node.Pos()), "Streaming aggregation inside switch handler is not supported"));
                };

                if (const auto& maybeAggregation = NodesInfo.at(&lambda).Aggregation) {
                    handler.RejectAggregationInput(TSource{maybeAggregation.Raw()}, introducedAggregation);
                }

                for (const auto& output : handler.Outputs) {
                    if (!selectedSources.contains(output.Source)) {
                        handler.RejectAggregationInput(output.Source, introducedAggregation);
                    }
                }

                summary.MergeAggregationInputRejections(handler);
                summary.Outputs.insert(summary.Outputs.end(), handler.Outputs.begin(), handler.Outputs.end());
            }

            for (ui32 i = 0; i < covered.size(); ++i) {
                if (!covered[i]) {
                    summary.RejectAggregationInput(input.Outputs[i].Source, NodeError(node, "Switch discards the streaming aggregation output"));
                }
            }
        } else if (TCoFlatMapBase::Match(&node) || TCoNarrowFlatMap::Match(&node)) {
            const auto& input = SummarizeNode(node.Head());
            summary.MergeAggregationInputRejections(input);

            // The supported streaming result is captured by a lambda over a finite input.
            auto body = ApplyLambda(node, node.Tail(), /* bindings */ {});
            summary.MergeAggregationInputRejections(body);
            summary.Outputs = std::move(body.Outputs);
            for (const auto& output : input.Outputs) {
                summary.RejectAggregationInput(output.Source, NodeError(node, "Flattening streaming aggregation results is not supported, please write result into intermediate table"));
            }
        } else if (IsMapNode(node) || TCoMapJoinCore::Match(&node) || TCoToFlow::Match(&node) || TCoFromFlow::Match(&node) || TCoWideToBlocks::Match(&node) || TCoWideFromBlocks::Match(&node)) {
            const auto& input = SummarizeNode(node.Head());
            summary.MergeAggregationInputRejections(input);

            if (IsMapNode(node)) {
                Y_VALIDATE(!summary.Outputs.empty(), "Map must have at least one output");

                for (ui32 i = 0; i < summary.Outputs.size(); ++i) {
                    if (const auto aggregationError = ValidateAggregationProcessorCallable(node, i)) {
                        for (const auto& output : input.Outputs) {
                            summary.RejectAggregationInput(output.Source, aggregationError);
                        }

                        return summary;
                    }
                }

                Y_VALIDATE(input.Outputs.size() == 1, "Map input must have exactly one output");

                for (auto& output : summary.Outputs) {
                    output.Source = input.Outputs.front().Source;
                }

                ApplyMapToOutputs(node, input.Outputs.front(), summary);

                return summary;
            }

            if (const auto maybeJoin = TMaybeNode<TCoMapJoinCore>(&node)) {
                Y_VALIDATE(input.Outputs.size() == 1 && summary.Outputs.size() == 1, "Map join must have exactly one input and output");
                const auto& inputColumnsMapping = input.Outputs.front().ColumnMapping;

                auto& output = summary.Outputs.front();
                output.Source = input.Outputs.front().Source;
                const auto& renames = maybeJoin.Cast().LeftRenames().Ref();
                for (ui32 i = 0; i < renames.ChildrenSize(); i += 2) {
                    if (const auto column = inputColumnsMapping.find(renames.Child(i)->Content()); column != inputColumnsMapping.end()) {
                        output.ColumnMapping.emplace(renames.Child(i + 1)->Content(), column->second);
                    }
                }
            } else {
                summary.Outputs = input.Outputs;
            }
        } else {
            for (const auto& child : node.Children()) {
                if (!NodesInfo.at(child.Get()).IsStreaming) {
                    continue;
                }

                const auto& input = SummarizeNode(*child);
                summary.MergeAggregationInputRejections(input);
                const auto validation = ValidateAggregationProcessorCallable(node, /* output */ 0);
                Y_VALIDATE(validation, "Unhandled aggregation processor must be rejected");

                for (const auto& output : input.Outputs) {
                    summary.RejectAggregationInput(output.Source, validation);
                }
            }

            return summary;
        }

        for (ui32 i = 0; i < summary.Outputs.size(); ++i) {
            summary.RejectAggregationInput(summary.Outputs[i].Source, ValidateAggregationProcessorCallable(node, i));
        }

        return summary;
    }

    const TStringBuf Cluster;
    const TKikimrTablesData& Tables;
    const TKikimrConfiguration& Config;
    const bool CollectColumnMappings = false;
    TExprContext& Ctx;

    YDB_ACCESSOR_DEF(bool, HasStreamingNodes);
    // Static information about expr nodes inferred from types and constraints, independent from node usage context
    TNodeMap<TNodeInfo> NodesInfo;
    // Dynamic nodes context that used for propagation of features that depends from operator / lambda usage context
    TNodeMap<TOperatorSummary> OperatorSummaries;
    // For single optput operators. Mapping of columns that operator preserve without value change (with possible renames or optionality change)
    TNodeMap<TColumnOriginMapping> OperatorsColumnsMapping;
    // For multi outputs operators (with variant type annotation).
    // For each variant index stored: mapping of preserved input columns, does this variant may be produced and is there possible values filtering for this output variant
    TNodeMap<TVariantOutputs> VariantOperatorsColumnsMapping;
    // DQ physical graph representation with stages topological order
    TStagesGraph Graph;
};

} // anonymous namespace

IGraphTransformer::TStatus KqpBuildStreamingFlow(
    const ui64 txIdx,
    const TKqpPhysicalTx& tx,
    TExprNode::TPtr& output,
    THashSet<std::pair<ui64, ui64>>& streamingTxResults,
    const TKikimrConfiguration& config,
    const TKikimrTablesData& tables,
    const TStringBuf cluster,
    TExprContext& ctx)
{
    TStreamingFlowBuilder builder(cluster, config, tables, ctx);
    if (!builder.ValidateStreamingConstraints(txIdx, tx, streamingTxResults)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!builder.GetHasStreamingNodes()) {
        return IGraphTransformer::TStatus::Ok;
    }

    if (!builder.ValidateCheckpointsUsage() || !builder.ValidateStreamingAggregation(tx)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!builder.TieStreamingAggregationWithOutputTable(tx, output)) {
        return IGraphTransformer::TStatus::Error;
    }

    return output != tx.Ptr() ? IGraphTransformer::TStatus::Repeat : IGraphTransformer::TStatus::Ok;
}

} // namespace NKikimr::NKqp::NOpt
