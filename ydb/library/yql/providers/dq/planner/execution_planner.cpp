#include "execution_planner.h"

#include <ydb/library/yql/dq/integration/yql_dq_integration.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/dq/opt/dqs_opt.h>
#include <ydb/library/yql/providers/dq/opt/logical_optimize.h>
#include <ydb/library/yql/providers/dq/opt/physical_optimize.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/dq/mkql/dqs_mkql_compiler.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>

#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/core/type_ann/type_ann_expr.h>
#include <ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/dq/opt/dq_opt_build.h>
#include <ydb/library/yql/dq/opt/dq_opt.h>
#include <ydb/library/yql/dq/tasks/dq_connection_builder.h>
#include <ydb/library/yql/dq/tasks/dq_task_program.h>
#include <ydb/library/yql/dq/type_ann/dq_type_ann.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/core/services/yql_transform_pipeline.h>
#include <ydb/library/yql/minikql/aligned_page_pool.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/actors/core/event_pb.h>

#include <stack>

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NDqProto;
using namespace NYql::NNodes;

using namespace NKikimr::NMiniKQL;

using namespace Yql::DqsProto;

namespace {
    TString RemoveAliases(TString attributeName) {
        if (auto idx = attributeName.find_last_of('.'); idx != TString::npos) {
            return attributeName.substr(idx+1);
        }
        return attributeName;
    }
}

namespace NYql::NDqs {
    namespace {
        TVector<TDqPhyStage> GetStages(const TExprNode::TPtr& exprRoot) {
            TVector<TDqPhyStage> stages;
            VisitExpr(
                exprRoot,
                [](const TExprNode::TPtr& exprNode) {
                    const auto& node = TExprBase(exprNode);
                    return !node.Maybe<TCoLambda>();
                },
                [&stages](const TExprNode::TPtr& exprNode) {
                    const auto& node = TExprBase(exprNode);
                    if (auto maybeStage = node.Maybe<TDqPhyStage>()) {
                        stages.push_back(maybeStage.Cast());
                    }

                    return true;
                });

            return stages;
        }

        bool HasReadWraps(const TExprNode::TPtr& node) {
            bool result = false;
            VisitExpr(node, [&result](const TExprNode::TPtr& exprNode) {
                result |= TMaybeNode<TDqReadWrapBase>(exprNode).IsValid();
                return !result;
            });

            return result;
        }

        static bool HasDqSource(const TDqPhyStage& stage) {
            for (size_t inputIndex = 0; inputIndex < stage.Inputs().Size(); ++inputIndex) {
                const auto& input = stage.Inputs().Item(inputIndex);
                if (input.Maybe<TDqSource>()) {
                    return true;
                }
            }
            return false;
        }
    }

    TDqsExecutionPlanner::TDqsExecutionPlanner(const TDqSettings::TPtr& settings,
                                               TIntrusivePtr<TTypeAnnotationContext> typeContext,
                                               NYql::TExprContext& exprContext,
                                               const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
                                               NYql::TExprNode::TPtr dqExprRoot,
                                               NActors::TActorId executerID,
                                               NActors::TActorId resultID)
        : Settings(settings)
        , TypeContext(std::move(typeContext))
        , ExprContext(exprContext)
        , FunctionRegistry(functionRegistry)
        , DqExprRoot(std::move(dqExprRoot))
        , ExecuterID(executerID)
        , ResultID(resultID)
    {
    }

    void TDqsExecutionPlanner::Clear() {
        TasksGraph.Clear();
        _MaxDataSizePerJob = 0;
    }

    bool TDqsExecutionPlanner::CanFallback() {
        THashSet<TString> sources;
        bool flag = true;

        VisitExpr(
            DqExprRoot,
            [&](const TExprNode::TPtr& exprNode) {
                if (exprNode->IsCallable("DataSource")) {
                    sources.insert(TString{exprNode->Child(0)->Content()});
                }
                return true;
            });

        for (const auto& dataSourceName : sources) {
            auto datasource = TypeContext->DataSourceMap.FindPtr(dataSourceName);
            if (auto dqIntegration = (*datasource)->GetDqIntegration()) {
                flag &= dqIntegration->CanFallback();
            }
        }

        return flag;
    }

    ui64 TDqsExecutionPlanner::StagesCount() {
        auto stages = GetStages(DqExprRoot);
        return stages.size();
    }

    bool TDqsExecutionPlanner::PlanExecution(bool canFallback) {
        TExprBase expr(DqExprRoot);
        auto result = expr.Maybe<TDqCnResult>();
        auto query = expr.Maybe<TDqQuery>();
        auto value = expr.Maybe<TDqPhyPrecompute>();
        const auto maxTasksPerOperation = Settings->MaxTasksPerOperation.Get().GetOrElse(TDqSettings::TDefault::MaxTasksPerOperation);

        YQL_CLOG(DEBUG, ProviderDq) << "Execution Plan " << NCommon::ExprToPrettyString(ExprContext, *DqExprRoot);

        auto stages = GetStages(DqExprRoot);
        YQL_ENSURE(!stages.empty());

        for (const auto& stage : stages) {
            TasksGraph.AddStageInfo(TStageInfo(stage, {stage}));
        }

        for (const auto& stage : stages) {
            const bool hasDqSource = HasDqSource(stage);
            if ((hasDqSource || HasReadWraps(stage.Program().Ptr())) && BuildReadStage(stage, hasDqSource, canFallback)) {
                YQL_CLOG(TRACE, ProviderDq) << "Read stage " << NCommon::ExprToPrettyString(ExprContext, *stage.Ptr());
            } else {
                YQL_CLOG(TRACE, ProviderDq) << "Common stage " << NCommon::ExprToPrettyString(ExprContext, *stage.Ptr());
                double hashShuffleTasksRatio =  Settings->HashShuffleTasksRatio.Get().GetOrElse(TDqSettings::TDefault::HashShuffleTasksRatio);
                ui64 maxHashShuffleTasks = Settings->HashShuffleMaxTasks.Get().GetOrElse(TDqSettings::TDefault::HashShuffleMaxTasks);
                NDq::CommonBuildTasks(hashShuffleTasksRatio, maxHashShuffleTasks, TasksGraph, stage);
            }

            // Sinks
            if (auto maybeDqOutputsList = stage.Outputs()) {
                auto dqOutputsList = maybeDqOutputsList.Cast();
                for (const auto& output : dqOutputsList) {
                    const ui64 index = FromString(output.Ptr()->Child(TDqOutputAnnotationBase::idx_Index)->Content());
                    auto& stageInfo = TasksGraph.GetStageInfo(stage);
                    YQL_ENSURE(index < stageInfo.OutputsCount);

                    auto dataSinkName = output.Ptr()->Child(TDqOutputAnnotationBase::idx_DataSink)->Child(0)->Content();
                    auto datasink = TypeContext->DataSinkMap.FindPtr(dataSinkName);
                    YQL_ENSURE(datasink);
                    auto dqIntegration = (*datasink)->GetDqIntegration();
                    YQL_ENSURE(dqIntegration, "DqSink assumes that datasink has a dq integration impl");

                    NDq::TTransform outputTransform;
                    TString sinkType;
                    ::google::protobuf::Any sinkSettings;
                    if (output.Maybe<TDqSink>()) {
                        auto sink = output.Cast<TDqSink>();
                        dqIntegration->FillSinkSettings(sink.Ref(), sinkSettings, sinkType);
                        YQL_ENSURE(!sinkSettings.type_url().empty(), "Data sink provider \"" << dataSinkName << "\" did't fill dq sink settings for its dq sink node");
                        YQL_ENSURE(sinkType, "Data sink provider \"" << dataSinkName << "\" did't fill dq sink settings type for its dq sink node");
                    } else if (output.Maybe<NNodes::TDqTransform>()) {
                        auto transform = output.Cast<NNodes::TDqTransform>();
                        outputTransform.Type = transform.Type();
                        const auto inputTypeAnnotation = transform.InputType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                        outputTransform.InputType = GetSerializedTypeAnnotation(inputTypeAnnotation);

                        const auto outputTypeAnnotation = transform.OutputType().Ref().GetTypeAnn()->Cast<TTypeExprType>()->GetType();
                        outputTransform.OutputType = GetSerializedTypeAnnotation(outputTypeAnnotation);
                        dqIntegration->FillTransformSettings(transform.Ref(), outputTransform.Settings);
                    } else {
                        YQL_ENSURE(false, "Unknown stage output type");
                    }

                    for (ui64 taskId : stageInfo.Tasks) {
                        auto& task = TasksGraph.GetTask(taskId);
                        YQL_ENSURE(index < task.Outputs.size());
                        auto& taskOutput = task.Outputs[index];

                        if (output.Maybe<TDqSink>()) {
                            taskOutput.SinkType = sinkType;
                            taskOutput.SinkSettings = sinkSettings;
                            taskOutput.Type = NDq::TTaskOutputType::Sink;
                        } else if (output.Maybe<NNodes::TDqTransform>()) {
                            taskOutput.Transform.ConstructInPlace();

                            auto& transform = taskOutput.Transform;
                            transform->Type = outputTransform.Type;
                            transform->InputType = outputTransform.InputType;
                            transform->OutputType = outputTransform.OutputType;
                            transform->Settings = outputTransform.Settings;
                        }
                    }
                }
            }

            BuildConnections(stage);

            if (TasksGraph.GetTasks().size() > maxTasksPerOperation) {
                return false;
            }
        }

        for (const auto& stage : stages) {
            auto& stageInfo = TasksGraph.GetStageInfo(stage);
            YQL_ENSURE(!stageInfo.Tasks.empty());

            auto stageSettings = NDq::TDqStageSettings::Parse(stage);
            if (stageSettings.PartitionMode == NDq::TDqStageSettings::EPartitionMode::Single) {
                YQL_ENSURE(stageInfo.Tasks.size() == 1, "Unexpected multiple tasks in single-partition stage");
            }
        }

        TMaybeNode<TDqPhyStage> finalStage;
        if (result) {
            finalStage = result.Cast().Output().Stage().Maybe<TDqPhyStage>();
        } else if (value) {
            finalStage = value.Cast().Connection().Output().Stage().Maybe<TDqPhyStage>();
        }

        if (finalStage) {
            auto& resultStageInfo = TasksGraph.GetStageInfo(finalStage.Cast());
            YQL_ENSURE(resultStageInfo.Tasks.size() == 1);
            auto& resultTask = TasksGraph.GetTask(resultStageInfo.Tasks[0]);
            YQL_ENSURE(resultTask.Outputs.size() == 1);
            auto& output = resultTask.Outputs[0];
            output.Type = NDq::TTaskOutputType::Map;
            auto& channel = TasksGraph.AddChannel();
            channel.SrcTask = resultTask.Id;
            channel.SrcOutputIndex = 0;
            channel.DstTask = 0;
            channel.DstInputIndex = 0;
            output.Channels.emplace_back(channel.Id);
            SourceTaskID = resultTask.Id;
        }

        BuildCheckpointingAndWatermarksMode(true, Settings->WatermarksMode.Get().GetOrElse("") == "default");

        return TasksGraph.GetTasks().size() <= maxTasksPerOperation;
    }

    bool TDqsExecutionPlanner::IsEgressTask(const TDqsTasksGraph::TTaskType& task) const {
        for (const auto& output : task.Outputs) {
            for (ui64 channelId : output.Channels) {
                if (TasksGraph.GetChannel(channelId).DstTask) {
                    return false;
                }
            }
        }
        return true;
    }

    static bool IsInfiniteSourceType(const TString& sourceType) {
        return sourceType == "PqSource" || sourceType == "PqRdSource"; // Now it is the only infinite source type. Others are finite.
    }

    void TDqsExecutionPlanner::BuildCheckpointingAndWatermarksMode(bool enableCheckpoints, bool enableWatermarks) {
        std::stack<TDqsTasksGraph::TTaskType*> tasksStack;
        std::vector<bool> processedTasks(TasksGraph.GetTasks().size());
        for (TDqsTasksGraph::TTaskType& task : TasksGraph.GetTasks()) {
            if (IsEgressTask(task)) {
                tasksStack.push(&task);
            }
        }

        while (!tasksStack.empty()) {
            TDqsTasksGraph::TTaskType& task = *tasksStack.top();
            Y_ABORT_UNLESS(task.Id && task.Id <= processedTasks.size());
            if (processedTasks[task.Id - 1]) {
                tasksStack.pop();
                continue;
            }

            // Make sure that all input tasks are processed
            bool allInputsAreReady = true;
            for (const auto& input : task.Inputs) {
                for (ui64 channelId : input.Channels) {
                    const NDq::TChannel& channel = TasksGraph.GetChannel(channelId);
                    Y_ABORT_UNLESS(channel.SrcTask && channel.SrcTask <= processedTasks.size());
                    if (!processedTasks[channel.SrcTask - 1]) {
                        allInputsAreReady = false;
                        tasksStack.push(&TasksGraph.GetTask(channel.SrcTask));
                    }
                }
            }
            if (!allInputsAreReady) {
                continue;
            }

            // Current task has all inputs processed, so determine its checkpointing and watermarks mode now.
            NDqProto::ECheckpointingMode checkpointingMode = NDqProto::CHECKPOINTING_MODE_DISABLED;
            if (enableCheckpoints) {
                for (const auto& input : task.Inputs) {
                    if (input.SourceType) {
                        if (IsInfiniteSourceType(input.SourceType)) {
                            checkpointingMode = NDqProto::CHECKPOINTING_MODE_DEFAULT;
                            break;
                        }
                    } else {
                        for (ui64 channelId : input.Channels) {
                            const NDq::TChannel& channel = TasksGraph.GetChannel(channelId);
                            if (channel.CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED) {
                                checkpointingMode = NDqProto::CHECKPOINTING_MODE_DEFAULT;
                                break;
                            }
                        }
                        if (checkpointingMode == NDqProto::CHECKPOINTING_MODE_DEFAULT) {
                            break;
                        }
                    }
                }
            }

            NDqProto::EWatermarksMode watermarksMode = NDqProto::WATERMARKS_MODE_DISABLED;
            if (enableWatermarks) {
                for (auto& input : task.Inputs) {
                    if (input.SourceType) {
                        if (IsInfiniteSourceType(input.SourceType)) {
                            watermarksMode = NDqProto::WATERMARKS_MODE_DEFAULT;
                            input.WatermarksMode = NDqProto::WATERMARKS_MODE_DEFAULT;
                            break;
                        }
                    } else {
                        for (ui64 channelId : input.Channels) {
                            const NDq::TChannel& channel = TasksGraph.GetChannel(channelId);
                            if (channel.WatermarksMode != NDqProto::WATERMARKS_MODE_DEFAULT) {
                                watermarksMode = NDqProto::WATERMARKS_MODE_DEFAULT;
                                break;
                            }
                        }
                        if (watermarksMode == NDqProto::WATERMARKS_MODE_DEFAULT) {
                            break;
                        }
                    }
                }
            }

            // Apply mode to task and its outputs.
            task.CheckpointingMode = checkpointingMode;
            task.WatermarksMode = watermarksMode;
            for (const auto& output : task.Outputs) {
                for (ui64 channelId : output.Channels) {
                    auto& channel = TasksGraph.GetChannel(channelId);
                    channel.CheckpointingMode = checkpointingMode;
                    channel.WatermarksMode = watermarksMode;
                }
            }

            processedTasks[task.Id - 1] = true;
            tasksStack.pop();
        }
    }

    // TODO: Split Build and Get stages
    TVector<TDqTask>& TDqsExecutionPlanner::GetTasks() {
        if (Tasks.empty()) {
            auto workerCount = TasksGraph.GetTasks().size();
            TVector<NActors::TActorId> workers;
            for (unsigned int i = 0; i < workerCount; ++i) {
                NActors::TActorId fakeActorId(i+1, 0, 0, 0);
                workers.emplace_back(fakeActorId);
            }
            Tasks = GetTasks(workers);
        }
        return Tasks;
    }

    TVector<TDqTask> TDqsExecutionPlanner::GetTasks(const TVector<NActors::TActorId>& workers) {
        auto& tasks = TasksGraph.GetTasks();
        YQL_ENSURE(tasks.size() == workers.size());

        for (size_t i = 0; i < workers.size(); i++) {
            tasks[i].ComputeActorId = workers[i];
        }

        BuildAllPrograms();
        TVector<TDqTask> plan;
        THashSet<TString> clusterNameHints;
        for (const auto& task : tasks) {
            if (task.Meta.ClusterNameHint) {
                clusterNameHints.insert(task.Meta.ClusterNameHint);
            }
        }
        for (const auto& task : tasks) {
            Yql::DqsProto::TTaskMeta taskMeta;
            TDqTask taskDesc;
            taskDesc.SetId(task.Id);
            NActors::ActorIdToProto(ExecuterID, taskDesc.MutableExecuter()->MutableActorId());
            auto& taskParams = *taskMeta.MutableTaskParams();
            for (const auto& [k, v]: task.Meta.TaskParams) {
                taskParams[k] = v;
            }
            if (clusterNameHints.size() == 1) {
                taskMeta.SetClusterNameHint(*clusterNameHints.begin());
            } else {
                taskMeta.SetClusterNameHint(task.Meta.ClusterNameHint);
            }

            for (auto& input : task.Inputs) {
                auto& inputDesc = *taskDesc.AddInputs();
                if (input.SourceSettings) {
                    auto* sourceProto = inputDesc.MutableSource();
                    *sourceProto->MutableSettings() = *input.SourceSettings;
                    sourceProto->SetType(input.SourceType);
                    sourceProto->SetWatermarksMode(input.WatermarksMode);
                } else {
                    FillInputDesc(inputDesc, input);
                }
            }

            bool enableSpilling = false;
            if (task.Outputs.size() > 1) {
                enableSpilling = Settings->IsSpillingInChannelsEnabled();
            }
            for (auto& output : task.Outputs) {
                FillOutputDesc(*taskDesc.AddOutputs(), output, enableSpilling);
            }

            auto& program = *taskDesc.MutableProgram();
            program.SetRuntimeVersion(NYql::NDqProto::ERuntimeVersion::RUNTIME_VERSION_YQL_1_0);
            TString programStr;
            ui64 stageId, publicId;
            std::tie(programStr, stageId, publicId) = StagePrograms[task.StageId];
            program.SetRaw(programStr);
            taskMeta.SetStageId(publicId);
            taskDesc.MutableMeta()->PackFrom(taskMeta);
            taskDesc.SetStageId(stageId);
            taskDesc.SetEnableSpilling(Settings->GetEnabledSpillingNodes());

            if (Settings->DisableLLVMForBlockStages.Get().GetOrElse(true)) {
                auto& stage = TasksGraph.GetStageInfo(task.StageId).Meta.Stage;
                auto settings = TDqStageSettings::Parse(stage);
                if (settings.BlockStatus.Defined() && settings.BlockStatus == TDqStageSettings::EBlockStatus::Full) {
                    taskDesc.SetUseLlvm(false);
                }
            }
            plan.emplace_back(std::move(taskDesc));
        }

        return plan;
    }

    NActors::TActorId TDqsExecutionPlanner::GetSourceID() const {
        if (SourceID) {
            return *SourceID;
        } else {
            return {};
        }
    }

    TString TDqsExecutionPlanner::GetResultType() const {
        if (SourceTaskID) {
            auto& stage = TasksGraph.GetStageInfo(TasksGraph.GetTask(SourceTaskID).StageId).Meta.Stage;
            auto result = stage.Ref().GetTypeAnn();
            YQL_ENSURE(result->GetKind() == ETypeAnnotationKind::Tuple);
            YQL_ENSURE(result->Cast<TTupleExprType>()->GetItems().size() == 1);
            auto& item = result->Cast<TTupleExprType>()->GetItems()[0];
            YQL_ENSURE(item->GetKind() == ETypeAnnotationKind::List);
            auto exprType = item->Cast<TListExprType>()->GetItemType();
            return GetSerializedTypeAnnotation(exprType);
        }
        return {};
    }

    bool TDqsExecutionPlanner::BuildReadStage(const TDqPhyStage& stage, bool dqSource, bool canFallback) {
        auto& stageInfo = TasksGraph.GetStageInfo(stage);

        for (ui32 i = 0; i < stageInfo.InputsCount; i++) {
            const auto& input = stage.Inputs().Item(i);
            YQL_ENSURE(input.Maybe<TDqCnBroadcast>() || (dqSource && input.Maybe<TDqSource>()));
        }

        const TExprNode* read = nullptr;
        ui32 dqSourceInputIndex = std::numeric_limits<ui32>::max();
        if (dqSource) {
            for (ui32 i = 0; i < stageInfo.InputsCount; ++i) {
                const auto& input = stage.Inputs().Item(i);
                if (const auto& maybeDqSource = input.Maybe<TDqSource>()) {
                    read = maybeDqSource.Cast().Ptr().Get();
                    dqSourceInputIndex = i;
                    break;
                }
            }
            YQL_ENSURE(dqSourceInputIndex < stageInfo.InputsCount);
        } else {
            if (const auto& wrap = FindNode(stage.Program().Ptr(), [](const TExprNode::TPtr& exprNode) {
                if (const auto wrap = TMaybeNode<TDqReadWrapBase>(exprNode)) {
                    for (const auto& flag : wrap.Cast().Flags())
                        if (flag.Value() == "Solid")
                            return false;

                    return true;
                }
                return false;
            })) {
                read = wrap->Child(TDqReadWrapBase::idx_Input);
            } else {
                return false;
            }
        }

        const ui32 dataSourceChildIndex = dqSource ? 0 : 1;
        YQL_ENSURE(read->ChildrenSize() > 1);
        YQL_ENSURE(read->Child(dataSourceChildIndex)->IsCallable("DataSource"));

        auto dataSourceName = read->Child(dataSourceChildIndex)->Child(0)->Content();
        auto datasource = TypeContext->DataSourceMap.FindPtr(dataSourceName);
        YQL_ENSURE(datasource);
        const auto stageSettings = TDqStageSettings::Parse(stage);
        auto tasksPerStage = Settings->MaxTasksPerStage.Get().GetOrElse(TDqSettings::TDefault::MaxTasksPerStage);
        const size_t maxPartitions = TDqStageSettings::EPartitionMode::Single == stageSettings.PartitionMode ? 1ULL : tasksPerStage;
        TVector<TString> parts;
        if (auto dqIntegration = (*datasource)->GetDqIntegration()) {
            TString clusterName;
            _MaxDataSizePerJob = Max(_MaxDataSizePerJob, dqIntegration->Partition(*Settings, maxPartitions, *read, parts, &clusterName, ExprContext, canFallback));
            TMaybe<::google::protobuf::Any> sourceSettings;
            TString sourceType;
            if (dqSource) {
                sourceSettings.ConstructInPlace();
                dqIntegration->FillSourceSettings(*read, *sourceSettings, sourceType, maxPartitions, ExprContext);
                YQL_ENSURE(!sourceSettings->type_url().empty(), "Data source provider \"" << dataSourceName << "\" did't fill dq source settings for its dq source node");
                YQL_ENSURE(sourceType, "Data source provider \"" << dataSourceName << "\" did't fill dq source settings type for its dq source node");
            }
            for (const auto& p : parts) {
                auto& task = TasksGraph.AddTask(stageInfo);
                task.Meta.TaskParams[dataSourceName] = p;
                task.Meta.ClusterNameHint = clusterName;
                if (dqSource) {
                    task.Inputs[dqSourceInputIndex].SourceSettings = sourceSettings;
                    task.Inputs[dqSourceInputIndex].SourceType = sourceType;
                }
            }
        }
        return !parts.empty();
    }

    const static std::unordered_map<
        std::string_view,
        void(*)(TDqsTasksGraph&, const NNodes::TDqPhyStage&, ui32, const TChannelLogFunc&)
    > ConnectionBuilders = {
        {TDqCnUnionAll::CallableName(), &BuildUnionAllChannels<TDqsTasksGraph>},
        {TDqCnHashShuffle::CallableName(), &BuildHashShuffleChannels},
        {TDqCnBroadcast::CallableName(), &BuildBroadcastChannels},
        {TDqCnMap::CallableName(), &BuildMapChannels},
        {TDqCnStreamLookup::CallableName(), &BuildStreamLookupChannels},
        {TDqCnMerge::CallableName(), &BuildMergeChannels},
    };

    void TDqsExecutionPlanner::ConfigureInputTransformStreamLookup(const NNodes::TDqCnStreamLookup& streamLookup, const NNodes::TDqPhyStage& stage, ui32 inputIndex) {
        auto rightInput = streamLookup.RightInput().Cast<TDqLookupSourceWrap>();
        auto dataSourceName = rightInput.DataSource().Category().StringValue();
        auto dataSource = TypeContext->DataSourceMap.FindPtr(dataSourceName);
        YQL_ENSURE(dataSource);
        auto dqIntegration = (*dataSource)->GetDqIntegration();
        YQL_ENSURE(dqIntegration);
        
        google::protobuf::Any providerSpecificLookupSourceSettings;
        TString sourceType;
        dqIntegration->FillLookupSourceSettings(*rightInput.Raw(), providerSpecificLookupSourceSettings, sourceType);
        YQL_ENSURE(!providerSpecificLookupSourceSettings.type_url().empty(), "Data source provider \"" << dataSourceName << "\" did't fill dq source settings for its dq source node");
        YQL_ENSURE(sourceType, "Data source provider \"" << dataSourceName << "\" did't fill dq source settings type for its dq source node");

        NDqProto::TDqStreamLookupSource streamLookupSource;
        streamLookupSource.SetProviderName(sourceType);
        *streamLookupSource.MutableLookupSource() = providerSpecificLookupSourceSettings;
        streamLookupSource.SetSerializedRowType(NYql::NCommon::GetSerializedTypeAnnotation(rightInput.RowType().Raw()->GetTypeAnn()));
        NDqProto::TDqInputTransformLookupSettings settings;
        settings.SetLeftLabel(streamLookup.LeftLabel().Cast<NNodes::TCoAtom>().StringValue());
        *settings.MutableRightSource() = streamLookupSource;
        settings.SetRightLabel(streamLookup.RightLabel().StringValue());
        settings.SetJoinType(streamLookup.JoinType().StringValue());
        for (const auto& k: streamLookup.LeftJoinKeyNames()) {
            *settings.AddLeftJoinKeyNames() = RemoveAliases(k.StringValue());
        }
        for (const auto& k: streamLookup.RightJoinKeyNames()) {
            *settings.AddRightJoinKeyNames() = RemoveAliases(k.StringValue());
        }
        const auto narrowInputRowType = GetSeqItemType(streamLookup.Output().Ptr()->GetTypeAnn());
        Y_ABORT_UNLESS(narrowInputRowType->GetKind() == ETypeAnnotationKind::Struct);
        settings.SetNarrowInputRowType(NYql::NCommon::GetSerializedTypeAnnotation(narrowInputRowType));
        const auto narrowOutputRowType = GetSeqItemType(streamLookup.Ptr()->GetTypeAnn());
        Y_ABORT_UNLESS(narrowOutputRowType->GetKind() == ETypeAnnotationKind::Struct);
        settings.SetNarrowOutputRowType(NYql::NCommon::GetSerializedTypeAnnotation(narrowOutputRowType));

        const auto inputRowType = GetSeqItemType(streamLookup.Output().Stage().Program().Ref().GetTypeAnn());
        const auto outputRowType = GetSeqItemType(stage.Program().Args().Arg(inputIndex).Ref().GetTypeAnn());
        TTransform streamLookupTransform {
            .Type = "StreamLookupInputTransform",
            .InputType = NYql::NCommon::GetSerializedTypeAnnotation(inputRowType),
            .OutputType = NYql::NCommon::GetSerializedTypeAnnotation(outputRowType),
            .Settings = {} //set up in the next line
        };
        Y_ABORT_UNLESS(streamLookupTransform.Settings.PackFrom(settings));
        auto& stageInfo = TasksGraph.GetStageInfo(stage);
        for (auto taskId : stageInfo.Tasks) {
            auto& task = TasksGraph.GetTask(taskId);
            task.Inputs[inputIndex].Transform = streamLookupTransform;
        }
    }


    void TDqsExecutionPlanner::BuildConnections(const NNodes::TDqPhyStage& stage) {
        NDq::TChannelLogFunc logFunc = [](ui64, ui64, ui64, TStringBuf, bool) {};
        for (ui32 inputIndex = 0; inputIndex < stage.Inputs().Size(); ++inputIndex) {
            const auto &input = stage.Inputs().Item(inputIndex);
            if (input.Maybe<TDqConnection>()) {
                if (const auto it = ConnectionBuilders.find(input.Cast<NNodes::TCallable>().CallableName()); it != ConnectionBuilders.cend()) {
                    it->second(TasksGraph, stage, inputIndex, logFunc);
                    if (auto streamLookup = input.Maybe<TDqCnStreamLookup>())  {
                        ConfigureInputTransformStreamLookup(streamLookup.Cast(), stage, inputIndex);
                    }
                } else {
                    YQL_ENSURE(false, "Unknown stage connection type: " << input.Cast<NNodes::TCallable>().CallableName());
                }
            } else {
                YQL_ENSURE(input.Maybe<TDqSource>(), "Unknown stage input: " << input.Cast<NNodes::TCallable>().CallableName());
            }
        }
    }

    void TDqsExecutionPlanner::BuildAllPrograms() {
        using namespace NKikimr::NMiniKQL;

        StagePrograms.clear();

        TScopedAlloc alloc(__LOCATION__, NKikimr::TAlignedPagePoolCounters(), FunctionRegistry->SupportsSizedAllocators());
        TTypeEnvironment typeEnv(alloc);
        TVector<NNodes::TExprBase> fakeReads;
        NCommon::TMkqlCommonCallableCompiler compiler;
        RegisterDqsMkqlCompilers(compiler, *TypeContext);

        for (const auto& stageInfo : TasksGraph.GetStagesInfo()) {
            const auto& stage = stageInfo.second.Meta.Stage;
            auto paramsType = NDq::CollectParameters(stage.Program(), ExprContext);
            YQL_ENSURE(paramsType->GetItems().empty()); // TODO support parameters

            auto settings = NDq::TDqStageSettings::Parse(stage);
            ui64 stageId = stage.Ref().UniqueId();
            ui64 publicId = PublicIds.Value(settings.LogicalId, stageId);

/* TODO:
            ui64 stageId = stage.Ref().UniqueId();
            if (auto publicId = TypeContext->TranslateOperationId(stageId)) {
                stageId = *publicId;
            }

            TExprNode::TPtr lambdaInput = ExprContext.DeepCopyLambda(stage.Program().Ref());
            bool hasNonDeterministicFunctions;
            auto status = PeepHoleOptimizeNode(lambdaInput, lambdaInput, ExprContext, *TypeContext, nullptr, hasNonDeterministicFunctions);
            if (status != IGraphTransformer::TStatus::Ok) {
                ExprContext.AddError(TIssue(ExprContext.GetPosition(lambdaInput->Pos()), TString("Peephole optimization failed for Dq stage")));
                ExprContext.IssueManager.GetIssues().PrintTo(Cerr);
                Y_ABORT_UNLESS(false);
            }
*/
            TSpillingSettings spillingSettings{Settings->GetEnabledSpillingNodes()};
            StagePrograms[stageInfo.first] = std::make_tuple(
                NDq::BuildProgram(
                    stage.Program(), *paramsType, compiler, typeEnv, *FunctionRegistry,
                    ExprContext, fakeReads, spillingSettings),
                stageId, publicId);
        }
    }

    void TDqsExecutionPlanner::FillChannelDesc(NDqProto::TChannel& channelDesc, const NDq::TChannel& channel, bool enableSpilling) {
        channelDesc.SetId(channel.Id);
        channelDesc.SetSrcStageId(std::get<2>(StagePrograms[channel.SrcStageId]));
        channelDesc.SetDstStageId(std::get<2>(StagePrograms[channel.DstStageId]));
        channelDesc.SetSrcTaskId(channel.SrcTask);
        channelDesc.SetDstTaskId(channel.DstTask);
        channelDesc.SetCheckpointingMode(channel.CheckpointingMode);
        channelDesc.SetTransportVersion(Settings->GetDataTransportVersion());
        channelDesc.SetEnableSpilling(enableSpilling);

        if (channel.SrcTask) {
            NActors::ActorIdToProto(TasksGraph.GetTask(channel.SrcTask).ComputeActorId,
                                        channelDesc.MutableSrcEndpoint()->MutableActorId());
        }

        if (channel.DstTask) {
            NActors::ActorIdToProto(TasksGraph.GetTask(channel.DstTask).ComputeActorId,
                                        channelDesc.MutableDstEndpoint()->MutableActorId());
        } else {
            auto& stageInfo = TasksGraph.GetStageInfo(TasksGraph.GetTask(channel.SrcTask).StageId);
            YQL_ENSURE(stageInfo.Tasks.size() == 1);
            YQL_ENSURE(!SourceID);
            ActorIdToProto(ResultID, channelDesc.MutableDstEndpoint()->MutableActorId());
            SourceID = TasksGraph.GetTask(channel.SrcTask).ComputeActorId;
        }
    }

    void TDqsExecutionPlanner::FillInputDesc(NDqProto::TTaskInput& inputDesc, const TTaskInput& input) {
        switch (input.Type()) {
            case TTaskInputType::UnionAll: {
                inputDesc.MutableUnionAll();
                break;
            }
            case TTaskInputType::Merge: {
                auto& mergeProto = *inputDesc.MutableMerge();
                auto& sortColumns = std::get<NYql::NDq::TMergeTaskInput>(input.ConnectionInfo).SortColumns;
                for (const auto& sortColumn : sortColumns) {
                    auto newSortCol = mergeProto.AddSortColumns();
                    newSortCol->SetColumn(sortColumn.Column.c_str());
                    newSortCol->SetAscending(sortColumn.Ascending);
                }
                break;
            }
            default:
                YQL_ENSURE(false, "Unexpected task input type.");
        }
        if (input.Transform) {
            auto transform = inputDesc.MutableTransform();
            transform->SetType(input.Transform->Type);
            transform->SetInputType(input.Transform->InputType);
            transform->SetOutputType(input.Transform->OutputType);
            *transform->mutable_settings() = input.Transform->Settings;
        }

        for (ui64 channel : input.Channels) {
            auto& channelDesc = *inputDesc.AddChannels();
            FillChannelDesc(channelDesc, TasksGraph.GetChannel(channel), /*enableSpilling*/false);
        }
    }

    void TDqsExecutionPlanner::FillOutputDesc(NDqProto::TTaskOutput& outputDesc, const TTaskOutput& output, bool enableSpilling) {
        switch (output.Type) {
            case TTaskOutputType::Map:
                YQL_ENSURE(output.Channels.size() == 1);
                outputDesc.MutableMap(); //->SetChannelId(output.Channels[0]);
                break;

            case TTaskOutputType::HashPartition: {
                YQL_ENSURE(output.Channels.size() == output.PartitionsCount);
                auto& hashPartitionDesc = *outputDesc.MutableHashPartition();
                for (auto& column : output.KeyColumns) {
                    hashPartitionDesc.AddKeyColumns(column);
                }
                hashPartitionDesc.SetPartitionsCount(output.PartitionsCount);
                break;
            }

            case TTaskOutputType::Broadcast: {
                //for (const auto channel : output.Channels) {
                    outputDesc.MutableBroadcast(); //->AddChannelIds(channel);
                //}
                break;
            }

            case TTaskOutputType::Sink: {
                YQL_ENSURE(output.Channels.empty());
                YQL_ENSURE(output.SinkType);
                YQL_ENSURE(output.SinkSettings);
                auto* sinkProto = outputDesc.MutableSink();
                sinkProto->SetType(output.SinkType);
                *sinkProto->MutableSettings() = *output.SinkSettings;
                break;
            }

            case TTaskOutputType::Undefined: {
                YQL_ENSURE(output.Transform, "Unexpected task output type `TTaskOutputType::Undefined`");
            }
        }

        for (auto& channel : output.Channels) {
            auto& channelDesc = *outputDesc.AddChannels();
            FillChannelDesc(channelDesc, TasksGraph.GetChannel(channel), enableSpilling);
        }

        if (output.Transform) {
            auto* transformDesc = outputDesc.MutableTransform();
            auto& transform = output.Transform;

            transformDesc->SetType(transform->Type);
            transformDesc->SetInputType(transform->InputType);
            transformDesc->SetOutputType(transform->OutputType);
            *transformDesc->MutableSettings() = transform->Settings;
        }
    }


    TDqsSingleExecutionPlanner::TDqsSingleExecutionPlanner(
        const TString& program,
        NActors::TActorId executerID,
        NActors::TActorId resultID,
        const TTypeAnnotationNode* typeAnn)
        : Program(program)
        , ExecuterID(executerID)
        , ResultID(resultID)
        , TypeAnn(typeAnn)
    { }

    TVector<TDqTask>& TDqsSingleExecutionPlanner::GetTasks()
    {
        if (Tasks.empty()) {
            Tasks = GetTasks({NActors::TActorId(1, 0, 0, 0)});
        }
        return Tasks;
    }

    TVector<TDqTask> TDqsSingleExecutionPlanner::GetTasks(const TVector<NActors::TActorId>& workers)
    {
        YQL_ENSURE(workers.size() == 1);

        auto& worker = workers[0];

        TDqTask task;
        Yql::DqsProto::TTaskMeta taskMeta;
        task.SetId(1);
        taskMeta.SetStageId(1);
        task.SetStageId(1);
        task.MutableMeta()->PackFrom(taskMeta);

        NActors::ActorIdToProto(ExecuterID, task.MutableExecuter()->MutableActorId());
        auto& program = *task.MutableProgram();
        program.SetRuntimeVersion(NYql::NDqProto::ERuntimeVersion::RUNTIME_VERSION_YQL_1_0);
        program.SetRaw(Program);

        auto outputDesc = task.AddOutputs();
        outputDesc->MutableMap();

        auto channelDesc = outputDesc->AddChannels();
        channelDesc->SetId(1);
        channelDesc->SetSrcStageId(1);
        channelDesc->SetSrcTaskId(2);
        channelDesc->SetDstTaskId(1);

        NActors::ActorIdToProto(ExecuterID, channelDesc->MutableSrcEndpoint()->MutableActorId());
        NActors::ActorIdToProto(ResultID, channelDesc->MutableDstEndpoint()->MutableActorId());

        SourceID = worker;

        return {task};
    }

    NActors::TActorId TDqsSingleExecutionPlanner::GetSourceID() const
    {
        if (SourceID) {
            return *SourceID;
        } else {
            return {};
        }
    }

    TString TDqsSingleExecutionPlanner::GetResultType() const
    {
        if (TypeAnn && TypeAnn->GetKind() == ETypeAnnotationKind::List) {
            auto item = TypeAnn;
            YQL_ENSURE(item->GetKind() == ETypeAnnotationKind::List);
            auto exprType = item->Cast<TListExprType>()->GetItemType();
            return GetSerializedTypeAnnotation(exprType);
        } else {
            return GetSerializedResultType(Program);
        }
    }

    TGraphExecutionPlanner::TGraphExecutionPlanner(
        const TVector<TDqTask>& tasks,
        ui64 sourceId,
        const TString& resultType,
        NActors::TActorId executerID,
        NActors::TActorId resultID)
        : Tasks(tasks)
        , SourceId(sourceId)
        , ResultType(resultType)
        , ExecuterID(executerID)
        , ResultID(resultID)
    {
    }

    TVector<TDqTask> TGraphExecutionPlanner::GetTasks(const TVector<NActors::TActorId>& workers)
    {
        if (ResultType) {
            YQL_ENSURE(SourceId < workers.size());
            SourceID = workers[SourceId];
        }

        auto setActorId = [&](NYql::NDqProto::TEndpoint* endpoint) {
            if (endpoint->GetEndpointTypeCase() == NYql::NDqProto::TEndpoint::kActorId) {
                NActors::TActorId fakeId = NActors::ActorIdFromProto(endpoint->GetActorId());
                if (fakeId.NodeId() > 0) {
                    NActors::TActorId realId = fakeId.LocalId() == 0
                        ? workers[fakeId.NodeId()-1]
                        : ResultID;
                    NActors::ActorIdToProto(realId, endpoint->MutableActorId());
                }
            }
        };

        for (auto& taskDesc : Tasks) {
            NActors::ActorIdToProto(ExecuterID, taskDesc.MutableExecuter()->MutableActorId());

            for (auto& inputDesc : *taskDesc.MutableInputs()) {
                for (auto& channelDesc : *inputDesc.MutableChannels()) {
                    setActorId(channelDesc.MutableSrcEndpoint());
                    setActorId(channelDesc.MutableDstEndpoint());
                }
            }

            for (auto& outputDesc : *taskDesc.MutableOutputs()) {
                for (auto& channelDesc : *outputDesc.MutableChannels()) {
                    setActorId(channelDesc.MutableSrcEndpoint());
                    setActorId(channelDesc.MutableDstEndpoint());
                }
            }
        }

        return Tasks;
    }

    NActors::TActorId TGraphExecutionPlanner::GetSourceID() const
    {
        if (SourceID) {
            return *SourceID;
        } else {
            return {};
        }
    }

    TString TGraphExecutionPlanner::GetResultType() const
    {
        return ResultType;
    }

} // namespace NYql::NDqs
