#include "streaming_query_controller.h"
#include "physical_graph_rescaling.h"
#include "pq_topic_resolver.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/fq/libs/checkpointing/checkpoint_coordinator.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/federated_query/actors/streaming_query_nodes_manager.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>

#include <yql/essentials/providers/common/structured_token/yql_token_builder.h>

#include <library/cpp/protobuf/interop/cast.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_EXECUTER

namespace NKikimr::NKqp {

namespace {

using TSettings = IKqpStreamingQueryControllerFactory::TSettings;

FederatedQuery::StreamingDisposition MakeStreamingDisposition(const TUserRequestContext& context, NFq::TCheckpointCoordinatorSettings& settings) {
    FederatedQuery::StreamingDisposition result;
    const auto& disposition = context.StreamingDisposition;
    if (!disposition) {
        result.mutable_from_last_checkpoint()->set_force(true);
        return result;
    }

    if (disposition->has_output_start_time()) {
        settings.OutputStartTime = NProtoInterop::CastFromProto(disposition->output_start_time());
    }

    switch (disposition->GetDispositionCase()) {
        case NYql::NPq::NProto::StreamingDisposition::kOldest:
            *result.mutable_oldest() = disposition->oldest();
            break;
        case NYql::NPq::NProto::StreamingDisposition::kFresh:
            *result.mutable_fresh() = disposition->fresh();
            break;
        case NYql::NPq::NProto::StreamingDisposition::kFromTime:
            *result.mutable_from_time()->mutable_timestamp() = disposition->from_time().timestamp();
            break;
        case NYql::NPq::NProto::StreamingDisposition::kTimeAgo:
            *result.mutable_time_ago()->mutable_duration() = disposition->time_ago().duration();
            break;
        case NYql::NPq::NProto::StreamingDisposition::kFromLastCheckpoint:
            result.mutable_from_last_checkpoint()->set_force(disposition->from_last_checkpoint().force());
            break;
        case NYql::NPq::NProto::StreamingDisposition::DISPOSITION_NOT_SET:
            break;
    }
    return result;
}

class TStreamingQueryController : public IKqpStreamingQueryController {
public:
    TStreamingQueryController(TSettings settings, NYql::IPqGatewayFactory::TPtr pqGatewayFactory,
        NFq::TCheckpointProviderIntegrations checkpointProviderIntegrations)
        : Settings(std::move(settings))
        , PqGatewayFactory(std::move(pqGatewayFactory))
        , CheckpointProviderIntegrations(std::move(checkpointProviderIntegrations))
    {}

    bool NeedsPrepare() const override {
        return AppData()->FeatureFlags.GetEnableUpdatingPartitionsOnStreamingQueryRestart()
            && Settings.HasPqSources
            && Settings.Graph
            && PqGatewayFactory;
    }

    void StartPrepare(THashMap<TString, TString> secureParams) override {
        TActivationContext::RegisterWithSameMailbox(CreateKqpPqTopicResolver(
            Settings.ExecuterId,
            Settings.TxId,
            Settings.Database,
            std::move(secureParams),
            PqGatewayFactory,
            MutableGraph()), Settings.ExecuterId);
    }

    std::shared_ptr<const NKikimrKqp::TQueryPhysicalGraph> GetGraphToRestore(
        const TVector<NKikimrKqp::TKqpNodeResources>& resourcesSnapshot, ui32 usableThreadsPerNode) override
    {
        if (!Settings.Graph) {
            return nullptr;
        }

        if (Settings.HasPqSources && AppData()->FeatureFlags.GetEnablePqSourceRescaling()) {
            const auto& graph = MutableGraph();
            const auto taskCount = graph->TasksSize();
            PatchQueryPhysicalGraphForRescaling(*graph, resourcesSnapshot, usableThreadsPerNode);
            RescalingChangedTaskCount = graph->TasksSize() != taskCount;
        }

        return PatchedGraph ? PatchedGraph : Settings.Graph;
    }

    TStartedActors Start(std::shared_ptr<const NKikimrKqp::TQueryPhysicalGraph> graph) override {
        const auto& context = Settings.UserRequestContext;
        const bool enabled = AppData()->FeatureFlags.GetEnableStreamingQueries()
            && graph && context && context->CheckpointId;
        if (!enabled) {
            return {};
        }

        const auto graphParams = MakeGraphParams(*graph);

        if (Settings.HasPqSources) {
            Actors.NodesManager = TActivationContext::Register(CreateStreamingQueryNodesManager(
                Settings.ExecuterId,
                Settings.Database,
                context->StreamingQueryPath,
                graphParams.GetTasks(),
                TDuration::Seconds(300),
                TDuration::Seconds(120),
                graph->GetPreparedQuery().GetPhysicalQuery().GetMaxTasksPerStage()), Settings.ExecuterId);
        }

        if (graph->GetPreparedQuery().GetPhysicalQuery().GetDisableCheckpoints()) {
            return Actors;
        }

        NFq::TCheckpointCoordinatorSettings coordinatorSettings;
        coordinatorSettings.ProviderIntegrations = CheckpointProviderIntegrations;
        if (const auto& checkpointInterval = context->CheckpointInterval) {
            coordinatorSettings.SetCheckpointingPeriod(*checkpointInterval);
        }

        const auto streamingDisposition = MakeStreamingDisposition(*context, coordinatorSettings);
        const auto stateLoadMode = graph->GetZeroCheckpointSaved()
            ? FederatedQuery::FROM_LAST_CHECKPOINT
            : FederatedQuery::EMPTY;
        const bool restoreOffsetsFromForeignCheckpoint =
            (stateLoadMode == FederatedQuery::StateLoadMode::EMPTY && streamingDisposition.has_from_last_checkpoint())
            || RescalingChangedTaskCount;

        auto counters = Settings.KqpCounters;
        if (AppData()->FeatureFlags.GetEnableStreamingQueriesCounters() && !context->StreamingQueryPath.empty()) {
            counters = counters->GetSubgroup("host", "")->GetSubgroup("path", context->StreamingQueryPath);
        }

        const auto generation = context->CurrentExecutionGeneration;
        Y_VALIDATE(generation, "Missing current execution generation");

        Actors.CheckpointCoordinator = TActivationContext::Register(MakeCheckpointCoordinator(
            NFq::TCoordinatorId(context->CheckpointId, generation),
            NYql::NDq::MakeCheckpointStorageID(),
            Settings.ExecuterId,
            coordinatorSettings,
            counters,
            graphParams,
            stateLoadMode,
            streamingDisposition,
            restoreOffsetsFromForeignCheckpoint
        ).Release(), Settings.ExecuterId);

        YDB_LOG_DEBUG("Created new CheckpointCoordinator",
            {"txId", Settings.TxId},
            {"ctx", *context},
            {"checkpointCoordinatorId", Actors.CheckpointCoordinator},
            {"stateLoadMode", FederatedQuery::StateLoadMode_Name(stateLoadMode)},
            {"streamingDisposition", streamingDisposition.ShortDebugString()},
            {"enableWatermarks", graph->GetPreparedQuery().GetPhysicalQuery().GetEnableWatermarks()});

        return Actors;
    }

    void OnComputeActorsStarted(TVector<TTaskInfo> tasks, bool checkpointsEnabled) override {
        auto makeEvent = [&tasks]() {
            auto event = std::make_unique<NFq::TEvCheckpointCoordinator::TEvReadyState>();
            event->Tasks.reserve(tasks.size());
            for (const auto& task : tasks) {
                event->Tasks.push_back({
                    .Id = task.Id,
                    .IsCheckpointingEnabled = task.IsCheckpointingEnabled,
                    .IsIngress = task.IsIngress,
                    .IsEgress = task.IsEgress,
                    .HasState = task.HasState,
                    .ActorId = task.ActorId,
                });
            }
            return event;
        };

        if (Actors.NodesManager) {
            Send(Actors.NodesManager, makeEvent().release());
        }
        if (Actors.CheckpointCoordinator && checkpointsEnabled) {
            Send(Actors.CheckpointCoordinator, makeEvent().release());
        }
    }

    bool IsOwnEvent(const NActors::IEventHandle& ev) const override {
        switch (ev.GetTypeRewrite()) {
            case NFq::TEvCheckpointCoordinator::TEvZeroCheckpointDone::EventType:
            case NFq::TEvCheckpointCoordinator::TEvRaiseTransientIssues::EventType:
                return true;
            default:
                return false;
        }
    }

    void Handle(TAutoPtr<NActors::IEventHandle>& ev) override {
        switch (ev->GetTypeRewrite()) {
            case NFq::TEvCheckpointCoordinator::TEvZeroCheckpointDone::EventType: {
                YDB_LOG_DEBUG("Coordinator saved zero checkpoint", {"txId", Settings.TxId});
                Send(Actors.CheckpointCoordinator, new NFq::TEvCheckpointCoordinator::TEvRunGraph());
                if (const auto& context = Settings.UserRequestContext) {
                    TActivationContext::Send(ev->Forward(context->RunScriptActorId));
                }
                break;
            }
            case NFq::TEvCheckpointCoordinator::TEvRaiseTransientIssues::EventType: {
                const auto* msg = ev->Get<NFq::TEvCheckpointCoordinator::TEvRaiseTransientIssues>();
                YDB_LOG_NOTICE("TEvRaiseTransientIssues from checkpoint coordinator",
                    {"txId", Settings.TxId},
                    {"transientIssues", msg->TransientIssues.ToOneLineString()});
                break;
            }
            default:
                Y_ABORT("Unexpected streaming query event: %" PRIu32, ev->GetTypeRewrite());
        }
    }

    void Terminate() override {
        if (Actors.NodesManager) {
            Send(Actors.NodesManager, new NActors::TEvents::TEvPoisonPill());
        }

        if (Actors.CheckpointCoordinator) {
            Send(Actors.CheckpointCoordinator, new NActors::TEvents::TEvPoisonPill());

            const auto& context = Settings.UserRequestContext;
            if (AppData()->FeatureFlags.GetEnableStreamingQueriesCounters() && context && !context->StreamingQueryPath.empty()) {
                Settings.KqpCounters->GetSubgroup("host", "")->RemoveSubgroup("path", context->StreamingQueryPath);
            }
        }

        Actors = {};
    }

private:
    const std::shared_ptr<NKikimrKqp::TQueryPhysicalGraph>& MutableGraph() {
        // The saved graph is shared with the session and must not be patched in place.
        if (!PatchedGraph) {
            PatchedGraph = std::make_shared<NKikimrKqp::TQueryPhysicalGraph>(*Settings.Graph);
        }
        return PatchedGraph;
    }

    void Send(const NActors::TActorId& recipient, NActors::IEventBase* ev) const {
        TActivationContext::Send(new NActors::IEventHandle(recipient, Settings.ExecuterId, ev));
    }

    NFq::NProto::TGraphParams MakeGraphParams(const NKikimrKqp::TQueryPhysicalGraph& graph) const {
        const auto& physicalQuery = graph.GetPreparedQuery().GetPhysicalQuery();
        const auto& userToken = Settings.UserToken;

        NFq::NProto::TGraphParams graphParams;
        for (const auto& task : graph.GetTasks()) {
            auto& checkpointTask = *graphParams.AddTasks();
            checkpointTask = task.GetDqTask();
            checkpointTask.ClearSecureParams();

            auto& requestContext = *checkpointTask.MutableRequestContext();
            requestContext["Database"] = Settings.Database;
            requestContext["UserSID"] = userToken ? userToken->GetUserSID() : TString();
            requestContext["UserGroupSIDs"] = SequenceToJsonString(userToken ? userToken->GetGroupSIDs() : TVector<NACLib::TSID>{});

            const auto& stage = physicalQuery.GetTransactions(task.GetTxId()).GetStages(checkpointTask.GetStageId());
            const auto& program = stage.GetProgram();
            graphParams.MutableStageProgram()->try_emplace(checkpointTask.GetStageId(), program.GetRaw());
            checkpointTask.MutableProgram()->SetRuntimeVersion(program.GetRuntimeVersion());

            for (const auto& input : stage.GetSources()) {
                const auto& externalSource = input.GetExternalSource();
                NYql::NPq::NProto::TDqPqTopicSource source;
                if (externalSource.GetType() == "PqSource" && externalSource.GetSettings().UnpackTo(&source)) {
                    (*checkpointTask.MutableSecureParams())[source.GetToken().GetName()] = NYql::CreateStructuredTokenParser(externalSource.GetAuthInfo()).ToBuilder().RemoveSecrets().ToJson();
                }
            }

            for (const auto& output : stage.GetSinks()) {
                const auto& externalSink = output.GetExternalSink();
                NYql::NPq::NProto::TDqPqTopicSink sink;
                if (externalSink.GetType() == "PqSink" && externalSink.GetSettings().UnpackTo(&sink) && sink.GetDeferredPublicationExtIdPrefix()) {
                    (*checkpointTask.MutableSecureParams())[sink.GetToken().GetName()] = NYql::CreateStructuredTokenParser(externalSink.GetAuthInfo()).ToBuilder().RemoveSecrets().ToJson();
                }
            }
        }

        return graphParams;
    }

private:
    const TSettings Settings;
    const NYql::IPqGatewayFactory::TPtr PqGatewayFactory;
    const NFq::TCheckpointProviderIntegrations CheckpointProviderIntegrations;

    std::shared_ptr<NKikimrKqp::TQueryPhysicalGraph> PatchedGraph;
    bool RescalingChangedTaskCount = false;
    TStartedActors Actors;
};

class TStreamingQueryControllerFactory : public IKqpStreamingQueryControllerFactory {
public:
    TStreamingQueryControllerFactory(NYql::IPqGatewayFactory::TPtr pqGatewayFactory,
        NFq::TCheckpointProviderIntegrations checkpointProviderIntegrations)
        : PqGatewayFactory(std::move(pqGatewayFactory))
        , CheckpointProviderIntegrations(std::move(checkpointProviderIntegrations))
    {}

    std::unique_ptr<IKqpStreamingQueryController> Create(TSettings settings) const override {
        if (!settings.Graph && !settings.SaveGraph) {
            return nullptr;
        }
        return std::make_unique<TStreamingQueryController>(std::move(settings), PqGatewayFactory, CheckpointProviderIntegrations);
    }

private:
    const NYql::IPqGatewayFactory::TPtr PqGatewayFactory;
    const NFq::TCheckpointProviderIntegrations CheckpointProviderIntegrations;
};

} // anonymous namespace

IKqpStreamingQueryControllerFactory::TPtr CreateStreamingQueryControllerFactory(
    NYql::IPqGatewayFactory::TPtr pqGatewayFactory,
    NFq::TCheckpointProviderIntegrations checkpointProviderIntegrations)
{
    return std::make_shared<TStreamingQueryControllerFactory>(std::move(pqGatewayFactory), std::move(checkpointProviderIntegrations));
}

} // namespace NKikimr::NKqp
