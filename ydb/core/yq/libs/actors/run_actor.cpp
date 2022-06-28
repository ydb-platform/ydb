#include "proxy.h"
#include "clusters_from_connections.h"
#include "system_clusters.h"
#include "table_bindings_from_bindings.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/providers/dq/actors/executer_actor.h>
#include <ydb/library/yql/providers/dq/actors/proto_builder.h>
#include <ydb/library/yql/providers/dq/actors/task_controller.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_provider.h>
#include <ydb/library/yql/providers/dq/provider/exec/yql_dq_exectransformer.h>
#include <ydb/library/yql/providers/dq/interface/yql_dq_task_transform.h>
#include <ydb/library/yql/providers/pq/gateway/native/yql_pq_gateway.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_provider.h>
#include <ydb/library/yql/providers/pq/task_meta/task_meta.h>
#include <ydb/library/yql/providers/s3/provider/yql_s3_provider.h>
#include <ydb/library/yql/providers/ydb/provider/yql_ydb_provider.h>
#include <ydb/library/yql/providers/clickhouse/provider/yql_clickhouse_provider.h>
#include <ydb/library/yql/providers/solomon/gateway/yql_solomon_gateway.h>
#include <ydb/library/yql/providers/solomon/provider/yql_solomon_provider.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>
#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/providers/common/codec/yql_codec.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/public/issue/protos/issue_message.pb.h>

#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/core/protos/services.pb.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>
#include <ydb/core/yq/libs/common/entity_id.h>
#include <ydb/core/yq/libs/db_id_async_resolver_impl/db_async_resolver_impl.h>
#include <ydb/core/yq/libs/actors/nodes_manager.h>
#include <ydb/core/yq/libs/gateway/empty_gateway.h>
#include <ydb/core/yq/libs/read_rule/read_rule_creator.h>
#include <ydb/core/yq/libs/read_rule/read_rule_deleter.h>
#include <ydb/core/yq/libs/tasks_packer/tasks_packer.h>
#include <util/system/hostname.h>

#include <library/cpp/json/yson/json2yson.h>

#include <ydb/core/yq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/yq/libs/control_plane_storage/events/events.h>
#include <google/protobuf/util/time_util.h>

#include <util/string/split.h>
#include <ydb/core/yq/libs/checkpointing/checkpoint_coordinator.h>
#include <ydb/core/yq/libs/checkpointing_common/defs.h>
#include <ydb/core/yq/libs/checkpoint_storage/storage_service.h>
#include <ydb/core/yq/libs/private_client/private_client.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, Params.QueryId << " RunActor : " << stream)

#define LOG_D(stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PROXY, Params.QueryId << " RunActor : " << stream)

namespace NYq {

using namespace NActors;
using namespace NYql;
using namespace NDqs;

class TRunActor : public NActors::TActorBootstrapped<TRunActor> {
public:
    explicit TRunActor(
        const NActors::TActorId& fetcherId
        , const ::NYql::NCommon::TServiceCounters& queryCounters
        , TRunActorParams&& params)
        : FetcherId(fetcherId)
        , Params(std::move(params))
        , CreatedAt(Params.CreatedAt)
        , QueryCounters(queryCounters)
        , EnableCheckpointCoordinator(Params.QueryType == YandexQuery::QueryContent::STREAMING && Params.CheckpointCoordinatorConfig.GetEnabled())
        , MaxTasksPerOperation(Params.CommonConfig.GetMaxTasksPerOperation() ? Params.CommonConfig.GetMaxTasksPerOperation() : 40)
    {
        QueryCounters.SetUptimePublicAndServiceCounter(0);
    }

    static constexpr char ActorName[] = "YQ_RUN_ACTOR";

    void Bootstrap() {
        LOG_D("Start run actor. Compute state: " << YandexQuery::QueryMeta::ComputeStatus_Name(Params.Status));
        QueryCounters.SetUptimePublicAndServiceCounter((TInstant::Now() - CreatedAt).Seconds());
        LogReceivedParams();
        Pinger = Register(
            CreatePingerActor(
                Params.TenantName,
                Params.Scope,
                Params.UserId,
                Params.QueryId,
                Params.Owner,
                SelfId(),
                Params.PingerConfig,
                Params.Deadline,
                QueryCounters,
                CreatedAt
                ));
        Become(&TRunActor::StateFuncWrapper<&TRunActor::StateFunc>);
        try {
            Run();
        } catch (const std::exception&) {
            FailOnException();
        }
    }

private:
    template <void (TRunActor::* DelegatedStateFunc)(STFUNC_SIG)>
    STFUNC(StateFuncWrapper) {
        try {
            (this->*DelegatedStateFunc)(ev, ctx);
        } catch (...) {
            FailOnException();
        }
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvents::TEvAsyncContinue, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(TEvents::TEvGraphParams, Handle);
        hFunc(TEvents::TEvDataStreamsReadRulesCreationResult, Handle);
        hFunc(NYql::NDqs::TEvQueryResponse, Handle);
        hFunc(TEvents::TEvQueryActionResult, Handle);
        hFunc(TEvents::TEvForwardPingResponse, Handle);
        hFunc(TEvCheckpointCoordinator::TEvZeroCheckpointDone, Handle);
        hFunc(TEvents::TEvRaiseTransientIssues, Handle);
    )

    STRICT_STFUNC(FinishStateFunc,
        hFunc(TEvents::TEvDataStreamsReadRulesCreationResult, HandleFinish);
        hFunc(TEvents::TEvDataStreamsReadRulesDeletionResult, HandleFinish);
        hFunc(NYql::NDqs::TEvQueryResponse, HandleFinish);
        hFunc(TEvents::TEvForwardPingResponse, HandleFinish);

        // Ignore tail of action events after normal work.
        IgnoreFunc(TEvents::TEvAsyncContinue);
        IgnoreFunc(NActors::TEvents::TEvUndelivered);
        IgnoreFunc(TEvents::TEvGraphParams);
        IgnoreFunc(TEvents::TEvQueryActionResult);
        IgnoreFunc(TEvCheckpointCoordinator::TEvZeroCheckpointDone);
        IgnoreFunc(TEvents::TEvRaiseTransientIssues);
    )

    void KillExecuter() {
        if (ExecuterId) {
            Send(ExecuterId, new NActors::TEvents::TEvPoison());

            // Clear finished actors ids
            ExecuterId = {};
            CheckpointCoordinatorId = {};
            ControlId = {};
        }
    }

    void KillChildrenActors() {
        if (ReadRulesCreatorId) {
            Send(ReadRulesCreatorId, new NActors::TEvents::TEvPoison());
        }

        KillExecuter();
    }

    void CancelRunningQuery() {
        if (ReadRulesCreatorId) {
            LOG_D("Cancel read rules creation");
            Send(ReadRulesCreatorId, new NActors::TEvents::TEvPoison());
        }

        if (ControlId) {
            LOG_D("Cancel running query");
            Send(ControlId, new NDq::TEvDq::TEvAbortExecution(NYql::NDqProto::StatusIds::ABORTED, YandexQuery::QueryMeta::ComputeStatus_Name(FinalQueryStatus)));
        } else {
            QueryResponseArrived = true;
        }
    }

    void PassAway() override {
        Send(FetcherId, new NActors::TEvents::TEvPoisonTaken());
        KillChildrenActors();
        NActors::TActorBootstrapped<TRunActor>::PassAway();
    }

    void Run() {
        if (!Params.DqGraphs.empty() && Params.Status != YandexQuery::QueryMeta::STARTING) {
            FillDqGraphParams();
        }

        switch (Params.Status) {
        case YandexQuery::QueryMeta::ABORTING_BY_USER:
        case YandexQuery::QueryMeta::ABORTING_BY_SYSTEM:
        case YandexQuery::QueryMeta::FAILING:
        case YandexQuery::QueryMeta::COMPLETING:
            FinalizingStatusIsWritten = true;
            Finish(GetFinalStatusFromFinalizingStatus(Params.Status));
            break;
        case YandexQuery::QueryMeta::STARTING:
            HandleConnections();
            RunProgram();
            break;
        case YandexQuery::QueryMeta::RESUMING:
        case YandexQuery::QueryMeta::RUNNING:
            ReRunQuery();
            break;
        default:
            Abort("Fail to start query from unexpected status " + YandexQuery::QueryMeta::ComputeStatus_Name(Params.Status), YandexQuery::QueryMeta::FAILED);
            break;
        }
    }

    void HandleConnections() {
        LOG_D("HandleConnections");

        for (const auto& connection : Params.Connections) {
            if (!connection.content().name()) {
                LOG_D("Connection with empty name " << connection.meta().id());
                continue;
            }
            YqConnections.emplace(connection.meta().id(), connection);
        }
    }

    void RunProgram() {
        LOG_D("RunProgram");
        if (!CompileQuery()) {
            Abort("Failed to compile query", YandexQuery::QueryMeta::FAILED);
        }
    }

    void FailOnException() {
        Fail(CurrentExceptionMessage());
    }

    void Fail(const TString& errorMessage) {
        LOG_E("Fail for query " << Params.QueryId << ", finishing: " << Finishing << ", details: " << errorMessage);

        if (YqConnections.empty()) {
            Issues.AddIssue("YqConnections array is empty");
        }

        if (!Finishing) {
            Abort("Internal Error", YandexQuery::QueryMeta::FAILED);
            return;
        }

        // Already finishing. Fail instantly.
        Issues.AddIssue("Internal Error");

        if (!ConsumersAreDeleted) {
            for (const Yq::Private::TopicConsumer& c : Params.CreatedTopicConsumers) {
                TransientIssues.AddIssue(TStringBuilder() << "Created read rule `" << c.consumer_name() << "` for topic `" << c.topic_path() << "` (database id " << c.database_id() << ") maybe was left undeleted: internal error occurred");
                TransientIssues.back().Severity = NYql::TSeverityIds::S_WARNING;
            }
        }

        // If target status was successful, change it to failed because we are in internal error handler.
        if (QueryStateUpdateRequest.status() == YandexQuery::QueryMeta::COMPLETED || QueryStateUpdateRequest.status() == YandexQuery::QueryMeta::PAUSED) {
            QueryStateUpdateRequest.set_status(YandexQuery::QueryMeta::FAILED);
            QueryStateUpdateRequest.set_status_code(NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }

        SendPingAndPassAway();
    }

    void Handle(TEvents::TEvQueryActionResult::TPtr& ev) {
        Action = ev->Get()->Action;
        LOG_D("New query action received: " << YandexQuery::QueryAction_Name(Action));
        switch (Action) {
        case YandexQuery::ABORT:
        case YandexQuery::ABORT_GRACEFULLY: // not fully implemented
            // ignore issues in case of controlled abort
            Finish(YandexQuery::QueryMeta::ABORTED_BY_USER);
            break;
        case YandexQuery::PAUSE: // not implemented
        case YandexQuery::PAUSE_GRACEFULLY: // not implemented
        case YandexQuery::RESUME: // not implemented
            Abort(TStringBuilder() << "Unsupported query action: " << YandexQuery::QueryAction_Name(Action), YandexQuery::QueryMeta::FAILED);
            break;
        default:
            Abort(TStringBuilder() << "Unknown query action: " << YandexQuery::QueryAction_Name(Action), YandexQuery::QueryMeta::FAILED);
            break;
        }
    }

    void CheckForConsumers() {
        struct TTopicIndependentConsumers {
            struct TTopicIndependentConsumer {
                TString ConsumerName;
                std::vector<NYql::NPq::TTopicPartitionsSet> PartitionsSets;
            };

            std::pair<TString, bool> AddPartitionsSet(const TMaybe<NYql::NPq::TTopicPartitionsSet>& set, const TString& consumerNamePrefix) {
                if (!ConsumerNamePrefix) { // Init
                    ConsumerNamePrefix = consumerNamePrefix;
                }

                if (!set) {
                    return {AddNewConsumer(set), true};
                }

                for (TTopicIndependentConsumer& consumer : IndependentConsumers) {
                    if (!consumer.PartitionsSets.empty()) {
                        bool intersects = false;
                        for (const NYql::NPq::TTopicPartitionsSet& consumerSet : consumer.PartitionsSets) {
                            if (consumerSet.Intersects(*set)) {
                                intersects = true;
                                break;
                            }
                        }
                        if (!intersects) {
                            consumer.PartitionsSets.push_back(*set);
                            return {consumer.ConsumerName, false};
                        }
                    }
                }
                return {AddNewConsumer(set), true};
            }

            TString AddNewConsumer(const TMaybe<NYql::NPq::TTopicPartitionsSet>& set) {
                TTopicIndependentConsumer& c = IndependentConsumers.emplace_back();
                c.ConsumerName = IndependentConsumers.size() == 1 ? ConsumerNamePrefix : TStringBuilder() << ConsumerNamePrefix << '-' << IndependentConsumers.size();
                if (set) {
                    c.PartitionsSets.push_back(*set);
                }
                return c.ConsumerName;
            }

            TString ConsumerNamePrefix;
            std::vector<TTopicIndependentConsumer> IndependentConsumers;
        };

        THashMap<TString, TTopicIndependentConsumers> topicToIndependentConsumers;
        ui32 graphIndex = 0;
        for (auto& graphParams : DqGraphParams) {
            LOG_D("Graph " << graphIndex);
            graphIndex++;
            const TString consumerNamePrefix = graphIndex == 1 ? Params.QueryId : TStringBuilder() << Params.QueryId << '-' << graphIndex; // Simple name in simple case
            const auto& secureParams = graphParams.GetSecureParams();
            for (NYql::NDqProto::TDqTask& task : *graphParams.MutableTasks()) {
                for (NYql::NDqProto::TTaskInput& taskInput : *task.MutableInputs()) {
                    if (taskInput.GetTypeCase() == NYql::NDqProto::TTaskInput::kSource && taskInput.GetSource().GetType() == "PqSource") {
                        google::protobuf::Any& settingsAny = *taskInput.MutableSource()->MutableSettings();
                        YQL_ENSURE(settingsAny.Is<NYql::NPq::NProto::TDqPqTopicSource>());
                        NYql::NPq::NProto::TDqPqTopicSource srcDesc;
                        YQL_ENSURE(settingsAny.UnpackTo(&srcDesc));

                        if (!srcDesc.GetConsumerName()) {
                            const auto [consumerName, isNewConsumer] =
                                topicToIndependentConsumers[srcDesc.GetTopicPath()]
                                    .AddPartitionsSet(NYql::NPq::GetTopicPartitionsSet(task.GetMeta()), consumerNamePrefix);
                            srcDesc.SetConsumerName(consumerName);
                            settingsAny.PackFrom(srcDesc);
                            if (isNewConsumer) {
                                auto s = consumerName;
                                LOG_D("Create consumer \"" << s << "\" for topic \"" << srcDesc.GetTopicPath() << "\"");
                                if (const TString& tokenName = srcDesc.GetToken().GetName()) {
                                    const auto token = secureParams.find(tokenName);
                                    YQL_ENSURE(token != secureParams.end(), "Token " << tokenName << " was not found in secure params");
                                    CredentialsForConsumersCreation.emplace_back(
                                        CreateCredentialsProviderFactoryForStructuredToken(Params.CredentialsFactory, token->second, srcDesc.GetAddBearerToToken()));
                                } else {
                                    CredentialsForConsumersCreation.emplace_back(NYdb::CreateInsecureCredentialsProviderFactory());
                                }

                                TopicsForConsumersCreation.emplace_back(std::move(srcDesc));
                            }
                        }
                    }
                }
            }
        }
    }

    void Handle(TEvents::TEvForwardPingResponse::TPtr& ev) {
        LOG_D("Forward ping response. Success: " << ev->Get()->Success << ". Cookie: " << ev->Cookie);
        if (!ev->Get()->Success) { // Failed setting new status or lease was lost
            ResignQuery(NYql::NDqProto::StatusIds::UNAVAILABLE);
            return;
        }

        if (ev->Cookie == SaveQueryInfoCookie) {
            if (TopicsForConsumersCreation.size()) {
                ReadRulesCreatorId = Register(
                    ::NYq::MakeReadRuleCreatorActor(
                        SelfId(),
                        Params.QueryId,
                        Params.YqSharedResources->UserSpaceYdbDriver,
                        std::move(TopicsForConsumersCreation),
                        std::move(CredentialsForConsumersCreation)
                    )
                );
            } else {
                RunDqGraphs();
            }
        } else if (ev->Cookie == SetLoadFromCheckpointModeCookie) {
            Send(CheckpointCoordinatorId, new TEvCheckpointCoordinator::TEvRunGraph());
        }
    }

    void HandleFinish(TEvents::TEvForwardPingResponse::TPtr& ev) {
        LOG_D("Forward ping response. Success: " << ev->Get()->Success << ". Cookie: " << ev->Cookie);
        if (!ev->Get()->Success) { // Failed setting new status or lease was lost
            Fail("Failed to write finalizing status");
            return;
        }

        if (ev->Cookie == SaveFinalizingStatusCookie) {
            FinalizingStatusIsWritten = true;
            ContinueFinish();
        }
    }

    TString CheckLimitsOfDqGraphs() {
        size_t dqTasks = 0;
        for (const auto& dqGraph : DqGraphParams) {
            dqTasks += dqGraph.TasksSize();
        }
        LOG_D("Overall dq tasks: " << dqTasks);
        if (dqTasks > MaxTasksPerOperation) {
            return TStringBuilder() << "Too many tasks per operation: " << dqTasks << ". Allowed: less than " << MaxTasksPerOperation;
        }
        return "";
    }

    bool AbortOnExceedingDqGraphsLimits() {
        TString errorMsg = CheckLimitsOfDqGraphs();
        if (errorMsg) {
            Abort(errorMsg, YandexQuery::QueryMeta::FAILED, Program->Issues());
            return true;
        }
        return false;
    }

    void Handle(TEvents::TEvGraphParams::TPtr& ev) {
        LOG_D("Graph params with tasks: " << ev->Get()->GraphParams.TasksSize());
        DqGraphParams.push_back(ev->Get()->GraphParams);
    }

    void Handle(TEvCheckpointCoordinator::TEvZeroCheckpointDone::TPtr&) {
        LOG_D("Coordinator saved zero checkpoint");
        Y_VERIFY(CheckpointCoordinatorId);
        SetLoadFromCheckpointMode();
    }

    void Handle(TEvents::TEvRaiseTransientIssues::TPtr& ev) {
        Yq::Private::PingTaskRequest request;

        NYql::IssuesToMessage(ev->Get()->TransientIssues, request.mutable_transient_issues());

        Send(Pinger, new TEvents::TEvForwardPingRequest(request), 0, RaiseTransientIssuesCookie);
    }

    i32 UpdateResultIndices() {
        i32 count = 0;
        for (const auto& graphParams : DqGraphParams) {
            DqGrapResultIndices.push_back(graphParams.GetResultType() ? count++ : -1);
        }
        return count;
    }

    void UpdateAstAndPlan() {
        Yq::Private::PingTaskRequest request;

        TStringStream exprOut;
        TStringStream planOut;
        Program->Print(&exprOut, &planOut);
        const auto planStr = NJson2Yson::ConvertYson2Json(planOut.Str());
        request.set_ast(exprOut.Str());
        request.set_plan(planStr);

        Send(Pinger, new TEvents::TEvForwardPingRequest(request));
    }

    void PrepareGraphs() {
        if (AbortOnExceedingDqGraphsLimits()) {
            return;
        }

        Yq::Private::PingTaskRequest request;

        request.set_result_set_count(UpdateResultIndices());
        QueryStateUpdateRequest.set_result_set_count(UpdateResultIndices());
        for (const auto& graphParams : DqGraphParams) {
            if (graphParams.GetResultType()) {
                TProtoBuilder builder(graphParams.GetResultType(), {graphParams.GetColumns().begin(), graphParams.GetColumns().end()});
                const auto emptyResultSet = builder.BuildResultSet({});
                auto* header = QueryStateUpdateRequest.add_result_set_meta();
                (*header->mutable_column()) = emptyResultSet.columns();
            }
        }
        *request.mutable_result_set_meta() = QueryStateUpdateRequest.result_set_meta();

        CheckForConsumers();

        Params.CreatedTopicConsumers.clear();
        Params.CreatedTopicConsumers.reserve(TopicsForConsumersCreation.size());
        for (const NYql::NPq::NProto::TDqPqTopicSource& src : TopicsForConsumersCreation) {
            auto& consumer = *request.add_created_topic_consumers();
            consumer.set_database_id(src.GetDatabaseId());
            consumer.set_database(src.GetDatabase());
            consumer.set_topic_path(src.GetTopicPath());
            consumer.set_consumer_name(src.GetConsumerName());
            consumer.set_cluster_endpoint(src.GetEndpoint());
            consumer.set_use_ssl(src.GetUseSsl());
            consumer.set_token_name(src.GetToken().GetName());
            consumer.set_add_bearer_to_token(src.GetAddBearerToToken());

            // Save for deletion
            Params.CreatedTopicConsumers.push_back(consumer);
        }

        for (const auto& graphParams : DqGraphParams) {
            request.add_dq_graph(graphParams.SerializeAsString());
        }

        Send(Pinger, new TEvents::TEvForwardPingRequest(request), 0, SaveQueryInfoCookie);
    }

    void SetLoadFromCheckpointMode() {
        Yq::Private::PingTaskRequest request;
        request.set_state_load_mode(YandexQuery::FROM_LAST_CHECKPOINT);
        request.mutable_disposition()->mutable_from_last_checkpoint();

        Send(Pinger, new TEvents::TEvForwardPingRequest(request), 0, SetLoadFromCheckpointModeCookie);
    }

    TString BuildNormalizedStatistics(const NDqProto::TQueryResponse& response) {

        struct TStatisticsNode {
            std::map<TString, TStatisticsNode> Children;
            i64 Avg;
            i64 Count;
            i64 Min;
            i64 Max;
            i64 Sum;
            void Write(NYson::TYsonWriter& writer) {
                writer.OnBeginMap();
                if (Children.empty()) {
                        writer.OnKeyedItem("sum");
                        writer.OnInt64Scalar(Sum);
                        writer.OnKeyedItem("count");
                        writer.OnInt64Scalar(Count);
                        writer.OnKeyedItem("avg");
                        writer.OnInt64Scalar(Avg);
                        writer.OnKeyedItem("max");
                        writer.OnInt64Scalar(Max);
                        writer.OnKeyedItem("min");
                        writer.OnInt64Scalar(Min);
                } else {
                    for (auto& [name, child]: Children) {
                        writer.OnKeyedItem(name);
                        child.Write(writer);
                    }
                }
                writer.OnEndMap();
            }
        };

        TStringStream out;

        TStatisticsNode statistics;
        for (const auto& metric : response.GetMetric()) {
            auto longName = metric.GetName();
            TString prefix;
            TString name;
            std::map<TString, TString> labels;
            if (!NYql::NCommon::ParseCounterName(&prefix, &labels, &name, longName)) {
                prefix = "";
                name = longName;
                labels.clear();
            }

            TStatisticsNode* node = &statistics;

            if (prefix) {
                node = &node->Children[prefix];
            }

            for (const auto& [k, v] : labels) {
                node = &node->Children[k + "=" + v];
            }

            node = &node->Children[name];

            node->Sum = metric.GetSum();
            node->Count = metric.GetCount();
            node->Avg = metric.GetAvg();
            node->Max = metric.GetMax();
            node->Min = metric.GetMin();
        }

        NYson::TYsonWriter writer(&out);
        statistics.Write(writer);

        return out.Str();
    }

    void SaveStatistics(const NYql::NDqProto::TQueryResponse& result) {
        // Yson routines are very strict, so it's better to try-catch them
        try {
            Statistics.push_back(BuildNormalizedStatistics(result));
            TStringStream out;
            NYson::TYsonWriter writer(&out);
            writer.OnBeginMap();
            ui32 graphIndex = 0;
            for (const auto& s : Statistics) {
                writer.OnKeyedItem("Graph=" + ToString(++graphIndex));
                writer.OnRaw(s);
            }
            writer.OnEndMap();
            QueryStateUpdateRequest.set_statistics(NJson2Yson::ConvertYson2Json(out.Str()));
        } catch (NYson::TYsonException& ex) {
            LOG_E(ex.what());
        }
    }

    void AddIssues(const google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& issuesProto) {
        TIssues issues;
        IssuesFromMessage(issuesProto, issues);
        Issues.AddIssues(issues);
    }

    void SaveQueryResponse(NYql::NDqs::TEvQueryResponse::TPtr& ev) {
        auto& result = ev->Get()->Record;
        LOG_D("Query response. Retryable: " << result.GetDeprecatedRetriable()
            << ". Result set index: " << DqGraphIndex
            << ". Issues count: " << result.IssuesSize()
            << ". Rows count: " << result.GetRowsCount());

        AddIssues(result.issues());

        if (Finishing && !result.issues_size()) { // Race between abort and successful finishing. Override with success and provide results to user.
            FinalQueryStatus = YandexQuery::QueryMeta::COMPLETED;
            Issues.Clear();
        }

        auto resultSetIndex = DqGrapResultIndices.at(DqGraphIndex);
        if (resultSetIndex >= 0) {
            auto& header = *QueryStateUpdateRequest.mutable_result_set_meta(resultSetIndex);
            header.set_truncated(result.GetTruncated());
            header.set_rows_count(result.GetRowsCount());
        }

        QueryStateUpdateRequest.mutable_result_id()->set_value(Params.ResultId);

        SaveStatistics(result);

        KillExecuter();
    }

    void Handle(NYql::NDqs::TEvQueryResponse::TPtr& ev) {
        SaveQueryResponse(ev);

        const bool failure = Issues.Size() > 0;
        {
            auto statusCode = ev->Get()->Record.GetStatusCode();
            if (statusCode == NYql::NDqProto::StatusIds::UNSPECIFIED
                || (failure != (ev->Get()->Record.GetStatusCode() != NYql::NDqProto::StatusIds::SUCCESS))
            ) {
                QueryCounters.Counters->GetCounter(NYql::NDqProto::StatusIds_StatusCode_Name(statusCode), false)->Inc();
            }
        }
        const bool finalize = failure || DqGraphIndex + 1 >= static_cast<i32>(DqGraphParams.size());
        if (finalize) {

            if (failure) {
                ResignQuery(ev->Get()->Record.GetStatusCode());
                return;
            }

            Finish(GetFinishStatus(!failure));
            return;
        }

        // Continue with the next graph
        QueryStateUpdateRequest.set_dq_graph_index(++DqGraphIndex);
        RunNextDqGraph();
        LOG_D("Send save query response request to pinger");
        Send(Pinger, new TEvents::TEvForwardPingRequest(QueryStateUpdateRequest));
    }

    void HandleFinish(NYql::NDqs::TEvQueryResponse::TPtr& ev) {
        // In this case we can have race between normal finishing of running query and aborting it.
        // If query is finished with success error code or failure != abort, we override abort with this result.
        // This logic is located in SaveQueryResponse() method.
        if (ev->Get()->Record.GetStatusCode() != NYql::NDqProto::StatusIds::SUCCESS) {
            ev->Get()->Record.SetStatusCode(NYql::NDqProto::StatusIds::CANCELLED);
        }
        ev->Get()->Record.SetDeprecatedRetriable(false); // User aborted => don't retry, only show issues

        QueryResponseArrived = true;
        SaveQueryResponse(ev);

        ContinueFinish();
    }

    void Handle(TEvents::TEvDataStreamsReadRulesCreationResult::TPtr& ev) {
        LOG_D("Read rules creation finished. Issues: " << ev->Get()->Issues.Size());
        ReadRulesCreatorId = {};
        if (ev->Get()->Issues) {
            AddIssueWithSubIssues("Problems with read rules creation", ev->Get()->Issues);
            LOG_D(Issues.ToOneLineString());
            Finish(YandexQuery::QueryMeta::FAILED);
        } else {
            RunDqGraphs();
        }
    }

    void HandleFinish(TEvents::TEvDataStreamsReadRulesCreationResult::TPtr& ev) {
        ReadRulesCreatorId = {};
        if (ev->Get()->Issues) {
            TransientIssues.AddIssues(ev->Get()->Issues);
            LOG_D(TransientIssues.ToOneLineString());
        }
        if (CanRunReadRulesDeletionActor()) {
            RunReadRulesDeletionActor();
        }
    }

    void HandleFinish(TEvents::TEvDataStreamsReadRulesDeletionResult::TPtr& ev) {
        ConsumersAreDeleted = true; // Don't print extra warnings.

        if (ev->Get()->TransientIssues) {
            for (const auto& issue : ev->Get()->TransientIssues) {
                TransientIssues.AddIssue(issue);
            }
        }

        ContinueFinish();
    }

    bool NeedDeleteReadRules() const {
        return !Params.CreatedTopicConsumers.empty();
    }

    bool CanRunReadRulesDeletionActor() const {
        return !ReadRulesCreatorId && FinalizingStatusIsWritten && QueryResponseArrived;
    }

    void RunReadRulesDeletionActor() {
        TVector<std::shared_ptr<NYdb::ICredentialsProviderFactory>> credentials;
        credentials.reserve(Params.CreatedTopicConsumers.size());
        for (const Yq::Private::TopicConsumer& c : Params.CreatedTopicConsumers) {
            if (const TString& tokenName = c.token_name()) {
                credentials.emplace_back(
                    CreateCredentialsProviderFactoryForStructuredToken(Params.CredentialsFactory, FindTokenByName(tokenName), c.add_bearer_to_token()));
            } else {
                credentials.emplace_back(NYdb::CreateInsecureCredentialsProviderFactory());
            }
        }

        Register(
            ::NYq::MakeReadRuleDeleterActor(
                SelfId(),
                Params.QueryId,
                Params.YqSharedResources->UserSpaceYdbDriver,
                Params.CreatedTopicConsumers,
                std::move(credentials)
            )
        );
    }

    void RunDqGraphs() {
        if (DqGraphParams.empty()) {
            *QueryStateUpdateRequest.mutable_started_at() = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(CreatedAt.MilliSeconds());
            QueryStateUpdateRequest.set_resign_query(false);
            const bool isOk = Issues.Size() == 0;
            Finish(GetFinishStatus(isOk));
            return;
        }

        {
            Params.Status = YandexQuery::QueryMeta::RUNNING;
            Yq::Private::PingTaskRequest request;
            request.set_status(YandexQuery::QueryMeta::RUNNING);
            *request.mutable_started_at() = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(Now().MilliSeconds());
            Send(Pinger, new TEvents::TEvForwardPingRequest(request), 0, UpdateQueryInfoCookie);
        }

        RunNextDqGraph();
    }

    void RunNextDqGraph() {
        auto& dqGraphParams = DqGraphParams.at(DqGraphIndex);
        TDqConfiguration::TPtr dqConfiguration = MakeIntrusive<TDqConfiguration>();
        dqConfiguration->Dispatch(dqGraphParams.GetSettings());
        dqConfiguration->FreezeDefaults();
        dqConfiguration->FallbackPolicy = "never";

        ExecuterId = NActors::TActivationContext::Register(NYql::NDq::MakeDqExecuter(MakeNodesManagerId(), SelfId(), Params.QueryId, "", dqConfiguration, QueryCounters.Counters, TInstant::Now(), EnableCheckpointCoordinator));

        NActors::TActorId resultId;
        if (dqGraphParams.GetResultType()) {
            TResultId writerResultId;
            {
                writerResultId.HistoryId = Params.QueryId;
                writerResultId.Id = Params.ResultId;
                writerResultId.Owner = Params.Owner;
                writerResultId.SetId = DqGrapResultIndices.at(DqGraphIndex);
            }
            TVector<TString> columns;
            for (const auto& column : dqGraphParams.GetColumns()) {
                columns.emplace_back(column);
            }
            resultId = NActors::TActivationContext::Register(
                    CreateResultWriter(
                        ExecuterId, dqGraphParams.GetResultType(),
                        writerResultId, columns, dqGraphParams.GetSession(), Params.Deadline, Params.ResultBytesLimit));
        } else {
            LOG_D("ResultWriter was NOT CREATED since ResultType is empty");
            resultId = ExecuterId;
        }

        ControlId = NActors::TActivationContext::Register(NYql::MakeTaskController(SessionId, ExecuterId, resultId, dqConfiguration, QueryCounters, TDuration::Seconds(3)).Release());
        if (EnableCheckpointCoordinator) {
            CheckpointCoordinatorId = NActors::TActivationContext::Register(MakeCheckpointCoordinator(
                ::NYq::TCoordinatorId(Params.QueryId + "-" + ToString(DqGraphIndex), Params.PreviousQueryRevision),
                ControlId,
                NYql::NDq::MakeCheckpointStorageID(),
                SelfId(),
                Params.CheckpointCoordinatorConfig,
                QueryCounters.Counters,
                dqGraphParams,
                Params.StateLoadMode,
                Params.StreamingDisposition).Release());
        }

        Yql::DqsProto::ExecuteGraphRequest request;
        request.SetSourceId(dqGraphParams.GetSourceId());
        request.SetResultType(dqGraphParams.GetResultType());
        request.SetSession(dqGraphParams.GetSession());
        *request.MutableSettings() = dqGraphParams.GetSettings();
        *request.MutableSecureParams() = dqGraphParams.GetSecureParams();
        *request.MutableColumns() = dqGraphParams.GetColumns();
        NTasksPacker::UnPack(*request.MutableTask(), dqGraphParams.GetTasks(), dqGraphParams.GetStageProgram());
        NActors::TActivationContext::Send(new IEventHandle(ExecuterId, SelfId(), new NYql::NDqs::TEvGraphRequest(request, ControlId, resultId, CheckpointCoordinatorId)));
        LOG_D("Executer: " << ExecuterId << ", Controller: " << ControlId << ", ResultIdActor: " << resultId << ", CheckPointCoordinatior " << CheckpointCoordinatorId);
    }

    void SetupDqSettings(NYql::TDqGatewayConfig& dqGatewaysConfig) const {
        ::google::protobuf::RepeatedPtrField<::NYql::TAttr>& dqSettings = *dqGatewaysConfig.MutableDefaultSettings();

        // Copy settings from config
        // They are stronger than settings from this function.
        dqSettings = Params.GatewaysConfig.GetDq().GetDefaultSettings();

        THashSet<TString> settingsInConfig;
        for (const auto& s : dqSettings) {
            settingsInConfig.insert(s.GetName());
        }

        auto apply = [&](const TString& name, const TString& value) {
            if (!settingsInConfig.contains(name)) {
                auto* attr = dqSettings.Add();
                attr->SetName(name);
                attr->SetValue(value);
            }
        };

        apply("MaxTasksPerStage", "500");
        apply("MaxTasksPerOperation", ToString(MaxTasksPerOperation));
        apply("EnableComputeActor", "1");
        apply("ComputeActorType", "async");
        apply("_EnablePrecompute", "0"); // TODO: enable together with removing TEmptyGateway

        switch (Params.QueryType) {
        case YandexQuery::QueryContent::STREAMING: {
            // - turn on check that query has one graph.
            apply("_OneGraphPerQuery", "1");
            apply("_TableTimeout", "0");
            apply("_LiteralTimeout", "0");
            break;
        }
        case YandexQuery::QueryContent::ANALYTICS: {
            apply("AnalyticsHopping", "1");
            const TString queryTimeoutMs = ToString(TDuration::Days(7).MilliSeconds());
            apply("_TableTimeout", queryTimeoutMs);
            apply("_LiteralTimeout", queryTimeoutMs);
            break;
        }
        default:
            Y_UNREACHABLE();
        }
    }

    void AddClustersFromConfig(NYql::TGatewaysConfig& gatewaysConfig, THashMap<TString, TString>& clusters) const {
        for (const auto& pq : Params.GatewaysConfig.GetPq().GetClusterMapping()) {
            auto& clusterCfg = *gatewaysConfig.MutablePq()->AddClusterMapping();
            clusterCfg = pq;
            clusters.emplace(clusterCfg.GetName(), PqProviderName);
        }

        for (const auto& solomon : Params.GatewaysConfig.GetSolomon().GetClusterMapping()) {
            auto& clusterCfg = *gatewaysConfig.MutableSolomon()->AddClusterMapping();
            clusterCfg = solomon;
            clusters.emplace(clusterCfg.GetName(), SolomonProviderName);
        }
    }

    YandexQuery::QueryMeta::ComputeStatus GetFinishStatus(bool isOk) const {
        if (isOk) {
            return YandexQuery::QueryMeta::COMPLETED;
        }

        switch (Action) {
        case YandexQuery::PAUSE:
        case YandexQuery::PAUSE_GRACEFULLY:
        case YandexQuery::ABORT:
        case YandexQuery::ABORT_GRACEFULLY:
            return YandexQuery::QueryMeta::ABORTED_BY_USER;
        case YandexQuery::RESUME:
            return YandexQuery::QueryMeta::ABORTED_BY_SYSTEM;
        case YandexQuery::QUERY_ACTION_UNSPECIFIED:
        case YandexQuery::QueryAction_INT_MIN_SENTINEL_DO_NOT_USE_:
        case YandexQuery::QueryAction_INT_MAX_SENTINEL_DO_NOT_USE_:
            return YandexQuery::QueryMeta::FAILED;
        }
    }

    YandexQuery::QueryMeta::ComputeStatus GetFinalizingStatus() { // Status before final. "*ING" one.
        switch (FinalQueryStatus) {
        case YandexQuery::QueryMeta_ComputeStatus_QueryMeta_ComputeStatus_INT_MIN_SENTINEL_DO_NOT_USE_:
        case YandexQuery::QueryMeta_ComputeStatus_QueryMeta_ComputeStatus_INT_MAX_SENTINEL_DO_NOT_USE_:
        case YandexQuery::QueryMeta::COMPUTE_STATUS_UNSPECIFIED:
        case YandexQuery::QueryMeta::STARTING:
        case YandexQuery::QueryMeta::ABORTING_BY_USER:
        case YandexQuery::QueryMeta::ABORTING_BY_SYSTEM:
        case YandexQuery::QueryMeta::RESUMING:
        case YandexQuery::QueryMeta::RUNNING:
        case YandexQuery::QueryMeta::COMPLETING:
        case YandexQuery::QueryMeta::FAILING:
        case YandexQuery::QueryMeta::PAUSING: {
            TStringBuilder msg;
            msg << "\"" << YandexQuery::QueryMeta::ComputeStatus_Name(FinalQueryStatus) << "\" is not a final status for query";
            Issues.AddIssue(msg);
            throw yexception() << msg;
        }

        case YandexQuery::QueryMeta::ABORTED_BY_USER:
            return YandexQuery::QueryMeta::ABORTING_BY_USER;
        case YandexQuery::QueryMeta::ABORTED_BY_SYSTEM:
            return YandexQuery::QueryMeta::ABORTING_BY_SYSTEM;
        case YandexQuery::QueryMeta::COMPLETED:
            return YandexQuery::QueryMeta::COMPLETING;
        case YandexQuery::QueryMeta::FAILED:
            return YandexQuery::QueryMeta::FAILING;
        case YandexQuery::QueryMeta::PAUSED:
            return YandexQuery::QueryMeta::PAUSING;
        }
    }

    static YandexQuery::QueryMeta::ComputeStatus GetFinalStatusFromFinalizingStatus(YandexQuery::QueryMeta::ComputeStatus status) {
        switch (status) {
        case YandexQuery::QueryMeta::ABORTING_BY_USER:
            return YandexQuery::QueryMeta::ABORTED_BY_USER;
        case YandexQuery::QueryMeta::ABORTING_BY_SYSTEM:
            return YandexQuery::QueryMeta::ABORTED_BY_SYSTEM;
        case YandexQuery::QueryMeta::COMPLETING:
            return YandexQuery::QueryMeta::COMPLETED;
        case YandexQuery::QueryMeta::FAILING:
            return YandexQuery::QueryMeta::FAILED;
        default:
            return YandexQuery::QueryMeta::COMPUTE_STATUS_UNSPECIFIED;
        }
    }

    void WriteFinalizingStatus() {
        const YandexQuery::QueryMeta::ComputeStatus finalizingStatus = GetFinalizingStatus();
        Params.Status = finalizingStatus;
        LOG_D("Write finalizing status: " << YandexQuery::QueryMeta::ComputeStatus_Name(finalizingStatus));
        Yq::Private::PingTaskRequest request;
        request.set_status(finalizingStatus);
        Send(Pinger, new TEvents::TEvForwardPingRequest(request), 0, SaveFinalizingStatusCookie);
    }

    void Finish(YandexQuery::QueryMeta::ComputeStatus status) {
        LOG_D("Is about to finish query with status " << YandexQuery::QueryMeta::ComputeStatus_Name(status));
        Finishing = true;
        FinalQueryStatus = status;
        QueryStateUpdateRequest.set_status(FinalQueryStatus); // Can be changed later.
        QueryStateUpdateRequest.set_status_code(NYql::NDqProto::StatusIds::SUCCESS);
        *QueryStateUpdateRequest.mutable_finished_at() = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(TInstant::Now().MilliSeconds());
        Become(&TRunActor::StateFuncWrapper<&TRunActor::FinishStateFunc>);

        if (!FinalizingStatusIsWritten) {
            WriteFinalizingStatus();
        }

        CancelRunningQuery();
        ContinueFinish();
    }

    void ContinueFinish() {
        if (NeedDeleteReadRules() && !ConsumersAreDeleted) {
            if (CanRunReadRulesDeletionActor()) {
                RunReadRulesDeletionActor();
            }
            return;
        }

        SendPingAndPassAway();
    }

    void ResignQuery(NYql::NDqProto::StatusIds::StatusCode statusCode) {
        QueryStateUpdateRequest.set_resign_query(true);
        QueryStateUpdateRequest.set_status_code(statusCode);
        SendPingAndPassAway();
    }

    void SendPingAndPassAway() {
        // Run ping.
        if (QueryStateUpdateRequest.resign_query()) { // Retry state => all issues are not fatal.
            TransientIssues.AddIssues(Issues);
            Issues.Clear();
        }

        NYql::IssuesToMessage(TransientIssues, QueryStateUpdateRequest.mutable_transient_issues());
        NYql::IssuesToMessage(Issues, QueryStateUpdateRequest.mutable_issues());

        Send(Pinger, new TEvents::TEvForwardPingRequest(QueryStateUpdateRequest, true));

        PassAway();
    }

    void Abort(const TString& message, YandexQuery::QueryMeta::ComputeStatus status, const NYql::TIssues& issues = {}) {
        AddIssueWithSubIssues(message, issues);
        Finish(status);
    }

    void FillDqGraphParams() {
        for (const auto& s : Params.DqGraphs) {
            NYq::NProto::TGraphParams dqGraphParams;
            Y_VERIFY(dqGraphParams.ParseFromString(s));
            DqGraphParams.emplace_back(std::move(dqGraphParams));
        }
    }

    void ReRunQuery() {
        if (AbortOnExceedingDqGraphsLimits()) {
            return;
        }
        for (const auto& m : Params.ResultSetMetas) {
            *QueryStateUpdateRequest.add_result_set_meta() = m;
        }
        DqGraphIndex = Params.DqGraphIndex;
        UpdateResultIndices();
        RunNextDqGraph();
    }

    bool CompileQuery() {
        LOG_D("Compiling query ...");
        NYql::TGatewaysConfig gatewaysConfig;
        SetupDqSettings(*gatewaysConfig.MutableDq());
        // the main idea of having Params.GatewaysConfig is to copy clusters only
        // but in this case we have to copy S3 provider limits
        *gatewaysConfig.MutableS3() = Params.GatewaysConfig.GetS3();
        gatewaysConfig.MutableS3()->ClearClusterMapping();

        THashMap<TString, TString> clusters;

        //todo: consider cluster name clashes
        AddClustersFromConfig(gatewaysConfig, clusters);
        AddSystemClusters(gatewaysConfig, clusters, Params.AuthToken);
        AddClustersFromConnections(YqConnections,
            Params.CommonConfig.GetUseBearerForYdb(),
            Params.CommonConfig.GetObjectStorageEndpoint(),
            Params.AuthToken,
            Params.AccountIdSignatures,
            // out params:
            gatewaysConfig,
            clusters);

        TVector<TDataProviderInitializer> dataProvidersInit;
        const std::shared_ptr<IDatabaseAsyncResolver> dbResolver = std::make_shared<TDatabaseAsyncResolverImpl>(NActors::TActivationContext::ActorSystem(), Params.DatabaseResolver,
            Params.CommonConfig.GetYdbMvpCloudEndpoint(), Params.CommonConfig.GetMdbGateway(), Params.CommonConfig.GetMdbTransformHost(), Params.QueryId);
        {
            // TBD: move init to better place
            QueryStateUpdateRequest.set_scope(Params.Scope.ToString());
            QueryStateUpdateRequest.mutable_query_id()->set_value(Params.QueryId);
            QueryStateUpdateRequest.set_owner_id(Params.Owner);
            dataProvidersInit.push_back(GetDqDataProviderInitializer(&CreateDqExecTransformer, NYq::CreateEmptyGateway(SelfId()), Params.DqCompFactory, {}, nullptr));
        }

        {
            dataProvidersInit.push_back(GetYdbDataProviderInitializer(Params.YqSharedResources->UserSpaceYdbDriver, Params.CredentialsFactory, dbResolver));
        }

        {
            dataProvidersInit.push_back(GetClickHouseDataProviderInitializer(Params.S3Gateway, dbResolver));
        }

        {
            dataProvidersInit.push_back(GetS3DataProviderInitializer(Params.S3Gateway, Params.CredentialsFactory));
        }

        {
            NYql::TPqGatewayServices pqServices(
                Params.YqSharedResources->UserSpaceYdbDriver,
                Params.PqCmConnections,
                Params.CredentialsFactory,
                std::make_shared<NYql::TPqGatewayConfig>(gatewaysConfig.GetPq()),
                Params.FunctionRegistry
            );
            const auto pqGateway = NYql::CreatePqNativeGateway(pqServices);
            dataProvidersInit.push_back(GetPqDataProviderInitializer(pqGateway, false, dbResolver));
        }

        {
            auto solomonConfig = gatewaysConfig.GetSolomon();
            auto solomonGateway = NYql::CreateSolomonGateway(solomonConfig);
            dataProvidersInit.push_back(GetSolomonDataProviderInitializer(solomonGateway, false));
        }

        TProgramFactory progFactory(false, Params.FunctionRegistry, Params.NextUniqueId, dataProvidersInit, "yq");
        progFactory.SetModules(Params.ModuleResolver);
        progFactory.SetUdfResolver(NYql::NCommon::CreateSimpleUdfResolver(Params.FunctionRegistry, nullptr));
        progFactory.SetGatewaysConfig(&gatewaysConfig);

        SessionId = TStringBuilder()
            << Params.QueryId << '#'
            << Params.ResultId << '#'
            << Params.Scope.ToString() << '#'
            << Params.Owner << '#'
            << Params.CloudId;

        Program = progFactory.Create("-stdin-", Params.Sql, SessionId);
        Program->EnableResultPosition();

        NSQLTranslation::TTranslationSettings sqlSettings;
        sqlSettings.ClusterMapping = clusters;
        sqlSettings.SyntaxVersion = 1;
        sqlSettings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
        sqlSettings.Flags.insert({ "DqEngineEnable", "DqEngineForce", "DisableAnsiOptionalAs" });
        try {
            AddTableBindingsFromBindings(Params.Bindings, YqConnections, sqlSettings);
        } catch (const std::exception& e) {
            Issues.AddIssue(ExceptionToIssue(e));
            return false;
        }

        // parse phase
        {
            if (!Program->ParseSql(sqlSettings)) {
                Issues.AddIssues(Program->Issues());
                return false;

            }

            if (Params.ExecuteMode == YandexQuery::ExecuteMode::PARSE) {
                return true;
            }
        }

        // compile phase
        {
            if (!Program->Compile("")) {
                Issues.AddIssues(Program->Issues());
                return false;
            }

            if (Params.ExecuteMode == YandexQuery::ExecuteMode::COMPILE) {
                return true;
            }
        }

        // next phases can be async: optimize, validate, run
        TProgram::TFutureStatus futureStatus;
        switch (Params.ExecuteMode) {
        case YandexQuery::ExecuteMode::EXPLAIN:
            futureStatus = Program->OptimizeAsync("");
            break;
        case YandexQuery::ExecuteMode::VALIDATE:
            futureStatus = Program->ValidateAsync("");
            break;
        case YandexQuery::ExecuteMode::RUN:
            futureStatus = Program->RunAsync("");
            break;
        default:
            Issues.AddIssue(TStringBuilder() << "Unexpected execute mode " << static_cast<int>(Params.ExecuteMode));
            return false;
        }

        futureStatus.Subscribe([actorSystem = NActors::TActivationContext::ActorSystem(), selfId = SelfId()](const TProgram::TFutureStatus& f) {
            actorSystem->Send(selfId, new TEvents::TEvAsyncContinue(f));
        });
        return true;
    }

    void Handle(TEvents::TEvAsyncContinue::TPtr& ev, const TActorContext& ctx) {
        LOG_D("Compiling finished");
        NYql::TProgram::TStatus status = TProgram::TStatus::Error;

        const auto& f = ev->Get()->Future;
        try {
            status = f.GetValue();
            if (status == TProgram::TStatus::Async) {
                auto futureStatus = Program->ContinueAsync();
                auto actorSystem = ctx.ActorSystem();
                auto selfId = ctx.SelfID;
                futureStatus.Subscribe([actorSystem, selfId](const TProgram::TFutureStatus& f) {
                    actorSystem->Send(selfId, new TEvents::TEvAsyncContinue(f));
                });
                return;
            }
        } catch (const std::exception& err) {
            Issues.AddIssue(ExceptionToIssue(err));
        }

        UpdateAstAndPlan();

        if (status == TProgram::TStatus::Ok || (DqGraphParams.size() > 0 && !DqGraphParams[0].GetResultType())) {
            PrepareGraphs();
        } else {
            Abort(TStringBuilder() << "Run query failed: " << ToString(status), YandexQuery::QueryMeta::FAILED, Program->Issues());
        }
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr&) {
        Fail("TRunActor::OnUndelivered");
    }

    TString FindTokenByName(const TString& tokenName) const {
        for (auto& graphParams : DqGraphParams) {
            const auto& secureParams = graphParams.GetSecureParams();
            const auto token = secureParams.find(tokenName);
            if (token != secureParams.end()) {
                return token->second;
            }
        }
        throw yexception() << "Token " << tokenName << " was not found in secure params";
    }

    void AddIssueWithSubIssues(const TString& message, const NYql::TIssues& issues) {
        NYql::TIssue issue(message);
        for (const NYql::TIssue& i : issues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }
        Issues.AddIssue(std::move(issue));
    }

    void LogReceivedParams() {
        LOG_D("Run actors params: { QueryId: " << Params.QueryId
            << " CloudId: " << Params.CloudId
            << " UserId: " << Params.UserId
            << " Owner: " << Params.Owner
            << " PreviousQueryRevision: " << Params.PreviousQueryRevision
            << " Connections: " << Params.Connections.size()
            << " Bindings: " << Params.Bindings.size()
            << " AccountIdSignatures: " << Params.AccountIdSignatures.size()
            << " QueryType: " << YandexQuery::QueryContent::QueryType_Name(Params.QueryType)
            << " ExecuteMode: " << YandexQuery::ExecuteMode_Name(Params.ExecuteMode)
            << " ResultId: " << Params.ResultId
            << " StateLoadMode: " << YandexQuery::StateLoadMode_Name(Params.StateLoadMode)
            << " StreamingDisposition: " << Params.StreamingDisposition
            << " Status: " << YandexQuery::QueryMeta::ComputeStatus_Name(Params.Status)
            << " DqGraphs: " << Params.DqGraphs.size()
            << " DqGraphIndex: " << Params.DqGraphIndex
            << " CreatedTopicConsumers: " << Params.CreatedTopicConsumers.size()
            << " }");
    }

private:
    TActorId FetcherId;
    TRunActorParams Params;
    THashMap<TString, YandexQuery::Connection> YqConnections;

    TProgramPtr Program;
    TIssues Issues;
    TIssues TransientIssues;
    TQueryResult QueryResult;
    TInstant Deadline;
    TActorId Pinger;
    TInstant CreatedAt;
    YandexQuery::QueryAction Action = YandexQuery::QueryAction::QUERY_ACTION_UNSPECIFIED;
    std::vector<NYq::NProto::TGraphParams> DqGraphParams;
    std::vector<i32> DqGrapResultIndices;
    i32 DqGraphIndex = 0;
    NActors::TActorId ExecuterId;
    NActors::TActorId ControlId;
    NActors::TActorId CheckpointCoordinatorId;
    TString SessionId;
    ::NYql::NCommon::TServiceCounters QueryCounters;
    const NMonitoring::TDynamicCounters::TCounterPtr QueryUptime;
    bool EnableCheckpointCoordinator = false;
    Yq::Private::PingTaskRequest QueryStateUpdateRequest;

    const ui64 MaxTasksPerOperation = 100;

    // Consumers creation
    TVector<NYql::NPq::NProto::TDqPqTopicSource> TopicsForConsumersCreation;
    TVector<std::shared_ptr<NYdb::ICredentialsProviderFactory>> CredentialsForConsumersCreation;
    TVector<TString> Statistics;
    NActors::TActorId ReadRulesCreatorId;

    // Finish
    bool Finishing = false;
    bool ConsumersAreDeleted = false;
    bool FinalizingStatusIsWritten = false;
    bool QueryResponseArrived = false;
    YandexQuery::QueryMeta::ComputeStatus FinalQueryStatus = YandexQuery::QueryMeta::COMPUTE_STATUS_UNSPECIFIED; // Status that will be assigned to query after it finishes.

    // Cookies for pings
    enum : ui64 {
        SaveQueryInfoCookie = 1,
        UpdateQueryInfoCookie,
        SaveFinalizingStatusCookie,
        SetLoadFromCheckpointModeCookie,
        RaiseTransientIssuesCookie,
    };
};


IActor* CreateRunActor(
    const NActors::TActorId& fetcherId,
    const ::NYql::NCommon::TServiceCounters& serviceCounters,
    TRunActorParams&& params
) {
    return new TRunActor(fetcherId, serviceCounters, std::move(params));
}

} /* NYq */
