#include "proxy.h"
#include "clusters_from_connections.h"
#include "table_bindings_from_bindings.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/dq/actors/compute/dq_checkpoints.h>
#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/utils/actor_log/log.h>
#include <ydb/library/yql/core/services/mounts/yql_mounts.h>
#include <ydb/library/yql/core/services/yql_out_transformers.h>
#include <ydb/library/yql/core/facade/yql_facade.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/providers/common/udf_resolve/yql_simple_udf_resolver.h>
#include <ydb/library/yql/providers/common/comp_nodes/yql_factory.h>
#include <ydb/library/yql/providers/common/schema/mkql/yql_mkql_schema.h>
#include <ydb/library/yql/providers/dq/actors/executer_actor.h>
#include <ydb/library/yql/providers/dq/actors/proto_builder.h>
#include <ydb/library/yql/providers/dq/actors/task_controller.h>
#include <ydb/library/yql/providers/dq/actors/result_receiver.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <ydb/library/yql/providers/dq/counters/counters.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_gateway.h>
#include <ydb/library/yql/providers/dq/provider/yql_dq_provider.h>
#include <ydb/library/yql/providers/dq/provider/exec/yql_dq_exectransformer.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_provider.h>
#include <ydb/library/yql/dq/integration/transform/yql_dq_task_transform.h>
#include <ydb/library/yql/providers/pq/gateway/native/yql_pq_gateway.h>
#include <ydb/library/yql/providers/pq/provider/yql_pq_provider.h>
#include <ydb/library/yql/providers/pq/proto/dq_io.pb.h>
#include <ydb/library/yql/providers/pq/task_meta/task_meta.h>
#include <ydb/library/yql/providers/s3/provider/yql_s3_provider.h>
#include <ydb/library/yql/providers/solomon/gateway/yql_solomon_gateway.h>
#include <ydb/library/yql/providers/solomon/provider/yql_solomon_provider.h>
#include <ydb/library/yql/providers/s3/proto/sink.pb.h>
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
#include <ydb/library/yql/utils/actor_log/log.h>

#include <ydb/library/mkql_proto/mkql_proto.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/core/fq/libs/actors/nodes_manager.h>
#include <ydb/core/fq/libs/checkpoint_storage/storage_service.h>
#include <ydb/core/fq/libs/checkpointing/checkpoint_coordinator.h>
#include <ydb/core/fq/libs/checkpointing_common/defs.h>
#include <ydb/core/fq/libs/common/compression.h>
#include <ydb/core/fq/libs/common/entity_id.h>
#include <ydb/core/fq/libs/compute/common/pinger.h>
#include <ydb/core/fq/libs/compute/common/utils.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>
#include <ydb/core/fq/libs/db_id_async_resolver_impl/db_async_resolver_impl.h>
#include <ydb/core/fq/libs/gateway/empty_gateway.h>
#include <ydb/core/fq/libs/private_client/events.h>
#include <ydb/core/fq/libs/private_client/private_client.h>
#include <ydb/core/fq/libs/rate_limiter/utils/path.h>
#include <ydb/core/fq/libs/read_rule/read_rule_creator.h>
#include <ydb/core/fq/libs/read_rule/read_rule_deleter.h>
#include <ydb/core/fq/libs/tasks_packer/tasks_packer.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>
#include <library/cpp/json/yson/json2yson.h>
#include <library/cpp/yson/node/node_io.h>

#include <google/protobuf/util/time_util.h>

#include <util/string/split.h>
#include <util/system/hostname.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "QueryId: " << Params.QueryId << " " << stream)
#define LOG_W(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "QueryId: " << Params.QueryId << " " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "QueryId: " << Params.QueryId << " " << stream)
#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "QueryId: " << Params.QueryId << " " << stream)
#define LOG_QE(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "QueryId: " << QueryId << " " << stream)
#define LOG_QW(stream) LOG_WARN_S( *TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "QueryId: " << QueryId << " " << stream)

namespace NFq {

using namespace NActors;
using namespace NYql;
using namespace NDqs;
using namespace NFq;

namespace {

struct TEvPrivate {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),

        EvProgramFinished = EvBegin,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

    // Events
    struct TEvProgramFinished : public NActors::TEventLocal<TEvProgramFinished, EvProgramFinished> {
        TEvProgramFinished(TIssues issues, const TString& plan, const TString& expr, NYql::TProgram::TStatus status, const TString& message)
            : Issues(issues), Plan(plan), Expr(expr), Status(status), Message(message)
        {
        }

        TIssues Issues;
        TString Plan;
        TString Expr;
        NYql::TProgram::TStatus Status;
        TString Message;
    };
};

class TTraceOptPipelineConfigurator : public IPipelineConfigurator {
public:
    TTraceOptPipelineConfigurator() = default;

    void AfterCreate(TTransformationPipeline* pipeline) const final {
        Y_UNUSED(pipeline);
    }

    void AfterTypeAnnotation(TTransformationPipeline* pipeline) const final {
        pipeline->Add(
            TExprLogTransformer::Sync(
                "OptimizedExpr",
                NYql::NLog::EComponent::Core,
                NYql::NLog::ELevel::TRACE),
            "OptTrace",
            TIssuesIds::CORE,
            "OptTrace");
    }

    void AfterOptimize(TTransformationPipeline* pipeline) const final {
        Y_UNUSED(pipeline);
    }
};

static TTraceOptPipelineConfigurator TraceOptPipelineConfigurator;

}

class TProgramRunnerActor : public NActors::TActorBootstrapped<TProgramRunnerActor> {
public:
    TProgramRunnerActor(
        const TActorId& runActorId,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        ui64 nextUniqueId,
        TVector<TDataProviderInitializer> dataProvidersInit,
        NYql::IModuleResolver::TPtr moduleResolver,
        const NYql::TGatewaysConfig& gatewaysConfig,
        const TString& sql,
        const TString& sessionId,
        const NSQLTranslation::TTranslationSettings& sqlSettings,
        FederatedQuery::ExecuteMode executeMode,
        const TString& queryId
    )
        : RunActorId(runActorId)
        , FunctionRegistry(functionRegistry)
        , NextUniqueId(nextUniqueId)
        , DataProvidersInit(std::move(dataProvidersInit))
        , ModuleResolver(moduleResolver)
        , GatewaysConfig(gatewaysConfig)
        , Sql(sql)
        , SessionId(sessionId)
        , SqlSettings(sqlSettings)
        , ExecuteMode(executeMode)
        , QueryId(queryId)
    {
    }

    void Bootstrap() {
        try {
            TProgramFactory progFactory(false, FunctionRegistry, NextUniqueId, DataProvidersInit, "yq");
            progFactory.SetModules(ModuleResolver);
            progFactory.SetUdfResolver(NYql::NCommon::CreateSimpleUdfResolver(FunctionRegistry, nullptr));
            progFactory.SetGatewaysConfig(&GatewaysConfig);

            Program = progFactory.Create("-stdin-", Sql, SessionId);
            Program->EnableResultPosition();

            // parse phase
            {
                if (!Program->ParseSql(SqlSettings)) {
                    SendStatusAndDie(TProgram::TStatus::Error, "Failed to parse query");
                    return;
                }

                if (ExecuteMode == FederatedQuery::ExecuteMode::PARSE) {
                    SendStatusAndDie(TProgram::TStatus::Ok);
                    return;
                }
            }

            // compile phase
            {
                if (!Program->Compile("")) {
                    SendStatusAndDie(TProgram::TStatus::Error, "Failed to compile query");
                    return;
                }

                if (ExecuteMode == FederatedQuery::ExecuteMode::COMPILE) {
                    SendStatusAndDie(TProgram::TStatus::Ok);
                    return;
                }
            }

            Compiled = true;

            // next phases can be async: optimize, validate, run
            TProgram::TFutureStatus futureStatus;
            switch (ExecuteMode) {
            case FederatedQuery::ExecuteMode::EXPLAIN:
                futureStatus = Program->OptimizeAsyncWithConfig("", TraceOptPipelineConfigurator);
                break;
            case FederatedQuery::ExecuteMode::VALIDATE:
                futureStatus = Program->ValidateAsync("");
                break;
            case FederatedQuery::ExecuteMode::RUN:
                futureStatus = Program->RunAsyncWithConfig("", TraceOptPipelineConfigurator);
                break;
            default:
                SendStatusAndDie(TProgram::TStatus::Error, TStringBuilder() << "Unexpected execute mode " << static_cast<int>(ExecuteMode));
                return;
            }

            futureStatus.Subscribe([actorSystem = NActors::TActivationContext::ActorSystem(), selfId = SelfId()](const TProgram::TFutureStatus& f) {
                actorSystem->Send(selfId, new TEvents::TEvAsyncContinue(f));
            });

            Become(&TProgramRunnerActor::StateFunc);
        } catch (...) {
            SendStatusAndDie(TProgram::TStatus::Error, CurrentExceptionMessage());
        }
    }

    void SendStatusAndDie(NYql::TProgram::TStatus status, const TString& message = "") {
        TString expr;
        TString plan;
        if (Compiled) {
            TStringStream exprOut;
            TStringStream planOut;
            Program->Print(&exprOut, &planOut);
            plan = Plan2Json(planOut.Str());
            expr = exprOut.Str();
        }
        Issues.AddIssues(Program->Issues());
        Send(RunActorId, new TEvPrivate::TEvProgramFinished(Issues, plan, expr, status, message));
        PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvents::TEvAsyncContinue, Handle);
    )

    void Handle(TEvents::TEvAsyncContinue::TPtr& ev) {
        NYql::TProgram::TStatus status = TProgram::TStatus::Error;

        const auto& f = ev->Get()->Future;
        try {
            status = f.GetValue();
            if (status == TProgram::TStatus::Async) {
                auto futureStatus = Program->ContinueAsync();
                futureStatus.Subscribe([actorSystem = TActivationContext::ActorSystem(), selfId = SelfId()](const TProgram::TFutureStatus& f) {
                    actorSystem->Send(selfId, new TEvents::TEvAsyncContinue(f));
                });
                return;
            }
        } catch (const std::exception& err) {
            Issues.AddIssue(ExceptionToIssue(err));
        }
        SendStatusAndDie(status);
    }

    TString Plan2Json(const TString& ysonPlan) {
        if (!ysonPlan) {
            LOG_QW("Can't convert plan from yson to json: plan is empty");
            return {};
        }
        try {
            return NJson2Yson::ConvertYson2Json(ysonPlan);
        } catch (...) {
            LOG_QE("Can't convert plan from yson to json: " << CurrentExceptionMessage());
        }
        return {};
    }

private:
    TProgramPtr Program;
    TIssues Issues;
    const TActorId RunActorId;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry;
    ui64 NextUniqueId;
    const TVector<TDataProviderInitializer> DataProvidersInit;
    const NYql::IModuleResolver::TPtr ModuleResolver;
    const NYql::TGatewaysConfig GatewaysConfig;
    const TString Sql;
    const TString SessionId;
    const NSQLTranslation::TTranslationSettings SqlSettings;
    const FederatedQuery::ExecuteMode ExecuteMode;
    const TString QueryId;
    bool Compiled = false;
};

struct TEvaluationGraphInfo {
    NActors::TActorId ExecuterId;
    NActors::TActorId ControlId;
    NActors::TActorId ResultId;
    NThreading::TPromise<NYql::IDqGateway::TResult> Result;
    ui64 Index = 0;
};

class TRunActor : public NActors::TActorBootstrapped<TRunActor> {
public:
    using NActors::TActorBootstrapped<TRunActor>::Register;
    using NActors::TActorBootstrapped<TRunActor>::Send;

    explicit TRunActor(
        const TActorId& fetcherId
        , const ::NYql::NCommon::TServiceCounters& queryCounters
        , TRunActorParams&& params)
        : FetcherId(fetcherId)
        , Params(std::move(params))
        , CreatedAt(Params.CreatedAt)
        , QueryCounters(queryCounters)
        , MaxTasksPerStage(Params.Config.GetCommon().GetMaxTasksPerStage() ? Params.Config.GetCommon().GetMaxTasksPerStage() : 500)
        , MaxTasksPerOperation(Params.Config.GetCommon().GetMaxTasksPerOperation() ? Params.Config.GetCommon().GetMaxTasksPerOperation() : 40)
        , Compressor(Params.Config.GetCommon().GetQueryArtifactsCompressionMethod(), Params.Config.GetCommon().GetQueryArtifactsCompressionMinSize())
    {
        QueryCounters.SetUptimePublicAndServiceCounter(0);

        switch (Params.Config.GetControlPlaneStorage().GetStatsMode()) {
            case Ydb::Query::StatsMode::STATS_MODE_NONE:
                StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_NONE;
                break;
            case Ydb::Query::StatsMode::STATS_MODE_BASIC:
                StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_BASIC;
                break;
            case Ydb::Query::StatsMode::STATS_MODE_FULL:
                StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_FULL;
                break;
            case Ydb::Query::StatsMode::STATS_MODE_PROFILE:
                StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_PROFILE;
                break;
            case Ydb::Query::StatsMode::STATS_MODE_UNSPECIFIED:
            default:
                StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_FULL;
                break;
        }
    }

    static constexpr char ActorName[] = "YQ_RUN_ACTOR";

    void Bootstrap() {
        LOG_D("Start run actor. Compute state: " << FederatedQuery::QueryMeta::ComputeStatus_Name(Params.Status));

        FillConnections();

        QueryCounters.SetUptimePublicAndServiceCounter((TInstant::Now() - CreatedAt).Seconds());
        QueryCounters.Counters->GetCounter("RetryCount", false)->Set(Params.RestartCount);
        LogReceivedParams();
        Pinger = Register(
            CreatePingerActor(
                Params.TenantName,
                Params.Scope,
                Params.UserId,
                Params.QueryId,
                Params.Owner,
                SelfId(),
                Params.Config.GetPinger(),
                Params.Deadline,
                QueryCounters,
                CreatedAt
                ));

        if (!Params.RequestStartedAt) {
            Params.RequestStartedAt = TInstant::Now();
            Fq::Private::PingTaskRequest request;
            *request.mutable_started_at() = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(Params.RequestStartedAt.MilliSeconds());
            Send(Pinger, new TEvents::TEvForwardPingRequest(request), 0, UpdateQueryInfoCookie);
        }

        Become(&TRunActor::StateFuncWrapper<&TRunActor::StateFunc>);

        try {
            if (!TimeLimitExceeded()) {
                Run();
            }
        } catch (const std::exception&) {
            FailOnException();
        }
    }

private:
    enum RunActorWakeupTag : ui64 {
        ExecutionTimeout = 1
    };


    template <void (TRunActor::* DelegatedStateFunc)(STFUNC_SIG)>
    STFUNC(StateFuncWrapper) {
        try {
            (this->*DelegatedStateFunc)(ev);
        } catch (...) {
            FailOnException();
        }
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvPrivate::TEvProgramFinished, Handle);
        hFunc(TEvents::TEvAsyncContinue, Handle);
        hFunc(NActors::TEvents::TEvUndelivered, Handle);
        hFunc(TEvents::TEvGraphParams, Handle);
        hFunc(TEvents::TEvDataStreamsReadRulesCreationResult, Handle);
        hFunc(NYql::NDqs::TEvQueryResponse, Handle);
        hFunc(NActors::TEvents::TEvWakeup, Handle);
        hFunc(TEvents::TEvQueryActionResult, Handle);
        hFunc(TEvents::TEvForwardPingResponse, Handle);
        hFunc(TEvCheckpointCoordinator::TEvZeroCheckpointDone, Handle);
        hFunc(TEvents::TEvRaiseTransientIssues, Handle);
        hFunc(NFq::TEvInternalService::TEvCreateRateLimiterResourceResponse, Handle);
        hFunc(TEvDqStats, Handle);
        hFunc(NMon::TEvHttpInfo, Handle);
    )

    STRICT_STFUNC(FinishStateFunc,
        hFunc(TEvents::TEvDataStreamsReadRulesCreationResult, HandleFinish);
        hFunc(TEvents::TEvDataStreamsReadRulesDeletionResult, HandleFinish);
        hFunc(NYql::NDqs::TEvQueryResponse, HandleFinish);
        hFunc(TEvents::TEvForwardPingResponse, HandleFinish);
        hFunc(NFq::TEvInternalService::TEvCreateRateLimiterResourceResponse, HandleFinish);
        hFunc(NFq::TEvInternalService::TEvDeleteRateLimiterResourceResponse, HandleFinish);
        hFunc(TEvents::TEvEffectApplicationResult, HandleFinish);

        // Ignore tail of action events after normal work.
        IgnoreFunc(TEvents::TEvAsyncContinue);
        IgnoreFunc(NActors::TEvents::TEvUndelivered);
        IgnoreFunc(TEvents::TEvGraphParams);
        IgnoreFunc(TEvents::TEvQueryActionResult);
        IgnoreFunc(TEvCheckpointCoordinator::TEvZeroCheckpointDone);
        IgnoreFunc(TEvents::TEvRaiseTransientIssues);
        IgnoreFunc(TEvDqStats);
    )

    void FillConnections() {
        LOG_D("FillConnections");

        for (const auto& connection : Params.Connections) {
            if (!connection.content().name()) {
                LOG_D("Connection with empty name " << connection.meta().id());
                continue;
            }
            YqConnections.emplace(connection.meta().id(), connection);
        }
    }

    void KillExecuter() {
        if (ExecuterId) {
            Send(ExecuterId, new NActors::TEvents::TEvPoison());

            // Clear finished actors ids
            ExecuterId = {};
            ControlId = {};
        }
    }

    void KillChildrenActors() {
        if (ReadRulesCreatorId) {
            Send(ReadRulesCreatorId, new NActors::TEvents::TEvPoison());
        }

        if (RateLimiterResourceCreatorId) {
            Send(RateLimiterResourceCreatorId, new NActors::TEvents::TEvPoison());
        }

        if (RateLimiterResourceDeleterId) {
            Send(RateLimiterResourceDeleterId, new NActors::TEvents::TEvPoison());
        }

        KillExecuter();
    }

    void CancelRunningQuery() {
        if (ReadRulesCreatorId) {
            LOG_D("Cancel read rules creation");
            Send(ReadRulesCreatorId, new NActors::TEvents::TEvPoison());
        }

        if (RateLimiterResourceCreatorId) {
            Send(RateLimiterResourceCreatorId, new NActors::TEvents::TEvPoison());
        }

        if (!EvalInfos.empty()) {
            for (auto& pair : EvalInfos) {
                auto& info = pair.second;
                Send(info.ControlId, new NDq::TEvDq::TEvAbortExecution(NYql::NDqProto::StatusIds::ABORTED, FederatedQuery::QueryMeta::ComputeStatus_Name(FinalQueryStatus)));
            }
        }

        if (ControlId) {
            LOG_D("Cancel running query");
            Send(ControlId, new NDq::TEvDq::TEvAbortExecution(NYql::NDqProto::StatusIds::ABORTED, FederatedQuery::QueryMeta::ComputeStatus_Name(FinalQueryStatus)));
        } else {
            QueryResponseArrived = true;
        }
    }

    void PassAway() override {
        Send(FetcherId, new NActors::TEvents::TEvPoisonTaken());
        KillChildrenActors();
        NActors::TActorBootstrapped<TRunActor>::PassAway();
    }

    void AbortByExecutionTimeout() {
        Abort("Execution time limit exceeded", FederatedQuery::QueryMeta::ABORTED_BY_SYSTEM);
    }

    bool TimeLimitExceeded() {
        if (Params.ExecutionTtl != TDuration::Zero()) {
            auto currentTime = TInstant::Now();
            auto startedAt = Params.RequestStartedAt ? Params.RequestStartedAt : currentTime;
            auto deadline = startedAt  + Params.ExecutionTtl;

            if (currentTime >= deadline) {
                AbortByExecutionTimeout();
                return true;
            } else {
                Schedule(deadline, new NActors::TEvents::TEvWakeup(RunActorWakeupTag::ExecutionTimeout));
            }
        }
        return false;
    }

    void ProcessQuery() {
        // should be called in case of some event, so sync state first
        Send(Pinger, new TEvents::TEvForwardPingRequest(QueryStateUpdateRequest));

        // must be in-sync, TODO: dedup
        Params.Resources = QueryStateUpdateRequest.resources();

        if (QueryStateUpdateRequest.resources().rate_limiter() == Fq::Private::TaskResources::PREPARE) {
            if (!RateLimiterResourceCreatorId) {
                LOG_D("Start rate limiter resource creator");
                RateLimiterResourceCreatorId = Register(CreateRateLimiterResourceCreator(SelfId(), Params.Owner, Params.QueryId, Params.Scope, Params.TenantName));
            }
            return;
        }

        if (QueryStateUpdateRequest.resources().compilation() == Fq::Private::TaskResources::PREPARE) {
            if (!ProgramRunnerId) {
                RunProgram();
            }
            return;
        }

        if (QueryStateUpdateRequest.resources().topic_consumers_state() == Fq::Private::TaskResources::PREPARE) {
            if (!ReadRulesCreatorId) {
                ReadRulesCreatorId = Register(
                    ::NFq::MakeReadRuleCreatorActor(
                        SelfId(),
                        Params.QueryId,
                        Params.YqSharedResources->UserSpaceYdbDriver,
                        Params.Resources.topic_consumers(),
                        PrepareReadRuleCredentials()
                    )
                );
            }
            return;
        }

        if (DqGraphParams.empty()) {
            QueryStateUpdateRequest.set_resign_query(false);
            const bool isOk = Issues.Size() == 0;
            Finish(GetFinishStatus(isOk));
            return;
        }

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

    void Run() {
        *QueryStateUpdateRequest.mutable_resources() = Params.Resources;

        if (!Params.DqGraphs.empty() && QueryStateUpdateRequest.resources().compilation() == Fq::Private::TaskResources::READY) {
            FillDqGraphParams();
        }

        switch (Params.Status) {
        case FederatedQuery::QueryMeta::ABORTING_BY_USER:
        case FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM:
        case FederatedQuery::QueryMeta::FAILING:
        case FederatedQuery::QueryMeta::COMPLETING:
            FinalizingStatusIsWritten = true;
            Finish(GetFinalStatusFromFinalizingStatus(Params.Status));
            break;
        case FederatedQuery::QueryMeta::STARTING:
            QueryStateUpdateRequest.mutable_resources()->set_rate_limiter(
                Params.Config.GetRateLimiter().GetEnabled() ? Fq::Private::TaskResources::PREPARE : Fq::Private::TaskResources::NOT_NEEDED);
            QueryStateUpdateRequest.mutable_resources()->set_compilation(Fq::Private::TaskResources::PREPARE);
            // know nothing about read rules yet
            Params.Status = FederatedQuery::QueryMeta::RUNNING; // ???
            QueryStateUpdateRequest.set_status(FederatedQuery::QueryMeta::RUNNING);
            // DO NOT break here
        case FederatedQuery::QueryMeta::RESUMING:
        case FederatedQuery::QueryMeta::RUNNING:
            ProcessQuery();
            break;
        default:
            Abort("Fail to start query from unexpected status " + FederatedQuery::QueryMeta::ComputeStatus_Name(Params.Status), FederatedQuery::QueryMeta::FAILED);
            break;
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
            Abort("Internal Error", FederatedQuery::QueryMeta::FAILED);
            return;
        }

        // Already finishing. Fail instantly.
        Issues.AddIssue("Internal Error");

        if (!ConsumersAreDeleted) {
            for (const Fq::Private::TopicConsumer& c : Params.Resources.topic_consumers()) {
                TransientIssues.AddIssue(TStringBuilder() << "Created read rule `" << c.consumer_name() << "` for topic `" << c.topic_path() << "` (database id " << c.database_id() << ") maybe was left undeleted: internal error occurred");
                TransientIssues.back().Severity = NYql::TSeverityIds::S_WARNING;
            }
        }

        // If target status was successful, change it to failed because we are in internal error handler.
        if (QueryStateUpdateRequest.status() == FederatedQuery::QueryMeta::COMPLETED || QueryStateUpdateRequest.status() == FederatedQuery::QueryMeta::PAUSED) {
            QueryStateUpdateRequest.set_status(FederatedQuery::QueryMeta::FAILED);
            QueryStateUpdateRequest.set_status_code(NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }

        SendPingAndPassAway();
    }

    void Handle(TEvents::TEvQueryActionResult::TPtr& ev) {
        Action = ev->Get()->Action;
        LOG_D("New query action received: " << FederatedQuery::QueryAction_Name(Action));
        switch (Action) {
        case FederatedQuery::ABORT:
        case FederatedQuery::ABORT_GRACEFULLY: // not fully implemented
            // ignore issues in case of controlled abort
            Finish(FederatedQuery::QueryMeta::ABORTED_BY_USER);
            break;
        case FederatedQuery::PAUSE: // not implemented
        case FederatedQuery::PAUSE_GRACEFULLY: // not implemented
        case FederatedQuery::RESUME: // not implemented
            Abort(TStringBuilder() << "Unsupported query action: " << FederatedQuery::QueryAction_Name(Action), FederatedQuery::QueryMeta::FAILED);
            break;
        default:
            Abort(TStringBuilder() << "Unknown query action: " << FederatedQuery::QueryAction_Name(Action), FederatedQuery::QueryMeta::FAILED);
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
                                LOG_D("Create consumer \"" << srcDesc.GetConsumerName() << "\" for topic \"" << srcDesc.GetTopicPath() << "\"");
                                auto& consumer = *QueryStateUpdateRequest.mutable_resources()->add_topic_consumers();
                                consumer.set_database_id(srcDesc.GetDatabaseId());
                                consumer.set_database(srcDesc.GetDatabase());
                                consumer.set_topic_path(srcDesc.GetTopicPath());
                                consumer.set_consumer_name(srcDesc.GetConsumerName());
                                consumer.set_cluster_endpoint(srcDesc.GetEndpoint());
                                consumer.set_use_ssl(srcDesc.GetUseSsl());
                                consumer.set_token_name(srcDesc.GetToken().GetName());
                                consumer.set_add_bearer_to_token(srcDesc.GetAddBearerToToken());
                            }
                        }
                    }
                }
            }
        }
    }

    void FillGraphMemoryInfo(NFq::NProto::TGraphParams& graphParams) {
        auto mkqlDefaultLimit = Params.Config.GetResourceManager().GetMkqlInitialMemoryLimit();
        if (mkqlDefaultLimit == 0) {
            mkqlDefaultLimit = 8_GB;
        }

        auto s3ReadDefaultInflightLimit = Params.Config.GetGateways().GetS3().GetDataInflight();
        if (s3ReadDefaultInflightLimit == 0) {
            s3ReadDefaultInflightLimit = 200_MB;
        }

        TMap<TString, NYql::NS3::TEffect> effects;
        for (NYql::NDqProto::TDqTask& task : *graphParams.MutableTasks()) {
            if (task.GetInitialTaskMemoryLimit() == 0) {
                ui64 limitTotal = mkqlDefaultLimit;
                for (auto& input : *task.MutableInputs()) {
                    if (input.HasSource() && input.GetSource().GetType() == "S3Source") {
                        limitTotal += s3ReadDefaultInflightLimit;
                    }
                }
                task.SetInitialTaskMemoryLimit(limitTotal);
            }
            for (auto& output : *task.MutableOutputs()) {
                if (output.HasSink() && output.GetSink().GetType() == "S3Sink") {
                    NS3::TSink s3SinkSettings;
                    YQL_ENSURE(output.GetSink().GetSettings().UnpackTo(&s3SinkSettings));
                    if (s3SinkSettings.GetAtomicUploadCommit()) {
                        auto prefix = s3SinkSettings.GetUrl() + s3SinkSettings.GetPath();
                        const auto& [it, isNew] = S3Prefixes.insert(prefix);
                        if (isNew) {
                            auto& sinkEffect = effects[prefix];
                            auto& cleanupEffect = *sinkEffect.MutableCleanup();
                            cleanupEffect.SetUrl(s3SinkSettings.GetUrl());
                            cleanupEffect.SetPrefix(s3SinkSettings.GetPath());
                            sinkEffect.SetToken(s3SinkSettings.GetToken());
                        }
                    }
                }
            }
        }
        if (!effects.empty()) {
            auto& stateExEffect = GetExternalEffects("S3Sink");
            for (auto& [prefix, sinkEffect] : effects) {
                TString data;
                YQL_ENSURE(sinkEffect.SerializeToString(&data));
                NYql::NDqProto::TEffect effect;
                effect.SetId(prefix);
                effect.SetData(data);
                *stateExEffect.AddEffects() = effect;
            }
        }
    }

    void Handle(NActors::TEvents::TEvWakeup::TPtr& ev) {
        auto tag = (RunActorWakeupTag) ev->Get()->Tag;
        switch (tag) {
            case RunActorWakeupTag::ExecutionTimeout: {
                AbortByExecutionTimeout();
                break;
            }
            default: {
                Y_ABORT_UNLESS(false);
            }
        }
    }

    void Handle(TEvents::TEvForwardPingResponse::TPtr& ev) {
        LOG_T("Forward ping response. Success: " << ev->Get()->Success << ". Cookie: " << ev->Cookie);
        if (!ev->Get()->Success) { // Failed setting new status or lease was lost
            ResignQuery(NYql::NDqProto::StatusIds::UNAVAILABLE);
            return;
        }

        if (ev->Cookie == SaveQueryInfoCookie) {
            QueryStateUpdateRequest.mutable_resources()->set_compilation(Fq::Private::TaskResources::READY);
            QueryStateUpdateRequest.mutable_resources()->set_topic_consumers_state(
                QueryStateUpdateRequest.resources().topic_consumers().size() ? Fq::Private::TaskResources::PREPARE : Fq::Private::TaskResources::NOT_NEEDED);
            ProcessQuery();
        } else if (ev->Cookie == SetLoadFromCheckpointModeCookie) {
            Send(ControlId, new TEvCheckpointCoordinator::TEvRunGraph());
        }
    }

    void HandleFinish(TEvents::TEvForwardPingResponse::TPtr& ev) {
        LOG_T("Forward ping response. Success: " << ev->Get()->Success << ". Cookie: " << ev->Cookie);
        if (!ev->Get()->Success) { // Failed setting new status or lease was lost
            Fail("Failed to write finalizing status");
            return;
        }

        if (ev->Cookie == SaveFinalizingStatusCookie) {
            FinalizingStatusIsWritten = true;
            ContinueFinish();
        }
    }

    void HandleFinish(TEvents::TEvEffectApplicationResult::TPtr ev) {
        if (ev->Get()->FatalError) {
            Fail(ev->Get()->Issues.ToOneLineString());
            return;
        }
        if (ev->Get()->Issues) {
            LOG_W("Effect Issues: " << ev->Get()->Issues.ToOneLineString());
            Issues.AddIssues(ev->Get()->Issues);
        }
        ++EffectApplicatorFinished;
        ContinueFinish();
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
            Abort(errorMsg, FederatedQuery::QueryMeta::FAILED, Program->Issues());
            return true;
        }
        return false;
    }

    void Handle(TEvents::TEvGraphParams::TPtr& ev) {
        LOG_D("Graph (" << (ev->Get()->IsEvaluation ? "evaluation" : "execution") << ") with tasks: " << ev->Get()->GraphParams.TasksSize());

        if (Params.Resources.rate_limiter_path()) {
            const TString rateLimiterResource = GetRateLimiterResourcePath(Params.CloudId, Params.Scope.ParseFolder(), Params.QueryId);
            for (auto& task : *ev->Get()->GraphParams.MutableTasks()) {
                task.SetRateLimiter(Params.Resources.rate_limiter_path());
                task.SetRateLimiterResource(rateLimiterResource);
            }
        }

        if (ev->Get()->IsEvaluation) {
            auto info = RunEvalDqGraph(ev->Get()->GraphParams);
            info.Result = ev->Get()->Result;
            EvalInfos.emplace(info.ExecuterId, info);
        } else {
            DqGraphParams.push_back(ev->Get()->GraphParams);
        }
    }

    void Handle(TEvCheckpointCoordinator::TEvZeroCheckpointDone::TPtr&) {
        LOG_D("Coordinator saved zero checkpoint");
        Y_ABORT_UNLESS(ControlId);
        SetLoadFromCheckpointMode();
    }

    void Handle(TEvents::TEvRaiseTransientIssues::TPtr& ev) {
        SendTransientIssues(ev->Get()->TransientIssues);
    }

    void Handle(TEvDqStats::TPtr& ev) {

        if (!CollectBasic()) {
            return;
        }

        auto& proto = ev->Get()->Record;

        TString GraphKey;
        auto it = EvalInfos.find(ev->Sender);
        if (it != EvalInfos.end()) {
            GraphKey = "Precompute=" + ToString(it->second.Index);
        } else {
            if (ev->Sender != ExecuterId) {
                LOG_E("TEvDqStats received from UNKNOWN Actor (TDqExecuter?) " << ev->Sender);
                return;
            }
            GraphKey = "Graph=" + ToString(DqGraphIndex);
        }

        if (proto.issues_size() || proto.metric_size()) {
            Fq::Private::PingTaskRequest request;
            if (proto.issues_size()) {
                *request.mutable_transient_issues() = proto.issues();
            }
            if (proto.metric_size()) {
                TString statistics;
                if (SaveAndPackStatistics(GraphKey, proto.metric(), statistics)) {
                    QueryStateUpdateRequest.set_statistics(statistics);
                    request.set_statistics(statistics);
                }
            }
            Send(Pinger, new TEvents::TEvForwardPingRequest(request), 0);
        }
    }

    void SendTransientIssues(const NYql::TIssues& issues) {
        Fq::Private::PingTaskRequest request;
        NYql::IssuesToMessage(issues, request.mutable_transient_issues());
        Send(Pinger, new TEvents::TEvForwardPingRequest(request), 0, RaiseTransientIssuesCookie);
    }

    i32 UpdateResultIndices() {
        i32 count = 0;
        DqGrapResultIndices.clear();
        for (const auto& graphParams : DqGraphParams) {
            DqGrapResultIndices.push_back(graphParams.GetResultType() ? count++ : -1);
        }
        return count;
    }

    void UpdateAstAndPlan(const TString& plan, const TString& expr) {
        Fq::Private::PingTaskRequest request;
        if (Compressor.IsEnabled()) {
            auto [astCompressionMethod, astCompressed] = Compressor.Compress(expr);
            request.mutable_ast_compressed()->set_method(astCompressionMethod);
            request.mutable_ast_compressed()->set_data(astCompressed);

            auto [planCompressionMethod, planCompressed] = Compressor.Compress(plan);
            request.mutable_plan_compressed()->set_method(planCompressionMethod);
            request.mutable_plan_compressed()->set_data(planCompressed);
        } else {
            request.set_ast(expr); // todo: remove after migration
            request.set_plan(plan); // todo: remove after migration
        }

        Send(Pinger, new TEvents::TEvForwardPingRequest(request));
    }

    void PrepareGraphs() {
        if (AbortOnExceedingDqGraphsLimits()) {
            return;
        }

        Fq::Private::PingTaskRequest request;

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

        for (const auto& graphParams : DqGraphParams) {
            const TString& serializedGraph = graphParams.SerializeAsString();
            if (Compressor.IsEnabled()) {
                auto& dq_graph_compressed = *request.add_dq_graph_compressed();
                auto [method, data] = Compressor.Compress(serializedGraph);
                dq_graph_compressed.set_method(method);
                dq_graph_compressed.set_data(data);
            } else {
                request.add_dq_graph(serializedGraph); // todo: remove after migration
            }
        }

        Send(Pinger, new TEvents::TEvForwardPingRequest(request), 0, SaveQueryInfoCookie);
    }

    void SetLoadFromCheckpointMode() {
        Fq::Private::PingTaskRequest request;
        request.set_state_load_mode(FederatedQuery::FROM_LAST_CHECKPOINT);
        request.mutable_disposition()->mutable_from_last_checkpoint();

        Send(Pinger, new TEvents::TEvForwardPingRequest(request), 0, SetLoadFromCheckpointModeCookie);
    }

    TString BuildNormalizedStatistics(const ::google::protobuf::RepeatedPtrField<::NYql::NDqProto::TMetric> metrics) {

        struct TStatisticsNode {
            std::map<TString, TStatisticsNode> Children;
            TString Name;
            i64 Avg;
            i64 Count;
            i64 Min;
            i64 Max;
            i64 Sum;
            void AssignStats(const TStatisticsNode& other) {
                Name  = other.Name;
                Avg   = other.Avg;
                Count = other.Count;
                Min   = other.Min;
                Max   = other.Max;
                Sum   = other.Sum;
            }
            void Write(NYson::TYsonWriter& writer) {
                writer.OnBeginMap();
                if (Children.empty()) {
                    if (Name.EndsWith("Us")) { // TDuration
                        writer.OnKeyedItem("sum");
                        writer.OnStringScalar(FormatDurationUs(Sum));
                        writer.OnKeyedItem("count");
                        writer.OnInt64Scalar(Count);
                        writer.OnKeyedItem("avg");
                        writer.OnStringScalar(FormatDurationUs(Avg));
                        writer.OnKeyedItem("max");
                        writer.OnStringScalar(FormatDurationUs(Max));
                        writer.OnKeyedItem("min");
                        writer.OnStringScalar(FormatDurationUs(Min));
                    } else if (Name.EndsWith("Ms")) { // TInstant
                        writer.OnKeyedItem("sum");
                        writer.OnStringScalar("N/A");
                        writer.OnKeyedItem("count");
                        writer.OnInt64Scalar(Count);
                        writer.OnKeyedItem("avg");
                        writer.OnStringScalar(FormatInstant(TInstant::MilliSeconds(Avg)));
                        writer.OnKeyedItem("max");
                        writer.OnStringScalar(FormatInstant(TInstant::MilliSeconds(Max)));
                        writer.OnKeyedItem("min");
                        writer.OnStringScalar(FormatInstant(TInstant::MilliSeconds(Min)));
                    } else {
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
                    }
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
        for (const auto& metric : metrics) {
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

            node->Name = name;
            node->Sum = metric.GetSum();
            node->Count = metric.GetCount();
            node->Avg = metric.GetAvg();
            node->Max = metric.GetMax();
            node->Min = metric.GetMin();
        }

        //
        // Copy Ingress/Egress to top level
        // TODO: move all TaskRunner::Stage=Total stats to top level???
        //
        auto& stageTotalNode = statistics.Children["TaskRunner"].Children["Stage=Total"];
        if (const auto& it = stageTotalNode.Children.find("IngressBytes"); it != stageTotalNode.Children.end()) {
            statistics.Children["IngressBytes"].AssignStats(it->second);
        }
        if (const auto& it = stageTotalNode.Children.find("EgressBytes"); it != stageTotalNode.Children.end()) {
            statistics.Children["EgressBytes"].AssignStats(it->second);
        }

        NYson::TYsonWriter writer(&out);
        statistics.Write(writer);

        return out.Str();
    }

    bool SaveAndPackStatistics(const TString& graphKey, const ::google::protobuf::RepeatedPtrField<::NYql::NDqProto::TMetric> metrics, TString& statistics) {
        // Yson routines are very strict, so it's better to try-catch them
        try {
            Statistics[graphKey] = BuildNormalizedStatistics(metrics);
            TStringStream out;
            NYson::TYsonWriter writer(&out);
            writer.OnBeginMap();
            for (const auto& p : Statistics) {
                writer.OnKeyedItem(p.first);
                writer.OnRaw(p.second);
            }
            writer.OnEndMap();
            statistics = NJson2Yson::ConvertYson2Json(out.Str());
            return true;
        } catch (NYson::TYsonException& ex) {
            LOG_E(ex.what());
            return false;
        }
    }

    void AddIssues(const google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& issuesProto) {
        TIssues issues;
        IssuesFromMessage(issuesProto, issues);
        Issues.AddIssues(issues);
    }

    TIssue WrapInternalIssues(const TIssues& issues) {
        NYql::IssuesToMessage(issues, QueryStateUpdateRequest.mutable_internal_issues());
        TString referenceId = GetEntityIdAsString(Params.Config.GetCommon().GetIdsPrefix(), EEntityType::UNDEFINED);
        LOG_E(referenceId << ": " << issues.ToOneLineString());
        return TIssue("Contact technical support and provide query information and this id: " + referenceId + "_" + Now().ToStringUpToSeconds());
    }

    void SaveQueryResponse(NYql::NDqs::TEvQueryResponse::TPtr& ev) {
        auto& result = ev->Get()->Record;
        LOG_D("Query response " << NYql::NDqProto::StatusIds_StatusCode_Name(ev->Get()->Record.GetStatusCode())
            << ". Result set index: " << DqGraphIndex
            << ". Issues count: " << result.IssuesSize()
            << ". Rows count: " << result.GetRowsCount());

        if (ev->Get()->Record.GetStatusCode() == NYql::NDqProto::StatusIds::INTERNAL_ERROR && !Params.Config.GetCommon().GetKeepInternalErrors()) {
            TIssues issues;
            IssuesFromMessage(result.issues(), issues);
            Issues.AddIssue(WrapInternalIssues(issues));
        } else {
            AddIssues(result.issues());
        }

        if (Finishing && !result.issues_size()) { // Race between abort and successful finishing. Override with success and provide results to user.
            FinalQueryStatus = FederatedQuery::QueryMeta::COMPLETED;
            TransientIssues.AddIssues(Issues);
            Issues.Clear();
        }

        auto resultSetIndex = DqGrapResultIndices.at(DqGraphIndex);
        if (resultSetIndex >= 0) {
            auto& header = *QueryStateUpdateRequest.mutable_result_set_meta(resultSetIndex);
            header.set_truncated(result.GetTruncated());
            header.set_rows_count(result.GetRowsCount());
        }

        QueryStateUpdateRequest.mutable_result_id()->set_value(Params.ResultId);

        if (CollectBasic()) {
            TString statistics;
            if (SaveAndPackStatistics("Graph=" + ToString(DqGraphIndex), result.metric(), statistics)) {
                QueryStateUpdateRequest.set_statistics(statistics);
            }
        }
        KillExecuter();
    }

    NYql::NDqProto::TExternalEffect& GetExternalEffects(const TString& providerName) {
        for (auto& exEffect : *QueryStateUpdateRequest.mutable_resources()->mutable_external_effects()) {
            if (exEffect.GetProviderName() == providerName) {
                return exEffect;
            }
        }
        auto& result = *QueryStateUpdateRequest.mutable_resources()->add_external_effects();
        result.SetProviderName(providerName);
        return result;
    }

    bool HandleEvalQueryResponse(NYql::NDqs::TEvQueryResponse::TPtr& ev) {
        auto it = EvalInfos.find(ev->Sender);
        auto eval = it != EvalInfos.end();
        if (eval) {
            IDqGateway::TResult queryResult;

            auto& result = ev->Get()->Record;

            QueryEvalStatusCode = result.GetStatusCode();

            LOG_D("Query evaluation " << NYql::NDqProto::StatusIds_StatusCode_Name(QueryEvalStatusCode)
                << ". " << it->second.Index << " response. Issues count: " << result.IssuesSize()
                << ". Rows count: " << result.GetRowsCount());

            TVector<NDq::TDqSerializedBatch> rows;
            for (const auto& s : result.GetSample()) {
                NDq::TDqSerializedBatch batch;
                batch.Proto = s;
                rows.emplace_back(std::move(batch));
            }

            TProtoBuilder protoBuilder(ResultFormatSettings->ResultType, ResultFormatSettings->Columns);

            bool ysonTruncated = false;
            queryResult.Data = protoBuilder.BuildYson(std::move(rows), ResultFormatSettings->SizeLimit.GetOrElse(Max<ui64>()),
                ResultFormatSettings->RowsLimit.GetOrElse(Max<ui64>()), &ysonTruncated);

            queryResult.RowsCount = result.GetRowsCount();
            queryResult.Truncated = result.GetTruncated() || ysonTruncated;

            TIssues issues;
            IssuesFromMessage(result.GetIssues(), issues);

            if (QueryEvalStatusCode == NYql::NDqProto::StatusIds::INTERNAL_ERROR && !Params.Config.GetCommon().GetKeepInternalErrors()) {
                auto issue = WrapInternalIssues(issues);
                issues.Clear();
                issues.AddIssue(issue);
            }

            bool error = false;
            for (const auto& issue : issues) {
                if (issue.GetSeverity() <= TSeverityIds::S_ERROR) {
                    error = true;
                }
            }

            if (!error) {
                queryResult.SetSuccess();
            }

            if (CollectBasic()) {
                TString statistics;
                if (SaveAndPackStatistics("Precompute=" + ToString(it->second.Index), result.metric(), statistics)) {
                    QueryStateUpdateRequest.set_statistics(statistics);
                }
            }

            queryResult.AddIssues(issues);
            it->second.Result.SetValue(queryResult);
            EvalInfos.erase(it);
        }
        return eval;
    }

    void Handle(NYql::NDqs::TEvQueryResponse::TPtr& ev) {
        if (HandleEvalQueryResponse(ev)) {
            return;
        }

        if (ev->Sender != ExecuterId) {
           LOG_E("TEvQueryResponse received from UNKNOWN Actor (TDqExecuter?) " << ev->Sender);
           return;
        }

        SaveQueryResponse(ev);

        auto statusCode = ev->Get()->Record.GetStatusCode();
        if (statusCode == NYql::NDqProto::StatusIds::UNSPECIFIED) {
           LOG_E("StatusCode == NYql::NDqProto::StatusIds::UNSPECIFIED, it is not expected, the query will be failed.");
        }

        if (statusCode != NYql::NDqProto::StatusIds::SUCCESS) {
            // Error
            ResignQuery(statusCode);
            return;
        }

        if (DqGraphIndex + 1 >= static_cast<i32>(DqGraphParams.size())) {
            // Success
            QueryStateUpdateRequest.mutable_resources()->set_final_run_no(Params.RestartCount);
            Finish(GetFinishStatus(true));
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

        if (HandleEvalQueryResponse(ev)) {
            return;
        }

        if (ev->Sender != ExecuterId) {
           LOG_E("TEvQueryResponse received from UNKNOWN Actor (TDqExecuter?) when FINISHED " << ev->Sender);
           return;
        }

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
            Finish(FederatedQuery::QueryMeta::FAILED);
        } else {
            QueryStateUpdateRequest.mutable_resources()->set_topic_consumers_state(Fq::Private::TaskResources::READY);
            ProcessQuery();
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
        return Params.Resources.topic_consumers_state() == Fq::Private::TaskResources::PREPARE
            || Params.Resources.topic_consumers_state() == Fq::Private::TaskResources::READY;
    }

    bool CanRunReadRulesDeletionActor() const {
        return !ReadRulesCreatorId && FinalizingStatusIsWritten && QueryResponseArrived;
    }

    TVector<std::shared_ptr<NYdb::ICredentialsProviderFactory>> PrepareReadRuleCredentials() {
        TVector<std::shared_ptr<NYdb::ICredentialsProviderFactory>> credentials;
        credentials.reserve(Params.Resources.topic_consumers().size());
        for (const Fq::Private::TopicConsumer& c : Params.Resources.topic_consumers()) {
            if (const TString& tokenName = c.token_name()) {
                credentials.emplace_back(
                    CreateCredentialsProviderFactoryForStructuredToken(Params.CredentialsFactory, FindTokenByName(tokenName), c.add_bearer_to_token()));
            } else {
                credentials.emplace_back(NYdb::CreateInsecureCredentialsProviderFactory());
            }
        }
        return credentials;
    }

    void RunReadRulesDeletionActor() {
        Register(
            ::NFq::MakeReadRuleDeleterActor(
                SelfId(),
                Params.QueryId,
                Params.YqSharedResources->UserSpaceYdbDriver,
                Params.Resources.topic_consumers(),
                PrepareReadRuleCredentials()
            )
        );
    }

    void Handle(NFq::TEvInternalService::TEvCreateRateLimiterResourceResponse::TPtr& ev) {
        LOG_D("Rate limiter resource creation finished. Success: " << ev->Get()->Status.IsSuccess());
        RateLimiterResourceCreatorId = {};
        if (!ev->Get()->Status.IsSuccess()) {
            AddIssueWithSubIssues("Problems with rate limiter resource creation", ev->Get()->Status.GetIssues());
            LOG_D(Issues.ToOneLineString());
            Finish(FederatedQuery::QueryMeta::FAILED);
        } else {
            Params.Resources.set_rate_limiter_path(ev->Get()->Result.rate_limiter());
            QueryStateUpdateRequest.mutable_resources()->set_rate_limiter_path(ev->Get()->Result.rate_limiter());
            QueryStateUpdateRequest.mutable_resources()->set_rate_limiter(Fq::Private::TaskResources::READY);
            ProcessQuery();
        }
    }

    void HandleFinish(NFq::TEvInternalService::TEvCreateRateLimiterResourceResponse::TPtr& ev) {
        LOG_D("Rate limiter resource creation finished. Success: " << ev->Get()->Status.IsSuccess() << ". Issues: " << ev->Get()->Status.GetIssues().ToOneLineString());
        RateLimiterResourceCreatorId = {};

        StartRateLimiterResourceDeleterIfCan();
    }

    void HandleFinish(NFq::TEvInternalService::TEvDeleteRateLimiterResourceResponse::TPtr& ev) {
        LOG_D("Rate limiter resource deletion finished. Success: " << ev->Get()->Status.IsSuccess() << ". Issues: " << ev->Get()->Status.GetIssues().ToOneLineString());
        RateLimiterResourceDeleterId = {};
        RateLimiterResourceWasDeleted = true;

        ContinueFinish();
    }

    bool StartRateLimiterResourceDeleterIfCan() {
        if (!RateLimiterResourceDeleterId && !RateLimiterResourceCreatorId && FinalizingStatusIsWritten && QueryResponseArrived && Params.Config.GetRateLimiter().GetEnabled()) {
            LOG_D("Start rate limiter resource deleter");
            RateLimiterResourceDeleterId = Register(CreateRateLimiterResourceDeleter(SelfId(), Params.Owner, Params.QueryId, Params.Scope, Params.TenantName));
            return true;
        }
        return false;
    }

    TEvaluationGraphInfo RunEvalDqGraph(NFq::NProto::TGraphParams& dqGraphParams) {

        LOG_D("RunEvalDqGraph");

        FillGraphMemoryInfo(dqGraphParams);

        TDqConfiguration::TPtr dqConfiguration = MakeIntrusive<TDqConfiguration>();
        dqConfiguration->Dispatch(dqGraphParams.GetSettings());
        dqConfiguration->FreezeDefaults();
        dqConfiguration->FallbackPolicy = EFallbackPolicy::Never;

        TEvaluationGraphInfo info;

        info.Index = DqEvalIndex++;
        info.ExecuterId = Register(NYql::NDq::MakeDqExecuter(MakeNodesManagerId(), SelfId(), Params.QueryId, "", dqConfiguration, QueryCounters.Counters, TInstant::Now(), false));

        if (dqGraphParams.GetResultType()) {
            TVector<TString> columns;
            for (const auto& column : dqGraphParams.GetColumns()) {
                columns.emplace_back(column);
            }

            NActors::TActorId empty = {};
            THashMap<TString, TString> emptySecureParams; // NOT USED in RR
            info.ResultId = Register(
                    MakeResultReceiver(
                        columns, info.ExecuterId, dqGraphParams.GetSession(), dqConfiguration, emptySecureParams,
                        dqGraphParams.GetResultType(), empty, false).Release());

        } else {
            LOG_D("ResultReceiver was NOT CREATED since ResultType is empty");
            info.ResultId = info.ExecuterId;
        }

        info.ControlId = Register(NYql::MakeTaskController(SessionId, info.ExecuterId, info.ResultId, dqConfiguration, QueryCounters, TDuration::Seconds(3)).Release());

        Yql::DqsProto::ExecuteGraphRequest request;
        request.SetSourceId(dqGraphParams.GetSourceId());
        request.SetResultType(dqGraphParams.GetResultType());
        request.SetSession(dqGraphParams.GetSession());
        *request.MutableSettings() = dqGraphParams.GetSettings();
        *request.MutableSecureParams() = dqGraphParams.GetSecureParams();
        *request.MutableColumns() = dqGraphParams.GetColumns();
        PrepareResultFormatSettings(dqGraphParams, *dqConfiguration);
        NTasksPacker::UnPack(*request.MutableTask(), dqGraphParams.GetTasks(), dqGraphParams.GetStageProgram());
        Send(info.ExecuterId, new NYql::NDqs::TEvGraphRequest(request, info.ControlId, info.ResultId));
        LOG_D("Evaluation Executer: " << info.ExecuterId << ", Controller: " << info.ControlId << ", ResultActor: " << info.ResultId);
        return info;
    }

    void RunNextDqGraph() {
        auto& dqGraphParams = DqGraphParams.at(DqGraphIndex);
        TDqConfiguration::TPtr dqConfiguration = MakeIntrusive<TDqConfiguration>();
        dqConfiguration->Dispatch(dqGraphParams.GetSettings());
        dqConfiguration->FreezeDefaults();
        dqConfiguration->FallbackPolicy = EFallbackPolicy::Never;

        bool enableCheckpointCoordinator =
            Params.QueryType == FederatedQuery::QueryContent::STREAMING && 
            Params.Config.GetCheckpointCoordinator().GetEnabled() && 
            !dqConfiguration->DisableCheckpoints.Get().GetOrElse(false);

        ExecuterId = Register(NYql::NDq::MakeDqExecuter(MakeNodesManagerId(), SelfId(), Params.QueryId, "", dqConfiguration, QueryCounters.Counters, TInstant::Now(), enableCheckpointCoordinator));

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
            resultId = Register(
                    CreateResultWriter(
                        ExecuterId, dqGraphParams.GetResultType(),
                        writerResultId, columns, dqGraphParams.GetSession(), Params.Deadline, Params.ResultBytesLimit));

            PrepareResultFormatSettings(dqGraphParams, *dqConfiguration);
        } else {
            LOG_D("ResultWriter was NOT CREATED since ResultType is empty");
            resultId = ExecuterId;
            ClearResultFormatSettings();
        }

        if (enableCheckpointCoordinator) {
            ControlId = Register(MakeCheckpointCoordinator(
                ::NFq::TCoordinatorId(Params.QueryId + "-" + ToString(DqGraphIndex), Params.PreviousQueryRevision),
                NYql::NDq::MakeCheckpointStorageID(),
                SelfId(),
                Params.Config.GetCheckpointCoordinator(),
                QueryCounters.Counters,
                dqGraphParams,
                Params.StateLoadMode,
                Params.StreamingDisposition,
                // vvv TaskController temporary params vvv
                SessionId,
                ExecuterId,
                resultId,
                dqConfiguration,
                QueryCounters,
                TDuration::Seconds(3),
                TDuration::Seconds(1)
                ).Release());
        } else {
            ControlId = Register(NYql::MakeTaskController(
                SessionId,
                ExecuterId,
                resultId,
                dqConfiguration,
                QueryCounters,
                TDuration::Seconds(3)
            ).Release());
        }

        Yql::DqsProto::ExecuteGraphRequest request;
        request.SetSourceId(dqGraphParams.GetSourceId());
        request.SetResultType(dqGraphParams.GetResultType());
        request.SetSession(dqGraphParams.GetSession());
        *request.MutableSettings() = dqGraphParams.GetSettings();
        *request.MutableSecureParams() = dqGraphParams.GetSecureParams();
        *request.MutableColumns() = dqGraphParams.GetColumns();
        auto& commonTaskParams = *request.MutableCommonTaskParams();
        commonTaskParams["fq.job_id"] = Params.JobId;
        commonTaskParams["fq.restart_count"] = ToString(Params.RestartCount);
        request.SetStatsMode(StatsMode);

        NTasksPacker::UnPack(*request.MutableTask(), dqGraphParams.GetTasks(), dqGraphParams.GetStageProgram());
        Send(ExecuterId, new NYql::NDqs::TEvGraphRequest(request, ControlId, resultId));
        LOG_D("Executer: " << ExecuterId << ", Controller: " << ControlId << ", ResultIdActor: " << resultId);
    }

    void PrepareResultFormatSettings(NFq::NProto::TGraphParams& dqGraphParams, const TDqConfiguration& dqConfiguration) {
        ResultFormatSettings.ConstructInPlace();
        for (const auto& c : dqGraphParams.GetColumns()) {
            ResultFormatSettings->Columns.push_back(c);
        }

        ResultFormatSettings->ResultType = dqGraphParams.GetResultType();
        ResultFormatSettings->SizeLimit = dqConfiguration._AllResultsBytesLimit.Get();
        ResultFormatSettings->RowsLimit = dqConfiguration._RowsLimitPerWrite.Get();
    }

    void ClearResultFormatSettings() {
        ResultFormatSettings.Clear();
    }

    void SetupYqlCore(NYql::TYqlCoreConfig& yqlCore) const {
        auto flags = yqlCore.MutableFlags();
        *flags = Params.Config.GetGateways().GetYqlCore().GetFlags();
    }

    void SetupDqSettings(NYql::TDqGatewayConfig& dqGatewaysConfig) const {
        ::google::protobuf::RepeatedPtrField<::NYql::TAttr>& dqSettings = *dqGatewaysConfig.MutableDefaultSettings();

        // Copy settings from config
        // They are stronger than settings from this function.
        dqSettings = Params.Config.GetGateways().GetDq().GetDefaultSettings();

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

        apply("MaxTasksPerStage", ToString(MaxTasksPerStage));
        apply("MaxTasksPerOperation", ToString(MaxTasksPerOperation));
        apply("EnableComputeActor", "1");
        apply("ComputeActorType", "async");
        apply("_EnablePrecompute", "1");
        apply("WatermarksMode", "disable");
        apply("WatermarksGranularityMs", "1000");
        apply("WatermarksLateArrivalDelayMs", "5000");
        apply("WatermarksIdlePartitions", "true");
        apply("EnableChannelStats", "true");
        apply("ExportStats", "true");

        switch (Params.QueryType) {
        case FederatedQuery::QueryContent::STREAMING: {
            // - turn on check that query has one graph.
            apply("_OneGraphPerQuery", "1");
            apply("_TableTimeout", "0");
            apply("_LiteralTimeout", "0");
            break;
        }
        case FederatedQuery::QueryContent::ANALYTICS: {
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
        for (const auto& pq : Params.Config.GetGateways().GetPq().GetClusterMapping()) {
            auto& clusterCfg = *gatewaysConfig.MutablePq()->AddClusterMapping();
            clusterCfg = pq;
            clusters.emplace(clusterCfg.GetName(), PqProviderName);
        }

        for (const auto& solomon : Params.Config.GetGateways().GetSolomon().GetClusterMapping()) {
            auto& clusterCfg = *gatewaysConfig.MutableSolomon()->AddClusterMapping();
            clusterCfg = solomon;
            clusters.emplace(clusterCfg.GetName(), SolomonProviderName);
        }
    }

    FederatedQuery::QueryMeta::ComputeStatus GetFinishStatus(bool isOk) const {
        if (isOk) {
            return FederatedQuery::QueryMeta::COMPLETED;
        }

        switch (Action) {
        case FederatedQuery::PAUSE:
        case FederatedQuery::PAUSE_GRACEFULLY:
        case FederatedQuery::ABORT:
        case FederatedQuery::ABORT_GRACEFULLY:
            return FederatedQuery::QueryMeta::ABORTED_BY_USER;
        case FederatedQuery::RESUME:
            return FederatedQuery::QueryMeta::ABORTED_BY_SYSTEM;
        case FederatedQuery::QUERY_ACTION_UNSPECIFIED:
        case FederatedQuery::QueryAction_INT_MIN_SENTINEL_DO_NOT_USE_:
        case FederatedQuery::QueryAction_INT_MAX_SENTINEL_DO_NOT_USE_:
            return FederatedQuery::QueryMeta::FAILED;
        }
    }

    FederatedQuery::QueryMeta::ComputeStatus GetFinalizingStatus() { // Status before final. "*ING" one.
        switch (FinalQueryStatus) {
        case FederatedQuery::QueryMeta_ComputeStatus_QueryMeta_ComputeStatus_INT_MIN_SENTINEL_DO_NOT_USE_:
        case FederatedQuery::QueryMeta_ComputeStatus_QueryMeta_ComputeStatus_INT_MAX_SENTINEL_DO_NOT_USE_:
        case FederatedQuery::QueryMeta::COMPUTE_STATUS_UNSPECIFIED:
        case FederatedQuery::QueryMeta::STARTING:
        case FederatedQuery::QueryMeta::ABORTING_BY_USER:
        case FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM:
        case FederatedQuery::QueryMeta::RESUMING:
        case FederatedQuery::QueryMeta::RUNNING:
        case FederatedQuery::QueryMeta::COMPLETING:
        case FederatedQuery::QueryMeta::FAILING:
        case FederatedQuery::QueryMeta::PAUSING: {
            TStringBuilder msg;
            msg << "\"" << FederatedQuery::QueryMeta::ComputeStatus_Name(FinalQueryStatus) << "\" is not a final status for query";
            Issues.AddIssue(msg);
            throw yexception() << msg;
        }

        case FederatedQuery::QueryMeta::ABORTED_BY_USER:
            return FederatedQuery::QueryMeta::ABORTING_BY_USER;
        case FederatedQuery::QueryMeta::ABORTED_BY_SYSTEM:
            return FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM;
        case FederatedQuery::QueryMeta::COMPLETED:
            return FederatedQuery::QueryMeta::COMPLETING;
        case FederatedQuery::QueryMeta::FAILED:
            return FederatedQuery::QueryMeta::FAILING;
        case FederatedQuery::QueryMeta::PAUSED:
            return FederatedQuery::QueryMeta::PAUSING;
        }
    }

    static FederatedQuery::QueryMeta::ComputeStatus GetFinalStatusFromFinalizingStatus(FederatedQuery::QueryMeta::ComputeStatus status) {
        switch (status) {
        case FederatedQuery::QueryMeta::ABORTING_BY_USER:
            return FederatedQuery::QueryMeta::ABORTED_BY_USER;
        case FederatedQuery::QueryMeta::ABORTING_BY_SYSTEM:
            return FederatedQuery::QueryMeta::ABORTED_BY_SYSTEM;
        case FederatedQuery::QueryMeta::COMPLETING:
            return FederatedQuery::QueryMeta::COMPLETED;
        case FederatedQuery::QueryMeta::FAILING:
            return FederatedQuery::QueryMeta::FAILED;
        default:
            return FederatedQuery::QueryMeta::COMPUTE_STATUS_UNSPECIFIED;
        }
    }

    void WriteFinalizingStatus() {
        const FederatedQuery::QueryMeta::ComputeStatus finalizingStatus = GetFinalizingStatus();
        Params.Status = finalizingStatus;
        LOG_D("Write finalizing status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(finalizingStatus));
        Fq::Private::PingTaskRequest request;
        request.set_status(finalizingStatus);
        Send(Pinger, new TEvents::TEvForwardPingRequest(request), 0, SaveFinalizingStatusCookie);
    }

    void Finish(FederatedQuery::QueryMeta::ComputeStatus status) {
        LOG_D("Is about to finish query with status " << FederatedQuery::QueryMeta::ComputeStatus_Name(status));
        Finishing = true;
        FinalQueryStatus = status;

        QueryStateUpdateRequest.set_status(FinalQueryStatus); // Can be changed later.
        if (FinalQueryStatus == FederatedQuery::QueryMeta::COMPLETED && QueryStateUpdateRequest.status_code() == NYql::NDqProto::StatusIds::UNSPECIFIED) {
            QueryStateUpdateRequest.set_status_code(NYql::NDqProto::StatusIds::SUCCESS);
        }
        *QueryStateUpdateRequest.mutable_finished_at() = google::protobuf::util::TimeUtil::MillisecondsToTimestamp(TInstant::Now().MilliSeconds());
        Become(&TRunActor::StateFuncWrapper<&TRunActor::FinishStateFunc>);

        if (!FinalizingStatusIsWritten) {
            WriteFinalizingStatus();
        }

        CancelRunningQuery();
        ContinueFinish();
    }

    void StartEffectApplicators() {
        LOG_D("Apply effects with status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(Params.Status));
        for (const auto& externalEffect : QueryStateUpdateRequest.resources().external_effects()) {
            auto providerName = externalEffect.GetProviderName();
            if (providerName == "S3Sink") {
                EffectApplicatorCount++;

                THashMap<TString, TString> mergedSecureParams;
                for (auto& graphParams : DqGraphParams) {
                    for (auto& [name, value] : graphParams.GetSecureParams()) {
                        mergedSecureParams[name] = value;
                    }
                }

                Register(Params.S3ActorsFactory->CreateS3ApplicatorActor(SelfId()
                                            , Params.S3Gateway
                                            , Params.QueryId
                                            , Params.JobId
                                            , QueryStateUpdateRequest.resources().final_run_no()
                                            , Params.Status == FederatedQuery::QueryMeta::COMPLETING || Params.Status == FederatedQuery::QueryMeta::ABORTING_BY_USER
                                            , mergedSecureParams
                                            , Params.CredentialsFactory
                                            , externalEffect).Release()
                );

                if (Params.Status == FederatedQuery::QueryMeta::ABORTING_BY_USER) {
                    for (auto& prefix : S3Prefixes) {
                        LOG_W("Partial results are possible with prefix: " << prefix);
                        Issues.AddIssue(TIssue(TStringBuilder() << "Partial results are possible with prefix: " << prefix));
                    }
                }
            } else {
                LOG_E("Unknown effect applicator: " << providerName);
            }
        }
    }

     void ContinueFinish() {
        bool notFinished = false;
        if (NeedDeleteReadRules() && !ConsumersAreDeleted) {
            if (CanRunReadRulesDeletionActor()) {
                RunReadRulesDeletionActor();
            }
            notFinished = true;
        }

        if (!RateLimiterResourceWasDeleted && Params.Config.GetRateLimiter().GetEnabled()) {
            StartRateLimiterResourceDeleterIfCan();
            notFinished = true;
        }

        if (EffectApplicatorCount == 0 && !QueryStateUpdateRequest.resources().external_effects().empty()) {
            StartEffectApplicators();
            notFinished = true;
        } else if (EffectApplicatorFinished < EffectApplicatorCount) {
            notFinished = true;
        }

        if (notFinished) {
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
        /*
            1. If the execution has already started then the issue will be put through TEvAbortExecution
            2. If execution hasn't started then the issue will be put in this place
            3. This is necessary for symmetrical behavior in case of abortion
        */
        if (!QueryStateUpdateRequest.issues_size() && IsAbortedStatus(QueryStateUpdateRequest.status())) {
            auto& issue = *QueryStateUpdateRequest.add_issues();
            issue.set_message(FederatedQuery::QueryMeta::ComputeStatus_Name(QueryStateUpdateRequest.status()));
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
        }

        if (!QueryStateUpdateRequest.has_result_id() && FinalQueryStatus != FederatedQuery::QueryMeta::COMPLETED && FinalQueryStatus != FederatedQuery::QueryMeta::COMPLETING) {
            QueryStateUpdateRequest.mutable_result_id()->set_value("");
        }

        Send(Pinger, new TEvents::TEvForwardPingRequest(QueryStateUpdateRequest, true));

        PassAway();
    }

    void Abort(const TString& message, FederatedQuery::QueryMeta::ComputeStatus status, const NYql::TIssues& issues = {}) {
        AddIssueWithSubIssues(message, issues);
        Finish(status);
    }

    void FillDqGraphParams() {
        for (const auto& s : Params.DqGraphs) {
            NFq::NProto::TGraphParams dqGraphParams;
            Y_ABORT_UNLESS(dqGraphParams.ParseFromString(s));
            DqGraphParams.emplace_back(std::move(dqGraphParams));
        }
    }

    void RunProgram() {
        //NYql::NLog::YqlLogger().SetComponentLevel(NYql::NLog::EComponent::Core, NYql::NLog::ELevel::TRACE);
        //NYql::NLog::YqlLogger().SetComponentLevel(NYql::NLog::EComponent::CoreEval, NYql::NLog::ELevel::TRACE);
        //NYql::NLog::YqlLogger().SetComponentLevel(NYql::NLog::EComponent::CorePeepHole, NYql::NLog::ELevel::TRACE);

        LOG_D("Compiling query ...");
        NYql::TGatewaysConfig gatewaysConfig;

        SetupYqlCore(*gatewaysConfig.MutableYqlCore());

        SetupDqSettings(*gatewaysConfig.MutableDq());
        // the main idea of having Params.GatewaysConfig is to copy clusters only
        // but in this case we have to copy S3 provider limits
        *gatewaysConfig.MutableS3() = Params.Config.GetGateways().GetS3();
        gatewaysConfig.MutableS3()->ClearClusterMapping();

        *gatewaysConfig.MutableGeneric() = Params.Config.GetGateways().GetGeneric();
        gatewaysConfig.MutableGeneric()->ClearClusterMapping();

        THashMap<TString, TString> clusters;

        TString monitoringEndpoint = Params.Config.GetCommon().GetMonitoringEndpoint();

        //todo: consider cluster name clashes
        AddClustersFromConfig(gatewaysConfig, clusters);
        AddClustersFromConnections(Params.Config.GetCommon(),
            YqConnections,
            monitoringEndpoint,
            Params.AuthToken,
            Params.AccountIdSignatures,
            // out params:
            gatewaysConfig,
            clusters);

        TVector<TDataProviderInitializer> dataProvidersInit;
        const std::shared_ptr<IDatabaseAsyncResolver> dbResolver = std::make_shared<TDatabaseAsyncResolverImpl>(
            NActors::TActivationContext::ActorSystem(),
            Params.DatabaseResolver,
            Params.Config.GetCommon().GetYdbMvpCloudEndpoint(),
            Params.Config.GetCommon().GetMdbGateway(),
            Params.MdbEndpointGenerator,
            Params.QueryId);
        {
            // TBD: move init to better place
            QueryStateUpdateRequest.set_scope(Params.Scope.ToString());
            QueryStateUpdateRequest.mutable_query_id()->set_value(Params.QueryId);
            QueryStateUpdateRequest.set_owner_id(Params.Owner);
            dataProvidersInit.push_back(GetDqDataProviderInitializer(&CreateDqExecTransformer, NFq::CreateEmptyGateway(SelfId()), Params.DqCompFactory, {}, nullptr));
        }

        {
           dataProvidersInit.push_back(GetGenericDataProviderInitializer(Params.ConnectorClient, dbResolver, Params.CredentialsFactory));
        }

        {
           dataProvidersInit.push_back(GetS3DataProviderInitializer(Params.S3Gateway, Params.CredentialsFactory, Params.Config.GetGateways().GetS3().GetAllowLocalFiles()));
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

        SessionId = TStringBuilder()
            << Params.QueryId << '#'
            << Params.ResultId << '#'
            << Params.Scope.ToString() << '#'
            << Params.Owner << '#'
            << Params.CloudId;

        NSQLTranslation::TTranslationSettings sqlSettings;
        sqlSettings.ClusterMapping = clusters;
        sqlSettings.SyntaxVersion = 1;
        sqlSettings.PgParser = (Params.QuerySyntax == FederatedQuery::QueryContent::PG);
        sqlSettings.V0Behavior = NSQLTranslation::EV0Behavior::Disable;
        sqlSettings.Flags.insert({ "DqEngineEnable", "DqEngineForce", "DisableAnsiOptionalAs", "FlexibleTypes", "AnsiInForEmptyOrNullableItemsCollections" });
        try {
            AddTableBindingsFromBindings(Params.Bindings, YqConnections, sqlSettings);
        } catch (const std::exception& e) {
            Issues.AddIssue(ExceptionToIssue(e));
            QueryEvalStatusCode = NYql::NDqProto::StatusIds::INTERNAL_ERROR;
            FinishProgram(TProgram::TStatus::Error);
            return;
        }

        ProgramRunnerId = Register(new NYql::NDq::TLogWrapReceive(new TProgramRunnerActor(
            SelfId(),
            Params.FunctionRegistry,
            Params.NextUniqueId,
            dataProvidersInit,
            Params.ModuleResolver,
            gatewaysConfig,
            Params.Sql,
            SessionId,
            sqlSettings,
            Params.ExecuteMode,
            Params.QueryId
        ), Params.QueryId));
    }

    void Handle(TEvPrivate::TEvProgramFinished::TPtr& ev) {
        if (!Finishing) {
            UpdateAstAndPlan(ev->Get()->Plan, ev->Get()->Expr);
            FinishProgram(ev->Get()->Status, ev->Get()->Message, ev->Get()->Issues);
        }
    }

    void Handle(TEvents::TEvAsyncContinue::TPtr& ev) {
        LOG_D("Compiling finished");
        NYql::TProgram::TStatus status = TProgram::TStatus::Error;

        const auto& f = ev->Get()->Future;
        try {
            status = f.GetValue();
            if (status == TProgram::TStatus::Async) {
                auto futureStatus = Program->ContinueAsync();
                futureStatus.Subscribe([actorSystem = TActivationContext::ActorSystem(), selfId = SelfId()](const TProgram::TFutureStatus& f) {
                    actorSystem->Send(selfId, new TEvents::TEvAsyncContinue(f));
                });
                return;
            }
        } catch (const std::exception& err) {
            Issues.AddIssue(ExceptionToIssue(err));
        }

        TStringStream exprOut;
        TStringStream planOut;
        Program->Print(&exprOut, &planOut);

        UpdateAstAndPlan(NJson2Yson::ConvertYson2Json(planOut.Str()), exprOut.Str());
        FinishProgram(status);
    }

    void FinishProgram(NYql::TProgram::TStatus status, const TString& message = "", const NYql::TIssues& issues = {}) {
        if (status == TProgram::TStatus::Ok || (DqGraphParams.size() > 0 && !DqGraphParams[0].GetResultType())) {
            if (issues) {
                SendTransientIssues(issues);
            }
            for (auto& graphParams : DqGraphParams) {
                FillGraphMemoryInfo(graphParams);
            }
            PrepareGraphs(); // will compress and seal graphs
        } else {
            AddIssueWithSubIssues(message ? message : TStringBuilder() << "Run query failed: " << ToString(status), issues);
            ResignQuery(
                QueryEvalStatusCode != NYql::NDqProto::StatusIds::UNSPECIFIED ? QueryEvalStatusCode : NYql::NDqProto::StatusIds::ABORTED
            );
        }
    }

    void Handle(NActors::TEvents::TEvUndelivered::TPtr&) {
        Fail("TRunActor::OnUndelivered");
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream html;
        html << "<h2>RunActor</h2>";

        if (ExecuterId != NActors::TActorId{}) {
            html << "<table class='table simple-table1 table-hover table-condensed'>";
            html << "<thead><tr>";
            html << "<th>Graph</th>";
            html << "<th>Executer</th>";
            html << "<th>Controller</th>";
            html << "<th>Checkpoint Coord</th>";
            html << "</tr></thead><tbody>";
            html << "<tr>";
                for (auto& pair : EvalInfos) {
                    html << "<td>" << "Evaluation" << "</td>";
                    auto& info = pair.second;
                    html << "<td> Executer" << info.ExecuterId << "</td>";
                    html << "<td> Control" << info.ControlId << "</td>";
                    html << "<td> Result" << info.ResultId << "</td>";
                }
                if (!DqGraphParams.empty()) {
                    html << "<td>" << DqGraphIndex << " of " << DqGraphParams.size() << "</td>";
                    html << "<td> Executer" << ExecuterId << "</td>";
                    html << "<td> Control" << ControlId << "</td>";
                }
            html << "</tr>";
            html << "</tbody></table>";
        }

        html << "<table class='table simple-table1 table-hover table-condensed'>";
        html << "<thead><tr>";
        html << "<th>Param</th>";
        html << "<th>Value</th>";
        html << "</tr></thead><tbody>";
            html << "<tr><td>Cloud ID</td><td>"     << Params.CloudId                                              << "</td></tr>";
            html << "<tr><td>Scope</td><td>"        << Params.Scope.ToString()                                     << "</td></tr>";
            html << "<tr><td>Query ID</td><td>"     << Params.QueryId                                              << "</td></tr>";
            html << "<tr><td>User ID</td><td>"      << Params.UserId                                               << "</td></tr>";
            html << "<tr><td>Owner</td><td>"        << Params.Owner                                                << "</td></tr>";
            html << "<tr><td>Result ID</td><td>"    << Params.ResultId                                             << "</td></tr>";
            html << "<tr><td>Prev Rev</td><td>"     << Params.PreviousQueryRevision                                << "</td></tr>";
            html << "<tr><td>Query Type</td><td>"   << FederatedQuery::QueryContent::QueryType_Name(Params.QueryType) << "</td></tr>";
            html << "<tr><td>Exec Mode</td><td>"    << FederatedQuery::ExecuteMode_Name(Params.ExecuteMode)           << "</td></tr>";
            html << "<tr><td>St Load Mode</td><td>" << FederatedQuery::StateLoadMode_Name(Params.StateLoadMode)       << "</td></tr>";
            html << "<tr><td>Disposition</td><td>"  << Params.StreamingDisposition                                 << "</td></tr>";
            html << "<tr><td>Status</td><td>"       << FederatedQuery::QueryMeta::ComputeStatus_Name(Params.Status)   << "</td></tr>";
        html << "</tbody></table>";

        if (Params.Connections.size()) {
            html << "<table class='table simple-table1 table-hover table-condensed'>";
            html << "<thead><tr>";
            html << "<th>Connection</th>";
            html << "<th>Type</th>";
            html << "<th>ID</th>";
            html << "<th>Description</th>";
            html << "</tr></thead><tbody>";
            for (const auto& connection : Params.Connections) {
                html << "<tr>";
                html << "<td>" << connection.content().name() << "</td>";
                html << "<td>";
                switch (connection.content().setting().connection_case()) {
                case FederatedQuery::ConnectionSetting::kYdbDatabase:
                    html << "YDB";
                    break;
                case FederatedQuery::ConnectionSetting::kClickhouseCluster:
                    html << "CLICKHOUSE";
                    break;
                case FederatedQuery::ConnectionSetting::kObjectStorage:
                    html << "OBJECT STORAGE";
                    break;
                case FederatedQuery::ConnectionSetting::kDataStreams:
                    html << "DATA STREAMS";
                    break;
                case FederatedQuery::ConnectionSetting::kMonitoring:
                    html << "MONITORING";
                    break;
                case FederatedQuery::ConnectionSetting::kPostgresqlCluster:
                    html << "POSTGRESQL";
                    break;
                default:
                    html << "UNDEFINED";
                    break;
                }
                html << "</td>";
                html << "<td>" << connection.meta().id() << "</td>";
                html << "<td>" << connection.content().description() << "</td>";
                html << "</tr>";
            }
            html << "</tbody></table>";
        }

        if (Params.Bindings.size()) {
            html << "<table class='table simple-table1 table-hover table-condensed'>";
            html << "<thead><tr>";
            html << "<th>Binding</th>";
            html << "<th>Type</th>";
            html << "<th>ID</th>";
            html << "<th>Connection ID</th>";
            html << "<th>Description</th>";
            html << "</tr></thead><tbody>";
            for (const auto& binding : Params.Bindings) {
                html << "<tr>";
                html << "<td>" << binding.content().name() << "</td>";
                html << "<td>";
                switch (binding.content().setting().binding_case()) {
                case FederatedQuery::BindingSetting::kDataStreams:
                    html << "DATA STREAMS";
                    break;
                case FederatedQuery::BindingSetting::kObjectStorage:
                    html << "OBJECT STORAGE";
                    break;
                default:
                    html << "UNDEFINED";
                    break;
                }
                html << "</td>";
                html << "<td>" << binding.meta().id() << "</td>";
                html << "<td>" << binding.content().connection_id() << "</td>";
                html << "<td>" << binding.content().description() << "</td>";
                html << "</tr>";
            }
            html << "</tbody></table>";
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(html.Str()));
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
            << " QueryType: " << FederatedQuery::QueryContent::QueryType_Name(Params.QueryType)
            << " ExecuteMode: " << FederatedQuery::ExecuteMode_Name(Params.ExecuteMode)
            << " ResultId: " << Params.ResultId
            << " StateLoadMode: " << FederatedQuery::StateLoadMode_Name(Params.StateLoadMode)
            << " StreamingDisposition: " << Params.StreamingDisposition
            << " Status: " << FederatedQuery::QueryMeta::ComputeStatus_Name(Params.Status)
            << " DqGraphs: " << Params.DqGraphs.size()
            << " DqGraphIndex: " << Params.DqGraphIndex
            << " Resource.TopicConsumers: " << Params.Resources.topic_consumers().size()
            << " }");
    }

    bool CollectBasic() {
        return StatsMode >= NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_BASIC;
    }

private:
    TActorId FetcherId;
    TActorId ProgramRunnerId;
    TRunActorParams Params;
    THashMap<TString, FederatedQuery::Connection> YqConnections;

    TProgramPtr Program;
    TIssues Issues;
    TIssues TransientIssues;
    TQueryResult QueryResult;
    TInstant Deadline;
    TActorId Pinger;
    TInstant CreatedAt;
    FederatedQuery::QueryAction Action = FederatedQuery::QueryAction::QUERY_ACTION_UNSPECIFIED;
    TMap<NActors::TActorId, TEvaluationGraphInfo> EvalInfos;
    std::vector<NFq::NProto::TGraphParams> DqGraphParams;
    std::vector<i32> DqGrapResultIndices;
    i32 DqGraphIndex = 0;
    ui32 DqEvalIndex = 0;
    NActors::TActorId ExecuterId;
    NActors::TActorId ControlId;
    TString SessionId;
    ::NYql::NCommon::TServiceCounters QueryCounters;
    const ::NMonitoring::TDynamicCounters::TCounterPtr QueryUptime;
    Fq::Private::PingTaskRequest QueryStateUpdateRequest;

    const ui64 MaxTasksPerStage;
    const ui64 MaxTasksPerOperation;
    const TCompressor Compressor;

    NYql::NDqProto::EDqStatsMode StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_NONE;
    TMap<TString, TString> Statistics;

    TMaybe<NCommon::TResultFormatSettings> ResultFormatSettings;

    // Consumers creation
    NActors::TActorId ReadRulesCreatorId;

    // Rate limiter resource creation
    bool RateLimiterResourceWasDeleted = false;
    NActors::TActorId RateLimiterResourceCreatorId;
    NActors::TActorId RateLimiterResourceDeleterId;

    // Finish
    bool Finishing = false;
    bool ConsumersAreDeleted = false;
    bool FinalizingStatusIsWritten = false;
    bool QueryResponseArrived = false;
    FederatedQuery::QueryMeta::ComputeStatus FinalQueryStatus = FederatedQuery::QueryMeta::COMPUTE_STATUS_UNSPECIFIED; // Status that will be assigned to query after it finishes.
    NYql::NDqProto::StatusIds::StatusCode QueryEvalStatusCode = NYql::NDqProto::StatusIds::UNSPECIFIED;

    ui64 EffectApplicatorCount = 0;
    ui64 EffectApplicatorFinished = 0;
    TSet<TString> S3Prefixes;

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
    auto queryId = params.QueryId;
    return new NYql::NDq::TLogWrapReceive(new TRunActor(fetcherId, serviceCounters, std::move(params)), queryId);
}

} /* NFq */
