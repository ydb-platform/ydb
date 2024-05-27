#include "grpc_service.h"

#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <ydb/library/yql/providers/dq/actors/executer_actor.h>
#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>
#include <ydb/library/yql/providers/dq/actors/execution_helpers.h>
#include <ydb/library/yql/providers/dq/actors/result_aggregator.h>
#include <ydb/library/yql/providers/dq/actors/events.h>
#include <ydb/library/yql/providers/dq/actors/task_controller.h>
#include <ydb/library/yql/providers/dq/actors/graph_execution_events_actor.h>

#include <ydb/library/yql/providers/dq/counters/task_counters.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_common.h>

//#include <yql/tools/yqlworker/dq/worker_manager/benchmark.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

#include <ydb/library/grpc/server/grpc_counters.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/helpers/future_callback.h>
#include <library/cpp/build_info/build_info.h>
#include <library/cpp/svnversion/svnversion.h>

#include <util/string/split.h>
#include <util/system/env.h>

namespace NYql::NDqs {
    using namespace NYql::NDqs;
    using namespace NKikimr;
    using namespace NThreading;
    using namespace NMonitoring;
    using namespace NActors;

    namespace {
        NYdbGrpc::ICounterBlockPtr BuildCB(TIntrusivePtr<NMonitoring::TDynamicCounters>& counters, const TString& name) {
            auto grpcCB = counters->GetSubgroup("rpc_name", name);
            return MakeIntrusive<NYdbGrpc::TCounterBlock>(
                grpcCB->GetCounter("total", true),
                grpcCB->GetCounter("infly", true),
                grpcCB->GetCounter("notOkReq", true),
                grpcCB->GetCounter("notOkResp", true),
                grpcCB->GetCounter("reqBytes", true),
                grpcCB->GetCounter("inflyReqBytes", true),
                grpcCB->GetCounter("resBytes", true),
                grpcCB->GetCounter("notAuth", true),
                grpcCB->GetCounter("resExh", true),
                grpcCB);
        }

        template<typename RequestType, typename ResponseType>
        class TServiceProxyActor: public TSynchronizableRichActor<TServiceProxyActor<RequestType, ResponseType>> {
        public:
            static constexpr char ActorName[] = "SERVICE_PROXY";
            static constexpr char RetryName[] = "OperationRetry";

            explicit TServiceProxyActor(
                NYdbGrpc::IRequestContextBase* ctx,
                const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
                const TString& traceId, const TString& username)
                : TSynchronizableRichActor<TServiceProxyActor<RequestType, ResponseType>>(&TServiceProxyActor::Handler)
                , Ctx(ctx)
                , Counters(counters)
                , ServiceProxyActorCounters(counters->GetSubgroup("component", "ServiceProxyActor"))
                , ClientDisconnectedCounter(ServiceProxyActorCounters->GetCounter("ClientDisconnected", /*derivative=*/ true))
                , RetryCounter(ServiceProxyActorCounters->GetCounter(RetryName, /*derivative=*/ true))
                , FallbackCounter(ServiceProxyActorCounters->GetCounter("Fallback", /*derivative=*/ true))
                , ErrorCounter(ServiceProxyActorCounters->GetCounter("UnrecoverableError", /*derivative=*/ true))
                , Request(dynamic_cast<const RequestType*>(ctx->GetRequest()))
                , TraceId(traceId)
                , Username(username)
                , Promise(NewPromise<void>())
            {
                Settings->Dispatch(Request->GetSettings());
                Settings->FreezeDefaults();

                MaxRetries = Settings->MaxRetries.Get().GetOrElse(MaxRetries);
                RetryBackoff = TDuration::MilliSeconds(Settings->RetryBackoffMs.Get().GetOrElse(RetryBackoff.MilliSeconds()));
            }

            STRICT_STFUNC(Handler, {
                HFunc(TEvQueryResponse, OnReturnResult);
                cFunc(TEvents::TEvPoison::EventType, OnPoison);
                SFunc(TEvents::TEvBootstrap, DoBootstrap);
                hFunc(TEvDqStats, Handle);
            })

            TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
                return new IEventHandle(self, parentId, new TEvents::TEvBootstrap(), 0);
            }

            void OnPoison() {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
                YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__ ;
                ReplyError(grpc::UNAVAILABLE, "Unexpected error");
                *ClientDisconnectedCounter += 1;
            }

            void Handle(TEvDqStats::TPtr&) {
                // Do nothing
            }

            void DoPassAway() override {
                Promise.SetValue();
            }

            void DoBootstrap(const NActors::TActorContext& ctx) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
                if (!CtxSubscribed) {
                    auto selfId = ctx.SelfID;
                    auto* actorSystem = ctx.ExecutorThread.ActorSystem;
                    Ctx->GetFinishFuture().Subscribe([selfId, actorSystem](const NYdbGrpc::IRequestContextBase::TAsyncFinishResult& future) {
                        Y_ABORT_UNLESS(future.HasValue());
                        if (future.GetValue() == NYdbGrpc::IRequestContextBase::EFinishStatus::CANCEL) {
                            actorSystem->Send(selfId, new TEvents::TEvPoison());
                        }
                    });
                    CtxSubscribed = true;
                }
                Bootstrap();
            }

            virtual void Bootstrap() = 0;

            void SendResponse(TEvQueryResponse::TPtr& ev)
            {
                auto& result = ev->Get()->Record;
                Yql::DqsProto::ExecuteQueryResult queryResult;
                queryResult.Mutableresult()->CopyFrom(result.resultset());
                queryResult.set_yson(result.yson());

                auto statusCode = result.GetStatusCode();
                // this code guarantees that query will be considered failed unless the status is SUCCESS
                // fallback may be performed as an extra measure
                if (statusCode != NYql::NDqProto::StatusIds::SUCCESS) {
                    YQL_CLOG(ERROR, ProviderDq) << "Query is considered FAILED, status=" << static_cast<int>(statusCode);
                    NYql::TIssue rootIssue("Fatal Error");
                    rootIssue.SetCode(NCommon::NeedFallback(statusCode) ? TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR : TIssuesIds::DQ_GATEWAY_ERROR, TSeverityIds::S_ERROR);
                    NYql::TIssues issues;
                    NYql::IssuesFromMessage(result.GetIssues(), issues);
                    for (const auto& issue: issues) {
                        rootIssue.AddSubIssue(MakeIntrusive<TIssue>(issue));
                    }
                    result.MutableIssues()->Clear();
                    NYql::IssuesToMessage({rootIssue}, result.MutableIssues());
                }

                for (const auto& [k, v] : QueryStat.Get()) {
                    std::map<TString, TString> labels;
                    TString prefix, name;
                    if (NCommon::ParseCounterName(&prefix, &labels, &name, k)) {
                        if (prefix == "Actor") {
                            auto group = Counters->GetSubgroup("counters", "Actor");
                            for (const auto& [k, v] : labels) {
                                group = group->GetSubgroup(k, v);
                            }
                            group->GetHistogram(name, ExponentialHistogram(10, 2, 50000))->Collect(v.Sum);
                        }
                    }
                }

                if (Settings->AggregateStatsByStage.Get().GetOrElse(TDqSettings::TDefault::AggregateStatsByStage)) {
                    auto aggregatedQueryStat = AggregateQueryStatsByStage(QueryStat, Task2Stage);
                    aggregatedQueryStat.FlushCounters(ResponseBuffer);
                } else {
                    QueryStat.FlushCounters(ResponseBuffer);
                }

                auto& operation = *ResponseBuffer.mutable_operation();
                operation.Setready(true);
                operation.Mutableresult()->PackFrom(queryResult);
                *operation.Mutableissues() = result.GetIssues();
                ResponseBuffer.SetTruncated(result.GetTruncated());

                Reply(Ydb::StatusIds::SUCCESS, statusCode > 1 || result.GetIssues().size() > 0);
            }

            virtual void DoRetry()
            {
                this->CleanupChildren();
                auto selfId = this->SelfId();
                TActivationContext::Schedule(RetryBackoff, new IEventHandle(selfId, selfId, new TEvents::TEvBootstrap(), 0));
                Retry += 1;
                *RetryCounter +=1 ;
            }

            void OnReturnResult(TEvQueryResponse::TPtr& ev, const NActors::TActorContext& ctx) {
                auto& result = ev->Get()->Record;
                Y_UNUSED(ctx);
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(TraceId);
                YQL_CLOG(DEBUG, ProviderDq) << "TServiceProxyActor::OnReturnResult " << result.GetMetric().size();
                QueryStat.AddCounters(result);

                auto statusCode = result.GetStatusCode();
                if ((statusCode != NYql::NDqProto::StatusIds::SUCCESS || result.GetIssues().size() > 0) && NCommon::IsRetriable(statusCode)) {
                    if (Retry < MaxRetries) {
                        QueryStat.AddCounter(RetryName, TDuration::MilliSeconds(0));
                        NYql::TIssues issues;
                        NYql::IssuesFromMessage(result.GetIssues(), issues);
                        YQL_CLOG(WARN, ProviderDq) << RetryName << " " << Retry << " Issues: " << issues.ToString();
                        DoRetry();
                    } else {
                        YQL_CLOG(ERROR, ProviderDq) << "Retries limit exceeded, status= " << static_cast<int>(ev->Get()->Record.GetStatusCode());
                        if (statusCode == NYql::NDqProto::StatusIds::SUCCESS) {
                            ev->Get()->Record.SetStatusCode(NYql::NDqProto::StatusIds::INTERNAL_ERROR);
                        }
                        SendResponse(ev);
                    }
                } else {
                    SendResponse(ev);
                }
            }

            TFuture<void> GetFuture() {
                return Promise.GetFuture();
            }

            virtual void ReplyError(grpc::StatusCode code, const TString& msg) {
                Ctx->ReplyError(code, msg);
                this->PassAway();
            }

            virtual void Reply(ui32 status, bool hasIssues) {
                Y_UNUSED(hasIssues);
                Ctx->Reply(&ResponseBuffer, status);
                this->PassAway();
            }

        private:
            NYdbGrpc::IRequestContextBase* Ctx;
            bool CtxSubscribed = false;
            ResponseType ResponseBuffer;

        protected:
            TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
            TIntrusivePtr<NMonitoring::TDynamicCounters> ServiceProxyActorCounters;
            TDynamicCounters::TCounterPtr ClientDisconnectedCounter;
            TDynamicCounters::TCounterPtr RetryCounter;
            TDynamicCounters::TCounterPtr FallbackCounter;
            TDynamicCounters::TCounterPtr ErrorCounter;

            const RequestType* Request;
            const TString TraceId;
            const TString Username;
            TPromise<void> Promise;
            const TInstant RequestStartTime = TInstant::Now();

            TDqConfiguration::TPtr Settings = MakeIntrusive<TDqConfiguration>();

            int Retry = 0;
            int MaxRetries = 10;
            TDuration RetryBackoff = TDuration::MilliSeconds(1000);

            NYql::TTaskCounters QueryStat;
            THashMap<ui64, ui64> Task2Stage;

            void RestoreRequest() {
                Request = dynamic_cast<const RequestType*>(Ctx->GetRequest());
            }
        };

        class TExecuteGraphProxyActor: public TServiceProxyActor<Yql::DqsProto::ExecuteGraphRequest, Yql::DqsProto::ExecuteGraphResponse> {
        public:
            using TBase = TServiceProxyActor<Yql::DqsProto::ExecuteGraphRequest, Yql::DqsProto::ExecuteGraphResponse>;
            TExecuteGraphProxyActor(NYdbGrpc::IRequestContextBase* ctx,
                const TIntrusivePtr<NMonitoring::TDynamicCounters>& counters,
                const TString& traceId, const TString& username,
                const NActors::TActorId& graphExecutionEventsActorId)
                : TServiceProxyActor(ctx, counters, traceId, username)
                , GraphExecutionEventsActorId(graphExecutionEventsActorId)
            {
                ExecutionTimeout = Request->GetExecutionTimeout();
            }

            void DoRetry() override {
                YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__;
                SendEvent(NYql::NDqProto::EGraphExecutionEventType::FAIL, nullptr, [this](const auto& ev) {
                    if (ev->Get()->Record.GetErrorMessage()) {
                        TBase::ReplyError(grpc::UNAVAILABLE, ev->Get()->Record.GetErrorMessage());
                    } else {
                        RestoreRequest();
                        ModifiedRequest.Reset();
                        TBase::DoRetry();
                    }
                });
            }

            void Reply(ui32 status, bool hasIssues) override {
                auto eventType = hasIssues
                    ? NYql::NDqProto::EGraphExecutionEventType::FAIL
                    : NYql::NDqProto::EGraphExecutionEventType::SUCCESS;
                SendEvent(eventType, nullptr, [this, status, hasIssues](const auto& ev) {
                    if (!hasIssues && ev->Get()->Record.GetErrorMessage()) {
                        TBase::ReplyError(grpc::UNAVAILABLE, ev->Get()->Record.GetErrorMessage());
                    } else {
                        TBase::Reply(status, hasIssues);
                    }
                });
            }

            void ReplyError(grpc::StatusCode code, const TString& msg) override {
                SendEvent(NYql::NDqProto::EGraphExecutionEventType::FAIL, nullptr, [this, code, msg](const auto& ev) {
                    Y_UNUSED(ev);
                    TBase::ReplyError(code, msg);
                });
            }

        private:
            THolder<Yql::DqsProto::ExecuteGraphRequest> ModifiedRequest;

            void DoPassAway() override {
                YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__;
                Send(GraphExecutionEventsActorId, new TEvents::TEvPoison());
                TServiceProxyActor::DoPassAway();
            }

            NDqProto::TGraphExecutionEvent::TExecuteGraphDescriptor SerializeGraphDescriptor() const {
                NDqProto::TGraphExecutionEvent::TExecuteGraphDescriptor result;

                for (const auto& kv : Request->GetSecureParams()) {
                    result.MutableSecureParams()->MutableData()->insert(kv);
                }

                for (const auto& kv : Request->GetGraphParams()) {
                    result.MutableGraphParams()->MutableData()->insert(kv);
                }

                return result;
            }

            void Bootstrap() override {
                YQL_CLOG(DEBUG, ProviderDq) << "TServiceProxyActor::OnExecuteGraph";

                SendEvent(NYql::NDqProto::EGraphExecutionEventType::START, SerializeGraphDescriptor(), [this](const TEvGraphExecutionEvent::TPtr& ev) {
                    if (ev->Get()->Record.GetErrorMessage()) {
                        TBase::ReplyError(grpc::UNAVAILABLE, ev->Get()->Record.GetErrorMessage());
                    } else {
                        NDqProto::TGraphExecutionEvent::TMap params;
                        ev->Get()->Record.GetMessage().UnpackTo(&params);
                        FinishBootstrap(params);
                    }
                });
            }

            void MergeTaskMetas(const NDqProto::TGraphExecutionEvent::TMap& params) {
                if (!params.data().empty()) {
                    for (size_t i = 0; i < Request->TaskSize(); ++i) {
                        if (!ModifiedRequest) {
                            ModifiedRequest.Reset(new Yql::DqsProto::ExecuteGraphRequest());
                            ModifiedRequest->CopyFrom(*Request);
                        }

                        auto* task = ModifiedRequest->MutableTask(i);

                        Yql::DqsProto::TTaskMeta taskMeta;
                        task->GetMeta().UnpackTo(&taskMeta);

                        for (const auto&[key, value] : params.data()) {
                            (*taskMeta.MutableTaskParams())[key] = value;
                        }

                        task->MutableMeta()->PackFrom(taskMeta);
                    }
                }

                if (ModifiedRequest) {
                    Request = ModifiedRequest.Get();
                }
            }

            void FinishBootstrap(const NDqProto::TGraphExecutionEvent::TMap& params) {
                YQL_CLOG(DEBUG, ProviderDq) << __FUNCTION__;
                MergeTaskMetas(params);

                auto executerId = RegisterChild(NDq::MakeDqExecuter(MakeWorkerManagerActorID(SelfId().NodeId()), SelfId(), TraceId, Username, Settings, Counters, RequestStartTime, false, ExecutionTimeout));

                TVector<TString> columns;
                columns.reserve(Request->GetColumns().size());
                for (const auto& column : Request->GetColumns()) {
                    columns.push_back(column);
                }
                for (const auto& task : Request->GetTask()) {
                    Yql::DqsProto::TTaskMeta taskMeta;
                    task.GetMeta().UnpackTo(&taskMeta);
                    Task2Stage[task.GetId()] = taskMeta.GetStageId();
                }
                THashMap<TString, TString> secureParams;
                for (const auto& x :  Request->GetSecureParams()) {
                    secureParams[x.first] = x.second;
                }
                auto resultId = RegisterChild(NExecutionHelpers::MakeResultAggregator(
                    columns,
                    executerId,
                    TraceId,
                    secureParams,
                    Settings,
                    Request->GetResultType(),
                    Request->GetDiscard(),
                    GraphExecutionEventsActorId).Release());
                auto controlId = Settings->EnableComputeActor.Get().GetOrElse(false) == false ? resultId
                    :  RegisterChild(NYql::MakeTaskController(TraceId, executerId, resultId, Settings, NYql::NCommon::TServiceCounters(Counters, nullptr, ""), TDuration::Seconds(5)).Release());
                Send(executerId, MakeHolder<TEvGraphRequest>(
                    *Request,
                    controlId,
                    resultId));
            }

            template <class TPayload, class TCallback>
            void SendEvent(NYql::NDqProto::EGraphExecutionEventType eventType, const TPayload& payload, TCallback callback) {
                NDqProto::TGraphExecutionEvent record;
                record.SetEventType(eventType);
                if constexpr (!std::is_same_v<TPayload, std::nullptr_t>) {
                    record.MutableMessage()->PackFrom(payload);
                }
                Send(GraphExecutionEventsActorId, new TEvGraphExecutionEvent(record));
                Synchronize<TEvGraphExecutionEvent>([callback, traceId = TraceId](TEvGraphExecutionEvent::TPtr& ev) {
                    YQL_LOG_CTX_ROOT_SESSION_SCOPE(traceId);
                    Y_ABORT_UNLESS(ev->Get()->Record.GetEventType() == NYql::NDqProto::EGraphExecutionEventType::SYNC);
                    callback(ev);
                });
            }

            NActors::TActorId GraphExecutionEventsActorId;
            ui64 ExecutionTimeout;
        };

        TString GetVersionString() {
            TStringBuilder sb;
            sb << GetProgramSvnVersion() << "\n";
            sb << GetBuildInfo();
            TString sandboxTaskId = GetSandboxTaskId();
            if (sandboxTaskId != TString("0")) {
                sb << "\nSandbox task id: " << sandboxTaskId;
            }

            return sb;
        }
    }

    TDqsGrpcService::TDqsGrpcService(
            NActors::TActorSystem& system,
            TIntrusivePtr<NMonitoring::TDynamicCounters> counters,
            const TDqTaskPreprocessorFactoryCollection& dqTaskPreprocessorFactories)
        : ActorSystem(system)
        , Counters(std::move(counters))
        , DqTaskPreprocessorFactories(dqTaskPreprocessorFactories)
        , Promise(NewPromise<void>())
        , RunningRequests(0)
        , Stopping(false)
        , Sessions(&system, Counters->GetSubgroup("component", "grpc")->GetCounter("Sessions"))
    { }

#define ADD_REQUEST(NAME, IN, OUT, ACTION)                              \
    do {                                                                \
        MakeIntrusive<NYdbGrpc::TGRpcRequest<Yql::DqsProto::IN, Yql::DqsProto::OUT, TDqsGrpcService>>( \
            this,                                                       \
            &Service_,                                                  \
            CQ,                                                         \
            [this](NYdbGrpc::IRequestContextBase* ctx) { ACTION  },        \
            &Yql::DqsProto::DqService::AsyncService::Request##NAME,     \
            #NAME,                                                      \
            logger,                                                     \
            BuildCB(Counters, #NAME))                                   \
            ->Run();                                                    \
    } while (0)

    void TDqsGrpcService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
        using namespace google::protobuf;

        CQ = cq;

        using TDqTaskPreprocessorCollection = std::vector<NYql::IDqTaskPreprocessor::TPtr>;

        ADD_REQUEST(ExecuteGraph, ExecuteGraphRequest, ExecuteGraphResponse, {
            TGuard<TMutex> lock(Mutex);
            if (Stopping) {
                ctx->ReplyError(grpc::UNAVAILABLE, "Terminating in progress");
                return;
            }
            auto* request = dynamic_cast<const Yql::DqsProto::ExecuteGraphRequest*>(ctx->GetRequest());
            auto session = Sessions.GetSession(request->GetSession());
            if (!session) {
                TString message = TStringBuilder()
                                << "Bad session: "
                                << request->GetSession();
                YQL_CLOG(DEBUG, ProviderDq) << message;
                ctx->ReplyError(grpc::INVALID_ARGUMENT, message);
                return;
            }

            TDqTaskPreprocessorCollection taskPreprocessors;
            for (const auto& factory: DqTaskPreprocessorFactories) {
                taskPreprocessors.push_back(factory());
            }

            auto graphExecutionEventsActorId = ActorSystem.Register(NDqs::MakeGraphExecutionEventsActor(request->GetSession(), std::move(taskPreprocessors)));

            RunningRequests++;
            auto actor = MakeHolder<TExecuteGraphProxyActor>(ctx, Counters, request->GetSession(), session->GetUsername(), graphExecutionEventsActorId);
            auto future = actor->GetFuture();
            auto actorId = ActorSystem.Register(actor.Release());
            future.Apply([session, actorId, this] (const TFuture<void>&) mutable {
                    RunningRequests--;
                    if (Stopping && !RunningRequests) {
                        Promise.SetValue();
                    }

                    session->DeleteRequest(actorId);
                });
            session->AddRequest(actorId);
        });

        ADD_REQUEST(SvnRevision, SvnRevisionRequest, SvnRevisionResponse, {
            Y_UNUSED(this);
            Yql::DqsProto::SvnRevisionResponse result;
            result.SetRevision(GetVersionString());
            ctx->Reply(&result, Ydb::StatusIds::SUCCESS);
        });

        ADD_REQUEST(CloseSession, CloseSessionRequest, CloseSessionResponse, {
            Y_UNUSED(this);
            auto* request = dynamic_cast<const Yql::DqsProto::CloseSessionRequest*>(ctx->GetRequest());
            Y_ABORT_UNLESS(!!request);

            Yql::DqsProto::CloseSessionResponse result;
            Sessions.CloseSession(request->GetSession());
            ctx->Reply(&result, Ydb::StatusIds::SUCCESS);
        });

        ADD_REQUEST(PingSession, PingSessionRequest, PingSessionResponse, {
            Y_UNUSED(this);
            auto* request = dynamic_cast<const Yql::DqsProto::PingSessionRequest*>(ctx->GetRequest());
            Y_ABORT_UNLESS(!!request);

            YQL_CLOG(TRACE, ProviderDq) << "PingSession " << request->GetSession();

            Yql::DqsProto::PingSessionResponse result;
            auto session = Sessions.GetSession(request->GetSession());
            if (!session) {
                TString message = TStringBuilder()
                                << "Bad session: "
                                << request->GetSession();
                YQL_CLOG(DEBUG, ProviderDq) << message;
                ctx->ReplyError(grpc::INVALID_ARGUMENT, message);
            } else {
                ctx->Reply(&result, Ydb::StatusIds::SUCCESS);
            }
        });

        ADD_REQUEST(OpenSession, OpenSessionRequest, OpenSessionResponse, {
            Y_UNUSED(this);
            auto* request = dynamic_cast<const Yql::DqsProto::OpenSessionRequest*>(ctx->GetRequest());
            Y_ABORT_UNLESS(!!request);

            YQL_CLOG(DEBUG, ProviderDq) << "OpenSession for " << request->GetSession() << " " << request->GetUsername();

            Yql::DqsProto::OpenSessionResponse result;
            if (Sessions.OpenSession(request->GetSession(), request->GetUsername())) {
                ctx->Reply(&result, Ydb::StatusIds::SUCCESS);
            } else {
                ctx->ReplyError(grpc::INVALID_ARGUMENT, "Session `" + request->GetSession() + "' exists");
            }
        });

        ADD_REQUEST(JobStop, JobStopRequest, JobStopResponse, {
            auto* request = dynamic_cast<const Yql::DqsProto::JobStopRequest*>(ctx->GetRequest());
            Y_ABORT_UNLESS(!!request);

            auto ev = MakeHolder<TEvJobStop>(*request);

            auto* result = google::protobuf::Arena::CreateMessage<Yql::DqsProto::JobStopResponse>(ctx->GetArena());
            ctx->Reply(result, Ydb::StatusIds::SUCCESS);

            ActorSystem.Send(MakeWorkerManagerActorID(ActorSystem.NodeId), ev.Release());
        });

        ADD_REQUEST(ClusterStatus, ClusterStatusRequest, ClusterStatusResponse, {
            auto ev = MakeHolder<TEvClusterStatus>();

            using ResultEv = TEvClusterStatusResponse;

            TExecutorPoolStats poolStats;

            TExecutorThreadStats stat;
            TVector<TExecutorThreadStats> stats;
            ActorSystem.GetPoolStats(0, poolStats, stats);

            for (const auto& s : stats) {
                stat.Aggregate(s);
            }

            YQL_CLOG(DEBUG, ProviderDq) << "SentEvents: " << stat.SentEvents;
            YQL_CLOG(DEBUG, ProviderDq) << "ReceivedEvents: " << stat.ReceivedEvents;
            YQL_CLOG(DEBUG, ProviderDq) << "NonDeliveredEvents: " << stat.NonDeliveredEvents;
            YQL_CLOG(DEBUG, ProviderDq) << "EmptyMailboxActivation: " << stat.EmptyMailboxActivation;
            Sessions.PrintInfo();

            for (ui32 i = 0; i < stat.ActorsAliveByActivity.size(); i=i+1) {
                if (stat.ActorsAliveByActivity[i]) {
                    YQL_CLOG(DEBUG, ProviderDq) << "ActorsAliveByActivity[" << i << "]=" << stat.ActorsAliveByActivity[i];
                }
            }

            auto callback = MakeHolder<TRichActorFutureCallback<ResultEv>>(
                [ctx] (TAutoPtr<TEventHandle<ResultEv>>& event) mutable {
                    auto* result = google::protobuf::Arena::CreateMessage<Yql::DqsProto::ClusterStatusResponse>(ctx->GetArena());
                    result->MergeFrom(event->Get()->Record.GetResponse());
                    ctx->Reply(result, Ydb::StatusIds::SUCCESS);
                },
                [ctx] () mutable {
                    YQL_CLOG(INFO, ProviderDq) << "ClusterStatus failed";
                    ctx->ReplyError(grpc::UNAVAILABLE, "Error");
                },
                TDuration::MilliSeconds(2000));

            TActorId callbackId = ActorSystem.Register(callback.Release());

            ActorSystem.Send(new IEventHandle(MakeWorkerManagerActorID(ActorSystem.NodeId), callbackId, ev.Release(), IEventHandle::FlagTrackDelivery));
        });

        ADD_REQUEST(OperationStop, OperationStopRequest, OperationStopResponse, {
            auto* request = dynamic_cast<const Yql::DqsProto::OperationStopRequest*>(ctx->GetRequest());
            auto ev = MakeHolder<TEvOperationStop>(*request);

            auto callback = MakeHolder<TRichActorFutureCallback<TEvOperationStopResponse>>(
                [ctx] (TAutoPtr<TEventHandle<TEvOperationStopResponse>>& event) mutable {
                    Y_UNUSED(event);
                    auto* result = google::protobuf::Arena::CreateMessage<Yql::DqsProto::OperationStopResponse>(ctx->GetArena());
                    ctx->Reply(result, Ydb::StatusIds::SUCCESS);
                },
                [ctx] () mutable {
                    YQL_CLOG(INFO, ProviderDq) << "OperationStopResponse failed";
                    ctx->ReplyError(grpc::UNAVAILABLE, "Error");
                },
                TDuration::MilliSeconds(2000));

            TActorId callbackId = ActorSystem.Register(callback.Release());

            ActorSystem.Send(new IEventHandle(MakeWorkerManagerActorID(ActorSystem.NodeId), callbackId, ev.Release(), IEventHandle::FlagTrackDelivery));
        });

        ADD_REQUEST(QueryStatus, QueryStatusRequest, QueryStatusResponse, {
            auto* request = dynamic_cast<const Yql::DqsProto::QueryStatusRequest*>(ctx->GetRequest());

            auto ev = MakeHolder<TEvQueryStatus>(*request);

            auto callback = MakeHolder<TRichActorFutureCallback<TEvQueryStatusResponse>>(
                [ctx] (TAutoPtr<TEventHandle<TEvQueryStatusResponse>>& event) mutable {
                    auto* result = google::protobuf::Arena::CreateMessage<Yql::DqsProto::QueryStatusResponse>(ctx->GetArena());
                    result->MergeFrom(event->Get()->Record.GetResponse());
                    ctx->Reply(result, Ydb::StatusIds::SUCCESS);
                },
                [ctx] () mutable {
                    YQL_CLOG(INFO, ProviderDq) << "QueryStatus failed";
                    ctx->ReplyError(grpc::UNAVAILABLE, "Error");
                },
                TDuration::MilliSeconds(2000));

            TActorId callbackId = ActorSystem.Register(callback.Release());

            ActorSystem.Send(new IEventHandle(MakeWorkerManagerActorID(ActorSystem.NodeId), callbackId, ev.Release(), IEventHandle::FlagTrackDelivery));
        });

        ADD_REQUEST(RegisterNode, RegisterNodeRequest, RegisterNodeResponse, {
            auto* request = dynamic_cast<const Yql::DqsProto::RegisterNodeRequest*>(ctx->GetRequest());
            Y_ABORT_UNLESS(!!request);

            if (!request->GetPort()
                || request->GetRole().empty()
                || request->GetAddress().empty())
            {
                ctx->ReplyError(grpc::INVALID_ARGUMENT, "Invalid argument");
                return;
            }

            auto ev = MakeHolder<TEvRegisterNode>(*request);

            using ResultEv = TEvRegisterNodeResponse;

            auto callback = MakeHolder<TRichActorFutureCallback<ResultEv>>(
                [ctx] (TAutoPtr<TEventHandle<ResultEv>>& event) mutable {
                    auto* result = google::protobuf::Arena::CreateMessage<Yql::DqsProto::RegisterNodeResponse>(ctx->GetArena());
                    result->MergeFrom(event->Get()->Record.GetResponse());
                    ctx->Reply(result, Ydb::StatusIds::SUCCESS);
                },
                [ctx] () mutable {
                    YQL_CLOG(INFO, ProviderDq) << "RegisterNode failed";
                    ctx->ReplyError(grpc::UNAVAILABLE, "Error");
                },
                TDuration::MilliSeconds(5000));

            TActorId callbackId = ActorSystem.Register(callback.Release());

            ActorSystem.Send(new IEventHandle(MakeWorkerManagerActorID(ActorSystem.NodeId), callbackId, ev.Release(), IEventHandle::FlagTrackDelivery));
        });

        ADD_REQUEST(GetMaster, GetMasterRequest, GetMasterResponse, {
            auto* request = dynamic_cast<const Yql::DqsProto::GetMasterRequest*>(ctx->GetRequest());
            Y_ABORT_UNLESS(!!request);

            auto requestEvent = MakeHolder<TEvGetMasterRequest>();

            auto callback = MakeHolder<TActorFutureCallback<TEvGetMasterResponse>>(
                    [ctx] (TAutoPtr<TEventHandle<TEvGetMasterResponse>>& event) mutable {
                        auto* result = google::protobuf::Arena::CreateMessage<Yql::DqsProto::GetMasterResponse>(ctx->GetArena());
                        result->MergeFrom(event->Get()->Record.GetResponse());
                        ctx->Reply(result, Ydb::StatusIds::SUCCESS);
                    });

            TActorId callbackId = ActorSystem.Register(callback.Release());

            ActorSystem.Send(new IEventHandle(MakeWorkerManagerActorID(ActorSystem.NodeId), callbackId, requestEvent.Release()));
        });

        ADD_REQUEST(ConfigureFailureInjector, ConfigureFailureInjectorRequest, ConfigureFailureInjectorResponse,{
            auto* request = dynamic_cast<const Yql::DqsProto::ConfigureFailureInjectorRequest*>(ctx->GetRequest());
            Y_ABORT_UNLESS(!!request);

            auto requestEvent = MakeHolder<TEvConfigureFailureInjectorRequest>(*request);

            auto callback = MakeHolder<TActorFutureCallback<TEvConfigureFailureInjectorResponse>>(
                    [ctx] (TAutoPtr<TEventHandle<TEvConfigureFailureInjectorResponse>>& event) mutable {
                        auto* result = google::protobuf::Arena::CreateMessage<Yql::DqsProto::ConfigureFailureInjectorResponse>(ctx->GetArena());
                        result->MergeFrom(event->Get()->Record.GetResponse());
                        ctx->Reply(result, Ydb::StatusIds::SUCCESS);
                    });

            TActorId callbackId = ActorSystem.Register(callback.Release());

            ActorSystem.Send(new IEventHandle(MakeWorkerManagerActorID(ActorSystem.NodeId), callbackId, requestEvent.Release()));
        });

        ADD_REQUEST(IsReady, IsReadyRequest, IsReadyResponse, {
            auto* request = dynamic_cast<const Yql::DqsProto::IsReadyRequest*>(ctx->GetRequest());
            Y_ABORT_UNLESS(!!request);

            auto ev = MakeHolder<TEvIsReady>(*request);

            auto callback = MakeHolder<TRichActorFutureCallback<TEvIsReadyResponse>>(
                    [ctx] (TAutoPtr<TEventHandle<TEvIsReadyResponse>>& event) mutable {
                        Yql::DqsProto::IsReadyResponse result;
                        result.SetIsReady(event->Get()->Record.GetIsReady());
                        ctx->Reply(&result, Ydb::StatusIds::SUCCESS);
                    },
                    [ctx] () mutable {
                        YQL_CLOG(INFO, ProviderDq) << "IsReadyForRevision failed";
                        ctx->ReplyError(grpc::UNAVAILABLE, "Error");
                    },
                    TDuration::MilliSeconds(2000));

            TActorId callbackId = ActorSystem.Register(callback.Release());

            ActorSystem.Send(new IEventHandle(MakeWorkerManagerActorID(ActorSystem.NodeId), callbackId, ev.Release()));
        });

        ADD_REQUEST(Routes, RoutesRequest, RoutesResponse, {
            auto* request = dynamic_cast<const Yql::DqsProto::RoutesRequest*>(ctx->GetRequest());
            Y_ABORT_UNLESS(!!request);

            auto ev = MakeHolder<TEvRoutesRequest>();

            auto callback = MakeHolder<TRichActorFutureCallback<TEvRoutesResponse>>(
                    [ctx] (TAutoPtr<TEventHandle<TEvRoutesResponse>>& event) mutable {
                        Yql::DqsProto::RoutesResponse result;
                        result.MergeFrom(event->Get()->Record.GetResponse());
                        ctx->Reply(&result, Ydb::StatusIds::SUCCESS);
                    },
                    [ctx] () mutable {
                        YQL_CLOG(INFO, ProviderDq) << "Routes failed";
                        ctx->ReplyError(grpc::UNAVAILABLE, "Error");
                    },
                    TDuration::MilliSeconds(5000));

            TActorId callbackId = ActorSystem.Register(callback.Release());

            ActorSystem.Send(new IEventHandle(MakeWorkerManagerActorID(request->GetNodeId()), callbackId, ev.Release()));
        });

/* 1. move grpc to providers/dq, 2. move benchmark to providers/dq 3. uncomment
        ADD_REQUEST(Benchmark, BenchmarkRequest, BenchmarkResponse, {
            auto* req = dynamic_cast<const Yql::DqsProto::BenchmarkRequest*>(ctx->GetRequest());
            Y_ABORT_UNLESS(!!req);

            TWorkerManagerBenchmarkOptions options;
            if (req->GetWorkerCount()) {
                options.WorkerCount = req->GetWorkerCount();
            }
            if (req->GetInflight()) {
                options.Inflight = req->GetInflight();
            }
            if (req->GetTotalRequests()) {
                options.TotalRequests = req->GetTotalRequests();
            }
            if (req->GetMaxRunTimeMs()) {
                options.MaxRunTimeMs = TDuration::MilliSeconds(req->GetMaxRunTimeMs());
            }

            auto benchmarkId = ActorSystem.Register(
                CreateWorkerManagerBenchmark(
                    MakeWorkerManagerActorID(ActorSystem.NodeId), options
                    ));

            ActorSystem.Send(benchmarkId, new TEvents::TEvBootstrap);

            auto* result = google::protobuf::Arena::CreateMessage<Yql::DqsProto::BenchmarkResponse>(ctx->GetArena());
            ctx->Reply(result, Ydb::StatusIds::SUCCESS);
            });
*/
    }

    void TDqsGrpcService::SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter* limiter) {
        Limiter = limiter;
    }

    bool TDqsGrpcService::IncRequest() {
        return Limiter->Inc();
    }

    void TDqsGrpcService::DecRequest() {
        Limiter->Dec();
        Y_ASSERT(Limiter->GetCurrentInFlight() >= 0);
    }

    TFuture<void> TDqsGrpcService::Stop() {
        TGuard<TMutex> lock(Mutex);
        Stopping = true;
        auto future = Promise.GetFuture();
        if (RunningRequests == 0) {
            Promise.SetValue();
        }
        return future;
    }
}
