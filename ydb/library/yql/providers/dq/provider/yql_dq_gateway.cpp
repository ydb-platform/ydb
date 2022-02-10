#include "yql_dq_gateway.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/dq/api/grpc/api.grpc.pb.h>
#include <ydb/library/yql/providers/dq/backtrace/backtrace.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/providers/dq/config/config.pb.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <library/cpp/grpc/client/grpc_client_low.h>
#include <library/cpp/yson/node/node_io.h> 
#include <library/cpp/threading/task_scheduler/task_scheduler.h>

#include <util/system/thread.h>

#include <utility>

namespace NYql {

class TDqGateway: public IDqGateway
{
    enum ETaskPriority {
        PRIO_NORMAL = 1,
        PRIO_RT = 2
    };

public:
    TDqGateway(const TString& host, int port, int threads, const TString& vanillaJobPath, const TString& vanillaJobMd5, TDuration timeout = TDuration::Minutes(60))
        : GrpcConf(TStringBuilder() << host << ":" << port)
        , GrpcClient(1)
        , Service(GrpcClient.CreateGRpcServiceConnection<Yql::DqsProto::DqService>(GrpcConf))
        , VanillaJobPath(vanillaJobPath)
        , VanillaJobMd5(vanillaJobMd5)
        , TaskScheduler(threads)
        , RtTaskScheduler(1)
        , OpenSessionTimeout(timeout)
    {
        TaskScheduler.Start();
        RtTaskScheduler.Start();
    }

    TString GetVanillaJobPath() override {
        return VanillaJobPath;
    }

    TString GetVanillaJobMd5() override {
        return VanillaJobMd5;
    }

    template<typename RespType>
    void OnResponse(NThreading::TPromise<TResult> promise, TString sessionId, NGrpc::TGrpcStatus&& status, RespType&& resp, const THashMap<TString, TString>& modulesMapping, bool alwaysFallback = false)
    {
        YQL_LOG_CTX_SCOPE(sessionId);
        YQL_CLOG(TRACE, ProviderDq) << "TDqGateway::callback";

        {
            TGuard<TMutex> lock(ProgressMutex);
            RunningQueries.erase(sessionId);
        }

        TResult result;

        bool error = false;
        bool fallback = false;

        if (status.Ok()) {
            YQL_CLOG(TRACE, ProviderDq) << "TDqGateway::Ok";

            result.Truncated = resp.GetTruncated();

            TOperationStatistics statistics;

            for (const auto& t : resp.GetMetric()) {
                YQL_CLOG(TRACE, ProviderDq) << "Counter: " << t.GetName() << " : " << t.GetSum() << " : " << t.GetCount();
                TOperationStatistics::TEntry entry(
                    t.GetName(),
                    t.GetSum(),
                    t.GetMax(),
                    t.GetMin(),
                    t.GetAvg(),
                    t.GetCount());
                statistics.Entries.push_back(entry);
            }

            result.Statistics = statistics;

            NYql::TIssues issues;
            auto operation = resp.operation();
            for (auto& message : *operation.Mutableissues()) {
                message.Setmessage(NBacktrace::Symbolize(message.Getmessage(), modulesMapping));
            }
            NYql::IssuesFromMessage(operation.issues(), issues);
            error = false;
            for (const auto& issue : issues) {
                if (issue.GetSeverity() <= TSeverityIds::S_ERROR) {
                    error = true;
                }
                if (issue.GetCode() == TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR) {
                    fallback = true;
                }
            }

            // TODO: Save statistics in case of result failure
            if (!error) {
                Yql::DqsProto::ExecuteQueryResult queryResult;
                resp.operation().result().UnpackTo(&queryResult);
                result.Data = queryResult.yson().empty()
                    ? NYdb::FormatResultSetYson(queryResult.result(), NYson::EYsonFormat::Binary)
                    : queryResult.yson();
                result.Issues.AddIssues(issues);
                result.SetSuccess();
            } else {
                YQL_CLOG(ERROR, ProviderDq) << "Issue " << issues.ToString();
                result.Issues.AddIssues(issues);
                if (fallback) {
                    result.Fallback = true;
                    result.SetSuccess();
                }
            }
        } else {
            YQL_CLOG(ERROR, ProviderDq) << "Issue " << status.Msg;
            auto issue = TIssue(TStringBuilder{} << "Error " << status.GRpcStatusCode << " message: " << status.Msg);
            result.Retriable = status.GRpcStatusCode == grpc::CANCELLED;
            if ((status.GRpcStatusCode == grpc::UNAVAILABLE /* terminating state */
                || status.GRpcStatusCode == grpc::CANCELLED /* server crashed or stopped before task process */)
                || status.GRpcStatusCode == grpc::RESOURCE_EXHAUSTED /* send message limit */
                )
            {
                YQL_CLOG(ERROR, ProviderDq) << "Fallback " << status.GRpcStatusCode;
                result.Fallback = true;
                result.SetSuccess();
                result.Issues.AddIssue(issue.SetCode(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, TSeverityIds::S_ERROR));
            } else {
                error = true;
                result.Issues.AddIssue(issue.SetCode(TIssuesIds::DQ_GATEWAY_ERROR, TSeverityIds::S_ERROR));
            }
        }

        if (error && alwaysFallback) {
            YQL_CLOG(ERROR, ProviderDq) << "Force Fallback";
            result.Fallback = true;
            result.ForceFallback = true;
            result.SetSuccess();
        }

        if (!result.Success()) {
            result.AddIssues(result.Issues);
        }

        Async([promise=std::move(promise), result=std::move(result)]() mutable { promise.SetValue(result); });
    }

    NThreading::TFuture<void> Delay(TDuration duration, ETaskPriority prio = PRIO_NORMAL) {
        NThreading::TPromise<void> promise = NThreading::NewPromise();

        auto future = promise.GetFuture();

        auto& taskScheduler = prio == PRIO_NORMAL ? TaskScheduler : RtTaskScheduler;

        if (!taskScheduler.Add(MakeIntrusive<TDelay>(promise), TInstant::Now() + duration)) {
            promise.SetException("cannot delay");
        }

        return future;
    }

    void Async(const std::function<void(void)>& f) {
        NThreading::TPromise<void> promise = NThreading::NewPromise();

        promise.GetFuture().Apply([=](const NThreading::TFuture<void>&) {
            f();
        });

        Y_VERIFY(TaskScheduler.Add(MakeIntrusive<TDelay>(promise), TInstant()));
    }

    template <typename TResponse, typename TRequest, typename TStub>
    NThreading::TFuture<TResult> WithRetry(
        const TString& sessionId,
        const TRequest& queryPB,
        TStub stub,
        int retry,
        const TDqSettings::TPtr& settings,
        const THashMap<TString, TString>& modulesMapping
    ) {
        auto backoff = TDuration::MilliSeconds(settings->RetryBackoffMs.Get().GetOrElse(1000));
        auto promise = NThreading::NewPromise<TResult>();
        auto fallbackPolicy = settings->FallbackPolicy.Get().GetOrElse("default");
        auto alwaysFallback = fallbackPolicy == "always";
        auto callback =  [this, promise, sessionId, alwaysFallback, modulesMapping](NGrpc::TGrpcStatus&& status, TResponse&& resp) mutable {
            return OnResponse(std::move(promise), std::move(sessionId), std::move(status), std::move(resp), modulesMapping, alwaysFallback);
        };

        Service->DoRequest<TRequest, TResponse>(queryPB, callback, stub);

        {
            TGuard<TMutex> lock(ProgressMutex);
            auto i = RunningQueries.find(sessionId);
            if (i != RunningQueries.end()) {
                if (i->second.first) {
                    ScheduleQueryStatusRequest(sessionId);
                }
            } else {
                return NThreading::MakeFuture(TResult());
            }
        }

        return promise.GetFuture().Apply([=](const NThreading::TFuture<TResult>& result) {
            if (result.HasException()) {
                return result;
            }
            auto value = result.GetValue();
            if (value.Success() || retry == 0 || !value.Retriable) {
                return result;
            }

            return Delay(backoff)
                .Apply([=](const NThreading::TFuture<void>& result) {
                    try {
                        result.TryRethrow();
                    } catch (...) {
                        return NThreading::MakeErrorFuture<TResult>(std::current_exception());
                    }
                    return WithRetry<TResponse>(sessionId, queryPB, stub, retry - 1, settings, modulesMapping);
                });
        });
    }

    NThreading::TFuture<TResult>
    ExecutePlan(const TString& sessionId, NDqs::IDqsExecutionPlanner& plan, const TVector<TString>& columns,
                const THashMap<TString, TString>& secureParams, const THashMap<TString, TString>& graphParams,
                const TDqSettings::TPtr& settings,
                const TDqProgressWriter& progressWriter, const THashMap<TString, TString>& modulesMapping,
                bool discard) override
    {
        YQL_LOG_CTX_SCOPE(sessionId);
        auto tasks = plan.GetTasks();

        Yql::DqsProto::ExecuteGraphRequest queryPB;
        for (const auto& task : tasks) {
            auto* t = queryPB.AddTask();
            *t = task;

            Yql::DqsProto::TTaskMeta taskMeta;
            task.GetMeta().UnpackTo(&taskMeta);

            for (auto& file : taskMeta.GetFiles()) {
                YQL_ENSURE(!file.GetObjectId().empty());
            }
        }
        queryPB.SetSession(sessionId);
        queryPB.SetResultType(plan.GetResultType());
        queryPB.SetSourceId(plan.GetSourceID().NodeId()-1);
        for (const auto& column : columns) {
            *queryPB.AddColumns() = column;
        }
        settings->Save(queryPB);

        {
            auto& secParams = *queryPB.MutableSecureParams();
            for (const auto&[k, v] : secureParams) {
                secParams[k] = v;
            }
        }

        {
            auto& gParams = *queryPB.MutableGraphParams();
            for (const auto&[k, v] : graphParams) {
                gParams[k] = v;
            }
        }

        queryPB.SetDiscard(discard);

        int retry = settings->MaxRetries.Get().GetOrElse(5);

        {
            TGuard<TMutex> lock(ProgressMutex);
            RunningQueries.emplace(sessionId, std::make_pair(progressWriter, TString("")));
        }

        YQL_LOG(DEBUG) << "Send query of size " << queryPB.ByteSizeLong();

        return WithRetry<Yql::DqsProto::ExecuteGraphResponse>(
            sessionId,
            queryPB,
            &Yql::DqsProto::DqService::Stub::AsyncExecuteGraph,
            retry,
            settings,
            modulesMapping);
    }

    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) override {
        YQL_LOG_CTX_SCOPE(sessionId);
        YQL_CLOG(INFO, ProviderDq) << "OpenSession";
        Yql::DqsProto::OpenSessionRequest request;
        request.SetSession(sessionId);
        request.SetUsername(username);

        NGrpc::TCallMeta meta;
        meta.Timeout = OpenSessionTimeout;

        auto promise = NThreading::NewPromise<void>();
        auto callback = [this, promise, sessionId](NGrpc::TGrpcStatus&& status, Yql::DqsProto::OpenSessionResponse&& resp) mutable {
            Y_UNUSED(resp);
            YQL_LOG_CTX_SCOPE(sessionId);
            if (status.Ok()) {
                YQL_CLOG(INFO, ProviderDq) << "OpenSession OK";
                SchedulePingSessionRequest(sessionId);
                Async([promise=std::move(promise)]() mutable { promise.SetValue(); });
            } else {
                YQL_CLOG(ERROR, ProviderDq) << "OpenSession error: " << status.Msg;
                Async([promise=std::move(promise), status]() mutable { promise.SetException(status.Msg); });
            }
        };

        Service->DoRequest<Yql::DqsProto::OpenSessionRequest, Yql::DqsProto::OpenSessionResponse>(
            request, callback, &Yql::DqsProto::DqService::Stub::AsyncOpenSession, meta);
        return promise.GetFuture();
    }

    void CloseSession(const TString& sessionId) override {
        Yql::DqsProto::CloseSessionRequest request;
        request.SetSession(sessionId);

        auto callback = [](NGrpc::TGrpcStatus&& status, Yql::DqsProto::CloseSessionResponse&& resp) {
            Y_UNUSED(resp);
            Y_UNUSED(status);
        };

        {
            TGuard<TMutex> lock(ProgressMutex);
            RunningQueries.erase(sessionId);
        }

        Service->DoRequest<Yql::DqsProto::CloseSessionRequest, Yql::DqsProto::CloseSessionResponse>(
            request, callback, &Yql::DqsProto::DqService::Stub::AsyncCloseSession);
    }

    void RequestQueryStatus(const TString& sessionId) {
        Yql::DqsProto::QueryStatusRequest request;
        request.SetSession(sessionId);
        IDqGateway::TPtr self = this;
        auto callback = [this, self, sessionId](NGrpc::TGrpcStatus&& status, Yql::DqsProto::QueryStatusResponse&& resp) {
            if (status.Ok()) {
                TGuard<TMutex> lock(ProgressMutex);
                TString stage;
                TDqProgressWriter* dqProgressWriter = nullptr;
                auto it = RunningQueries.find(sessionId);
                if (it != RunningQueries.end()) {
                    dqProgressWriter = &it->second.first;
                    auto lastStatus = it->second.second;
                    if (dqProgressWriter && lastStatus != resp.GetStatus()) {
                        stage = resp.GetStatus();
                        it->second.second = stage;
                    }

                    ScheduleQueryStatusRequest(sessionId);
                }

                if (!stage.empty() && dqProgressWriter) {
                    (*dqProgressWriter)(stage);
                }
            } else {
                TGuard<TMutex> lock(ProgressMutex);
                RunningQueries.erase(sessionId);
            }
        };

        Service->DoRequest<Yql::DqsProto::QueryStatusRequest, Yql::DqsProto::QueryStatusResponse>(
            request, callback, &Yql::DqsProto::DqService::Stub::AsyncQueryStatus, {}, nullptr);
    }

    void ScheduleQueryStatusRequest(const TString& sessionId) {
        Delay(TDuration::MilliSeconds(1000)).Subscribe([this, sessionId](NThreading::TFuture<void> fut) {
            if (fut.HasException()) {
                TGuard<TMutex> lock(ProgressMutex);
                RunningQueries.erase(sessionId);
            } else {
                TGuard<TMutex> lock(ProgressMutex);
                auto it = RunningQueries.find(sessionId);
                if (it != RunningQueries.end()) {
                    RequestQueryStatus(sessionId);
                }
            }
        });
    }

    void SchedulePingSessionRequest(const TString& sessionId) {
        auto callback = [this, sessionId](
            NGrpc::TGrpcStatus&& status,
            Yql::DqsProto::PingSessionResponse&&) mutable
        {
            if (status.GRpcStatusCode == grpc::INVALID_ARGUMENT) {
                YQL_LOG(INFO) << "Session closed " << sessionId;
            } else {
                SchedulePingSessionRequest(sessionId);
            }
        };
        Delay(TDuration::Seconds(10), PRIO_RT).Subscribe([this, callback, sessionId](const NThreading::TFuture<void>&) {
            Yql::DqsProto::PingSessionRequest query;
            query.SetSession(sessionId);
            Service->DoRequest<Yql::DqsProto::PingSessionRequest, Yql::DqsProto::PingSessionResponse>(
                query,
                callback,
                &Yql::DqsProto::DqService::Stub::AsyncPingSession);
        });
    }

    struct TDelay: public TTaskScheduler::ITask {
        TDelay(NThreading::TPromise<void> p)
            : Promise(std::move(p))
        { }

        TInstant Process() override {
            Promise.SetValue();
            return TInstant::Max();
        }

        NThreading::TPromise<void> Promise;
    };

private:
    NGrpc::TGRpcClientConfig GrpcConf;
    NGrpc::TGRpcClientLow GrpcClient;
    std::unique_ptr<NGrpc::TServiceConnection<Yql::DqsProto::DqService>> Service;

    TMutex ProgressMutex;
    TMutex Mutex;
    THashMap<TString, std::pair<TDqProgressWriter, TString>> RunningQueries;
    TString VanillaJobPath;
    TString VanillaJobMd5;

    TTaskScheduler TaskScheduler;
    TTaskScheduler RtTaskScheduler;

    const TDuration OpenSessionTimeout;
};

TIntrusivePtr<IDqGateway> CreateDqGateway(const TString& host, int port, int threads) {
    return new TDqGateway(host, port, threads, "", "");
}

TIntrusivePtr<IDqGateway> CreateDqGateway(const NProto::TDqConfig& config) {
    return new TDqGateway("localhost", config.GetPort(), 8,
        config.GetYtBackends()[0].GetVanillaJob(),
        config.GetYtBackends()[0].GetVanillaJobMd5(),
        TDuration::Seconds(15));
}

} // namespace NYql
