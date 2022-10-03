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

using namespace NThreading;

class TDqGatewayImpl: public std::enable_shared_from_this<TDqGatewayImpl>
{
public:
    using TResult = IDqGateway::TResult;
    using TDqProgressWriter = IDqGateway::TDqProgressWriter;

    TDqGatewayImpl(const TString& host, int port, const TString& vanillaJobPath, const TString& vanillaJobMd5, TDuration timeout, TDuration requestTimeout)
        : GrpcConf(TStringBuilder() << host << ":" << port, requestTimeout)
        , GrpcClient(1)
        , Service(GrpcClient.CreateGRpcServiceConnection<Yql::DqsProto::DqService>(GrpcConf))
        , VanillaJobPath(vanillaJobPath)
        , VanillaJobMd5(vanillaJobMd5)
        , TaskScheduler(1)
        , OpenSessionTimeout(timeout)
    {
        TaskScheduler.Start();
    }

    TString GetVanillaJobPath() {
        return VanillaJobPath;
    }

    TString GetVanillaJobMd5() {
        return VanillaJobMd5;
    }

    template<typename RespType>
    void OnResponse(TPromise<TResult> promise, TString sessionId, NGrpc::TGrpcStatus&& status, RespType&& resp, const THashMap<TString, TString>& modulesMapping, bool alwaysFallback = false)
    {
        YQL_LOG_CTX_ROOT_SCOPE(sessionId);
        YQL_CLOG(TRACE, ProviderDq) << "TDqGateway::callback";

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
                result.AddIssues(issues);
                result.SetSuccess();
            } else {
                YQL_CLOG(ERROR, ProviderDq) << "Issue " << issues.ToString();
                result.AddIssues(issues);
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
                result.AddIssue(issue.SetCode(TIssuesIds::DQ_GATEWAY_NEED_FALLBACK_ERROR, TSeverityIds::S_ERROR));
            } else {
                error = true;
                result.AddIssue(issue.SetCode(TIssuesIds::DQ_GATEWAY_ERROR, TSeverityIds::S_ERROR));
            }
        }

        if (error && alwaysFallback) {
            YQL_CLOG(ERROR, ProviderDq) << "Force Fallback";
            result.Fallback = true;
            result.ForceFallback = true;
            result.SetSuccess();
        }

        promise.SetValue(result);
    }

    TFuture<void> Delay(TDuration duration) {
        TPromise<void> promise = NewPromise();

        auto future = promise.GetFuture();

        if (!TaskScheduler.Add(MakeIntrusive<TDelay>(promise), TInstant::Now() + duration)) {
            promise.SetException("cannot delay");
        }

        return future;
    }

    template <typename TResponse, typename TRequest, typename TStub>
    TFuture<TResult> WithRetry(
        const TString& sessionId,
        const TRequest& queryPB,
        TStub stub,
        int retry,
        const TDqSettings::TPtr& settings,
        const THashMap<TString, TString>& modulesMapping
    ) {
        auto backoff = TDuration::MilliSeconds(settings->RetryBackoffMs.Get().GetOrElse(1000));
        auto promise = NewPromise<TResult>();
        auto fallbackPolicy = settings->FallbackPolicy.Get().GetOrElse("default");
        auto alwaysFallback = fallbackPolicy == "always";
        auto self = weak_from_this();
        auto callback =  [self, promise, sessionId, alwaysFallback, modulesMapping](NGrpc::TGrpcStatus&& status, TResponse&& resp) mutable {
            auto this_ = self.lock();
            if (!this_) {
                YQL_CLOG(DEBUG, ProviderDq) << "Gateway was closed: " << sessionId;
                promise.SetException("Gateway was closed");
                return;
            }

            this_->OnResponse(std::move(promise), std::move(sessionId), std::move(status), std::move(resp), modulesMapping, alwaysFallback);
        };

        Service->DoRequest<TRequest, TResponse>(queryPB, callback, stub);

        {
            TGuard<TMutex> lock(ProgressMutex);
            auto i = RunningQueries.find(sessionId);
            if (i != RunningQueries.end()) {
                if (i->second.ProgressWriter) {
                    ScheduleQueryStatusRequest(sessionId);
                }
            } else {
                return MakeFuture(TResult());
            }
        }

        return promise.GetFuture().Apply([=](const TFuture<TResult>& result) {
            if (result.HasException()) {
                return result;
            }
            auto value = result.GetValue();
            auto this_ = self.lock();

            if (value.Success() || retry == 0 || !value.Retriable || !this_) {
                return result;
            }

            return this_->Delay(backoff)
                .Apply([=](const TFuture<void>& result) {
                    auto this_ = self.lock();
                    try {
                        result.TryRethrow();
                        if (!this_) {
                            YQL_CLOG(DEBUG, ProviderDq) << "Gateway was closed: " << sessionId;
                            throw std::runtime_error("Gateway was closed");
                        }
                    } catch (...) {
                        return MakeErrorFuture<TResult>(std::current_exception());
                    }
                    return this_->WithRetry<TResponse>(sessionId, queryPB, stub, retry - 1, settings, modulesMapping);
                });
        });
    }

    TFuture<TResult>
    ExecutePlan(const TString& sessionId, NDqs::TPlan&& plan, const TVector<TString>& columns,
                const THashMap<TString, TString>& secureParams, const THashMap<TString, TString>& graphParams,
                const TDqSettings::TPtr& settings,
                const TDqProgressWriter& progressWriter, const THashMap<TString, TString>& modulesMapping,
                bool discard)
    {
        YQL_LOG_CTX_ROOT_SCOPE(sessionId);

        Yql::DqsProto::ExecuteGraphRequest queryPB;
        for (const auto& task : plan.Tasks) {
            auto* t = queryPB.AddTask();
            *t = task;

            Yql::DqsProto::TTaskMeta taskMeta;
            task.GetMeta().UnpackTo(&taskMeta);

            for (auto& file : taskMeta.GetFiles()) {
                YQL_ENSURE(!file.GetObjectId().empty());
            }
        }
        queryPB.SetSession(sessionId);
        queryPB.SetResultType(plan.ResultType);
        queryPB.SetSourceId(plan.SourceID.NodeId()-1);
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

        TFuture<void> sessionFuture;
        {
            TGuard<TMutex> lock(ProgressMutex);
            auto it = RunningQueries.find(sessionId);
            if (it == RunningQueries.end()) {
                YQL_CLOG(DEBUG, ProviderDq) << "Session was closed: " << sessionId;
                return MakeErrorFuture<TResult>(std::make_exception_ptr(std::runtime_error("Session was closed")));
            }
            it->second.ProgressWriter = progressWriter;
            sessionFuture = it->second.OpenSessionFuture;
        }

        YQL_CLOG(DEBUG, ProviderDq) << "Send query of size " << queryPB.ByteSizeLong();

        auto self = weak_from_this();
        return sessionFuture.Apply([self, sessionId, queryPB, retry, settings, modulesMapping](const TFuture<void>& ) {
            auto this_ = self.lock();
            if (!this_) {
                YQL_CLOG(DEBUG, ProviderDq) << "Gateway was closed: " << sessionId;
                return MakeErrorFuture<TResult>(std::make_exception_ptr(std::runtime_error("Gateway was closed")));
            }

            return this_->WithRetry<Yql::DqsProto::ExecuteGraphResponse>(
                sessionId,
                queryPB,
                &Yql::DqsProto::DqService::Stub::AsyncExecuteGraph,
                retry,
                settings,
                modulesMapping);
        });
    }

    TFuture<void> OpenSession(const TString& sessionId, const TString& username) {
        YQL_LOG_CTX_ROOT_SCOPE(sessionId);
        YQL_CLOG(INFO, ProviderDq) << "OpenSession";
        Yql::DqsProto::OpenSessionRequest request;
        request.SetSession(sessionId);
        request.SetUsername(username);

        {
            TGuard<TMutex> lock(ProgressMutex);
            if (RunningQueries.find(sessionId) != RunningQueries.end()) {
                return MakeFuture();
            }
        }

        NGrpc::TCallMeta meta;
        meta.Timeout = OpenSessionTimeout;

        auto promise = NewPromise<void>();
        auto self = weak_from_this();
        auto callback = [self, promise, sessionId](NGrpc::TGrpcStatus&& status, Yql::DqsProto::OpenSessionResponse&& resp) mutable {
            Y_UNUSED(resp);
            YQL_LOG_CTX_ROOT_SCOPE(sessionId);
            auto this_ = self.lock();
            if (!this_) {
                YQL_CLOG(DEBUG, ProviderDq) << "Gateway was closed: " << sessionId;
                promise.SetException("Gateway was closed");
                return;
            }
            if (status.Ok()) {
                YQL_CLOG(INFO, ProviderDq) << "OpenSession OK";
                this_->SchedulePingSessionRequest(sessionId);
                promise.SetValue();
            } else {
                YQL_CLOG(ERROR, ProviderDq) << "OpenSession error: " << status.Msg;
                promise.SetException(status.Msg);
            }
        };

        Service->DoRequest<Yql::DqsProto::OpenSessionRequest, Yql::DqsProto::OpenSessionResponse>(
            request, callback, &Yql::DqsProto::DqService::Stub::AsyncOpenSession, meta);

        {
            TGuard<TMutex> lock(ProgressMutex);
            RunningQueries.emplace(sessionId, TSession {
                    std::optional<TDqProgressWriter> {},
                        "",
                        promise.GetFuture()
                        });
        }

        return MakeFuture();
    }

    void CloseSession(const TString& sessionId) {
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

    void OnRequestQueryStatus(const TString& sessionId, const TString& status, bool ok) {
        TGuard<TMutex> lock(ProgressMutex);
        TString stage;
        TDqProgressWriter* dqProgressWriter = nullptr;
        auto it = RunningQueries.find(sessionId);
        if (it != RunningQueries.end() && ok) {
            dqProgressWriter = it->second.ProgressWriter ? &*it->second.ProgressWriter:nullptr;
            auto lastStatus = it->second.Status;
            if (dqProgressWriter && lastStatus != status) {
                stage = status;
                it->second.Status = stage;
            }

            ScheduleQueryStatusRequest(sessionId);
        } else if (it != RunningQueries.end()) {
            it->second.ProgressWriter = {};
        }

        if (!stage.empty() && dqProgressWriter) {
            (*dqProgressWriter)(stage);
        }
    }

    void RequestQueryStatus(const TString& sessionId) {
        Yql::DqsProto::QueryStatusRequest request;
        request.SetSession(sessionId);
        auto self = weak_from_this();
        auto callback = [self, sessionId](NGrpc::TGrpcStatus&& status, Yql::DqsProto::QueryStatusResponse&& resp) {
            auto this_ = self.lock();
            if (!this_) {
                return;
            }

            this_->OnRequestQueryStatus(sessionId, resp.GetStatus(), status.Ok());
        };

        Service->DoRequest<Yql::DqsProto::QueryStatusRequest, Yql::DqsProto::QueryStatusResponse>(
            request, callback, &Yql::DqsProto::DqService::Stub::AsyncQueryStatus, {}, nullptr);
    }

    void StartQueryStatusRequest(const TString& sessionId, bool ok) {
        TGuard<TMutex> lock(ProgressMutex);
        auto it = RunningQueries.find(sessionId);
        if (it != RunningQueries.end() && ok) {
            RequestQueryStatus(sessionId);
        } else if (it != RunningQueries.end()) {
            it->second.ProgressWriter = {};
        }
    }

    void ScheduleQueryStatusRequest(const TString& sessionId) {
        auto self = weak_from_this();
        Delay(TDuration::MilliSeconds(1000)).Subscribe([self, sessionId](TFuture<void> fut) {
            auto this_ = self.lock();
            if (!this_) {
                return;
            }

            this_->StartQueryStatusRequest(sessionId, !fut.HasException());
        });
    }

    void SchedulePingSessionRequest(const TString& sessionId) {
        auto self = weak_from_this();
        auto callback = [self, sessionId](
            NGrpc::TGrpcStatus&& status,
            Yql::DqsProto::PingSessionResponse&&) mutable
        {
            auto this_ = self.lock();
            if (!this_) {
                return;
            }

            if (status.GRpcStatusCode == grpc::INVALID_ARGUMENT) {
                YQL_CLOG(INFO, ProviderDq) << "Session closed " << sessionId;
            } else {
                this_->SchedulePingSessionRequest(sessionId);
            }
        };
        Delay(TDuration::Seconds(10)).Subscribe([self, callback, sessionId](const TFuture<void>&) {
            auto this_ = self.lock();
            if (!this_) {
                return;
            }

            Yql::DqsProto::PingSessionRequest query;
            query.SetSession(sessionId);

            this_->Service->DoRequest<Yql::DqsProto::PingSessionRequest, Yql::DqsProto::PingSessionResponse>(
                query,
                callback,
                &Yql::DqsProto::DqService::Stub::AsyncPingSession);
        });
    }

    struct TDelay: public TTaskScheduler::ITask {
        TDelay(TPromise<void> p)
            : Promise(std::move(p))
        { }

        TInstant Process() override {
            Promise.SetValue();
            return TInstant::Max();
        }

        TPromise<void> Promise;
    };

private:
    NGrpc::TGRpcClientConfig GrpcConf;
    NGrpc::TGRpcClientLow GrpcClient;
    std::unique_ptr<NGrpc::TServiceConnection<Yql::DqsProto::DqService>> Service;

    TMutex ProgressMutex;
    TMutex Mutex;

    struct TSession {
        std::optional<TDqProgressWriter> ProgressWriter;
        TString Status;
        TFuture<void> OpenSessionFuture;
    };
    THashMap<TString, TSession> RunningQueries;
    TString VanillaJobPath;
    TString VanillaJobMd5;

    TTaskScheduler TaskScheduler;
    const TDuration OpenSessionTimeout;
};

class TDqGateway: public IDqGateway {
public:
    TDqGateway(const TString& host, int port, const TString& vanillaJobPath, const TString& vanillaJobMd5, TDuration timeout = TDuration::Minutes(60), TDuration requestTimeout = TDuration::Max())
        : Impl(std::make_shared<TDqGatewayImpl>(host, port, vanillaJobPath, vanillaJobMd5, timeout, requestTimeout))
    { }

    TFuture<void> OpenSession(const TString& sessionId, const TString& username) override
    {
        return Impl->OpenSession(sessionId, username);
    }

    void CloseSession(const TString& sessionId) override
    {
        Impl->CloseSession(sessionId);
    }

    TFuture<TResult> ExecutePlan(const TString& sessionId, NDqs::TPlan&& plan, const TVector<TString>& columns,
                const THashMap<TString, TString>& secureParams, const THashMap<TString, TString>& graphParams,
                const TDqSettings::TPtr& settings,
                const TDqProgressWriter& progressWriter, const THashMap<TString, TString>& modulesMapping,
                bool discard) override
    {
        return Impl->ExecutePlan(sessionId, std::move(plan), columns, secureParams, graphParams, settings, progressWriter, modulesMapping, discard);
    }

    TString GetVanillaJobPath() override {
        return Impl->GetVanillaJobPath();
    }

    TString GetVanillaJobMd5() override {
        return Impl->GetVanillaJobMd5();
    }

private:
    std::shared_ptr<TDqGatewayImpl> Impl;
};

TIntrusivePtr<IDqGateway> CreateDqGateway(const TString& host, int port) {
    return new TDqGateway(host, port, "", "");
}

TIntrusivePtr<IDqGateway> CreateDqGateway(const NProto::TDqConfig& config) {
    return new TDqGateway("localhost", config.GetPort(),
        config.GetYtBackends()[0].GetVanillaJob(),
        config.GetYtBackends()[0].GetVanillaJobMd5(),
        TDuration::MilliSeconds(config.GetOpenSessionTimeoutMs()),
        TDuration::MilliSeconds(config.GetRequestTimeoutMs()));
}

} // namespace NYql
