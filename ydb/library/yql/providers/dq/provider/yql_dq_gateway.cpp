#include "yql_dq_gateway.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/dq/api/grpc/api.grpc.pb.h>
#include <ydb/library/yql/utils/backtrace/backtrace.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/providers/dq/config/config.pb.h>
#include <ydb/library/yql/utils/log/log.h>

#include <ydb/public/lib/yson_value/ydb_yson_value.h>

#include <ydb/library/grpc/client/grpc_client_low.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/threading/task_scheduler/task_scheduler.h>

#include <util/system/mutex.h>
#include <util/generic/hash.h>
#include <util/string/builder.h>

#include <utility>

namespace NYql {

using namespace NThreading;

class TDqTaskScheduler : public TTaskScheduler {
private:
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

public:
    TDqTaskScheduler()
        : TTaskScheduler(1) // threads
    {}

    TFuture<void> Delay(TDuration duration) {
        TPromise<void> promise = NewPromise();

        auto future = promise.GetFuture();

        if (!Add(MakeIntrusive<TDelay>(promise), TInstant::Now() + duration)) {
            promise.SetException("cannot delay");
        }

        return future;
    }
};

class TDqGatewaySession: public std::enable_shared_from_this<TDqGatewaySession> {
public:
    using TResult = IDqGateway::TResult;
    using TDqProgressWriter = IDqGateway::TDqProgressWriter;

    TDqGatewaySession(const TString& sessionId, TDqTaskScheduler& taskScheduler, NYdbGrpc::TServiceConnection<Yql::DqsProto::DqService>& service, TFuture<void>&& openSessionFuture)
        : SessionId(sessionId)
        , TaskScheduler(taskScheduler)
        , Service(service)
        , OpenSessionFuture(std::move(openSessionFuture))
    {
    }

    const TString& GetSessionId() const {
        return SessionId;
    }

    template<typename RespType>
    void OnResponse(TPromise<TResult> promise, NYdbGrpc::TGrpcStatus&& status, RespType&& resp, const THashMap<TString, TString>& modulesMapping, bool alwaysFallback = false) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(SessionId);
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

            for (auto& message_ : *operation.Mutableissues()) {
                TDeque<std::remove_reference_t<decltype(message_)>*> queue;
                queue.push_front(&message_);
                while (!queue.empty()) {
                    auto& message = *queue.front();
                    queue.pop_front();
                    message.Setmessage(NBacktrace::Symbolize(message.Getmessage(), modulesMapping));
                    for (auto& subMsg : *message.Mutableissues()) {
                        queue.push_back(&subMsg);
                    }
                }
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
                || status.GRpcStatusCode == grpc::INVALID_ARGUMENT /* Bad session */
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

    template <typename TResponse, typename TRequest, typename TStub>
    TFuture<TResult> WithRetry(
        const TRequest& queryPB,
        TStub stub,
        int retry,
        const TDqSettings::TPtr& settings,
        const THashMap<TString, TString>& modulesMapping,
        const TDqProgressWriter& progressWriter
    ) {
        auto backoff = TDuration::MilliSeconds(settings->RetryBackoffMs.Get().GetOrElse(1000));
        auto promise = NewPromise<TResult>();
        const auto fallbackPolicy = settings->FallbackPolicy.Get().GetOrElse(EFallbackPolicy::Default);
        const auto alwaysFallback = EFallbackPolicy::Always == fallbackPolicy;
        auto self = weak_from_this();
        auto callback = [self, promise, sessionId = SessionId, alwaysFallback, modulesMapping](NYdbGrpc::TGrpcStatus&& status, TResponse&& resp) mutable {
            auto this_ = self.lock();
            if (!this_) {
                YQL_CLOG(DEBUG, ProviderDq) << "Session was closed: " << sessionId;
                promise.SetException("Session was closed");
                return;
            }

            this_->OnResponse(std::move(promise), std::move(status), std::move(resp), modulesMapping, alwaysFallback);
        };

        Service.DoRequest<TRequest, TResponse>(queryPB, callback, stub);

        ScheduleQueryStatusRequest(progressWriter);

        return promise.GetFuture().Apply([=](const TFuture<TResult>& result) {
            if (result.HasException()) {
                return result;
            }
            auto value = result.GetValue();
            auto this_ = self.lock();

            if (value.Success() || retry == 0 || !value.Retriable || !this_) {
                return result;
            }

            return this_->TaskScheduler.Delay(backoff)
                .Apply([=, sessionId = this_->GetSessionId()](const TFuture<void>& result) {
                    auto this_ = self.lock();
                    try {
                        result.TryRethrow();
                        if (!this_) {
                            YQL_CLOG(DEBUG, ProviderDq) << "Session was closed: " << sessionId;
                            throw std::runtime_error("Session was closed");
                        }
                    } catch (...) {
                        return MakeErrorFuture<TResult>(std::current_exception());
                    }
                    return this_->WithRetry<TResponse>(queryPB, stub, retry - 1, settings, modulesMapping, progressWriter);
                });
        });
    }

    TFuture<TResult>
    ExecutePlan(NDqs::TPlan&& plan, const TVector<TString>& columns,
                const THashMap<TString, TString>& secureParams, const THashMap<TString, TString>& graphParams,
                const TDqSettings::TPtr& settings,
                const TDqProgressWriter& progressWriter, const THashMap<TString, TString>& modulesMapping,
                bool discard, ui64 executionTimeout)
    {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(SessionId);

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
        queryPB.SetExecutionTimeout(executionTimeout);
        queryPB.SetSession(SessionId);
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

        YQL_CLOG(DEBUG, ProviderDq) << "Send query of size " << queryPB.ByteSizeLong();

        auto self = weak_from_this();
        return OpenSessionFuture.Apply([self, sessionId = SessionId, queryPB, retry, settings, modulesMapping, progressWriter](const TFuture<void>& f) {
            f.TryRethrow();
            auto this_ = self.lock();
            if (!this_) {
                YQL_CLOG(DEBUG, ProviderDq) << "Session was closed: " << sessionId;
                return MakeErrorFuture<TResult>(std::make_exception_ptr(std::runtime_error("Session was closed")));
            }

            return this_->WithRetry<Yql::DqsProto::ExecuteGraphResponse>(
                queryPB,
                &Yql::DqsProto::DqService::Stub::AsyncExecuteGraph,
                retry,
                settings,
                modulesMapping,
                progressWriter);
        });
    }

    TFuture<void> Close() {
        Yql::DqsProto::CloseSessionRequest request;
        request.SetSession(SessionId);

        auto promise = NewPromise<void>();
        auto callback = [promise, sessionId = SessionId](NYdbGrpc::TGrpcStatus&& status, Yql::DqsProto::CloseSessionResponse&& resp) mutable {
            Y_UNUSED(resp);
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
            if (status.Ok()) {
                YQL_CLOG(DEBUG, ProviderDq) << "Async close session OK";
                promise.SetValue();
            } else {
                YQL_CLOG(ERROR, ProviderDq) << "Async close session error: " << status.GRpcStatusCode << ", message: " << status.Msg;
                promise.SetException(TStringBuilder() << "Async close session error: " << status.GRpcStatusCode << ", message: " << status.Msg);
            }
        };

        Service.DoRequest<Yql::DqsProto::CloseSessionRequest, Yql::DqsProto::CloseSessionResponse>(
            request, callback, &Yql::DqsProto::DqService::Stub::AsyncCloseSession);
        return promise.GetFuture();
    }

    void OnRequestQueryStatus(const TDqProgressWriter& progressWriter, const TString& status, bool ok) {
        if (ok) {
            ScheduleQueryStatusRequest(progressWriter);
            if (!status.empty()) {
                progressWriter(status);
            }
        }
    }

    void RequestQueryStatus(const TDqProgressWriter& progressWriter) {
        Yql::DqsProto::QueryStatusRequest request;
        request.SetSession(SessionId);
        auto self = weak_from_this();
        auto callback = [self, progressWriter](NYdbGrpc::TGrpcStatus&& status, Yql::DqsProto::QueryStatusResponse&& resp) {
            auto this_ = self.lock();
            if (!this_) {
                return;
            }

            this_->OnRequestQueryStatus(progressWriter, resp.GetStatus(), status.Ok());
        };

        Service.DoRequest<Yql::DqsProto::QueryStatusRequest, Yql::DqsProto::QueryStatusResponse>(
            request, callback, &Yql::DqsProto::DqService::Stub::AsyncQueryStatus, {}, nullptr);
    }

    void ScheduleQueryStatusRequest(const TDqProgressWriter& progressWriter) {
        auto self = weak_from_this();
        TaskScheduler.Delay(TDuration::MilliSeconds(1000)).Subscribe([self, progressWriter](const TFuture<void>& f) {
            auto this_ = self.lock();
            if (!this_) {
                return;
            }

            if (!f.HasException()) {
                this_->RequestQueryStatus(progressWriter);
            }
        });
    }

private:
    const TString SessionId;
    TDqTaskScheduler& TaskScheduler;
    NYdbGrpc::TServiceConnection<Yql::DqsProto::DqService>& Service;

    TMutex ProgressMutex;

    std::optional<TDqProgressWriter> ProgressWriter;
    TString Status;
    TFuture<void> OpenSessionFuture;
};

class TDqGatewayImpl: public std::enable_shared_from_this<TDqGatewayImpl> {
    using TResult = IDqGateway::TResult;
    using TDqProgressWriter = IDqGateway::TDqProgressWriter;

public:
    TDqGatewayImpl(const TString& host, int port, TDuration timeout = TDuration::Minutes(60), TDuration requestTimeout = TDuration::Max())
        : GrpcConf(TStringBuilder() << host << ":" << port, requestTimeout)
        , GrpcClient(1)
        , Service(GrpcClient.CreateGRpcServiceConnection<Yql::DqsProto::DqService>(GrpcConf))
        , TaskScheduler()
        , OpenSessionTimeout(timeout)
    {
        TaskScheduler.Start();
    }

    ~TDqGatewayImpl() {
        Stop();
    }

    void Stop() {
        decltype(Sessions) sessions;
        with_lock (Mutex) {
            sessions = std::move(Sessions);
        }
        for (auto& pair: sessions) {
            try {
                pair.second->Close().GetValueSync();
            } catch (...) {
                YQL_LOG_CTX_ROOT_SESSION_SCOPE(pair.first);
                YQL_CLOG(ERROR, ProviderDq) << "Error closing session " << pair.first << ": " << CurrentExceptionMessage();
            }
        }
        sessions.clear(); // Destroy session objects explicitly before stopping grpc
        TaskScheduler.Stop();
        try {
            GrpcClient.Stop(/* wait = */ true);
        } catch (...) {
            YQL_CLOG(ERROR, ProviderDq) << "Error while stopping GRPC client: " << CurrentExceptionMessage();
        }
    }

    void DropSession(const TString& sessionId) {
        with_lock (Mutex) {
            Sessions.erase(sessionId);
        }
    }

    TFuture<void> OpenSession(const TString& sessionId, const TString& username) {
        YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
        YQL_CLOG(INFO, ProviderDq) << "OpenSession";

        auto promise = NewPromise<void>();
        std::shared_ptr<TDqGatewaySession> session = std::make_shared<TDqGatewaySession>(sessionId, TaskScheduler, *Service, promise.GetFuture());
        with_lock (Mutex) {
            if (!Sessions.emplace(sessionId, session).second) {
                return MakeErrorFuture<void>(std::make_exception_ptr(yexception() << "Duplicate session id: " << sessionId));
            }
        }

        Yql::DqsProto::OpenSessionRequest request;
        request.SetSession(sessionId);
        request.SetUsername(username);

        NYdbGrpc::TCallMeta meta;
        meta.Timeout = OpenSessionTimeout;

        auto self = weak_from_this();
        auto callback = [self, promise, sessionId](NYdbGrpc::TGrpcStatus&& status, Yql::DqsProto::OpenSessionResponse&& resp) mutable {
            Y_UNUSED(resp);
            YQL_LOG_CTX_ROOT_SESSION_SCOPE(sessionId);
            auto this_ = self.lock();
            if (!this_) {
                YQL_CLOG(ERROR, ProviderDq) << "Session was closed: " << sessionId;
                promise.SetException("Session was closed");
                return;
            }
            if (status.Ok()) {
                YQL_CLOG(INFO, ProviderDq) << "OpenSession OK";
                this_->SchedulePingSessionRequest(sessionId);
                promise.SetValue();
            } else {
                YQL_CLOG(ERROR, ProviderDq) << "OpenSession error: " << status.Msg;
                this_->DropSession(sessionId);
                promise.SetException(status.Msg);
            }
        };

        Service->DoRequest<Yql::DqsProto::OpenSessionRequest, Yql::DqsProto::OpenSessionResponse>(
            request, callback, &Yql::DqsProto::DqService::Stub::AsyncOpenSession, meta);

       return MakeFuture();
    }

    void SchedulePingSessionRequest(const TString& sessionId) {
        auto self = weak_from_this();
        auto callback = [self, sessionId] (NYdbGrpc::TGrpcStatus&& status, Yql::DqsProto::PingSessionResponse&&) mutable {
            auto this_ = self.lock();
            if (!this_) {
                return;
            }

            if (status.GRpcStatusCode == grpc::INVALID_ARGUMENT || status.GRpcStatusCode == grpc::CANCELLED) {
                YQL_CLOG(INFO, ProviderDq) << "Session closed " << sessionId;
                this_->DropSession(sessionId);
            } else {
                this_->SchedulePingSessionRequest(sessionId);
            }
        };
        TaskScheduler.Delay(TDuration::Seconds(10)).Subscribe([self, callback, sessionId](const TFuture<void>&) {
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

    TFuture<void> CloseSessionAsync(const TString& sessionId) {
        std::shared_ptr<TDqGatewaySession> session;
        with_lock (Mutex) {
            auto it = Sessions.find(sessionId);
            if (it != Sessions.end()) {
                session = it->second;
                Sessions.erase(it);
            }
        }
        if (session) {
            return session->Close();
        }
        return MakeFuture();
    }

    TFuture<TResult> ExecutePlan(const TString& sessionId, NDqs::TPlan&& plan, const TVector<TString>& columns,
        const THashMap<TString, TString>& secureParams, const THashMap<TString, TString>& graphParams,
        const TDqSettings::TPtr& settings,
        const TDqProgressWriter& progressWriter, const THashMap<TString, TString>& modulesMapping,
        bool discard, ui64 executionTimeout)
    {
        std::shared_ptr<TDqGatewaySession> session;
        with_lock(Mutex) {
            auto it = Sessions.find(sessionId);
            if (it != Sessions.end()) {
                session = it->second;
            }
        }
        if (!session) {
            YQL_CLOG(ERROR, ProviderDq) << "Session was closed: " << sessionId;
            return MakeFuture(NCommon::ResultFromException<TResult>(yexception() << "Session was closed"));
        }
        return session->ExecutePlan(std::move(plan), columns, secureParams, graphParams, settings, progressWriter, modulesMapping, discard, executionTimeout)
            .Apply([](const TFuture<TResult>& f) {
                try {
                    f.TryRethrow();
                } catch (const std::exception& e) {
                    YQL_CLOG(ERROR, ProviderDq) << e.what();
                    return MakeFuture(NCommon::ResultFromException<TResult>(e));
                }
                return f;
            });
    }

private:
    NYdbGrpc::TGRpcClientConfig GrpcConf;
    NYdbGrpc::TGRpcClientLow GrpcClient;
    std::unique_ptr<NYdbGrpc::TServiceConnection<Yql::DqsProto::DqService>> Service;

    TDqTaskScheduler TaskScheduler;
    const TDuration OpenSessionTimeout;

    TMutex Mutex;
    THashMap<TString, std::shared_ptr<TDqGatewaySession>> Sessions;
};

class TDqGateway: public IDqGateway {
public:
    TDqGateway(const TString& host, int port, const TString& vanillaJobPath, const TString& vanillaJobMd5, TDuration timeout = TDuration::Minutes(60), TDuration requestTimeout = TDuration::Max())
        : Impl(std::make_shared<TDqGatewayImpl>(host, port, timeout, requestTimeout))
        , VanillaJobPath(vanillaJobPath)
        , VanillaJobMd5(vanillaJobMd5)
    {
    }

    ~TDqGateway() {
    }

    void Stop() override {
        Impl->Stop();
    }

    TFuture<void> OpenSession(const TString& sessionId, const TString& username) override {
        return Impl->OpenSession(sessionId, username);
    }

    TFuture<void> CloseSessionAsync(const TString& sessionId) override {
        return Impl->CloseSessionAsync(sessionId);
    }

    TFuture<TResult> ExecutePlan(const TString& sessionId, NDqs::TPlan&& plan, const TVector<TString>& columns,
        const THashMap<TString, TString>& secureParams, const THashMap<TString, TString>& graphParams,
        const TDqSettings::TPtr& settings,
        const TDqProgressWriter& progressWriter, const THashMap<TString, TString>& modulesMapping,
        bool discard, ui64 executionTimeout) override
    {
        return Impl->ExecutePlan(sessionId, std::move(plan), columns, secureParams, graphParams, settings, progressWriter, modulesMapping, discard, executionTimeout);
    }

    TString GetVanillaJobPath() override {
        return VanillaJobPath;
    }

    TString GetVanillaJobMd5() override {
        return VanillaJobMd5;
    }

private:
    std::shared_ptr<TDqGatewayImpl> Impl;
    TString VanillaJobPath;
    TString VanillaJobMd5;
};

TIntrusivePtr<IDqGateway> CreateDqGateway(const TString& host, int port) {
    return new TDqGateway(host, port, "", "");
}

TIntrusivePtr<IDqGateway> CreateDqGateway(const NProto::TDqConfig& config) {
    return new TDqGateway("localhost", config.GetPort(),
        config.GetYtBackends()[0].GetVanillaJobLite(),
        config.GetYtBackends()[0].GetVanillaJobLiteMd5(),
        TDuration::MilliSeconds(config.GetOpenSessionTimeoutMs()),
        TDuration::MilliSeconds(config.GetRequestTimeoutMs()));
}

} // namespace NYql
