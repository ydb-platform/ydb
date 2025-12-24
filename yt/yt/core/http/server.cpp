#include "server.h"
#include "http.h"
#include "config.h"
#include "stream.h"
#include "private.h"
#include "helpers.h"

#include <yt/yt/core/net/listener.h>
#include <yt/yt/core/net/connection.h>

#include <yt/yt/core/concurrency/poller.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NHttp {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

TCallbackHandler::TCallbackHandler(TCallback<void(const IRequestPtr&, const IResponseWriterPtr&)> handler)
    : Handler_(std::move(handler))
{ }

void TCallbackHandler::HandleRequest(const IRequestPtr& req, const IResponseWriterPtr& rsp)
{
    Handler_(req, rsp);
}

////////////////////////////////////////////////////////////////////////////////

void IServer::AddHandler(
    const TString& pattern,
    TCallback<void(const IRequestPtr&, const IResponseWriterPtr&)> handler)
{
    AddHandler(pattern, New<TCallbackHandler>(handler));
}

namespace {

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IServer
{
public:
    TServer(
        TServerConfigPtr config,
        IListenerPtr listener,
        IPollerPtr poller,
        IPollerPtr acceptor,
        IInvokerPtr invoker,
        IRequestPathMatcherPtr requestPathMatcher,
        bool ownPoller = false)
        : Config_(std::move(config))
        , Logger(HttpLogger().WithTag("ServerName: %v", Config_->ServerName))
        , Listener_(std::move(listener))
        , Poller_(std::move(poller))
        , Acceptor_(std::move(acceptor))
        , Invoker_(std::move(invoker))
        , OwnPoller_(ownPoller)
        , Address_(Listener_ ? Listener_->GetAddress() : TNetworkAddress::CreateIPv6Any(Config_->Port))
        , Profiling_(HttpProfiler.WithTag("server", Config_->ServerName))
        , RequestPathMatcher_(std::move(requestPathMatcher))
    { }

    void AddHandler(const TString& path, const IHttpHandlerPtr& handler) override
    {
        YT_VERIFY(!Started_);
        RequestPathMatcher_->Add(path, handler);
    }

    const TNetworkAddress& GetAddress() const override
    {
        return Address_;
    }

    void Start() override
    {
        YT_VERIFY(!Started_);
        Started_ = true;

        if (!Listener_) {
            for (int i = 0;; ++i) {
                try {
                    Listener_ = CreateListener(Address_, Poller_, Acceptor_, Config_->MaxBacklogSize);
                    break;
                } catch (const std::exception& ex) {
                    if (i + 1 == Config_->BindRetryCount) {
                        throw;
                    } else {
                        YT_LOG_ERROR(ex, "HTTP server bind failed");
                        Sleep(Config_->BindRetryBackoff);
                    }
                }
            }
        }

        YT_LOG_INFO("Server started");

        AsyncAcceptConnection();
    }

    void Stop() override
    {
        Stopped_.store(true);

        if (OwnPoller_) {
            Poller_->Shutdown();
        }

        YT_LOG_INFO("Server stopped");
    }

    void SetPathMatcher(const IRequestPathMatcherPtr& matcher) override
    {
        YT_VERIFY(RequestPathMatcher_->IsEmpty());
        RequestPathMatcher_ = matcher;
        YT_LOG_INFO("Request path matcher changed");
    }

    IRequestPathMatcherPtr GetPathMatcher() override
    {
        return RequestPathMatcher_;
    }

private:
    class TProfiling
    {
    public:
        struct TStatusCodeCounter
        {
            explicit TStatusCodeCounter(TProfiler profiler)
                : Profiler_(std::move(profiler))
            { }

            TCounter* GetCounter(EStatusCode statusCode)
            {
                return StatusCodeCounter_.FindOrInsert(statusCode, [&] {
                    return Profiler_
                        .WithTag("status", ToString(statusCode))
                        .Counter("/status_count");
                }).first;
            }

        private:
            const TProfiler Profiler_;

            TSyncMap<EStatusCode, TCounter> StatusCodeCounter_;
        };

        struct TRequestProfiling
            : public TRefCounted
        {
            explicit TRequestProfiling(const TProfiler& profiler)
                : RequestCounter(profiler.Counter("/request_count"))
                , TotalTimeCounter(profiler.TimeCounter("/request_time/total"))
                , StatusCodeCounter(profiler)
            { }

            TCounter RequestCounter;
            TCounter FailedRequestCounter;
            TCounter RequestWithoutHandlerCount;

            TTimeCounter TotalTimeCounter;

            TStatusCodeCounter StatusCodeCounter;
        };

        explicit TProfiling(const TProfiler& profiler)
            : ConnectionsActive(profiler.Gauge("/connections_active"))
            , ConnectionsAccepted(profiler.Counter("/connections_accepted"))
            , ConnectionsDropped(profiler.Counter("/connections_dropped"))
            , RequestsMissingHeaders(profiler.Counter("/requests_missing_headers"))
            , Profiler_(profiler)
        { }

        TGauge ConnectionsActive;
        TCounter ConnectionsAccepted;
        TCounter ConnectionsDropped;
        TCounter RequestsMissingHeaders;

        TRequestProfiling* GetRequestProfiling(const THttpInputPtr& httpRequest)
        {
            TRequestProfilingKey profilingKey{httpRequest->GetUrl().Path};
            return RequestProfilingMap_.FindOrInsert(profilingKey, [&] {
                return New<TRequestProfiling>(Profiler_
                    .WithTag("path", std::string(std::get<0>(profilingKey))));
            }).first->Get();
        }

    private:
        TProfiler Profiler_;

        // Path.
        using TRequestProfilingKey = std::tuple<std::string>;
        NConcurrency::TSyncMap<TRequestProfilingKey, TIntrusivePtr<TRequestProfiling>> RequestProfilingMap_;
    };

    const TServerConfigPtr Config_;
    const NLogging::TLogger Logger;
    IListenerPtr Listener_;
    const IPollerPtr Poller_;
    const IPollerPtr Acceptor_;
    const IInvokerPtr Invoker_;
    const bool OwnPoller_ = false;
    const TNetworkAddress Address_;

    TProfiling Profiling_;
    IRequestPathMatcherPtr RequestPathMatcher_;
    bool Started_ = false;
    std::atomic<bool> Stopped_ = false;

    std::atomic<int> ActiveConnections_ = 0;

    void AsyncAcceptConnection()
    {
        YT_VERIFY(Listener_);
        Listener_->Accept().Subscribe(
            BIND(&TServer::OnConnectionAccepted, MakeWeak(this))
                .Via(Acceptor_->GetInvoker()));
    }

    void OnConnectionAccepted(const TErrorOr<IConnectionPtr>& connectionOrError)
    {
        if (Stopped_.load()) {
            return;
        }

        AsyncAcceptConnection();

        if (!connectionOrError.IsOK()) {
            YT_LOG_INFO(connectionOrError, "Error accepting connection");
            return;
        }

        auto connection = connectionOrError.ValueOrThrow();

        auto count = ActiveConnections_.fetch_add(1) + 1;
        if (count >= Config_->MaxSimultaneousConnections) {
            Profiling_.ConnectionsDropped.Increment();
            ActiveConnections_--;
            YT_LOG_WARNING("Server is over max active connection limit (RemoteAddress: %v)",
                connection->GetRemoteAddress());
            return;
        }
        Profiling_.ConnectionsActive.Update(count);
        Profiling_.ConnectionsAccepted.Increment();

        YT_LOG_DEBUG("Connection accepted (ConnectionId: %v, RemoteAddress: %v, LocalAddress: %v)",
            connection->GetId(),
            connection->GetRemoteAddress(),
            connection->GetLocalAddress());

        Invoker_->Invoke(
            BIND(&TServer::HandleConnection, MakeStrong(this), std::move(connection)));
    }

    bool HandleRequest(const THttpInputPtr& request, const THttpOutputPtr& response)
    {
        response->SetStatus(EStatusCode::InternalServerError);

        bool closeResponse = true;
        try {
            if (!request->ReceiveHeaders()) {
                Profiling_.RequestsMissingHeaders.Increment();
                return false;
            }
            auto* requestProfiling = Profiling_.GetRequestProfiling(request);
            requestProfiling->RequestCounter.Increment();
            auto finallyGuard = Finally([requestProfiling, &response] {
                requestProfiling->StatusCodeCounter.GetCounter(*response->GetStatus())->Increment();
            });

            const auto& path = request->GetUrl().Path;

            NProfiling::TWallTimer timer;

            YT_LOG_DEBUG("Received HTTP request ("
                "ConnectionId: %v, "
                "RequestId: %v, "
                "Method: %v, "
                "Path: %v, "
                "L7RequestId: %v, "
                "L7RealIP: %v, "
                "UserAgent: %v)",
                request->GetConnectionId(),
                request->GetRequestId(),
                request->GetMethod(),
                path,
                FindBalancerRequestId(request),
                FindBalancerRealIP(request),
                FindUserAgent(request));

            auto handler = RequestPathMatcher_->Match(path);
            if (handler) {
                closeResponse = false;

                if (request->IsExpecting100Continue()) {
                    response->Flush100Continue();
                }

                auto traceContext = GetOrCreateTraceContext(request);
                NTracing::TTraceContextGuard guard(traceContext);
                SetTraceId(response, traceContext->GetTraceId());

                SetRequestId(response, request->GetRequestId());

                handler->HandleRequest(request, response);

                NTracing::FlushCurrentTraceContextElapsedTime();

                requestProfiling->TotalTimeCounter.Add(timer.GetElapsedTime());

                YT_LOG_DEBUG("Finished handling HTTP request (RequestId: %v, WallTime: %v, CpuTime: %v)",
                    request->GetRequestId(),
                    timer.GetElapsedTime(),
                    traceContext->GetElapsedTime());
            } else {
                YT_LOG_INFO("Missing HTTP handler for given URL (RequestId: %v, Path: %v)",
                    request->GetRequestId(),
                    path);

                response->SetStatus(EStatusCode::NotFound);
            }
        } catch (const std::exception& ex) {
            closeResponse = true;
            YT_LOG_INFO(ex, "Error handling HTTP request (RequestId: %v)",
                request->GetRequestId());

            if (!response->AreHeadersFlushed()) {
                response->SetStatus(EStatusCode::InternalServerError);
            }
        }

        try {
            if (closeResponse) {
                WaitFor(response->Close())
                    .ThrowOnError();
            }
        } catch (const std::exception& ex) {
            YT_LOG_INFO(ex, "Error flushing HTTP response stream (RequestId: %v)",
                request->GetRequestId());
        }

        return true;
    }

    void HandleConnection(const IConnectionPtr& connection)
    {
        try {
            connection->SubscribePeerDisconnect(BIND([Logger = Logger, config = Config_, canceler = GetCurrentFiberCanceler(), connectionId = connection->GetId()] {
                YT_LOG_DEBUG("Client closed TCP socket (ConnectionId: %v)", connectionId);

                if (config->CancelFiberOnConnectionClose.value_or(false)) {
                    canceler(TError("Client closed TCP socket; HTTP connection closed"));
                }
            }));

            auto finally = Finally([&] {
                auto count = ActiveConnections_.fetch_sub(1) - 1;
                Profiling_.ConnectionsActive.Update(count);
            });

            if (Config_->NoDelay) {
                connection->SetNoDelay();
            }

            DoHandleConnection(connection);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Unhandled exception (ConnectionId: %v)", connection->GetId());
        }
    }

    void DoHandleConnection(const IConnectionPtr& connection)
    {
        auto request = New<THttpInput>(
            connection,
            connection->GetRemoteAddress(),
            GetCurrentInvoker(),
            EMessageType::Request,
            /*requestMethod*/ std::nullopt,
            Config_);

        if (Config_->IsHttps) {
            request->SetHttps();
        }

        request->SetPort(Config_->Port);

        auto response = New<THttpOutput>(
            connection,
            EMessageType::Response,
            Config_);

        while (true) {
            auto requestId = TRequestId::Create();
            request->SetRequestId(requestId);
            response->SetRequestId(requestId);

            bool ok = HandleRequest(request, response);

            if (!ok) {
                break;
            }

            auto logDrop = [&] (auto reason) {
                YT_LOG_DEBUG("Dropping HTTP connection (ConnectionId: %v, Reason: %v)",
                    connection->GetId(),
                    reason);
            };

            if (!Config_->EnableKeepAlive) {
                break;
            }

            // Arcadia decompressors might return eof earlier than
            // underlying stream. From HTTP server standpoint that
            // looks like request that wasn't fully consumed, even if
            // next Read() on that request would have returned eof.
            //
            // So we perform one last Read() here and check that
            // there is no data left inside stream.
            bool bodyConsumed = false;
            try {
                auto chunk = WaitFor(request->Read())
                    .ValueOrThrow();
                bodyConsumed = chunk.Empty();
            } catch (const std::exception& ) { }
            if (!bodyConsumed) {
                logDrop("Body is not fully consumed by the handler");
                break;
            }

            if (request->IsSafeToReuse()) {
                request->Reset();
            } else {
                logDrop("Request is not safe to reuse");
                break;
            }

            if (response->IsSafeToReuse()) {
                response->Reset();
            } else {
                logDrop("Response is not safe to reuse");
                break;
            }

            if (!connection->IsIdle()) {
                logDrop("Connection not idle");
                break;
            }
        }

        auto connectionResult = WaitFor(connection->Close());
        if (connectionResult.IsOK()) {
            YT_LOG_DEBUG("HTTP connection closed (ConnectionId: %v)",
                connection->GetId());
        } else {
            YT_LOG_DEBUG(connectionResult, "Error closing HTTP connection (ConnectionId: %v)",
                connection->GetId());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TServerConfigPtr config,
    IListenerPtr listener,
    IPollerPtr poller,
    IPollerPtr acceptor,
    IInvokerPtr invoker,
    bool ownPoller)
{
    auto handlers = New<TRequestPathMatcher>();
    return New<TServer>(
        std::move(config),
        std::move(listener),
        std::move(poller),
        std::move(acceptor),
        std::move(invoker),
        std::move(handlers),
        ownPoller);
}

IServerPtr CreateServer(
    TServerConfigPtr config,
    IPollerPtr poller,
    IPollerPtr acceptor,
    IInvokerPtr invoker,
    bool ownPoller)
{
    return CreateServer(
        std::move(config),
        /*listener*/ nullptr,
        std::move(poller),
        std::move(acceptor),
        std::move(invoker),
        ownPoller);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TServerConfigPtr config,
    IListenerPtr listener,
    IPollerPtr poller)
{
    auto acceptor = poller;
    auto invoker = poller->GetInvoker();
    return CreateServer(
        std::move(config),
        std::move(listener),
        std::move(poller),
        std::move(acceptor),
        std::move(invoker),
        /*ownPoller*/ false);
}

IServerPtr CreateServer(
    TServerConfigPtr config,
    IListenerPtr listener,
    IPollerPtr poller,
    IPollerPtr acceptor)
{
    auto invoker = poller->GetInvoker();
    return CreateServer(
        std::move(config),
        std::move(listener),
        std::move(poller),
        std::move(acceptor),
        std::move(invoker),
        /*ownPoller*/ false);
}

IServerPtr CreateServer(
    TServerConfigPtr config,
    IPollerPtr poller,
    IPollerPtr acceptor)
{
    auto invoker = poller->GetInvoker();
    return CreateServer(
        std::move(config),
        std::move(poller),
        std::move(acceptor),
        std::move(invoker),
        /*ownPoller*/ false);
}

IServerPtr CreateServer(TServerConfigPtr config, IPollerPtr poller)
{
    auto acceptor = poller;
    return CreateServer(
        std::move(config),
        std::move(poller),
        std::move(acceptor));
}

IServerPtr CreateServer(int port, IPollerPtr poller)
{
    auto config = New<TServerConfig>();
    config->Port = port;
    return CreateServer(std::move(config), std::move(poller));
}

IServerPtr CreateServer(TServerConfigPtr config, int pollerThreadCount)
{
    auto poller = CreateThreadPoolPoller(pollerThreadCount, config->ServerName);
    auto acceptor = poller;
    auto invoker = poller->GetInvoker();
    return CreateServer(
        std::move(config),
        std::move(poller),
        std::move(acceptor),
        std::move(invoker),
        /*ownPoller*/ true);
}

IServerPtr CreateServer(
    TServerConfigPtr config,
    NConcurrency::IPollerPtr poller,
    IInvokerPtr invoker)
{
    auto acceptor = poller;
    return CreateServer(
        std::move(config),
        std::move(poller),
        std::move(acceptor),
        std::move(invoker),
        /*ownPoller*/ false);
}

////////////////////////////////////////////////////////////////////////////////

/*!
 *  Path matching semantic is copied from go standard library.
 *  See https://golang.org/pkg/net/http/#ServeMux
 *
 *  Supported features:
 *  - matching path exactly: "/path/name"
 *  - matching path prefix: "/path/" matches all with prefix "/path/"
 *  - trailing-slash redirection: matching "/path/" implies "/path"
 *  - end of path wildcard: "/path/{$}" matches only "/path/" and "/path"
 */
void TRequestPathMatcher::Add(const TString& pattern, const IHttpHandlerPtr& handler)
{
    if (pattern.empty()) {
        THROW_ERROR_EXCEPTION("Empty pattern is invalid");
    }

    if (pattern.EndsWith("/{$}")) {
        auto withoutWildcard = pattern.substr(0, pattern.size() - 3);

        Exact_[withoutWildcard] = handler;
        if (withoutWildcard.size() > 1) {
            Exact_[withoutWildcard.substr(0, withoutWildcard.size() - 1)] = handler;
        }
    } else if (pattern.back() == '/') {
        Subtrees_[pattern] = handler;

        auto withoutSlash = pattern.substr(0, pattern.size() - 1);
        Subtrees_[withoutSlash] = handler;
    } else {
        Exact_[pattern] = handler;
    }
}

void TRequestPathMatcher::Add(const TString& pattern, TCallback<void(const IRequestPtr&, const IResponseWriterPtr&)> handler)
{
    Add(pattern, New<TCallbackHandler>(handler));
}

IHttpHandlerPtr TRequestPathMatcher::Match(TStringBuf path)
{
    {
        auto it = Exact_.find(path);
        if (it != Exact_.end()) {
            return it->second;
        }
    }

    while (true) {
        auto it = Subtrees_.find(path);
        if (it != Subtrees_.end()) {
            return it->second;
        }

        if (path.empty()) {
            break;
        }

        path.Chop(1);
        while (!path.empty() && path.back() != '/') {
            path.Chop(1);
        }
    }

    return nullptr;
}

bool TRequestPathMatcher::IsEmpty() const
{
    return Exact_.empty() && Subtrees_.empty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
