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

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NHttp {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NNet;

static constexpr auto& Logger = HttpLogger;

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
        const TServerConfigPtr& config,
        const IListenerPtr& listener,
        const IPollerPtr& poller,
        const IPollerPtr& acceptor,
        const IInvokerPtr& invoker,
        const IRequestPathMatcherPtr& requestPathMatcher,
        bool ownPoller = false)
        : Config_(config)
        , Listener_(listener)
        , Poller_(poller)
        , Acceptor_(acceptor)
        , Invoker_(invoker)
        , RequestPathMatcher_(requestPathMatcher)
        , OwnPoller_(ownPoller)
    { }

    void AddHandler(const TString& path, const IHttpHandlerPtr& handler) override
    {
        YT_VERIFY(!Started_);
        RequestPathMatcher_->Add(path, handler);
    }

    const TNetworkAddress& GetAddress() const override
    {
        return Listener_->GetAddress();
    }

    void Start() override
    {
        YT_VERIFY(!Started_);
        Started_ = true;

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
    const TServerConfigPtr Config_;
    const IListenerPtr Listener_;
    const IPollerPtr Poller_;
    const IPollerPtr Acceptor_;
    const IInvokerPtr Invoker_;
    IRequestPathMatcherPtr RequestPathMatcher_;
    const bool OwnPoller_ = false;

    bool Started_ = false;
    std::atomic<bool> Stopped_ = false;

    std::atomic<int> ActiveConnections_ = 0;
    TGauge ConnectionsActive_ = HttpProfiler.Gauge("/connections_active");
    TCounter ConnectionsAccepted_ = HttpProfiler.Counter("/connections_accepted");
    TCounter ConnectionsDropped_ = HttpProfiler.Counter("/connections_dropped");

    void AsyncAcceptConnection()
    {
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
            ConnectionsDropped_.Increment();
            ActiveConnections_--;
            YT_LOG_WARNING("Server is over max active connection limit (RemoteAddress: %v)",
                connection->RemoteAddress());
            return;
        }
        ConnectionsActive_.Update(count);
        ConnectionsAccepted_.Increment();

        auto connectionId = TGuid::Create();
        YT_LOG_DEBUG("Connection accepted (ConnectionId: %v, RemoteAddress: %v, LocalAddress: %v)",
            connectionId,
            connection->RemoteAddress(),
            connection->LocalAddress());

        Invoker_->Invoke(
            BIND(&TServer::HandleConnection, MakeStrong(this), std::move(connection), connectionId));
    }

    bool HandleRequest(const THttpInputPtr& request, const THttpOutputPtr& response)
    {
        response->SetStatus(EStatusCode::InternalServerError);

        bool closeResponse = true;
        try {
            if (!request->ReceiveHeaders()) {
                return false;
            }

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

    void HandleConnection(const IConnectionPtr& connection, TGuid connectionId)
    {
        try {
            connection->SubscribePeerDisconnect(BIND([config = Config_, canceler = GetCurrentFiberCanceler(), connectionId = connectionId] {
                YT_LOG_DEBUG("Client closed TCP socket (ConnectionId: %v)", connectionId);

                if (config->CancelFiberOnConnectionClose.value_or(false)) {
                    canceler(TError("Client closed TCP socket; HTTP connection closed"));
                }
            }));

            auto finally = Finally([&] {
                auto count = ActiveConnections_.fetch_sub(1) - 1;
                ConnectionsActive_.Update(count);
            });

            if (Config_->NoDelay) {
                connection->SetNoDelay();
            }

            DoHandleConnection(connection, connectionId);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Unhandled exception (ConnectionId: %v)", connectionId);
        }
    }

    void DoHandleConnection(const IConnectionPtr& connection, TGuid connectionId)
    {
        auto request = New<THttpInput>(
            connection,
            connection->RemoteAddress(),
            GetCurrentInvoker(),
            EMessageType::Request,
            Config_);

        if (Config_->IsHttps) {
            request->SetHttps();
        }

        request->SetPort(Config_->Port);

        auto response = New<THttpOutput>(
            connection,
            EMessageType::Response,
            Config_);

        request->SetConnectionId(connectionId);
        response->SetConnectionId(connectionId);

        while (true) {
            auto requestId = TGuid::Create();
            request->SetRequestId(requestId);
            response->SetRequestId(requestId);

            bool ok = HandleRequest(request, response);
            if (!ok) {
                break;
            }

            auto logDrop = [&] (auto reason) {
                YT_LOG_DEBUG("Dropping HTTP connection (ConnectionId: %v, Reason: %v)",
                    connectionId,
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
                connectionId);
        } else {
            YT_LOG_DEBUG(connectionResult, "Error closing HTTP connection (ConnectionId: %v)",
                connectionId);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const IListenerPtr& listener,
    const IPollerPtr& poller,
    const IPollerPtr& acceptor,
    const IInvokerPtr& invoker,
    bool ownPoller)
{
    auto handlers = New<TRequestPathMatcher>();
    return New<TServer>(
        config,
        listener,
        poller,
        acceptor,
        invoker,
        handlers,
        ownPoller);
}

IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const IPollerPtr& poller,
    const IPollerPtr& acceptor,
    const IInvokerPtr& invoker,
    bool ownPoller)
{
    auto address = TNetworkAddress::CreateIPv6Any(config->Port);
    for (int i = 0;; ++i) {
        try {
            auto listener = CreateListener(address, poller, acceptor, config->MaxBacklogSize);
            return CreateServer(config, listener, poller, acceptor, invoker, ownPoller);
        } catch (const std::exception& ex) {
            if (i + 1 == config->BindRetryCount) {
                throw;
            } else {
                YT_LOG_ERROR(ex, "HTTP server bind failed");
                Sleep(config->BindRetryBackoff);
            }
        }
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const IListenerPtr& listener,
    const IPollerPtr& poller)
{
    return CreateServer(
        config,
        listener,
        poller,
        poller,
        poller->GetInvoker(),
        /*ownPoller*/ false);
}

IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const IListenerPtr& listener,
    const IPollerPtr& poller,
    const IPollerPtr& acceptor)
{
    return CreateServer(
        config,
        listener,
        poller,
        acceptor,
        poller->GetInvoker(),
        /*ownPoller*/ false);
}

IServerPtr CreateServer(const TServerConfigPtr& config, const IPollerPtr& poller, const IPollerPtr& acceptor)
{
    return CreateServer(
        config,
        poller,
        acceptor,
        poller->GetInvoker(),
        /*ownPoller*/ false);
}

IServerPtr CreateServer(const TServerConfigPtr& config, const IPollerPtr& poller)
{
    return CreateServer(
        config,
        poller,
        poller);
}

IServerPtr CreateServer(int port, const IPollerPtr& poller)
{
    auto config = New<TServerConfig>();
    config->Port = port;
    return CreateServer(config, poller);
}

IServerPtr CreateServer(const TServerConfigPtr& config, int pollerThreadCount)
{
    auto poller = CreateThreadPoolPoller(pollerThreadCount, config->ServerName);
    return CreateServer(
        config,
        poller,
        poller,
        poller->GetInvoker(),
        /*ownPoller*/ true);
}

IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const NConcurrency::IPollerPtr& poller,
    const IInvokerPtr& invoker)
{
    return CreateServer(
        config,
        poller,
        poller,
        invoker,
        /*ownPoller*/ false);
}

////////////////////////////////////////////////////////////////////////////////

void TRequestPathMatcher::Add(const TString& pattern, const IHttpHandlerPtr& handler)
{
    if (pattern.empty()) {
        THROW_ERROR_EXCEPTION("Empty pattern is invalid");
    }

    if (pattern.back() == '/') {
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
