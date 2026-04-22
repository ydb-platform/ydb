#pragma once

#include <ydb/public/lib/ydb_cli/common/interruptable.h>

#include <library/cpp/http/simple/http_client.h>

#include <util/generic/fwd.h>
#include <util/generic/size_literals.h>

#include <thread>

namespace NYdb::NConsoleClient::NAi {

class TProgressWaiterBase {
public:
    explicit TProgressWaiterBase(TDuration granularity = TDuration::MilliSeconds(100));

    virtual ~TProgressWaiterBase();

    TDuration Success();

    TDuration Fail(const TString& message);

    TDuration Interrupted();

protected:
    virtual TString PrintProgress(TDuration elapsed) = 0;

private:
    TDuration Stop(bool success = false);

    const TDuration Granularity;
    const TInstant StartTime = TInstant::Now();
    std::atomic<bool> Running = true;
    std::thread Worker;
};

class TStaticProgressWaiter final : public TProgressWaiterBase {
public:
    explicit TStaticProgressWaiter(const TString& message);

protected:
    TString PrintProgress(TDuration elapsed) final;

private:
    const TString Message;
};

// Not thread safe, supposed to be called at most once in parallel inside CLI process.
class THttpExecutor : public TInterruptableCommand {
    struct TContext {
        TString Host;
        ui32 Port;
        TString Uri;
        TKeepAliveHttpClient::THeaders ApiHeaders;

        TContext(const TString& host, ui32 port, const TString& uri, const TKeepAliveHttpClient::THeaders& apiHeaders);
    };

public:
    THttpExecutor(const TString& apiUrl, const TString& authToken, TDuration timeout = TDuration::Hours(12));

    struct TResponse {
        TResponse() = default;

        TResponse(TString&& content, ui64 httpCode);

        bool IsSuccess() const;

        TString Content;
        ui64 HttpCode = 0;
        bool Interrupted = false;
    };

    TResponse TestConnection();

    TResponse Post(TString&& body);

    TResponse Get();

    static TString PrettifyModelApiError(ui64 httpCode, const TString& response);

private:
    static std::shared_ptr<TContext> CreateContext(const TString& apiUrl, const TString& authToken);

    static std::shared_ptr<TKeepAliveHttpClient> CreateHttpClient(const TContext& context, TDuration timeout);

    TResponse ExecuteRequestAsync(std::function<TResponse(std::shared_ptr<TKeepAliveHttpClient>, NThreading::TCancellationToken)>);

    const std::shared_ptr<TContext> Context;
    const TDuration Timeout;
    std::shared_ptr<TKeepAliveHttpClient> HttpClient;
};

TString CreateApiUrl(const TString& baseUrl, const TString& uri);

// Returns nullopt if interrupted

std::optional<std::vector<TString>> ListModelNames(const TString& apiBaseEndpoint, const TString& authToken);

std::optional<bool> TestConnection(const TString& apiBaseEndpoint);

} // namespace NYdb::NConsoleClient::NAi
