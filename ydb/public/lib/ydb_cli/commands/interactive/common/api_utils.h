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

    TDuration Interupted();

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

// Not thread safe, spoused to be called at most once in parallel inside CLI process.
class THttpExecutor : public TInterruptableCommand {
    struct TEndpoint {
        TString Host;
        ui32 Port;
        TString Uri;
    };

public:
    THttpExecutor(const TString& apiUrl, const TString& authToken);

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
    static TEndpoint ParseApiUrl(const TString& apiUrl);

    TResponse ExecuteRequestAsync(std::function<TResponse(TKeepAliveHttpClient&, NThreading::TCancellationToken)>);

    const TEndpoint Endpoint;
    const TKeepAliveHttpClient::THeaders ApiHeaders;
    TKeepAliveHttpClient HttpClient;
};

TString CreateApiUrl(const TString& baseUrl, const TString& uri);

// Returns nullopt if interrupted

std::optional<std::vector<TString>> ListModelNames(const TString& apiBaseEndpoint, const TString& authToken);

std::optional<bool> TestConnection(const TString& apiBaseEndpoint);

} // namespace NYdb::NConsoleClient::NAi
