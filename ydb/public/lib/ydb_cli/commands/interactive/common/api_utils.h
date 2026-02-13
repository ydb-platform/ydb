#pragma once

#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/public/lib/ydb_cli/common/interruptable.h>

#include <util/generic/fwd.h>
#include <util/generic/size_literals.h>

#include <thread>

namespace NYdb::NConsoleClient::NAi {

class TProgressWaiterBase {
public:
    explicit TProgressWaiterBase(TDuration granularity = TDuration::MilliSeconds(100));

    virtual ~TProgressWaiterBase();

    TDuration Stop(bool success = false);

protected:
    virtual TString PrintProgress(TDuration elapsed) = 0;

private:
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

class THttpExecutor : public TInterruptableCommand {
public:
    THttpExecutor(const TString& apiUrl, const TString& authToken);

    struct TResponse {
        TResponse() = default;

        TResponse(TString&& content, ui64 httpCode);

        bool IsSuccess() const;

        TString Content;
        ui64 HttpCode = 0;
    };

    TResponse Post(TString&& body);

    TResponse Get(size_t sizeLimit = 100_MB);

    static TString PrettifyModelApiError(ui64 httpCode, const TString& response);

private:
    NYql::IHTTPGateway::TOnResult GetHttpCallback(NThreading::TPromise<TResponse> response) const;

private:
    const TString ApiUrl;
    const NYql::THttpHeader ApiHeaders;
    const NYql::IHTTPGateway::TPtr HttpGateway;
};

TString CreateApiUrl(const TString& baseUrl, const TString& uri);

std::vector<TString> ListModelNames(const TString& apiBaseEndpoint, const TString& authToken);

} // namespace NYdb::NConsoleClient::NAi
