#pragma once

#include "interactive_log.h"

#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>
#include <ydb/public/lib/ydb_cli/common/interruptable.h>

#include <util/generic/fwd.h>
#include <util/generic/size_literals.h>

namespace NYdb::NConsoleClient::NAi {

class THttpExecutor : public TInterruptableCommand {
public:
    THttpExecutor(const TString& apiUrl, const TString& authToken, const TInteractiveLogger& log);

    struct TResponse {
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
    const TInteractiveLogger Log;
    const TString ApiUrl;
    const NYql::THttpHeader ApiHeaders;
    const NYql::IHTTPGateway::TPtr HttpGateway;
};

TString CreateApiUrl(const TString& baseUrl, const TString& uri);

} // namespace NYdb::NConsoleClient::NAi
