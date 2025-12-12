#include "api_utils.h"

#include "interactive_log_defs.h"
#include "json_utils.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/generic/scope.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <library/cpp/string_utils/url/url.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

NYql::THttpHeader CreateApiHeaders(const TString& authToken) {
    TSmallVec<TString> headers = {"Content-Type: application/json"};

    if (authToken) {
        headers.emplace_back(TStringBuilder() << "Authorization: Bearer " << authToken);
    }

    return {.Fields = std::move(headers)};
}

} // anonymous namespace

THttpExecutor::TResponse::TResponse(TString&& content, ui64 httpCode)
    : Content(std::move(content))
    , HttpCode(httpCode)
{}

bool THttpExecutor::TResponse::IsSuccess() const {
    return HttpCode >= 200 && HttpCode < 300;
}

THttpExecutor::THttpExecutor(const TString& apiUrl, const TString& authToken, const TInteractiveLogger& log)
    : Log(log)
    , ApiUrl(apiUrl)
    , ApiHeaders(CreateApiHeaders(authToken))
    , HttpGateway(NYql::IHTTPGateway::Make())
{}

THttpExecutor::TResponse THttpExecutor::Post(TString&& body) {
    Y_DEFER { ResetInterrupted(); };
    YDB_CLI_LOG(Debug, "POST request to API '" << ApiUrl << "', body:\n" << FormatJsonValue(body));

    auto responsePromise = NThreading::NewPromise<TResponse>();
    HttpGateway->Upload(ApiUrl, ApiHeaders, std::move(body), GetHttpCallback(responsePromise));

    auto responseFeature = responsePromise.GetFuture();
    if (!WaitInterruptable(responseFeature)) {
        throw yexception() << "API request interrupted";
    }

    auto response = responseFeature.ExtractValue();
    YDB_CLI_LOG(Info, "API response http code: " << response.HttpCode);
    YDB_CLI_LOG(Debug, "API response:" << Endl << FormatJsonValue(response.Content));
    return response;
}

THttpExecutor::TResponse THttpExecutor::Get(size_t sizeLimit) {
    Y_DEFER { ResetInterrupted(); };
    YDB_CLI_LOG(Debug, "GET request to API '" << ApiUrl << "'");

    auto responsePromise = NThreading::NewPromise<TResponse>();
    HttpGateway->Download(ApiUrl, ApiHeaders, 0, sizeLimit, GetHttpCallback(responsePromise));

    auto responseFeature = responsePromise.GetFuture();
    if (!WaitInterruptable(responseFeature)) {
        throw yexception() << "API request interrupted";
    }

    auto response = responseFeature.ExtractValue();
    YDB_CLI_LOG(Info, "API response http code: " << response.HttpCode);
    YDB_CLI_LOG(Debug, "API response:" << Endl << FormatJsonValue(response.Content));
    return response;
}

TString THttpExecutor::PrettifyModelApiError(ui64 httpCode, const TString& response) {
    TJsonParser parser;
    if (parser.Parse(response)) {
        auto error = TStringBuilder() << "Request to model API failed:\n";

        if (const auto& info = parser.MaybeKey("raw_response")) {
            return error << info->ToString();
        }

        if (const auto& info = parser.MaybeKey("error")) {
            return error << info->ToString();
        }

        if (const auto& response = parser.MaybeKey("response")) {
            if (const auto& info = parser.MaybeKey("error")) {
                return error << info->ToString();
            }
            return error << response->ToString();
        }

        if (const auto& info = parser.MaybeKey("message")) {
            return error << info->ToString();
        }
    }

    auto error = TStringBuilder() << "Request to model API failed with code: " << httpCode;
    if (response) {
        error << ". Response:\n" << FormatJsonValue(response);
    }

    return error;
}

NYql::IHTTPGateway::TOnResult THttpExecutor::GetHttpCallback(NThreading::TPromise<TResponse> response) const {
    return [response, Log = Log](NYql::IHTTPGateway::TResult result) mutable -> void {
        const auto curlCode = result.CurlResponseCode;
        if (curlCode == CURLE_OK) {
            if (result.Issues) {
                YDB_CLI_LOG(Warning, "API response has issues:\n" << result.Issues.ToString());
            }

            auto& content = result.Content;
            response.SetValue(TResponse(content.Extract(), content.HttpResponseCode));
            return;
        }

        auto error = TStringBuilder() << "Failed to connect to API server or process response, internal code: " << static_cast<ui64>(curlCode);
        if (result.Issues) {
            error << ". Reason:\n" << result.Issues.ToString();
        }
        response.SetException(error);
    };
}

TString CreateApiUrl(const TString& baseUrl, const TString& uri) {
    Y_VALIDATE(uri, "Uri should not be empty for model API");

    TStringBuf sanitizedUrl;
    TStringBuf query;
    TStringBuf fragment;
    SeparateUrlFromQueryAndFragment(baseUrl, sanitizedUrl, query, fragment);

    if (query || fragment) {
        auto error = yexception() << "Invalid model API base url: '" << baseUrl << "'";
        if (query) {
            error << ". Query part should be empty, but got: '" << query << "'";
        }
        if (fragment) {
            error << ". Fragment part should be empty, but got: '" << fragment << "'";
        }
        throw error;
    }

    return TStringBuilder() << RemoveFinalSlash(sanitizedUrl) << uri;
}

} // namespace NYdb::NConsoleClient::NAi
