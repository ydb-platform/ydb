#include "api_utils.h"
#include "json_utils.h"

#include <ydb/public/lib/ydb_cli/common/log.h>

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <util/generic/scope.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

#include <library/cpp/json/json_reader.h>
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

TProgressWaiterBase::TProgressWaiterBase(TDuration granularity)
    : Granularity(granularity)
{
    Worker = std::thread([this]() {
        const char* frames[] = {"🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"};
        int frameIndex = 0;
        while (Running) {
            Cout << "\r" << frames[frameIndex] << PrintProgress(TInstant::Now() - StartTime) << Flush;

            frameIndex = (frameIndex + 1) % std::size(frames);
            Sleep(Granularity);
        }
    });
}

TProgressWaiterBase::~TProgressWaiterBase() {
    try {
        Stop(false);
    } catch (...) {
        // ¯\_(ツ)_/¯
    }
}

TDuration TProgressWaiterBase::Stop(bool success) {
    bool expected = true;
    if (!Running.compare_exchange_strong(expected, false)) {
        return TDuration::Zero();
    }

    if (Worker.joinable()) {
        Worker.join();
    }

    Cout << "\r\x1b[K" << Flush; // Clear line
    Cerr << Flush;

    if (success) {
        return TInstant::Now() - StartTime;
    } else {
        auto now = TInstant::Now();
        auto elapsed = (now - StartTime).SecondsFloat();
        Cout << "Error after " << Sprintf("%.2fs", elapsed) << ": " << Flush;
    }

    return TDuration::Zero();
}

TStaticProgressWaiter::TStaticProgressWaiter(const TString& message)
    : Message(message)
{}

TString TStaticProgressWaiter::PrintProgress(TDuration elapsed) {
    return TStringBuilder() << " " << Message << " " << Sprintf("%.1fs", elapsed.SecondsFloat());
}

THttpExecutor::TResponse::TResponse(TString&& content, ui64 httpCode)
    : Content(std::move(content))
    , HttpCode(httpCode)
{}

bool THttpExecutor::TResponse::IsSuccess() const {
    return HttpCode >= 200 && HttpCode < 300;
}

THttpExecutor::THttpExecutor(const TString& apiUrl, const TString& authToken)
    : ApiUrl(apiUrl)
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

        if (const auto& response = parser.MaybeKey("message")) {
            if (const auto& info = parser.MaybeKey("error")) {
                return error << info->ToString();
            }
            return error << response->ToString();
        }
    }

    auto error = TStringBuilder() << "Request to model API failed with code: " << httpCode;
    if (response) {
        error << ". Response:\n" << FormatJsonValue(response);
    }

    return error;
}

NYql::IHTTPGateway::TOnResult THttpExecutor::GetHttpCallback(NThreading::TPromise<TResponse> response) const {
    return [response](NYql::IHTTPGateway::TResult result) mutable -> void {
        const auto curlCode = result.CurlResponseCode;
        if (curlCode == CURLE_OK) {
            if (result.Issues) {
                GetGlobalLogger().Warning() << "API response has issues:\n" << result.Issues.ToString();
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

std::vector<TString> ListModelNames(const TString& apiBaseEndpoint, const TString& authToken) {
    THttpExecutor::TResponse response;
    {
        const auto spinner = std::make_shared<TStaticProgressWaiter>("Listing models...");
        response = NAi::THttpExecutor(NAi::CreateApiUrl(apiBaseEndpoint, "/models"), authToken).Get();
    }

    if (!response.IsSuccess()) {
        throw yexception() << NAi::THttpExecutor::PrettifyModelApiError(response.HttpCode, response.Content);
    }

    NJson::TJsonValue responseJson;
    try {
        NJson::ReadJsonTree(response.Content, &responseJson, /* throwOnError */ true);
    } catch (const std::exception& e) {
        throw yexception() << "Model API response is not valid JSON, reason: " << e.what();
    }

    NAi::TJsonParser parser(responseJson);
    if (auto child = parser.MaybeKey("response")) {
        parser = std::move(*child);
    }

    std::vector<TString> allowedModels;
    parser.GetKey("data").Iterate([&](NAi::TJsonParser item) {
        if (const auto id = item.MaybeKey("id")) {
            allowedModels.emplace_back(id->GetString());
        }
    });

    return allowedModels;
}

} // namespace NYdb::NConsoleClient::NAi
