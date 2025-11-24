#include "model_base.h"

#include <ydb/core/base/validation.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_ai/common/json_utils.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/string_utils/url/url.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/string/builder.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

NYql::THttpHeader CreateApiHeaders(const std::optional<TString>& authToken) {
    TSmallVec<TString> headers = {"Content-Type: application/json"};

    if (authToken) {
        headers.emplace_back(TStringBuilder() << "Authorization: Bearer " << *authToken);
    }

    return {.Fields = std::move(headers)};
}

struct THttpResponse {
    THttpResponse(TString&& content, ui64 httpCode)
        : Content(std::move(content))
        , HttpCode(httpCode)
    {}

    bool IsSuccess() const {
        return HttpCode >= 200 && HttpCode < 300;
    }

    TString Content;
    ui64 HttpCode = 0;
};

} // anonymous namespace

TModelBase::TModelBase(const TString& apiUrl, const std::optional<TString>& authToken, const TClientCommand::TConfig& config)
    : Verbosity(config.VerbosityLevel)
    , ApiUrl(apiUrl)
    , ApiHeaders(CreateApiHeaders(authToken))
    , HttpGateway(NYql::IHTTPGateway::Make())
{
    Y_DEBUG_VERIFY(apiUrl, "Internal error. Url should not be empty for model API");

    if (Verbosity >= VERB_INFO) {
        Cerr << "Using model API url: " << apiUrl << " with "
            << (authToken ? TStringBuilder() << "auth token " << TString(authToken->size(), '*') : TStringBuilder() << "anonymous access") << Endl;
    }
}

TModelBase::TResponse TModelBase::HandleMessages(const std::vector<TMessage>& messages) {
    Y_DEBUG_VERIFY(!messages.empty(), "Internal error. Messages should not be empty for advance conversation");

    AdvanceConversation(messages);

    if (Verbosity >= VERB_TRACE) {
        Cerr << "Request to model API:\n" << FormatJsonValue(ChatCompletionRequest) << Endl;
    }

    NJsonWriter::TBuf requestJsonWriter;
    requestJsonWriter.WriteJsonValue(&ChatCompletionRequest);
    auto request = requestJsonWriter.Str();

    auto responsePromise = NThreading::NewPromise<THttpResponse>();
    auto httpCallback = [&responsePromise](NYql::IHTTPGateway::TResult result) -> void {
        const auto curlCode = result.CurlResponseCode;
        if (curlCode == CURLE_OK) {
            auto& content = result.Content;
            responsePromise.SetValue(THttpResponse(content.Extract(), content.HttpResponseCode));
            return;
        }

        auto error = TStringBuilder() << "Failed to connect to API server or process response, internal code: " << static_cast<ui64>(curlCode);
        if (result.Issues) {
            error << ". Reason:\n" << result.Issues.ToString();
        }
        responsePromise.SetException(error);
    };

    HttpGateway->Upload(ApiUrl, ApiHeaders, std::move(request), std::move(httpCallback));
    const auto response = responsePromise.GetFuture().ExtractValueSync();

    if (Verbosity >= VERB_TRACE) {
        Cerr << "Model API response http code: " << response.HttpCode;
        if (response.Content) {
            Cerr << ". Response data:\n" << FormatJsonValue(response.Content);
        }
        Cerr << Endl;
    }

    if (!response.IsSuccess()) {
        throw yexception() << HandleErrorResponse(response.HttpCode, response.Content);
    }

    NJson::TJsonValue responseJson;
    try {
        NJson::ReadJsonTree(response.Content, &responseJson, /* throwOnError */ true);
    } catch (const std::exception& e) {
        throw yexception() << "Model API response is not valid JSON, reason: " << e.what();
    }

    try {
        return HandleModelResponse(responseJson);
    } catch (const std::exception& e) {
        throw yexception() << "Processing model response error. " << e.what();
    }
}

TString TModelBase::HandleErrorResponse(ui64 httpCode, const TString& response) {
    auto error = TStringBuilder() << "Request to model API failed with code: " << httpCode;
    if (response) {
        error << ". Response:\n" << FormatJsonValue(response);
    }
    return error;
}

TString TModelBase::CreateApiUrl(const TString& baseUrl, const TString& uri) {
    Y_DEBUG_VERIFY(uri, "Internal error. Uri should not be empty for model API");

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
