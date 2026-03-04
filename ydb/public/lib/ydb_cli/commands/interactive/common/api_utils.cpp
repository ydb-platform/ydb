#include "api_utils.h"
#include "json_utils.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/common/log.h>

#include <util/generic/scope.h>
#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/url/url.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

TKeepAliveHttpClient::THeaders CreateApiHeaders(const TString& authToken) {
    TKeepAliveHttpClient::THeaders headers = {{"Content-Type", "application/json"}};

    if (authToken) {
        headers.emplace("Authorization", TStringBuilder() << "Bearer " << authToken);
    }

    return headers;
}

} // anonymous namespace

TProgressWaiterBase::TProgressWaiterBase(TDuration granularity)
    : Granularity(granularity)
{
    Worker = std::thread([this]() {
        const char* frames[] = {"🌑", "🌒", "🌓", "🌔", "🌕", "🌖", "🌗", "🌘"};
        int frameIndex = 0;
        Cout << Endl;
        while (Running) {
            Cout << "\r" << frames[frameIndex] << PrintProgress(TInstant::Now() - StartTime) << Flush;

            frameIndex = (frameIndex + 1) % std::size(frames);
            Sleep(Granularity);
        }
    });
}

TProgressWaiterBase::~TProgressWaiterBase() {
    try {
        Stop(/* success */ false);
    } catch (...) {
        // ¯\_(ツ)_/¯
    }
}

TDuration TProgressWaiterBase::Success() {
    return Stop(/* success */ true);
}

TDuration TProgressWaiterBase::Fail(const TString& message) {
    auto result = Stop(/* success */ false);
    const auto& colors = NConsoleClient::AutoColors(Cout);
    Cout << ": " << colors.Red() << message << colors.OldColor() << Endl;
    return result;
}

TDuration TProgressWaiterBase::Interupted() {
    return Fail("<INTERRUPTED>");
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
        Cout << "\033[A" << Flush; // Move cursor up
        return TInstant::Now() - StartTime;
    } else {
        auto now = TInstant::Now();
        auto elapsed = (now - StartTime).SecondsFloat();
        Cout << "Error after " << Sprintf("%.2fs", elapsed) << Flush;
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
    : Endpoint(ParseApiUrl(apiUrl))
    , ApiHeaders(CreateApiHeaders(authToken))
    , HttpClient(Endpoint.Host, Endpoint.Port, TDuration::Seconds(30), TDuration::Seconds(5))
{
    YDB_CLI_LOG(Debug, "Setup http executor for API url '" << apiUrl << "'");
}

THttpExecutor::TResponse THttpExecutor::TestConnection() {
    YDB_CLI_LOG(Debug, "Check API availability");

    try {
        return ExecuteRequestAsync([&](TKeepAliveHttpClient& client, NThreading::TCancellationToken cancellationToken) {
            cancellationToken.SetDeadline(TInstant::Now() + TDuration::Seconds(5));
            client.DoRequest("HEAD", Endpoint.Uri, {}, nullptr, ApiHeaders, nullptr, cancellationToken);
            return TResponse();
        });
    } catch (...) {
        TString message = CurrentExceptionMessage();
        YDB_CLI_LOG(Notice, "Check availability failed: " << message);
        return TResponse(std::move(message), 0);
    }
}

THttpExecutor::TResponse THttpExecutor::Post(TString&& body) {
    YDB_CLI_LOG(Debug, "POST request to API, body:\n" << FormatJsonValue(body));

    return ExecuteRequestAsync([&, b = std::move(body)](TKeepAliveHttpClient& client, NThreading::TCancellationToken cancellationToken) {
        TString response;
        TStringOutput responseOutput(response);
        const auto code = client.DoPost(Endpoint.Uri, b, &responseOutput, ApiHeaders, nullptr, cancellationToken);

        YDB_CLI_LOG(Info, "API response http code: " << code);
        YDB_CLI_LOG(Debug, "API response:" << Endl << FormatJsonValue(response));
        return TResponse(std::move(response), code);
    });
}

THttpExecutor::TResponse THttpExecutor::Get() {
    YDB_CLI_LOG(Debug, "GET request to API");

    return ExecuteRequestAsync([&](TKeepAliveHttpClient& client, NThreading::TCancellationToken cancellationToken) {
        TString response;
        TStringOutput responseOutput(response);
        const auto code = client.DoGet(Endpoint.Uri, &responseOutput, ApiHeaders, nullptr, cancellationToken);

        YDB_CLI_LOG(Info, "API response http code: " << code);
        YDB_CLI_LOG(Debug, "API response:" << Endl << FormatJsonValue(response));
        return TResponse(std::move(response), code);
    });
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

THttpExecutor::TEndpoint THttpExecutor::ParseApiUrl(const TString& apiUrl) {
    TStringBuf schemeHostAndPort;
    TStringBuf uri;
    SplitUrlToHostAndPath(apiUrl, schemeHostAndPort, uri);

    TStringBuf scheme;
    TStringBuf host;
    ui16 port = 0;
    GetSchemeHostAndPort(schemeHostAndPort, scheme, host, port);

    return {
        .Host = TStringBuilder() << scheme << host,
        .Port = port,
        .Uri = TString(uri),
    };
}

THttpExecutor::TResponse THttpExecutor::ExecuteRequestAsync(std::function<TResponse(TKeepAliveHttpClient&, NThreading::TCancellationToken)> request) {
    NThreading::TCancellationTokenSource cancellationTokenSource;
    auto promise = NThreading::NewPromise<TResponse>();

    std::thread([promise, client = &HttpClient, request, t = cancellationTokenSource.Token()]() mutable {
        try {
            promise.SetValue(request(*client, t));
        } catch (...) {
            promise.SetException(CurrentExceptionMessage());
        }
    }).detach();

    Y_DEFER { ResetInterrupted(); };

    auto responseFeature = promise.GetFuture();
    if (!WaitInterruptable(responseFeature)) {
        cancellationTokenSource.Cancel();
        TResponse response;
        response.Interrupted = true;
        return response;
    }

    return responseFeature.ExtractValue();
}

TString CreateApiUrl(const TString& baseUrl, const TString& uri) {
    Y_VALIDATE(uri, "Uri should not be empty for model API");

    TStringBuf sanitizedUrl;
    TStringBuf query;
    TStringBuf fragment;
    SeparateUrlFromQueryAndFragment(baseUrl, sanitizedUrl, query, fragment);

    if (query) {
        throw yexception() << "Endpoint query part should be empty, but got: '" << query << "'";
    }
    if (fragment) {
        throw yexception() << "Endpoint fragment part should be empty, but got: '" << fragment << "'";
    }

    return TStringBuilder() << RemoveFinalSlash(sanitizedUrl) << uri;
}

std::optional<std::vector<TString>> ListModelNames(const TString& apiBaseEndpoint, const TString& authToken) {
    const auto spinner = std::make_shared<TStaticProgressWaiter>("Listing models...");
    std::vector<TString> allowedModels;

    try {
        auto response = NAi::THttpExecutor(NAi::CreateApiUrl(apiBaseEndpoint, "/models"), authToken).Get();
        if (response.Interrupted) {
            spinner->Interupted();
            return std::nullopt;
        }

        if (!response.IsSuccess()) {
            spinner->Fail(NAi::THttpExecutor::PrettifyModelApiError(response.HttpCode, response.Content));
            return allowedModels;
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

        spinner->Success();
    } catch (const std::exception& e) {
        spinner->Fail(e.what());
    }

    return allowedModels;
}

std::optional<bool> TestConnection(const TString& apiBaseEndpoint) {
    const auto spinner = std::make_shared<TStaticProgressWaiter>("Checking connection...");
    auto result = NAi::THttpExecutor(apiBaseEndpoint, "").TestConnection();

    if (result.Interrupted) {
        spinner->Interupted();
        return std::nullopt;
    }

    const bool success = result.Content.empty();
    if (success) {
        spinner->Success();
    } else {
        spinner->Fail(result.Content);
    }

    return success;
}

} // namespace NYdb::NConsoleClient::NAi
