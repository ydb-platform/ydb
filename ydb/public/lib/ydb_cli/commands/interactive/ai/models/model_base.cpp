#include "model_base.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/string_utils/url/url.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/generic/guid.h>
#include <util/string/builder.h>

namespace NYdb::NConsoleClient::NAi {

TModelBase::TModelBase(const TString& apiUrl, const TString& authToken)
    : CacheKey(TStringBuilder() << "cli_" << CreateGuidAsString())
    , HttpExecutor(apiUrl, authToken)
{
    Y_VALIDATE(apiUrl, "Url should not be empty for model API");
    YDB_CLI_LOG(Notice, "Using model API url: \"" << apiUrl << "\" with " << (authToken ? TStringBuilder() << "auth token " << BlurSecret(authToken) : TStringBuilder() << "anonymous access"));
}

TModelBase::TResponse TModelBase::HandleMessages(const std::vector<TMessage>& messages, IResponseProcessor& responseProcessor) {
    Y_VALIDATE(!messages.empty(), "Messages should not be empty for advance conversation");
    AdvanceConversation(messages);

    NJsonWriter::TBuf requestJsonWriter;
    requestJsonWriter.WriteJsonValue(&ChatCompletionRequest);
    auto request = requestJsonWriter.Str();

    NJson::TJsonValue responseJson;
    const bool useStreaming = UseStreaming();
    if (useStreaming) {
        // TODO: fallback to non-streaming if streaming is not supported
        responseJson = PerformStreamingRequest(std::move(request), responseProcessor);
    } else {
        responseJson = PerformNonStreamingRequest(std::move(request));
    }

    try {
        auto response = HandleModelResponse(responseJson);
        if (!useStreaming) {
            responseProcessor.OnTextDelta(response.Text);
        }
        return response;
    } catch (const std::exception& e) {
        throw yexception() << "Processing model response error. " << e.what();
    }
}

NJson::TJsonValue TModelBase::PerformNonStreamingRequest(TString&& request) {
    auto response = [&]() {
        try {
            auto res = HttpExecutor.Post(std::move(request));
            return res;
        } catch (...) {
            throw yexception() << "HTTP request to model API failed, reason:\n" << CurrentExceptionMessage();
        }
    }();

    if (response.Interrupted) {
        throw yexception() << "Request to model API was interrupted";
    }

    if (!response.IsSuccess()) {
        throw yexception() << THttpExecutor::PrettifyModelApiError(response.HttpCode, response.Content);
    }

    try {
        NJson::TJsonValue responseJson;
        NJson::ReadJsonTree(response.Content, &responseJson, /* throwOnError */ true);
        return responseJson;
    } catch (const std::exception& e) {
        throw yexception() << "Model API response is not valid JSON, reason: " << e.what();
    }
}

NJson::TJsonValue TModelBase::PerformStreamingRequest(TString&& request, IResponseProcessor& responseProcessor) {
    NJson::TJsonValue assembled;

    auto response = [&]() {
        try {
            auto res = HttpExecutor.Post(std::move(request), [&](TStringBuf eventJson) {
                try {
                    ConsumeStreamEvent(eventJson, assembled, responseProcessor);
                } catch (const std::exception& e) {
                    // TODO: errors are ignored
                    YDB_CLI_LOG(Notice, "Failed to process streaming event, ignoring. Reason: " << e.what() << ". Event: " << eventJson);
                }
            });
            return res;
        } catch (...) {
            throw yexception() << "HTTP streaming request to model API failed, reason:\n" << CurrentExceptionMessage();
        }
    }();

    if (response.Interrupted) {
        // TODO: interrupt is not working
        throw yexception() << "Request to model API was interrupted";
    }

    if (!response.IsSuccess()) {
        // TODO: broken error printing
        throw yexception() << THttpExecutor::PrettifyModelApiError(response.HttpCode, response.Content);
    }

    try {
        return FinalizeStreamingResponse(std::move(assembled));
    } catch (const std::exception& e) {
        throw yexception() << "Processing streaming response error. " << e.what();
    }
}

void TModelBase::AddMessages(const std::vector<TMessage>& messages) {
    AdvanceConversation(messages);
}

} // namespace NYdb::NConsoleClient::NAi
