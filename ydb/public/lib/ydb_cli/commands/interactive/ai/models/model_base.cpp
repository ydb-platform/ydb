#include "model_base.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/common/print_utils.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/string_utils/url/url.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/string/builder.h>

namespace NYdb::NConsoleClient::NAi {

TModelBase::TModelBase(const TString& apiUrl, const TString& authToken)
    : HttpExecutor(apiUrl, authToken)
{
    Y_VALIDATE(apiUrl, "Url should not be empty for model API");
    YDB_CLI_LOG(Notice, "Using model API url: \"" << apiUrl << "\" with " << (authToken ? TStringBuilder() << "auth token " << BlurSecret(authToken) : TStringBuilder() << "anonymous access"));
}

TModelBase::TResponse TModelBase::HandleMessages(const std::vector<TMessage>& messages, std::function<void()> onStartWaiting, std::function<void()> onFinishWaiting) {
    Y_VALIDATE(!messages.empty(), "Messages should not be empty for advance conversation");
    AdvanceConversation(messages);

    NJsonWriter::TBuf requestJsonWriter;
    requestJsonWriter.WriteJsonValue(&ChatCompletionRequest);
    auto request = requestJsonWriter.Str();

    if (onStartWaiting) {
        onStartWaiting();
    }

    auto response = [&]() {
        try {
            auto res = HttpExecutor.Post(std::move(request));
            if (onFinishWaiting) {
                onFinishWaiting();
            }
            return res;
        } catch (...) {
            if (onFinishWaiting) {
                onFinishWaiting();
            }
            throw;
        }
    }();

    if (!response.IsSuccess()) {
        throw yexception() << THttpExecutor::PrettifyModelApiError(response.HttpCode, response.Content);
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

void TModelBase::AddMessages(const std::vector<TMessage>& messages) {
    AdvanceConversation(messages);
}

} // namespace NYdb::NConsoleClient::NAi
