#include "model_openai.h"
#include "model_base.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/api_utils.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>

#include <library/cpp/json/json_reader.h>

#include <util/string/builder.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TModelOpenAi final : public TModelBase {
    using TBlase = TModelBase;

    static constexpr ui64 MAX_COMPLETION_TOKENS = 1024;

public:
    explicit TModelOpenAi(const TOpenAiModelSettings& settings)
        : TBlase(CreateApiUrl(settings.BaseUrl, "/chat/completions"), settings.ApiKey)
        , Tools(ChatCompletionRequest["tools"].SetType(NJson::JSON_ARRAY).GetArraySafe())
        , Conversation(ChatCompletionRequest["messages"].SetType(NJson::JSON_ARRAY).GetArraySafe())
    {
        if (settings.ModelId) {
            ChatCompletionRequest["model"] = settings.ModelId;
        }

        if (settings.SystemPrompt) {
            auto& systemMessage = Conversation.emplace_back();
            systemMessage["role"] = "system";
            systemMessage["content"] = settings.SystemPrompt;
        }

        ChatCompletionRequest["max_completion_tokens"] = MAX_COMPLETION_TOKENS;
    }

    void RegisterTool(const TString& name, const NJson::TJsonValue& parametersSchema, const TString& description) final {
        Y_VALIDATE(ValidateToolName(name), "Invalid tool name: " << name);

        auto& tool = Tools.emplace_back();
        tool["type"] = "function";

        auto& toolInfo = tool["function"];
        toolInfo["name"] = name;
        toolInfo["parameters"] = parametersSchema;
        toolInfo["description"] = description;
    }

    void ClearContext() final {
        Conversation.clear();
    }

protected:
    void AdvanceConversation(const std::vector<TMessage>& messages) final {
        for (const auto& message : messages) {
            auto& item = Conversation.emplace_back();
            auto& content = item["content"];
            auto& role = item["role"];

            if (std::holds_alternative<TUserMessage>(message)) {
                content = std::get<TUserMessage>(message).Text;
                role = "user";
            } else {
                const auto& toolResponse = std::get<TToolResponse>(message);
                item["tool_call_id"] = toolResponse.ToolCallId;
                content = toolResponse.Text;
                role = "tool";
            }
        }
    }

    TResponse HandleModelResponse(const NJson::TJsonValue& response) final {
        TResponse result;

        TJsonParser parser(response);
        if (auto child = parser.MaybeKey("response")) {
            parser = std::move(*child);
        }

        parser = parser.GetKey("choices").GetElement(0).GetKey("message");
        auto conversationPart = parser.GetValue();

        const auto& content = parser.MaybeKey("content");
        const bool hasContent = content && !content->IsNull();
        if (hasContent) {
            result.Text = Strip(content->GetString());
        }

        const auto& tollCalls = parser.MaybeKey("tool_calls");
        const bool hasToolsCalls = tollCalls && !tollCalls->IsNull();
        if (hasToolsCalls) {
            tollCalls->Iterate([&](TJsonParser toolCall) {
                auto function = toolCall.GetKey("function");

                NJson::TJsonValue argumentsJson;
                try {
                    NJson::ReadJsonTree(function.GetKey("arguments").GetString(), &argumentsJson, /* throwOnError */ true);
                } catch (const std::exception& e) {
                    throw yexception() << "Tool call arguments is not valid JSON, reason: " << e.what();
                }

                auto name = function.GetKey("name").GetString();
                if (!ValidateToolName(name)) {
                    throw yexception() << "Invalid tool name requested: " << name;
                }

                result.ToolCalls.push_back({
                    .Id = toolCall.GetKey("id").GetString(),
                    .Name = std::move(name),
                    .Parameters = std::move(argumentsJson),
                });
            });
        }

        if (!hasContent && !hasToolsCalls) {
            throw yexception() << "Not found either content or tool_calls keys in field " << parser.GetFieldName();
        }

        Conversation.emplace_back(std::move(conversationPart));
        return result;
    }

private:
    static bool ValidateToolName(const TString& name) {
        if (name.size() > 64) {
            return false;
        }

        for (const auto c : name) {
            if (('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') || IsIn({'_', '-'}, c)) {
                continue;
            }
            return false;
        }

        return true;
    }

private:
    NJson::TJsonValue::TArray& Tools;
    NJson::TJsonValue::TArray& Conversation;
};

} // anonymous namespace

IModel::TPtr CreateOpenAiModel(const TOpenAiModelSettings& settings) {
    return std::make_shared<TModelOpenAi>(settings);
}

} // namespace NYdb::NConsoleClient::NAi
