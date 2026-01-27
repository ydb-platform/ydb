#include "model_anthropic.h"
#include "model_base.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/api_utils.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>

#include <library/cpp/json/json_reader.h>

#include <util/string/builder.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TModelAnthropic final : public TModelBase {
    using TBlase = TModelBase;

    static constexpr ui64 MAX_COMPLETION_TOKENS = 1024;

public:
    explicit TModelAnthropic(const TAnthropicModelSettings& settings)
        : TBlase(CreateApiUrl(settings.BaseUrl, "/messages"), settings.ApiKey)
        , Tools(ChatCompletionRequest["tools"].SetType(NJson::JSON_ARRAY).GetArraySafe())
        , Conversation(ChatCompletionRequest["messages"].SetType(NJson::JSON_ARRAY).GetArraySafe())
    {
        if (settings.ModelId) {
            ChatCompletionRequest["model"] = settings.ModelId;
        }

        if (settings.SystemPrompt) {
            ChatCompletionRequest["system"] = settings.SystemPrompt;
        }

        ChatCompletionRequest["max_tokens"] = MAX_COMPLETION_TOKENS;
    }

    void RegisterTool(const TString& name, const NJson::TJsonValue& parametersSchema, const TString& description) final {
        Y_VALIDATE(ValidateToolName(name), "Invalid tool name: " << name);

        auto& tool = Tools.emplace_back();
        tool["name"] = name;
        tool["input_schema"] = parametersSchema;
        tool["description"] = description;
    }

    void ClearContext() final {
        Conversation.clear();
    }

protected:
    void AdvanceConversation(const std::vector<TMessage>& messages) final {
        auto& conversationItem = Conversation.emplace_back();
        conversationItem["role"] = "user";

        auto& content = conversationItem["content"].SetType(NJson::JSON_ARRAY).GetArraySafe();
        for (const auto& message : messages) {
            auto& item = content.emplace_back();
            auto& type = item["type"];

            if (std::holds_alternative<TUserMessage>(message)) {
                item["text"] = std::get<TUserMessage>(message).Text;
                type = "text";
            } else {
                const auto& toolResponse = std::get<TToolResponse>(message);
                item["content"] = toolResponse.Text;
                item["tool_use_id"] = toolResponse.ToolCallId;
                item["is_error"] = !toolResponse.IsSuccess;
                type = "tool_result";
            }
        }
    }

    TResponse HandleModelResponse(const NJson::TJsonValue& response) final {
        TResponse result;

        TJsonParser parser(response);
        if (auto child = parser.MaybeKey("response")) {
            parser = std::move(*child);
        }

        parser = parser.GetKey("content");
        auto conversationPart = parser.GetValue();

        parser.Iterate([&](TJsonParser item) {
            const auto& type = item.GetKey("type").GetString();
            if (type == "text") {
                if (result.Text) {
                    throw yexception() << "Multiple conversation items contains text";
                }
                result.Text = Strip(item.GetKey("text").GetString());
            } else if (type == "tool_use") {
                auto name = item.GetKey("name").GetString();
                if (!ValidateToolName(name)) {
                    throw yexception() << "Invalid tool name requested: " << name;
                }

                result.ToolCalls.push_back({
                    .Id = item.GetKey("id").GetString(),
                    .Name = std::move(name),
                    .Parameters = item.GetKey("input").GetValue(),
                });
            } else {
                throw yexception() << "Unknown conversation item type: " << type << ", expected text or tool_use";
            }
        });

        auto& conversationItem = Conversation.emplace_back();
        conversationItem["role"] = "assistant";
        conversationItem["content"] = std::move(conversationPart);
        return result;
    }

private:
    static bool ValidateToolName(const TString& name) {
        return 1 <= name.size() && name.size() <= 128;
    }

private:
    NJson::TJsonValue::TArray& Tools;
    NJson::TJsonValue::TArray& Conversation;
};

} // anonymous namespace

IModel::TPtr CreateAnthropicModel(const TAnthropicModelSettings& settings) {
    return std::make_shared<TModelAnthropic>(settings);
}

} // namespace NYdb::NConsoleClient::NAi
