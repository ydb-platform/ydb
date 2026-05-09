#include "model_anthropic.h"
#include "model_base.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/api_utils.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/strbuf.h>
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
        // TODO: use cache key
        ChatCompletionRequest["stream"] = true; // TODO: setting to disable streaming is not supported
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
    bool UseStreaming() final {
        return true;
    }

    void ConsumeStreamEvent(TStringBuf eventJson, NJson::TJsonValue& assembled, IResponseProcessor& responseProcessor) final {
        if (eventJson.empty()) {
            return;
        }

        NJson::TJsonValue eventStorage;
        // TODO: errors are ignored
        if (!NJson::ReadJsonTree(eventJson, &eventStorage, /* throwOnError */ false)) {
            return;
        }
        const NJson::TJsonValue& event = eventStorage;

        const auto& typeNode = event["type"];
        // TODO: errors are ignored
        if (!typeNode.IsString()) {
            return;
        }
        const auto& type = typeNode.GetString();

        auto& content = assembled["content"];
        if (!content.IsArray()) {
            content.SetType(NJson::JSON_ARRAY);
        }
        auto& contentArray = content.GetArraySafe();

        auto resizeBlocks = [&](size_t needed) {
            while (contentArray.size() < needed) {
                contentArray.emplace_back();
            }
        };

        if (type == "content_block_start") {
            // TODO: errors are ignored
            const auto index = event["index"].GetIntegerSafe(0);
            resizeBlocks(index + 1);
            const auto& block = event["content_block"];
            const auto& blockType = block["type"].GetStringSafe();
            auto& target = contentArray[index];
            target["type"] = blockType;
            if (blockType == "text") {
                target["text"] = block["text"].GetStringSafe();
            } else if (blockType == "thinking") {
                target["thinking"] = block["thinking"].GetStringSafe();
            } else if (blockType == "tool_use") {
                target["id"] = block["id"].GetStringSafe();
                target["name"] = block["name"].GetStringSafe();
                target["input"].SetType(NJson::JSON_MAP);
                target["__partial_input"] = TString();
            }
        } else if (type == "content_block_delta") {
            // TODO: errors are ignored
            const auto index = event["index"].GetIntegerSafe(0);
            resizeBlocks(index + 1);
            auto& target = contentArray[index];
            const auto& delta = event["delta"];
            const auto& deltaType = delta["type"].GetStringSafe();
            if (deltaType == "text_delta") {
                const auto& chunk = delta["text"].GetStringSafe();
                if (!target["type"].IsString()) {
                    target["type"] = "text";
                }
                target["text"] = target["text"].GetStringSafe() + chunk;
                responseProcessor.OnTextDelta(chunk);
            } else if (deltaType == "thinking_delta") {
                // TODO: actually not shown
                const auto& chunk = delta["thinking"].GetStringSafe();
                if (!target["type"].IsString()) {
                    target["type"] = "thinking";
                }
                target["thinking"] = target["thinking"].GetStringSafe() + chunk;
                responseProcessor.OnThinkingDelta(chunk);
            } else if (deltaType == "input_json_delta") {
                const auto& chunk = delta["partial_json"].GetStringSafe();
                target["__partial_input"] = target["__partial_input"].GetStringSafe() + chunk;
            }
        } else if (type == "content_block_stop") {
            const auto index = event["index"].GetIntegerSafe(0);
            if (index < static_cast<i64>(contentArray.size())) {
                auto& target = contentArray[index];
                if (target["type"].GetStringSafe() == "tool_use") {
                    const auto& partial = target["__partial_input"].GetStringSafe();
                    NJson::TJsonValue parsed;
                    // TODO: errors are ignored
                    if (!partial.empty() && NJson::ReadJsonTree(partial, &parsed, /* throwOnError */ false)) {
                        target["input"] = std::move(parsed);
                    }
                    target.EraseValue("__partial_input");
                }
            }
        }
    }

    NJson::TJsonValue FinalizeStreamingResponse(NJson::TJsonValue&& assembled) final {
        if (!assembled.IsMap() || !assembled["content"].IsArray()) {
            NJson::TJsonValue result;
            result["content"].SetType(NJson::JSON_ARRAY);
            return result;
        }

        auto& contentArray = assembled["content"].GetArraySafe();
        NJson::TJsonValue::TArray kept;
        for (auto& block : contentArray) {
            const auto& blockType = block["type"].GetStringSafe();
            if (blockType == "thinking") {
                continue;
            }
            block.EraseValue("__partial_input");
            kept.emplace_back(std::move(block));
        }
        contentArray = std::move(kept);
        return std::move(assembled);
    }

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
        NJson::TJsonValue conversationPart;
        auto& conversationPartArray = conversationPart.SetType(NJson::JSON_ARRAY).GetArraySafe();

        parser.Iterate([&](TJsonParser item) {
            const auto& type = item.GetKey("type").GetString();
            auto& conversationItem = conversationPartArray.emplace_back();
            conversationItem["type"] = type;

            if (type == "text") {
                if (result.Text) {
                    throw yexception() << "Multiple conversation items contains text";
                }
                result.Text = Strip(item.GetKey("text").GetString());
                conversationItem["text"] = result.Text;
            } else if (type == "tool_use") {
                auto name = item.GetKey("name").GetString();
                conversationItem["name"] = name;
                if (!ValidateToolName(name)) {
                    throw yexception() << "Invalid tool name requested: " << name;
                }

                auto id = item.GetKey("id").GetString();
                conversationItem["id"] = id;
                auto input = item.GetKey("input").GetValue();
                conversationItem["input"] = input;

                result.ToolCalls.push_back({
                    .Id = std::move(id),
                    .Name = std::move(name),
                    .Parameters = std::move(input),
                });

                if (const auto& caller = item.MaybeKey("caller")) {
                    conversationItem["caller"] = caller->GetValue();
                }
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
