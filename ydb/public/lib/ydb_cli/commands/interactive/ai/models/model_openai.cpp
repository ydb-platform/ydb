#include "model_openai.h"
#include "model_base.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/api_utils.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/common/log.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/strbuf.h>
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
        ChatCompletionRequest["parallel_tool_calls"] = true;
        ChatCompletionRequest["prompt_cache_key"] = CacheKey;
        ChatCompletionRequest["stream"] = true; // TODO: setting to disable streaming is not supported
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
    bool UseStreaming() final {
        return true;
    }

    void ConsumeStreamEvent(TStringBuf eventJson, NJson::TJsonValue& assembled, IResponseProcessor& responseProcessor) final {
        if (eventJson.empty() || eventJson == TStringBuf("[DONE]")) {
            return;
        }

        NJson::TJsonValue eventStorage;
        // TODO: errors are ignored
        if (!NJson::ReadJsonTree(eventJson, &eventStorage, /* throwOnError */ false)) {
            return;
        }
        const NJson::TJsonValue& event = eventStorage;

        auto& message = assembled["choices"][0]["message"];
        if (!message["role"].IsString()) {
            message["role"] = "assistant";
        }

        const auto& choices = event["choices"];
        // TODO: errors are ignored
        if (!choices.IsArray() || choices.GetArraySafe().empty()) {
            return;
        }
        const auto& choice = choices.GetArraySafe()[0];
        const auto& delta = choice["delta"];

        if (const auto& contentNode = delta["content"]; contentNode.IsString()) {
            if (const auto& chunk = contentNode.GetString()) {
                if (!message["content"].IsString()) {
                    message["content"] = TString();
                }
                message["content"] = message["content"].GetStringSafe() + chunk;
                responseProcessor.OnTextDelta(chunk);
            }
        }

        // TODO: not working
        if (const auto& reasoningNode = delta["reasoning"]; reasoningNode.IsString()) {
            if (const auto& chunk = reasoningNode.GetString()) {
                responseProcessor.OnThinkingDelta(chunk);
            }
        }

        if (const auto& toolCalls = delta["tool_calls"]; toolCalls.IsArray()) {
            auto& assembledToolCalls = message["tool_calls"];
            if (!assembledToolCalls.IsArray()) {
                assembledToolCalls.SetType(NJson::JSON_ARRAY);
            }
            auto& toolCallsArray = assembledToolCalls.GetArraySafe();

            for (const auto& tc : toolCalls.GetArraySafe()) {
                // TODO: errors are ignored
                const auto idx = tc["index"].GetIntegerSafe(0);
                while (static_cast<i64>(toolCallsArray.size()) <= idx) {
                    toolCallsArray.emplace_back();
                }
                auto& target = toolCallsArray[idx];

                // TODO: errors are ignored
                if (!target["type"].IsString()) {
                    target["type"] = "function";
                }
                // TODO: errors are ignored
                if (const auto& id = tc["id"]; id.IsString() && !id.GetString().empty()) {
                    target["id"] = id.GetString();
                }
                if (const auto& fn = tc["function"]; fn.IsMap()) {
                    auto& targetFn = target["function"];
                    // TODO: errors are ignored
                    if (const auto& name = fn["name"]; name.IsString() && !name.GetString().empty()) {
                        targetFn["name"] = name.GetString();
                    }
                    // TODO: errors are ignored
                    if (const auto& args = fn["arguments"]; args.IsString()) {
                        targetFn["arguments"] = targetFn["arguments"].GetStringSafe() + args.GetString();
                    }
                }
            }
        }
    }

    NJson::TJsonValue FinalizeStreamingResponse(NJson::TJsonValue&& assembled) final {
        if (!assembled.IsMap() || !assembled["choices"].IsArray() || assembled["choices"].GetArraySafe().empty()) {
            NJson::TJsonValue result;
            auto& message = result["choices"][0]["message"];
            message["role"] = "assistant";
            return result;
        }
        return std::move(assembled);
    }

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
        NJson::TJsonValue conversationPart;
        conversationPart["role"] = "assistant";

        const auto& content = parser.MaybeKey("content");
        const bool hasContent = content && !content->IsNull();
        if (hasContent) {
            result.Text = Strip(content->GetString());
            conversationPart["content"] = result.Text;
        }

        const auto& toolCalls = parser.MaybeKey("tool_calls");
        const bool hasToolsCalls = toolCalls && !toolCalls->IsNull();
        if (hasToolsCalls) {
            auto& conversationPartToolCalls = conversationPart["tool_calls"].SetType(NJson::JSON_ARRAY).GetArraySafe();
            toolCalls->Iterate([&](TJsonParser toolCall) {
                auto& conversationPartToolCall = conversationPartToolCalls.emplace_back();
                conversationPartToolCall["type"] = "function";
                const auto function = toolCall.GetKey("function");

                const auto& arguments = function.GetKey("arguments").GetString();
                auto& conversationPartToolCallFunction = conversationPartToolCall["function"];
                conversationPartToolCallFunction["arguments"] = arguments;

                NJson::TJsonValue argumentsJson;
                try {
                    NJson::ReadJsonTree(arguments, &argumentsJson, /* throwOnError */ true);
                } catch (const std::exception& e) {
                    YDB_CLI_LOG(Notice, "Tool call arguments is not valid JSON: '" << arguments << "', reason: " << e.what());
                    throw yexception() << "Tool call arguments is not valid JSON, reason: " << e.what();
                }

                auto name = function.GetKey("name").GetString();
                conversationPartToolCallFunction["name"] = name;
                if (!ValidateToolName(name)) {
                    throw yexception() << "Invalid tool name requested: " << name;
                }

                auto id = toolCall.GetKey("id").GetString();
                conversationPartToolCall["id"] = id;

                result.ToolCalls.push_back({
                    .Id = std::move(id),
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
