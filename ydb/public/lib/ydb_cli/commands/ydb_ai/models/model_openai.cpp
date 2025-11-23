#include "model_openai.h"

#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/string_utils/url/url.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/string/builder.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TModelOpenAi final : public IModel {
    class TToolInfo {
    public:
        TToolInfo(const TString& name, const NJson::TJsonValue& parametersSchema, const TString& description)
            : Name(name)
            , ParametersSchema(parametersSchema)
            , Description(description)
        {}

        NJson::TJsonValue ToJson() const {
            NJson::TJsonValue result;
            result["type"] = "function";

            auto& toolJson = result["function"];
            toolJson["strict"] = false;  // AI-TODO: enable after fixes
            toolJson["name"] = Name;
            toolJson["parameters"] = ParametersSchema;
            toolJson["description"] = Description;

            return result;
        }

    private:
        TString Name;
        NJson::TJsonValue ParametersSchema;
        TString Description;
    };

    class TConversationPart {
    public:
        enum class ERole {
            User,
            AI,
            Tool,
        };

        TConversationPart(const TString& content, ERole role)
            : Content(content)
            , Role(role)
        {}

        TConversationPart(const TString& content, const TString& toolCallId)
            : Content(content)
            , Role(ERole::Tool)
            , ToolCallId(toolCallId)
        {}

        NJson::TJsonValue ToJson() const {
            NJson::TJsonValue result;
            result["content"] = Content;

            auto& roleJson = result["role"];
            switch (Role) {
                case ERole::User: {
                    roleJson = "user";
                    break;
                }
                case ERole::AI: {
                    roleJson = "assistant";
                    break;
                }
                case ERole::Tool: {
                    roleJson = "tool";

                    Y_ENSURE(ToolCallId);
                    result["tool_call_id"] = *ToolCallId;
                    break;
                }
            }

            return result;
        }

    private:
        TString Content;
        ERole Role = ERole::User;
        std::optional<TString> ToolCallId;
    };

public:
    TModelOpenAi(NYql::IHTTPGateway::TPtr httpGateway, const TOpenAiModelSettings& settings)
        : HttpGateway(httpGateway)
        , Settings(settings)
    {
        TStringBuf sanitizedUrl;
        TStringBuf query;
        TStringBuf fragment;
        SeparateUrlFromQueryAndFragment(Settings.BaseUrl, sanitizedUrl, query, fragment);

        if (query || fragment) {
            throw yexception() << "BaseUrl must not contain query or fragment, got url: '" << Settings.BaseUrl << "' with query: '" << query << "' or fragment: '" << fragment << "'";
        }

        Settings.BaseUrl = RemoveFinalSlash(sanitizedUrl);
    }

    TResponse HandleMessages(const std::vector<NAi::IModel::TRequest>& requests) final {
        for (const auto& request : requests) {
            if (request.ToolCallId) {
                Conversation.emplace_back(request.Text, *request.ToolCallId);
            } else {
                Conversation.emplace_back(request.Text, TConversationPart::ERole::User);
            }
        }

        NJson::TJsonValue bodyJson;

        if (Settings.ModelId) {
            bodyJson["model"] = *Settings.ModelId;
        }

        auto& conversationJson = bodyJson["messages"].SetType(NJson::JSON_ARRAY).GetArraySafe();
        for (const auto& part : Conversation) {
            conversationJson.push_back(part.ToJson());
        }

        auto& toolsArray = bodyJson["tools"].SetType(NJson::JSON_ARRAY).GetArraySafe();
        for (const auto& tool : Tools) {
            toolsArray.push_back(tool.ToJson());
        }

        NJsonWriter::TBuf bodyWriter;
        bodyWriter.WriteJsonValue(&bodyJson);

        NYql::THttpHeader headers = {.Fields = {"Content-Type: application/json"}};

        // Cerr << "-------------------------- Request: " << bodyWriter.Str();

        if (Settings.ApiKey) {
            headers.Fields.emplace_back(TStringBuilder() << "Authorization: Bearer " << Settings.ApiKey);
        }

        auto answer = NThreading::NewPromise<TString>();
        HttpGateway->Upload(
            TStringBuilder() << Settings.BaseUrl << "/chat/completions",
            std::move(headers),
            bodyWriter.Str(),
            [&answer](NYql::IHTTPGateway::TResult result) {
                if (result.CurlResponseCode != CURLE_OK) {
                    // AI-TODO: proper error handling
                    answer.SetException(TStringBuilder() << "Request model failed: " << result.Issues.ToOneLineString() << ", internal code: " << curl_easy_strerror(result.CurlResponseCode) << ", response: " << result.Content.Extract());
                    return;
                }

                auto& content = result.Content;
                if (content.HttpResponseCode < 200 || content.HttpResponseCode >= 300) {
                    answer.SetException(TStringBuilder() << "Request model failed, internal code: " << content.HttpResponseCode << ", response: " << result.Content.Extract());
                    return;
                }

                answer.SetValue(content.Extract());
            }
        );

        const auto result = answer.GetFuture().ExtractValueSync();
        // Cerr << "-------------------------- Result: " << result;
        NJson::TJsonValue resultJson;
        if (!NJson::ReadJsonTree(result, &resultJson)) {
            throw yexception() << "Response of model is not JSON, got response: " << result;
        }

        ValidateJsonType(resultJson, NJson::JSON_MAP);

        const auto& resultMap = resultJson.GetMap();
        if (const auto it = resultMap.find("response"); it != resultMap.end()) {
            resultJson = it->second;
        }

        const auto& choices = ValidateJsonKey(resultJson, "choices");
        ValidateJsonType(choices, NJson::JSON_ARRAY, "choices");
        ValidateJsonArraySize(choices, 1, "choices");

        // AI-TODO: proper error description
        const auto& choiceVal = choices.GetArray()[0];
        ValidateJsonType(choiceVal, NJson::JSON_MAP, "choices[0]");

        const auto& message = ValidateJsonKey(choiceVal, "message", "choices[0]");
        ValidateJsonType(message, NJson::JSON_MAP, "choices[0].message");

        const auto& messageMap = message.GetMap();
        const auto* content = messageMap.FindPtr("content");
        const auto* tollsCalls = messageMap.FindPtr("tool_calls");
        if ((!content || content->GetType() == NJson::JSON_NULL) && (!tollsCalls || tollsCalls->GetType() == NJson::JSON_NULL)) {
            throw yexception() << "Response of model does not contain 'choices[0].message.content' or 'choices[0].message.tool_calls' fields, got response: " << result;
        }

        TResponse response;

        if (content && content->GetType() != NJson::JSON_NULL) {
            ValidateJsonType(*content, NJson::JSON_STRING, "choices[0].message.content");
            response.Text = content->GetString();
        }

        if (tollsCalls && tollsCalls->GetType() != NJson::JSON_NULL) {
            ValidateJsonType(*tollsCalls, NJson::JSON_ARRAY, "choices[0].message.tool_calls");

            for (const auto& toolCall : tollsCalls->GetArray()) {
                ValidateJsonType(toolCall, NJson::JSON_MAP, "choices[0].message.tool_calls[0]");

                const auto& function = ValidateJsonKey(toolCall, "function", "choices[0].message.tool_calls[0]");
                ValidateJsonType(function, NJson::JSON_MAP, "choices[0].message.tool_calls[0].function");

                const auto& name = ValidateJsonKey(function, "name", "choices[0].message.tool_calls[0].function");
                ValidateJsonType(name, NJson::JSON_STRING, "choices[0].message.tool_calls[0].function.name");

                const auto& arguments = ValidateJsonKey(function, "arguments", "choices[0].message.tool_calls[0].function");
                ValidateJsonType(arguments, NJson::JSON_STRING, "choices[0].message.tool_calls[0].function.arguments");

                const auto& callId = ValidateJsonKey(toolCall, "id", "choices[0].message.tool_calls[0]");
                ValidateJsonType(callId, NJson::JSON_STRING, "choices[0].message.tool_calls[0].id");

                NJson::TJsonValue argumentsJson;
                if (!NJson::ReadJsonTree(arguments.GetString(), &argumentsJson)) {
                    throw yexception() << "Tool call arguments is not valid JSON, got response: " << arguments.GetString();
                }

                response.ToolCalls.push_back({
                    .Id = callId.GetString(),
                    .Name = name.GetString(),
                    .Parameters = std::move(argumentsJson)
                });
            }
        }

        Y_ENSURE(response.Text || !response.ToolCalls.empty());
        return response;
    }

    void RegisterTool(const TString& name, const NJson::TJsonValue& parametersSchema, const TString& description) final {
        Tools.emplace_back(name, parametersSchema, description);
    }

private:
    void ValidateJsonType(const NJson::TJsonValue& value, NJson::EJsonValueType expectedType, const std::optional<TString>& fieldName = std::nullopt) const {
        if (const auto valueType = value.GetType(); valueType != expectedType) {
            throw yexception() << "Response" << (fieldName ? " field '" + *fieldName + "'" : "") << " of model has unexpected type: " << valueType << ", expected type: " << expectedType << ", got response: " << value;
        }
    }

    void ValidateJsonArraySize(const NJson::TJsonValue& value, size_t expectedSize, const std::optional<TString>& fieldName = std::nullopt) const {
        if (const auto valueSize = value.GetArray().size(); valueSize != expectedSize) {
            throw yexception() << "Response" << (fieldName ? " field '" + *fieldName + "'" : "") << " of model has unexpected size: " << valueSize << ", expected size: " << expectedSize << ", got response: " << value;
        }
    }

    const NJson::TJsonValue& ValidateJsonKey(const NJson::TJsonValue& value, const TString& key, const std::optional<TString>& fieldName = std::nullopt) const {
        const auto* output = value.GetMap().FindPtr(key);
        if (!output) {
            throw yexception() << "Response of model does not contain '" << key << "' field" << (fieldName ? " in '" + *fieldName + "'" : "") << ", got response: " << value;
        }

        return *output;
    }

private:
    const NYql::IHTTPGateway::TPtr HttpGateway;
    TOpenAiModelSettings Settings;

    std::vector<TToolInfo> Tools;
    std::vector<TConversationPart> Conversation;
};

} // anonymous namespace

IModel::TPtr CreateOpenAiModel(const TOpenAiModelSettings& settings) {
    return std::make_shared<TModelOpenAi>(NYql::IHTTPGateway::Make(), settings);
}

} // namespace NYdb::NConsoleClient::NAi
