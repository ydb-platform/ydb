#include "model_anthropic.h"

#include <ydb/library/yql/providers/common/http_gateway/yql_http_gateway.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/string_utils/url/url.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/string/builder.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

class TModelAnthropic final : public IModel {
    class TToolInfo {
    public:
        TToolInfo(const TString& name, const NJson::TJsonValue& parametersSchema, const TString& description)
            : Name(name)
            , ParametersSchema(parametersSchema)
            , Description(description)
        {}

        NJson::TJsonValue ToJson() const {
            NJson::TJsonValue result;
            result["name"] = Name;
            result["description"] = Description;
            result["input_schema"] = ParametersSchema;

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
            switch (Role) {
                case ERole::User: {
                    result["content"] = Content;
                    result["role"] = "user";
                    break;
                }
                case ERole::AI: {
                    Y_ENSURE(AssistantPart);
                    result["content"] = *AssistantPart;
                    result["role"] = "assistant";
                    break;
                }
                case ERole::Tool: {
                    result["role"] = "user";

                    auto& content = result["content"][0];
                    content["type"] = "tool_result";
                    content["content"] = Content;

                    Y_ENSURE(ToolCallId);
                    content["tool_use_id"] = *ToolCallId;
                    break;
                }
            }

            return result;
        }

    public:
        std::optional<NJson::TJsonValue> AssistantPart;
        TString Content;

    private:
        ERole Role = ERole::User;
        std::optional<TString> ToolCallId;
    };

public:
    TModelAnthropic(NYql::IHTTPGateway::TPtr httpGateway, const TAnthropicModelSettings& settings)
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
        bodyJson["model"] = Settings.ModelId;
        bodyJson["max_tokens"] = Settings.MaxTokens;

        auto& messagesArray = bodyJson["messages"].SetType(NJson::JSON_ARRAY).GetArraySafe();
        for (const auto& part : Conversation) {
            messagesArray.push_back(part.ToJson());
        }

        if (!Tools.empty()) {
            auto& toolsArray = bodyJson["tools"].SetType(NJson::JSON_ARRAY).GetArraySafe();
            for (const auto& tool : Tools) {
                toolsArray.push_back(tool.ToJson());
            }
        }

        NJsonWriter::TBuf bodyWriter;
        bodyWriter.WriteJsonValue(&bodyJson);

        NYql::THttpHeader headers = {.Fields = {
            "Content-Type: application/json",
            "anthropic-version: 2023-06-01"
        }};

        if (Settings.ApiKey) {
            headers.Fields.emplace_back(TStringBuilder() << "x-api-key: " << Settings.ApiKey);
        }

        auto answer = NThreading::NewPromise<TString>();
        HttpGateway->Upload(
            TStringBuilder() << Settings.BaseUrl << "/v1/messages",
            std::move(headers),
            bodyWriter.Str(),
            [&answer](NYql::IHTTPGateway::TResult result) {
                if (result.CurlResponseCode != CURLE_OK) {
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
        NJson::TJsonValue resultJson;
        if (!NJson::ReadJsonTree(result, &resultJson)) {
            throw yexception() << "Response of model is not JSON, got response: " << result;
        }

        ValidateJsonType(resultJson, NJson::JSON_MAP);

        const auto& resultMap = resultJson.GetMap();
        if (const auto it = resultMap.find("response"); it != resultMap.end()) {
            resultJson = it->second;
        }

        TResponse response;

        // Extract content from Anthropic response
        const auto& content = ValidateJsonKey(resultJson, "content");
        ValidateJsonType(content, NJson::JSON_ARRAY, "content");

        auto& conversationPart = Conversation.emplace_back(TString(), TConversationPart::ERole::AI);
        conversationPart.AssistantPart = content;
        for (const auto& contentItem : content.GetArray()) {
            ValidateJsonType(contentItem, NJson::JSON_MAP, "content[i]");

            const auto& type = ValidateJsonKey(contentItem, "type", "content[i]");
            ValidateJsonType(type, NJson::JSON_STRING, "content[i].type");

            if (type.GetString() == "text") {
                if (response.Text) {
                    throw yexception() << "Response of model contains multiple text responses, got response: " << result;
                }

                const auto& text = ValidateJsonKey(contentItem, "text", "content[i]");
                ValidateJsonType(text, NJson::JSON_STRING, "content[i].text");
                response.Text = text.GetString();
            } else if (type.GetString() == "tool_use") {
                const auto& id = ValidateJsonKey(contentItem, "id", "content[i]");
                ValidateJsonType(id, NJson::JSON_STRING, "content[i].id");

                const auto& name = ValidateJsonKey(contentItem, "name", "content[i]");
                ValidateJsonType(name, NJson::JSON_STRING, "content[i].name");

                const auto& input = ValidateJsonKey(contentItem, "input", "content[i]");

                response.ToolCalls.push_back({
                    .Id = id.GetString(),
                    .Name = name.GetString(),
                    .Parameters = input
                });
            } else {
                throw yexception() << "Response of model contains unknown type: " << type.GetString() << ", got response: " << result;
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
    TAnthropicModelSettings Settings;

    std::vector<TToolInfo> Tools;
    std::vector<TConversationPart> Conversation;
};

} // anonymous namespace

IModel::TPtr CreateAnthropicModel(const TAnthropicModelSettings& settings) {
    return std::make_shared<TModelAnthropic>(NYql::IHTTPGateway::Make(), settings);
}

} // namespace NYdb::NConsoleClient::NAi