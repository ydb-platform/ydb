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
    class TConversationPart {
    public:
        enum class ERole {
            User,
            AI,
        };

        TConversationPart(const TString& content, ERole role)
            : Content(content)
            , Role(role)
        {}

        NJson::TJsonValue ToJson() const {
            NJson::TJsonValue result;
            result["content"] = Content;

            auto& roleJson = result["role"];
            switch (Role) {
                case ERole::User:
                    roleJson = "user";
                    break;
                case ERole::AI:
                    roleJson = "assistant";
                    break;
            }

            return result;
        }

    private:
        TString Content;
        ERole Role;
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

    TString Chat(const TString& input) final {
        NJson::TJsonValue bodyJson;
        bodyJson["model"] = Settings.ModelId;

        Conversation.emplace_back(input, TConversationPart::ERole::User);
        auto& inputJson = bodyJson["input"];
        inputJson.SetType(NJson::JSON_ARRAY);

        auto& conversationJson = inputJson.GetArraySafe();
        for (const auto& part : Conversation) {
            conversationJson.push_back(part.ToJson());
        }

        NJsonWriter::TBuf bodyWriter;
        bodyWriter.WriteJsonValue(&bodyJson);

        NYql::THttpHeader headers = {.Fields = {"Content-Type: application/json"}};

        if (Settings.ApiKey) {
            headers.Fields.emplace_back(TStringBuilder() << "Authorization: Bearer " << Settings.ApiKey);
        }

        auto answer = NThreading::NewPromise<TString>();
        HttpGateway->Upload(
            TStringBuilder() << Settings.BaseUrl << "/responses",
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

        const auto& output = ValidateJsonKey(resultJson, "output");
        ValidateJsonType(output, NJson::JSON_ARRAY, "output");
        ValidateJsonArraySize(output, 1, "output");

        const auto& outputVal = output.GetArray()[0];
        ValidateJsonType(outputVal, NJson::JSON_MAP, "output[0]");

        const auto& content = ValidateJsonKey(outputVal, "content", "output[0]");
        ValidateJsonType(content, NJson::JSON_ARRAY, "output[0].content");
        ValidateJsonArraySize(content, 1, "output[0].content");

        const auto& contentVal = content.GetArray()[0];
        ValidateJsonType(contentVal, NJson::JSON_MAP, "output[0].content[0]");

        const auto& text = ValidateJsonKey(contentVal, "text", "output[0].content[0]");
        ValidateJsonType(text, NJson::JSON_STRING, "output[0].content[0].text");

        return text.GetString();
    }

private:
    void ValidateJsonType(const NJson::TJsonValue& value, NJson::EJsonValueType expectedType, const std::optional<TString>& fieldName = std::nullopt) const {
        if (const auto valueType = value.GetType(); valueType != expectedType) {
            throw yexception() << "Response" << (fieldName ? " field '" + *fieldName + "'" : "") << " of model has unexpected type: " << valueType << ", expected type: " << expectedType;
        }
    }

    void ValidateJsonArraySize(const NJson::TJsonValue& value, size_t expectedSize, const std::optional<TString>& fieldName = std::nullopt) const {
        if (const auto valueSize = value.GetArray().size(); valueSize != expectedSize) {
            throw yexception() << "Response" << (fieldName ? " field '" + *fieldName + "'" : "") << " of model has unexpected size: " << valueSize << ", expected size: " << expectedSize;
        }
    }

    const NJson::TJsonValue& ValidateJsonKey(const NJson::TJsonValue& value, const TString& key, const std::optional<TString>& fieldName = std::nullopt) const {
        const auto* output = value.GetMap().FindPtr(key);
        if (!output) {
            throw yexception() << "Response of model does not contain '" << key << "' field" << (fieldName ? " in '" + *fieldName + "'" : "");
        }

        return *output;
    }

private:
    const NYql::IHTTPGateway::TPtr HttpGateway;
    TOpenAiModelSettings Settings;

    std::vector<TConversationPart> Conversation;
};

} // anonymous namespace

IModel::TPtr CreateOpenAiModel(const TOpenAiModelSettings& settings) {
    return std::make_shared<TModelOpenAi>(NYql::IHTTPGateway::Make(), settings);
}

} // namespace NYdb::NConsoleClient::NAi
