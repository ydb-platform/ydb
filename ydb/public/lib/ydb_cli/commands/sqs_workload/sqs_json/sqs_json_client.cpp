#include "sqs_json_client.h"
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/memory/stl/SimpleStringStream.h>
#include <aws/sqs/model/DeleteMessageBatchRequest.h>
#include <aws/sqs/model/GetQueueUrlRequest.h>
#include <aws/sqs/model/MessageSystemAttributeNameForSends.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/SendMessageBatchRequest.h>
#include <library/cpp/string_utils/url/url.h>
#include <util/generic/guid.h>
#include <util/generic/string.h>
#include <util/string/cast.h>

namespace NYdb::NConsoleClient {

    namespace {

        constexpr auto kContentTypeHeader = "Content-Type";
        constexpr auto kContentTypeValue = "application/x-amz-json-1.1";
        constexpr auto kAmzSdkRequestHeader = "amz-sdk-request";
        constexpr auto kAmzSdkRequestValue = "attempt=1";
        constexpr auto kXAmzAPIVersionHeader = "x-amz-api-version";
        constexpr auto kXAmzCloudIamTokenHeader = "x-yacloud-subjecttoken";
        constexpr auto kXAmzAPIVersionValue = "2012-11-05";
        constexpr auto kAmzSdkInvocationIdHeader = "amz-sdk-invocation-id";
        constexpr auto kServiceName = "sqs";

        template <typename T>
        Aws::Utils::Json::JsonValue BuildAttributeValueJson(const T& attrValue) {
            Aws::Utils::Json::JsonValue attrValueJson;
            attrValueJson.WithString("DataType", attrValue.GetDataType());

            if (attrValue.StringValueHasBeenSet()) {
                attrValueJson.WithString("StringValue", attrValue.GetStringValue());
            }

            if (attrValue.StringListValuesHasBeenSet()) {
                Aws::Vector<Aws::Utils::Json::JsonValue> stringListValuesVector;
                stringListValuesVector.reserve(attrValue.GetStringListValues().size());
                for (const auto& stringValue : attrValue.GetStringListValues()) {
                    Aws::Utils::Json::JsonValue stringValueJson;
                    stringValueJson.AsString(stringValue);
                    stringListValuesVector.push_back(stringValueJson);
                }
                Aws::Utils::Array<Aws::Utils::Json::JsonValue> stringListValuesArray(
                    stringListValuesVector.size());
                for (size_t i = 0; i < stringListValuesVector.size(); ++i) {
                    stringListValuesArray[i] = stringListValuesVector[i];
                }
                attrValueJson.WithArray("StringListValues", stringListValuesArray);
            }

            if (attrValue.BinaryValueHasBeenSet()) {
                const auto& binaryValue = attrValue.GetBinaryValue();
                Aws::String base64Value =
                    Aws::Utils::HashingUtils::Base64Encode(binaryValue);
                attrValueJson.WithString("BinaryValue", base64Value);
            }

            if (attrValue.BinaryListValuesHasBeenSet()) {
                Aws::Vector<Aws::Utils::Json::JsonValue> binaryListValuesVector;
                binaryListValuesVector.reserve(attrValue.GetBinaryListValues().size());
                for (const auto& binaryValue : attrValue.GetBinaryListValues()) {
                    Aws::String base64Value =
                        Aws::Utils::HashingUtils::Base64Encode(binaryValue);
                    Aws::Utils::Json::JsonValue binaryValueJson;
                    binaryValueJson.AsString(base64Value);
                    binaryListValuesVector.push_back(binaryValueJson);
                }
                Aws::Utils::Array<Aws::Utils::Json::JsonValue> binaryListValuesArray(
                    binaryListValuesVector.size());
                for (size_t i = 0; i < binaryListValuesVector.size(); ++i) {
                    binaryListValuesArray[i] = binaryListValuesVector[i];
                }
                attrValueJson.WithArray("BinaryListValues", binaryListValuesArray);
            }

            return attrValueJson;
        }

        Aws::Utils::Json::JsonValue BuildMessageAttributesJson(
            const Aws::Map<Aws::String, MessageAttributeValue>& messageAttributes) {
            Aws::Utils::Json::JsonValue messageAttributesJson;
            for (const auto& attrPair : messageAttributes) {
                messageAttributesJson.WithObject(
                    attrPair.first, BuildAttributeValueJson(attrPair.second));
            }
            return messageAttributesJson;
        }

        Aws::Utils::Json::JsonValue BuildMessageSystemAttributesJson(
            const Aws::Map<MessageSystemAttributeNameForSends,
                           MessageSystemAttributeValue>& messageSystemAttributes) {
            Aws::Utils::Json::JsonValue messageSystemAttributesJson;
            for (const auto& attrPair : messageSystemAttributes) {
                Aws::String attrName =
                    MessageSystemAttributeNameForSendsMapper::
                        GetNameForMessageSystemAttributeNameForSends(attrPair.first);
                messageSystemAttributesJson.WithObject(
                    attrName, BuildAttributeValueJson(attrPair.second));
            }
            return messageSystemAttributesJson;
        }

    } // namespace

    TSQSJsonClient::TSQSJsonClient(
        const Aws::Auth::AWSCredentials& credentials,
        const Aws::Client::ClientConfiguration& clientConfiguration,
        const Aws::String& cloudIamToken)
        : SQSClient(credentials, clientConfiguration)
        ,
        HttpClient(Aws::Http::CreateHttpClient(clientConfiguration))
        ,
        EndpointOverride(clientConfiguration.endpointOverride)
        ,
        CloudIamToken(cloudIamToken)
    {
        auto credentialsProvider =
            Aws::MakeShared<Aws::Auth::SimpleAWSCredentialsProvider>(
                "credentials-provider", credentials);
        const Aws::String signingRegion = clientConfiguration.region.empty()
            ? Aws::String("ru-central1")
            : clientConfiguration.region;

        Signer = Aws::MakeShared<Aws::Client::AWSAuthV4Signer>(
            "aws-auth-v4-signer", credentialsProvider, kServiceName, signingRegion,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always, true,
            Aws::Auth::AWSSigningAlgorithm::SIGV4);
    }

    void TSQSJsonClient::AddHeaders(
        const Aws::Http::HeaderValueCollection& headers,
        std::shared_ptr<Aws::Http::HttpRequest>& request) const {
        for (const auto& header : headers) {
            request->SetHeaderValue(header.first, header.second);
        }

        request->SetHeaderValue(kContentTypeHeader, kContentTypeValue);
        request->SetHeaderValue(kAmzSdkRequestHeader, kAmzSdkRequestValue);
        request->SetHeaderValue(kXAmzAPIVersionHeader, kXAmzAPIVersionValue);
        if (!CloudIamToken.empty()) {
            request->SetHeaderValue(kXAmzCloudIamTokenHeader, CloudIamToken);
        }
        const TString invocationId = CreateGuidAsString();
        request->SetHeaderValue(
            kAmzSdkInvocationIdHeader,
            Aws::String(invocationId.c_str(), invocationId.size()));
    }

    Aws::Utils::Json::JsonValue TSQSJsonClient::ReadResponseBody(
        const Aws::Http::HttpResponse& response) const {
        auto& responseStream = response.GetResponseBody();

        Aws::String bodyString((std::istreambuf_iterator<char>(responseStream)),
                               std::istreambuf_iterator<char>());

        Aws::Utils::Json::JsonValue jsonValue(bodyString);

        if (!jsonValue.WasParseSuccessful()) {
            Cerr << "Failed to parse JSON: " << jsonValue.GetErrorMessage() << Endl;
            Cerr << "Response body was: " << bodyString << Endl;
        }

        return jsonValue;
    }

    std::shared_ptr<Aws::Http::HttpRequest>
    TSQSJsonClient::CreateBaseRequest(const Aws::String& queueUrl) const {
        auto responseStreamFactory = []() -> Aws::IOStream* {
            return Aws::New<Aws::SimpleStringStream>("response-stream");
        };

        Aws::String uri = Aws::String(queueUrl.c_str(), queueUrl.size());
        if (!EndpointOverride.empty()) {
            uri = Aws::String(EndpointOverride.c_str(), EndpointOverride.size());
        }

        auto request = Aws::Http::CreateHttpRequest(
            uri, Aws::Http::HttpMethod::HTTP_POST, responseStreamFactory);

        return request;
    }

    SendMessageBatchOutcome TSQSJsonClient::SendMessageBatch(
        const SendMessageBatchRequest& sendMessageBatchRequest)
        const {
        const auto& queueUrl = sendMessageBatchRequest.GetQueueUrl();
        auto request = CreateBaseRequest(queueUrl);
        AddHeaders(sendMessageBatchRequest.GetAdditionalCustomHeaders(), request);

        Aws::Utils::Json::JsonValue jsonRequest;
        jsonRequest.WithString("QueueUrl", queueUrl);

        Aws::Vector<Aws::Utils::Json::JsonValue> entriesVector;
        entriesVector.reserve(sendMessageBatchRequest.GetEntries().size());

        for (const auto& entry : sendMessageBatchRequest.GetEntries()) {
            Aws::Utils::Json::JsonValue jsonEntry;
            jsonEntry.WithString("Id", entry.GetId())
                .WithString("MessageBody", entry.GetMessageBody());

            if (entry.MessageGroupIdHasBeenSet()) {
                jsonEntry.WithString("MessageGroupId", entry.GetMessageGroupId());
            }

            if (entry.MessageDeduplicationIdHasBeenSet()) {
                jsonEntry.WithString("MessageDeduplicationId",
                                     entry.GetMessageDeduplicationId());
            }

            if (entry.MessageAttributesHasBeenSet()) {
                jsonEntry.WithObject(
                    "MessageAttributes",
                    BuildMessageAttributesJson(entry.GetMessageAttributes()));
            }

            if (entry.MessageSystemAttributesHasBeenSet()) {
                jsonEntry.WithObject("MessageSystemAttributes",
                                     BuildMessageSystemAttributesJson(
                                         entry.GetMessageSystemAttributes()));
            }

            entriesVector.push_back(jsonEntry);
        }

        Aws::Utils::Array<Aws::Utils::Json::JsonValue> entriesArray(
            entriesVector.size());
        for (size_t i = 0; i < entriesVector.size(); ++i) {
            entriesArray[i] = entriesVector[i];
        }

        jsonRequest.WithArray("Entries", entriesArray);

        auto jsonBody = jsonRequest.View().WriteCompact();
        auto bodyStream =
            Aws::MakeShared<Aws::SimpleStringStream>("json-body", jsonBody);
        request->SetContentLength(
            Aws::Utils::StringUtils::to_string(jsonBody.size()));
        request->AddContentBody(bodyStream);

        Signer->SignRequest(*request);

        auto response = HttpClient->MakeRequest(request);
        if (response->GetResponseCode() != Aws::Http::HttpResponseCode::OK) {
            Aws::SQS::SQSError error;
            error.SetResponseHeaders(response->GetHeaders());
            error.SetResponseCode(response->GetResponseCode());
            return SendMessageBatchOutcome(error);
        }

        auto responseJson = ReadResponseBody(*response);
        if (!responseJson.WasParseSuccessful()) {
            Aws::SQS::SQSError error;
            error.SetResponseHeaders(response->GetHeaders());
            error.SetResponseCode(response->GetResponseCode());
            error.SetMessage(responseJson.GetErrorMessage());
            return SendMessageBatchOutcome(error);
        }

        SendMessageBatchResult result;
        const auto& view = responseJson.View();

        if (view.KeyExists("Successful")) {
            const auto& successful = view.GetArray("Successful");
            for (size_t i = 0; i < successful.GetLength(); ++i) {
                result.AddSuccessful(
                    SendMessageBatchResultEntry()
                        .WithId(successful[i].GetString("Id"))
                        .WithMessageId(successful[i].GetString("MessageId"))
                        .WithMD5OfMessageBody(
                            successful[i].GetString("MD5OfMessageBody"))
                        .WithSequenceNumber(
                            successful[i].GetString("SequenceNumber")));
            }
        }

        if (view.KeyExists("Failed")) {
            const auto& failed = view.GetArray("Failed");
            for (size_t i = 0; i < failed.GetLength(); ++i) {
                result.AddFailed(
                    BatchResultErrorEntry()
                        .WithId(failed[i].GetString("Id"))
                        .WithSenderFault(failed[i].GetBool("SenderFault"))
                        .WithCode(failed[i].GetString("Code"))
                        .WithMessage(failed[i].GetString("Message")));
            }
        }

        SendMessageBatchOutcome outcome(result);
        return outcome;
    }

    ReceiveMessageOutcome TSQSJsonClient::ReceiveMessage(
        const ReceiveMessageRequest& receiveMessageRequest) const {
        const auto& queueUrl = receiveMessageRequest.GetQueueUrl();
        auto request = CreateBaseRequest(queueUrl);
        AddHeaders(receiveMessageRequest.GetAdditionalCustomHeaders(), request);

        Aws::Utils::Json::JsonValue jsonRequest;
        jsonRequest.WithString("QueueUrl", queueUrl);
        jsonRequest.WithInteger("MaxNumberOfMessages",
                                receiveMessageRequest.GetMaxNumberOfMessages());
        jsonRequest.WithInteger("VisibilityTimeout",
                                receiveMessageRequest.GetVisibilityTimeout());
        jsonRequest.WithInteger("WaitTimeSeconds",
                                receiveMessageRequest.GetWaitTimeSeconds());

        if (!receiveMessageRequest.GetAttributeNames().empty()) {
            Aws::Utils::Array<Aws::Utils::Json::JsonValue> attributeNames(
                receiveMessageRequest.GetAttributeNames().size());
            for (size_t i = 0; i < receiveMessageRequest.GetAttributeNames().size();
                 ++i) {
                const auto& attribute =
                    receiveMessageRequest.GetAttributeNames()[i];
                attributeNames[i] = QueueAttributeNameMapper::
                    GetNameForQueueAttributeName(attribute);
            }

            jsonRequest.WithArray("AttributeNames", attributeNames);
        }

        if (!receiveMessageRequest.GetMessageAttributeNames().empty()) {
            Aws::Utils::Array<Aws::Utils::Json::JsonValue> messageAttributeNames(
                receiveMessageRequest.GetMessageAttributeNames().size());
            for (size_t i = 0;
                 i < receiveMessageRequest.GetMessageAttributeNames().size(); ++i) {
                const auto& messageAttributeName =
                    receiveMessageRequest.GetMessageAttributeNames()[i];
                messageAttributeNames[i] = messageAttributeName;
            }

            jsonRequest.WithArray("MessageAttributeNames", messageAttributeNames);
        }

        auto jsonBody = jsonRequest.View().WriteCompact();
        auto bodyStream =
            Aws::MakeShared<Aws::SimpleStringStream>("json-body", jsonBody);
        request->SetContentLength(
            Aws::Utils::StringUtils::to_string(jsonBody.size()));
        request->AddContentBody(bodyStream);

        Signer->SignRequest(*request);

        auto response = HttpClient->MakeRequest(request);
        if (response->GetResponseCode() != Aws::Http::HttpResponseCode::OK) {
            Aws::SQS::SQSError error;
            error.SetResponseHeaders(response->GetHeaders());
            error.SetResponseCode(response->GetResponseCode());
            return ReceiveMessageOutcome(error);
        }

        auto responseJson = ReadResponseBody(*response);
        if (!responseJson.WasParseSuccessful()) {
            Aws::SQS::SQSError error;
            error.SetResponseHeaders(response->GetHeaders());
            error.SetResponseCode(response->GetResponseCode());
            error.SetMessage(responseJson.GetErrorMessage());
            return ReceiveMessageOutcome(error);
        }

        ReceiveMessageResult result;
        const auto& view = responseJson.View();

        if (!view.KeyExists("Messages")) {
            return ReceiveMessageOutcome(result);
        }

        const auto& messages = view.GetArray("Messages");
        for (size_t i = 0; i < messages.GetLength(); ++i) {
            Message message;
            message.WithBody(messages[i].GetString("Body"))
                .WithMessageId(messages[i].GetString("MessageId"))
                .WithReceiptHandle(messages[i].GetString("ReceiptHandle"))
                .WithMD5OfBody(messages[i].GetString("MD5OfBody"))
                .WithMD5OfMessageAttributes(messages[i].GetString("MD5OfMessageAttributes"));

            if (messages[i].KeyExists("Attributes")) {
                const auto& messageAttributes = messages[i].GetObject("Attributes");
                Aws::Map<MessageSystemAttributeName, Aws::String>
                    messageSystemAttributesMap;
                for (const auto& [attributeName, attributeValue] :
                     messageAttributes.GetAllObjects()) {

                    auto messageAttribute = MessageSystemAttributeNameMapper::GetMessageSystemAttributeNameForName(attributeName);
                    if (attributeValue.IsString()) {
                        messageSystemAttributesMap[messageAttribute] =
                            attributeValue.AsString();
                    } else {
                        Cerr << "Unknown attribute type: " << attributeName << Endl;

                        continue;
                    }
                }

                message.WithAttributes(messageSystemAttributesMap);
            }

            if (messages[i].KeyExists("MessageAttributes")) {
                const auto& messageAttributes = messages[i].GetObject("MessageAttributes");
                Aws::Map<Aws::String, MessageAttributeValue>
                    messageAttributesMap;

                for (const auto& [attributeName, attributeValue] :
                     messageAttributes.GetAllObjects()) {
                    if (attributeValue.IsString()) {
                        messageAttributesMap[attributeName] =
                            MessageAttributeValue()
                                .WithStringValue(attributeValue.AsString());
                    } else if (attributeValue.IsListType()) {
                        Aws::Vector<Aws::String> stringListValues;
                        stringListValues.reserve(
                            attributeValue.AsArray().GetLength());
                        for (size_t j = 0; j < attributeValue.AsArray().GetLength();
                             ++j) {
                            stringListValues.push_back(
                                attributeValue.AsArray()[j].AsString());
                        }
                        messageAttributesMap[attributeName] =
                            MessageAttributeValue()
                                .WithStringListValues(stringListValues);
                    } else {
                        Cerr << "Unknown attribute type: " << attributeName << Endl;

                        continue;
                    }
                }

                message.WithMessageAttributes(messageAttributesMap);
            }

            result.AddMessages(message);
        }

        return ReceiveMessageOutcome(result);
    }

    DeleteMessageBatchOutcome TSQSJsonClient::DeleteMessageBatch(
        const DeleteMessageBatchRequest& deleteMessageBatchRequest)
        const {
        const auto& queueUrl = deleteMessageBatchRequest.GetQueueUrl();
        auto request = CreateBaseRequest(queueUrl);
        AddHeaders(deleteMessageBatchRequest.GetAdditionalCustomHeaders(), request);

        Aws::Utils::Json::JsonValue jsonRequest;
        jsonRequest.WithString("QueueUrl", queueUrl);

        Aws::Utils::Array<Aws::Utils::Json::JsonValue> entriesArray(
            deleteMessageBatchRequest.GetEntries().size());
        for (size_t i = 0; i < deleteMessageBatchRequest.GetEntries().size(); ++i) {
            const auto& entry = deleteMessageBatchRequest.GetEntries()[i];
            entriesArray[i] =
                Aws::Utils::Json::JsonValue()
                    .WithString("Id", entry.GetId())
                    .WithString("ReceiptHandle", entry.GetReceiptHandle());
        }

        jsonRequest.WithArray("Entries", entriesArray);

        auto jsonBody = jsonRequest.View().WriteCompact();
        auto bodyStream =
            Aws::MakeShared<Aws::SimpleStringStream>("json-body", jsonBody);
        request->SetContentLength(
            Aws::Utils::StringUtils::to_string(jsonBody.size()));
        request->AddContentBody(bodyStream);

        Signer->SignRequest(*request);

        auto response = HttpClient->MakeRequest(request);
        if (response->GetResponseCode() != Aws::Http::HttpResponseCode::OK) {
            Aws::SQS::SQSError error;
            error.SetResponseHeaders(response->GetHeaders());
            error.SetResponseCode(response->GetResponseCode());
            error.SetMessage(response->GetClientErrorMessage());
            return DeleteMessageBatchOutcome(error);
        }

        auto responseJson = ReadResponseBody(*response);
        if (!responseJson.WasParseSuccessful()) {
            Aws::SQS::SQSError error;
            error.SetResponseHeaders(response->GetHeaders());
            error.SetResponseCode(response->GetResponseCode());
            error.SetMessage(responseJson.GetErrorMessage());
            return DeleteMessageBatchOutcome(error);
        }

        DeleteMessageBatchResult result;
        const auto& view = responseJson.View();

        if (view.KeyExists("Successful")) {
            const auto& successful = view.GetArray("Successful");
            for (size_t i = 0; i < successful.GetLength(); ++i) {
                result.AddSuccessful(
                    DeleteMessageBatchResultEntry().WithId(
                        successful[i].GetString("Id")));
            }
        }

        if (view.KeyExists("Failed")) {
            const auto& failed = view.GetArray("Failed");
            for (size_t i = 0; i < failed.GetLength(); ++i) {
                result.AddFailed(
                    BatchResultErrorEntry()
                        .WithId(failed[i].GetString("Id"))
                        .WithSenderFault(failed[i].GetBool("SenderFault"))
                        .WithCode(failed[i].GetString("Code"))
                        .WithMessage(failed[i].GetString("Message")));
            }
        }

        return DeleteMessageBatchOutcome(result);
    }

    GetQueueUrlOutcome TSQSJsonClient::GetQueueUrl(
        const GetQueueUrlRequest& getQueueUrlRequest) const {
        auto request = CreateBaseRequest(EndpointOverride);
        AddHeaders(getQueueUrlRequest.GetAdditionalCustomHeaders(), request);

        Aws::Utils::Json::JsonValue jsonRequest;
        jsonRequest.WithString("QueueName", getQueueUrlRequest.GetQueueName());

        auto jsonBody = jsonRequest.View().WriteCompact();
        auto bodyStream =
            Aws::MakeShared<Aws::SimpleStringStream>("json-body", jsonBody);
        request->SetContentLength(
            Aws::Utils::StringUtils::to_string(jsonBody.size()));
        request->AddContentBody(bodyStream);
        request->SetHeaderValue("x-amz-target", "AmazonSQS.GetQueueUrl");
        Signer->SignRequest(*request);

        auto response = HttpClient->MakeRequest(request);
        if (response->GetResponseCode() != Aws::Http::HttpResponseCode::OK) {
            Aws::OStringStream oss;
            auto responseBody = response->GetResponseBody().rdbuf();
            oss << response->GetClientErrorType() << " " << response->GetResponseCode() << " " << responseBody;
            Cerr << "got error response: " << oss.str() << Endl;
            Aws::SQS::SQSError error;
            error.SetResponseHeaders(response->GetHeaders());
            error.SetResponseCode(response->GetResponseCode());
            return GetQueueUrlOutcome(error);
        }

        auto responseJson = ReadResponseBody(*response);
        if (!responseJson.WasParseSuccessful()) {
            Aws::SQS::SQSError error;
            error.SetResponseHeaders(response->GetHeaders());
            error.SetResponseCode(response->GetResponseCode());
            error.SetMessage(responseJson.GetErrorMessage());
            return GetQueueUrlOutcome(error);
        }

        GetQueueUrlResult result;
        const auto& view = responseJson.View();
        if (view.KeyExists("QueueUrl")) {
            result.SetQueueUrl(view.GetString("QueueUrl"));
        }

        return GetQueueUrlOutcome(result);
    }

} // namespace NYdb::NConsoleClient
