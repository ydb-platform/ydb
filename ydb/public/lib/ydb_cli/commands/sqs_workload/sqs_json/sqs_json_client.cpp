#include "sqs_json_client.h"
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/memory/stl/SimpleStringStream.h>
#include <aws/sqs/model/SendMessageRequest.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/ChangeMessageVisibilityRequest.h>
#include <aws/sqs/model/ChangeMessageVisibilityBatchRequest.h>
#include <aws/sqs/model/GetQueueAttributesRequest.h>
#include <aws/sqs/model/ListQueuesRequest.h>
#include <aws/sqs/model/PurgeQueueRequest.h>
#include <aws/sqs/model/CreateQueueRequest.h>
#include <aws/sqs/model/DeleteQueueRequest.h>
#include <aws/sqs/model/SetQueueAttributesRequest.h>
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

        // SQS operations with no result may return an empty HTTP body.
        Aws::Utils::Json::JsonValue jsonValue(bodyString.empty() ? Aws::String("{}") : bodyString);

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

        Aws::Utils::Json::JsonValue jsonRequest;
        jsonRequest.WithString("QueueUrl", queueUrl);

        Aws::Vector<Aws::Utils::Json::JsonValue> entriesVector;
        entriesVector.reserve(sendMessageBatchRequest.GetEntries().size());

        for (const auto& entry : sendMessageBatchRequest.GetEntries()) {
            Aws::Utils::Json::JsonValue jsonEntry;
            jsonEntry.WithString("Id", entry.GetId())
                .WithString("MessageBody", entry.GetMessageBody());

            if (entry.DelaySecondsHasBeenSet()) {
                jsonEntry.WithInteger("DelaySeconds", entry.GetDelaySeconds());
            }
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

        const auto response = ExecuteJsonRequest("SendMessageBatch", jsonRequest,
            sendMessageBatchRequest.GetAdditionalCustomHeaders(), queueUrl);
        if (!response.IsSuccess()) {
            return SendMessageBatchOutcome(response.GetError());
        }
        const auto& responseJson = response.GetResult();

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
                        .WithSequenceNumber(successful[i].KeyExists("SequenceNumber")
                            ? successful[i].GetString("SequenceNumber") : Aws::String{}));
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

        Aws::Utils::Json::JsonValue jsonRequest;
        jsonRequest.WithString("QueueUrl", queueUrl);
        if (receiveMessageRequest.MaxNumberOfMessagesHasBeenSet()) {
            jsonRequest.WithInteger("MaxNumberOfMessages", receiveMessageRequest.GetMaxNumberOfMessages());
        }
        if (receiveMessageRequest.VisibilityTimeoutHasBeenSet()) {
            jsonRequest.WithInteger("VisibilityTimeout", receiveMessageRequest.GetVisibilityTimeout());
        }
        if (receiveMessageRequest.WaitTimeSecondsHasBeenSet()) {
            jsonRequest.WithInteger("WaitTimeSeconds", receiveMessageRequest.GetWaitTimeSeconds());
        }

        if (receiveMessageRequest.ReceiveRequestAttemptIdHasBeenSet()) {
            jsonRequest.WithString("ReceiveRequestAttemptId", receiveMessageRequest.GetReceiveRequestAttemptId());
        }
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

        const auto response = ExecuteJsonRequest("ReceiveMessage", jsonRequest,
            receiveMessageRequest.GetAdditionalCustomHeaders(), queueUrl);
        if (!response.IsSuccess()) {
            return ReceiveMessageOutcome(response.GetError());
        }
        const auto& responseJson = response.GetResult();

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
                .WithMD5OfBody(messages[i].GetString("MD5OfBody"));
            if (messages[i].KeyExists("MD5OfMessageAttributes")) {
                message.SetMD5OfMessageAttributes(messages[i].GetString("MD5OfMessageAttributes"));
            }

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

        const auto response = ExecuteJsonRequest("DeleteMessageBatch", jsonRequest,
            deleteMessageBatchRequest.GetAdditionalCustomHeaders(), queueUrl);
        if (!response.IsSuccess()) {
            return DeleteMessageBatchOutcome(response.GetError());
        }
        const auto& responseJson = response.GetResult();

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

        Aws::Utils::Json::JsonValue jsonRequest;
        jsonRequest.WithString("QueueName", getQueueUrlRequest.GetQueueName());
        if (getQueueUrlRequest.QueueOwnerAWSAccountIdHasBeenSet()) {
            jsonRequest.WithString("QueueOwnerAWSAccountId", getQueueUrlRequest.GetQueueOwnerAWSAccountId());
        }

        const auto response = ExecuteJsonRequest("GetQueueUrl", jsonRequest,
            getQueueUrlRequest.GetAdditionalCustomHeaders(), EndpointOverride);
        if (!response.IsSuccess()) {
            return GetQueueUrlOutcome(response.GetError());
        }
        const auto& responseJson = response.GetResult();

        GetQueueUrlResult result;
        const auto& view = responseJson.View();
        if (view.KeyExists("QueueUrl")) {
            result.SetQueueUrl(view.GetString("QueueUrl"));
        }

        return GetQueueUrlOutcome(result);
    }

    TSQSJsonClient::TJsonOutcome TSQSJsonClient::ExecuteJsonRequest(
        const char* operation,
        const Aws::Utils::Json::JsonValue& payload,
        const Aws::Http::HeaderValueCollection& headers,
        const Aws::String& queueUrl) const {
        auto request = CreateBaseRequest(queueUrl);
        AddHeaders(headers, request);
        request->SetHeaderValue("x-amz-target", Aws::String("AmazonSQS.") + operation);
        const auto body = payload.View().WriteCompact();
        request->SetContentLength(Aws::Utils::StringUtils::to_string(body.size()));
        request->AddContentBody(Aws::MakeShared<Aws::SimpleStringStream>("sqs-json-body", body));
        if (!Signer->SignRequest(*request)) {
            return Aws::SQS::SQSError(Aws::Client::AWSError<Aws::Client::CoreErrors>(
                Aws::Client::CoreErrors::CLIENT_SIGNING_FAILURE, "", "Failed to sign SQS request", false));
        }

        const auto response = HttpClient->MakeRequest(request);
        if (!response) {
            return Aws::SQS::SQSError(Aws::Client::AWSError<Aws::Client::CoreErrors>(
                Aws::Client::CoreErrors::NETWORK_CONNECTION, "", "No HTTP response", true));
        }
        const auto code = response->GetResponseCode();
        if (response->HasClientError() || code == Aws::Http::HttpResponseCode::REQUEST_NOT_MADE) {
            const auto type = response->HasClientError()
                ? response->GetClientErrorType() : Aws::Client::CoreErrors::NETWORK_CONNECTION;
            Aws::SQS::SQSError error(Aws::Client::AWSError<Aws::Client::CoreErrors>(
                type, "", response->GetClientErrorMessage(), type == Aws::Client::CoreErrors::NETWORK_CONNECTION));
            error.SetResponseHeaders(response->GetHeaders());
            error.SetResponseCode(code);
            return error;
        }

        auto json = ReadResponseBody(*response);
        if (code != Aws::Http::HttpResponseCode::OK) {
            auto error = Aws::Client::CoreErrorsMapper::GetErrorForHttpResponseCode(code);
            Aws::String name;
            Aws::String message = Aws::String("SQS request failed: ") + operation;
            if (json.WasParseSuccessful()) {
                const auto view = json.View();
                if (view.KeyExists("__type")) {
                    name = view.GetString("__type");
                } else if (view.KeyExists("code")) {
                    name = view.GetString("code");
                }
                // AWS JSON errors can include a namespace before '#'.
                const auto hash = name.find('#');
                if (hash != Aws::String::npos) {
                    name = name.substr(hash + 1);
                }
                const auto colon = name.find(':');
                if (colon != Aws::String::npos) {
                    name.resize(colon);
                }
                if (!name.empty()) {
                    auto mapped = Aws::SQS::SQSErrorMapper::GetErrorForName(name.c_str());
                    if (mapped.GetErrorType() == Aws::Client::CoreErrors::UNKNOWN) {
                        mapped = Aws::SQS::SQSErrorMapper::GetErrorForName(
                            (Aws::String("AWS.SimpleQueueService.") + name).c_str());
                    }
                    if (mapped.GetErrorType() == Aws::Client::CoreErrors::UNKNOWN) {
                        mapped = Aws::Client::CoreErrorsMapper::GetErrorForName(name.c_str());
                    }
                    if (mapped.GetErrorType() != Aws::Client::CoreErrors::UNKNOWN) {
                        error = std::move(mapped);
                    }
                }
                if (view.KeyExists("message")) {
                    message = view.GetString("message");
                } else if (view.KeyExists("Message")) {
                    message = view.GetString("Message");
                }
            } else {
                message = json.GetErrorMessage();
            }
            error.SetExceptionName(name);
            error.SetMessage(message);
            error.SetResponseHeaders(response->GetHeaders());
            error.SetResponseCode(code);
            return Aws::SQS::SQSError(std::move(error));
        }
        if (!json.WasParseSuccessful()) {
            Aws::SQS::SQSError error(Aws::Client::AWSError<Aws::Client::CoreErrors>(
                Aws::Client::CoreErrors::UNKNOWN, "InvalidJson", json.GetErrorMessage(), false));
            error.SetResponseHeaders(response->GetHeaders());
            error.SetResponseCode(code);
            return error;
        }
        return json;
    }

    SendMessageOutcome TSQSJsonClient::SendMessage(const SendMessageRequest& request) const {
        Aws::Utils::Json::JsonValue payload;
        if (request.QueueUrlHasBeenSet()) {
            payload.WithString("QueueUrl", request.GetQueueUrl());
        }
        if (request.MessageBodyHasBeenSet()) {
            payload.WithString("MessageBody", request.GetMessageBody());
        }
        if (request.DelaySecondsHasBeenSet()) {
            payload.WithInteger("DelaySeconds", request.GetDelaySeconds());
        }
        if (request.MessageGroupIdHasBeenSet()) {
            payload.WithString("MessageGroupId", request.GetMessageGroupId());
        }
        if (request.MessageDeduplicationIdHasBeenSet()) {
            payload.WithString("MessageDeduplicationId", request.GetMessageDeduplicationId());
        }
        if (request.MessageAttributesHasBeenSet()) {
            payload.WithObject("MessageAttributes", BuildMessageAttributesJson(request.GetMessageAttributes()));
        }
        if (request.MessageSystemAttributesHasBeenSet()) {
            payload.WithObject("MessageSystemAttributes", BuildMessageSystemAttributesJson(request.GetMessageSystemAttributes()));
        }
        const auto response = ExecuteJsonRequest("SendMessage", payload,
            request.GetAdditionalCustomHeaders(), request.GetQueueUrl());
        if (!response.IsSuccess()) {
            return SendMessageOutcome(response.GetError());
        }
        SendMessageResult result;
        const auto view = response.GetResult().View();
        if (view.KeyExists("MessageId")) {
            result.SetMessageId(view.GetString("MessageId"));
        }
        if (view.KeyExists("MD5OfMessageBody")) {
            result.SetMD5OfMessageBody(view.GetString("MD5OfMessageBody"));
        }
        if (view.KeyExists("MD5OfMessageAttributes")) {
            result.SetMD5OfMessageAttributes(view.GetString("MD5OfMessageAttributes"));
        }
        if (view.KeyExists("MD5OfMessageSystemAttributes")) {
            result.SetMD5OfMessageSystemAttributes(view.GetString("MD5OfMessageSystemAttributes"));
        }
        if (view.KeyExists("SequenceNumber")) {
            result.SetSequenceNumber(view.GetString("SequenceNumber"));
        }
        return SendMessageOutcome(std::move(result));
    }

    DeleteMessageOutcome TSQSJsonClient::DeleteMessage(const DeleteMessageRequest& request) const {
        Aws::Utils::Json::JsonValue payload;
        if (request.QueueUrlHasBeenSet()) {
            payload.WithString("QueueUrl", request.GetQueueUrl());
        }
        if (request.ReceiptHandleHasBeenSet()) {
            payload.WithString("ReceiptHandle", request.GetReceiptHandle());
        }
        const auto response = ExecuteJsonRequest("DeleteMessage", payload,
            request.GetAdditionalCustomHeaders(), request.GetQueueUrl());
        if (!response.IsSuccess()) {
            return DeleteMessageOutcome(response.GetError());
        }
        return DeleteMessageOutcome(Aws::NoResult{});
    }

    ChangeMessageVisibilityOutcome TSQSJsonClient::ChangeMessageVisibility(const ChangeMessageVisibilityRequest& request) const {
        Aws::Utils::Json::JsonValue payload;
        if (request.QueueUrlHasBeenSet()) {
            payload.WithString("QueueUrl", request.GetQueueUrl());
        }
        if (request.ReceiptHandleHasBeenSet()) {
            payload.WithString("ReceiptHandle", request.GetReceiptHandle());
        }
        if (request.VisibilityTimeoutHasBeenSet()) {
            payload.WithInteger("VisibilityTimeout", request.GetVisibilityTimeout());
        }
        const auto response = ExecuteJsonRequest("ChangeMessageVisibility", payload,
            request.GetAdditionalCustomHeaders(), request.GetQueueUrl());
        if (!response.IsSuccess()) {
            return ChangeMessageVisibilityOutcome(response.GetError());
        }
        return ChangeMessageVisibilityOutcome(Aws::NoResult{});
    }

    ChangeMessageVisibilityBatchOutcome TSQSJsonClient::ChangeMessageVisibilityBatch(const ChangeMessageVisibilityBatchRequest& request) const {
        Aws::Utils::Json::JsonValue payload;
        if (request.QueueUrlHasBeenSet()) {
            payload.WithString("QueueUrl", request.GetQueueUrl());
        }
        Aws::Utils::Array<Aws::Utils::Json::JsonValue> entries(request.GetEntries().size());
        for (size_t i = 0; i < request.GetEntries().size(); ++i) {
            const auto& entry = request.GetEntries()[i];
            entries[i].WithString("Id", entry.GetId());
            entries[i].WithString("ReceiptHandle", entry.GetReceiptHandle());
            if (entry.VisibilityTimeoutHasBeenSet()) {
                entries[i].WithInteger("VisibilityTimeout", entry.GetVisibilityTimeout());
            }
        }
        payload.WithArray("Entries", std::move(entries));
        const auto response = ExecuteJsonRequest("ChangeMessageVisibilityBatch", payload,
            request.GetAdditionalCustomHeaders(), request.GetQueueUrl());
        if (!response.IsSuccess()) {
            return ChangeMessageVisibilityBatchOutcome(response.GetError());
        }
        ChangeMessageVisibilityBatchResult result;
        const auto view = response.GetResult().View();
        if (view.KeyExists("Successful")) {
            const auto entries = view.GetArray("Successful");
            for (size_t i = 0; i < entries.GetLength(); ++i) {
                result.AddSuccessful(ChangeMessageVisibilityBatchResultEntry().WithId(entries[i].GetString("Id")));
            }
        }
        if (view.KeyExists("Failed")) {
            const auto entries = view.GetArray("Failed");
            for (size_t i = 0; i < entries.GetLength(); ++i) {
                BatchResultErrorEntry entry;
                entry.SetId(entries[i].GetString("Id"));
                entry.SetCode(entries[i].GetString("Code"));
                entry.SetSenderFault(entries[i].GetBool("SenderFault"));
                if (entries[i].KeyExists("Message")) {
                    entry.SetMessage(entries[i].GetString("Message"));
                }
                result.AddFailed(std::move(entry));
            }
        }
        return ChangeMessageVisibilityBatchOutcome(std::move(result));
    }

    GetQueueAttributesOutcome TSQSJsonClient::GetQueueAttributes(const GetQueueAttributesRequest& request) const {
        Aws::Utils::Json::JsonValue payload;
        if (request.QueueUrlHasBeenSet()) {
            payload.WithString("QueueUrl", request.GetQueueUrl());
        }
        if (request.AttributeNamesHasBeenSet()) {
            Aws::Utils::Array<Aws::Utils::Json::JsonValue> names(request.GetAttributeNames().size());
            for (size_t i = 0; i < request.GetAttributeNames().size(); ++i) {
                names[i].AsString(QueueAttributeNameMapper::GetNameForQueueAttributeName(request.GetAttributeNames()[i]));
            }
            payload.WithArray("AttributeNames", std::move(names));
        }
        const auto response = ExecuteJsonRequest("GetQueueAttributes", payload,
            request.GetAdditionalCustomHeaders(), request.GetQueueUrl());
        if (!response.IsSuccess()) {
            return GetQueueAttributesOutcome(response.GetError());
        }
        GetQueueAttributesResult result;
        const auto view = response.GetResult().View();
        if (view.KeyExists("Attributes")) {
            for (const auto& [name, value] : view.GetObject("Attributes").GetAllObjects()) {
                result.AddAttributes(QueueAttributeNameMapper::GetQueueAttributeNameForName(name), value.AsString());
            }
        }
        return GetQueueAttributesOutcome(std::move(result));
    }

    ListQueuesOutcome TSQSJsonClient::ListQueues(const ListQueuesRequest& request) const {
        Aws::Utils::Json::JsonValue payload;
        if (request.QueueNamePrefixHasBeenSet()) {
            payload.WithString("QueueNamePrefix", request.GetQueueNamePrefix());
        }
        if (request.NextTokenHasBeenSet()) {
            payload.WithString("NextToken", request.GetNextToken());
        }
        if (request.MaxResultsHasBeenSet()) {
            payload.WithInteger("MaxResults", request.GetMaxResults());
        }
        const auto response = ExecuteJsonRequest("ListQueues", payload,
            request.GetAdditionalCustomHeaders(), EndpointOverride);
        if (!response.IsSuccess()) {
            return ListQueuesOutcome(response.GetError());
        }
        ListQueuesResult result;
        const auto view = response.GetResult().View();
        if (view.KeyExists("QueueUrls")) {
            const auto urls = view.GetArray("QueueUrls");
            for (size_t i = 0; i < urls.GetLength(); ++i) {
                result.AddQueueUrls(urls[i].AsString());
            }
        }
        if (view.KeyExists("NextToken")) {
            result.SetNextToken(view.GetString("NextToken"));
        }
        return ListQueuesOutcome(std::move(result));
    }

    PurgeQueueOutcome TSQSJsonClient::PurgeQueue(const PurgeQueueRequest& request) const {
        Aws::Utils::Json::JsonValue payload;
        if (request.QueueUrlHasBeenSet()) {
            payload.WithString("QueueUrl", request.GetQueueUrl());
        }
        const auto response = ExecuteJsonRequest("PurgeQueue", payload,
            request.GetAdditionalCustomHeaders(), request.GetQueueUrl());
        if (!response.IsSuccess()) {
            return PurgeQueueOutcome(response.GetError());
        }
        return PurgeQueueOutcome(Aws::NoResult{});
    }

    CreateQueueOutcome TSQSJsonClient::CreateQueue(const CreateQueueRequest& request) const {
        Aws::Utils::Json::JsonValue payload;
        if (request.QueueNameHasBeenSet()) {
            payload.WithString("QueueName", request.GetQueueName());
        }
        Aws::Utils::Json::JsonValue attributes;
        for (const auto& [name, value] : request.GetAttributes()) {
            attributes.WithString(QueueAttributeNameMapper::GetNameForQueueAttributeName(name), value);
        }
        if (request.AttributesHasBeenSet()) {
            payload.WithObject("Attributes", std::move(attributes));
        }
        if (request.TagsHasBeenSet()) {
            Aws::Utils::Json::JsonValue tags;
            for (const auto& [name, value] : request.GetTags()) {
                tags.WithString(name, value);
            }
            payload.WithObject("tags", std::move(tags));
        }
        const auto response = ExecuteJsonRequest("CreateQueue", payload,
            request.GetAdditionalCustomHeaders(), EndpointOverride);
        if (!response.IsSuccess()) {
            return CreateQueueOutcome(response.GetError());
        }
        CreateQueueResult result;
        const auto view = response.GetResult().View();
        if (view.KeyExists("QueueUrl")) {
            result.SetQueueUrl(view.GetString("QueueUrl"));
        }
        return CreateQueueOutcome(std::move(result));
    }

    DeleteQueueOutcome TSQSJsonClient::DeleteQueue(const DeleteQueueRequest& request) const {
        Aws::Utils::Json::JsonValue payload;
        if (request.QueueUrlHasBeenSet()) {
            payload.WithString("QueueUrl", request.GetQueueUrl());
        }
        const auto response = ExecuteJsonRequest("DeleteQueue", payload,
            request.GetAdditionalCustomHeaders(), request.GetQueueUrl());
        if (!response.IsSuccess()) {
            return DeleteQueueOutcome(response.GetError());
        }
        return DeleteQueueOutcome(Aws::NoResult{});
    }

    SetQueueAttributesOutcome TSQSJsonClient::SetQueueAttributes(const SetQueueAttributesRequest& request) const {
        Aws::Utils::Json::JsonValue payload;
        if (request.QueueUrlHasBeenSet()) {
            payload.WithString("QueueUrl", request.GetQueueUrl());
        }
        Aws::Utils::Json::JsonValue attributes;
        for (const auto& [name, value] : request.GetAttributes()) {
            attributes.WithString(QueueAttributeNameMapper::GetNameForQueueAttributeName(name), value);
        }
        if (request.AttributesHasBeenSet()) {
            payload.WithObject("Attributes", std::move(attributes));
        }
        const auto response = ExecuteJsonRequest("SetQueueAttributes", payload,
            request.GetAdditionalCustomHeaders(), request.GetQueueUrl());
        if (!response.IsSuccess()) {
            return SetQueueAttributesOutcome(response.GetError());
        }
        return SetQueueAttributesOutcome(Aws::NoResult{});
    }

} // namespace NYdb::NConsoleClient
