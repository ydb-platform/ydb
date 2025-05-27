#include "xml.h"
#include "xml_builder.h"

#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/core/protos/sqs.pb.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NSQS {

using NKikimrClient::TSqsResponse;

// https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
// https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList - list of error codes
TString BuildErrorXmlString(const TString& message, const TString& errorCode, const TString& requestId) {
    XML_BUILDER() {
        XML_DOC() {
            XML_ELEM("ErrorResponse") {
                XML_ELEM("Error") {
                    XML_ELEM_CONT("Message", message);
                    XML_ELEM_CONT("Code", errorCode);
                }
                XML_ELEM_CONT("RequestId", requestId);
            }
        }
    }
    return XML_RESULT();
}

TString BuildErrorXmlString(const TError& error, const TString& requestId) {
    return BuildErrorXmlString(error.GetMessage(), error.GetErrorCode(), requestId);
}

static void AddError(const TString& errorCode, TUserCounters* userCounters) {
    if (userCounters) {
        if (auto* detailed = userCounters->GetDetailedCounters()) {
            detailed->APIStatuses.AddError(errorCode);
        }
    }
}

TSqsHttpResponse MakeErrorXmlResponse(const TErrorClass& errorClass, TUserCounters* userCounters, const TString& message, const TString& requestId) {
    AddError(errorClass.ErrorCode, userCounters);
    return TSqsHttpResponse(BuildErrorXmlString(message.empty() ? errorClass.DefaultMessage : message, errorClass.ErrorCode, requestId), errorClass.HttpStatusCode);
}

TSqsHttpResponse MakeErrorXmlResponseFromCurrentException(TUserCounters* userCounters, const TString& requestId) {
    try {
        try {
            throw;
        } catch (const TSQSException& ex) {
            return MakeErrorXmlResponse(ex.ErrorClass, userCounters, ex.what(), requestId);
        } catch (const std::exception&) {
            return MakeErrorXmlResponse(NErrors::INTERNAL_FAILURE, userCounters, TString(), requestId);
        }
    } catch (const TWriteXmlError& error) {
        AddError("InternalFailure", userCounters);
        return TSqsHttpResponse("InternalFailure", 500, PLAIN_TEXT_CONTENT_TYPE);
    }
}

template <typename T>
static bool MaybeErrorResponse(const T& resp, TStringBuilder& builder) {
    if (resp.HasError()) {
        builder << BuildErrorXmlString(resp.GetError(), resp.GetRequestId());
        return true;
    }
    return false;
}

static TString BoolToString(const bool b) {
    return TString(b ? "true" : "false");
}

void WriteQueueTagsToXml(const TListQueueTagsResponse& rec, TXmlStringBuilder& xmlBuilder) {
    for (size_t i = 0; i < rec.TagsSize(); ++i) {
        const auto& tag = rec.GetTags(i);
        XML_ELEM("Tag") {
            XML_ELEM_CONT("Key", tag.GetKey());
            XML_ELEM_CONT("Value", tag.GetValue());
        }
    }
}

void WriteQueueAttributesToXml(const TGetQueueAttributesResponse& rec, TXmlStringBuilder& xmlBuilder) {
    if (rec.HasApproximateNumberOfMessages()) {
        XML_ELEM("Attribute") {
            XML_ELEM_CONT("Name", "ApproximateNumberOfMessages");
            XML_ELEM_CONT("Value", ToString(rec.GetApproximateNumberOfMessages()));
        }
    }
    if (rec.HasApproximateNumberOfMessagesDelayed()) {
        XML_ELEM("Attribute") {
            XML_ELEM_CONT("Name", "ApproximateNumberOfMessagesDelayed");
            XML_ELEM_CONT("Value", ToString(rec.GetApproximateNumberOfMessagesDelayed()));
        }
    }
    if (rec.HasApproximateNumberOfMessagesNotVisible()) {
        XML_ELEM("Attribute") {
            XML_ELEM_CONT("Name", "ApproximateNumberOfMessagesNotVisible");
            XML_ELEM_CONT("Value", ToString(rec.GetApproximateNumberOfMessagesNotVisible()));
        }
    }
    if (rec.HasContentBasedDeduplication()) {
        XML_ELEM("Attribute") {
            XML_ELEM_CONT("Name", "ContentBasedDeduplication");
            XML_ELEM_CONT("Value", BoolToString(rec.GetContentBasedDeduplication()));
        }
    }
    if (rec.HasCreatedTimestamp()) {
        XML_ELEM("Attribute") {
            XML_ELEM_CONT("Name", "CreatedTimestamp");
            XML_ELEM_CONT("Value", ToString(rec.GetCreatedTimestamp()));
        }
    }
    if (rec.HasDelaySeconds()) {
        XML_ELEM("Attribute") {
            XML_ELEM_CONT("Name", "DelaySeconds");
            XML_ELEM_CONT("Value", ToString(rec.GetDelaySeconds()));
        }
    }
    if (rec.HasFifoQueue()) {
        XML_ELEM("Attribute") {
            XML_ELEM_CONT("Name", "FifoQueue");
            XML_ELEM_CONT("Value", BoolToString(rec.GetFifoQueue()));
        }
    }
    if (rec.HasMaximumMessageSize()) {
        XML_ELEM("Attribute") {
            XML_ELEM_CONT("Name", "MaximumMessageSize");
            XML_ELEM_CONT("Value", ToString(rec.GetMaximumMessageSize()));
        }
    }
    if (rec.HasMessageRetentionPeriod()) {
        XML_ELEM("Attribute") {
            XML_ELEM_CONT("Name", "MessageRetentionPeriod");
            XML_ELEM_CONT("Value", ToString(rec.GetMessageRetentionPeriod()));
        }
    }
    if (rec.HasReceiveMessageWaitTimeSeconds()) {
        XML_ELEM("Attribute") {
            XML_ELEM_CONT("Name", "ReceiveMessageWaitTimeSeconds");
            XML_ELEM_CONT("Value", ToString(rec.GetReceiveMessageWaitTimeSeconds()));
        }
    }
    if (rec.HasVisibilityTimeout()) {
        XML_ELEM("Attribute") {
            XML_ELEM_CONT("Name", "VisibilityTimeout");
            XML_ELEM_CONT("Value", ToString(rec.GetVisibilityTimeout()));
        }
    }
    if (rec.HasRedrivePolicy()) {
        XML_ELEM("Attribute") {
            XML_ELEM_CONT("Name", "RedrivePolicy");
            XML_ELEM_CONT("Value", ToString(rec.GetRedrivePolicy()));
        }
    }
    if (rec.HasQueueArn()) {
        XML_ELEM("Attribute") {
            XML_ELEM_CONT("Name", "QueueArn");
            XML_ELEM_CONT("Value", ToString(rec.GetQueueArn()));
        }
    }
}

TSqsHttpResponse ResponseToAmazonXmlFormat(const TSqsResponse& resp) {
    TStringBuilder result;

#define HANDLE_ERROR(METHOD)                                                               \
    if (MaybeErrorResponse(resp.Y_CAT(Get, METHOD)(), result)) {                           \
        return TSqsHttpResponse(result, resp.Y_CAT(Get, METHOD)().GetError().GetStatus()); \
    }                                                                                      \
    /**/

    switch (resp.GetResponseCase()) {
        case TSqsResponse::kChangeMessageVisibility: {
            HANDLE_ERROR(ChangeMessageVisibility);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("ChangeMessageVisibilityResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetChangeMessageVisibility().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kChangeMessageVisibilityBatch: {
            HANDLE_ERROR(ChangeMessageVisibilityBatch);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("ChangeMessageVisibilityBatchResponse") {
                        XML_ELEM("ChangeMessageVisibilityBatchResult") {
                            for (size_t i = 0; i < resp.GetChangeMessageVisibilityBatch().EntriesSize(); ++i) {
                                const auto& entry = resp.GetChangeMessageVisibilityBatch().GetEntries(i);
                                if (entry.HasError()) {
                                    XML_ELEM("BatchResultErrorEntry") {
                                        XML_ELEM_CONT("Code", entry.GetError().GetErrorCode());
                                        XML_ELEM_CONT("Id", entry.GetId());
                                        XML_ELEM_CONT("Message", entry.GetError().GetMessage());
                                    }
                                } else {
                                    XML_ELEM("ChangeMessageVisibilityBatchResultEntry") {
                                        XML_ELEM_CONT("Id", entry.GetId());
                                    }
                                }
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetChangeMessageVisibilityBatch().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kCreateQueue: {
            HANDLE_ERROR(CreateQueue);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("CreateQueueResponse") {
                        XML_ELEM("CreateQueueResult") {
                            XML_ELEM_CONT("QueueUrl", resp.GetCreateQueue().GetQueueUrl());
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetCreateQueue().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kDeleteMessage: {
            HANDLE_ERROR(DeleteMessage);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("DeleteMessageResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetDeleteMessage().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kDeleteMessageBatch: {
            HANDLE_ERROR(DeleteMessageBatch);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("DeleteMessageBatchResponse") {
                        XML_ELEM("DeleteMessageBatchResult") {
                            for (size_t i = 0; i < resp.GetDeleteMessageBatch().EntriesSize(); ++i) {
                                const auto& entry = resp.GetDeleteMessageBatch().GetEntries(i);
                                if (entry.HasError()) {
                                    XML_ELEM("BatchResultErrorEntry") {
                                        XML_ELEM_CONT("Code", entry.GetError().GetErrorCode());
                                        XML_ELEM_CONT("Id", entry.GetId());
                                        XML_ELEM_CONT("Message", entry.GetError().GetMessage());
                                    }
                                } else {
                                    XML_ELEM("DeleteMessageBatchResultEntry") {
                                        XML_ELEM_CONT("Id", entry.GetId());
                                    }
                                }
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetDeleteMessageBatch().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kDeleteQueue: {
            HANDLE_ERROR(DeleteQueue);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("DeleteQueueResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetDeleteQueue().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kDeleteQueueBatch: {
            HANDLE_ERROR(DeleteQueueBatch);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("DeleteQueueBatchResponse") {
                        XML_ELEM("DeleteQueueBatchResult") {
                            for (size_t i = 0; i < resp.GetDeleteQueueBatch().EntriesSize(); ++i) {
                                const auto& entry = resp.GetDeleteQueueBatch().GetEntries(i);
                                if (entry.HasError()) {
                                    XML_ELEM("BatchResultErrorEntry") {
                                        XML_ELEM_CONT("Code", entry.GetError().GetErrorCode());
                                        XML_ELEM_CONT("Id", entry.GetId());
                                        XML_ELEM_CONT("Message", entry.GetError().GetMessage());
                                    }
                                } else {
                                    XML_ELEM("DeleteQueueBatchResultEntry") {
                                        XML_ELEM_CONT("Id", entry.GetId());
                                    }
                                }
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetDeleteQueueBatch().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kGetQueueAttributes: {
            HANDLE_ERROR(GetQueueAttributes);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("GetQueueAttributesResponse") {
                        XML_ELEM("GetQueueAttributesResult") {
                            const auto& rec = resp.GetGetQueueAttributes();
                            WriteQueueAttributesToXml(rec, xmlBuilder);
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetGetQueueAttributes().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kGetQueueAttributesBatch: {
            HANDLE_ERROR(GetQueueAttributesBatch);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("GetQueueAttributesBatchResponse") {
                        XML_ELEM("GetQueueAttributesBatchResult") {
                            const auto& result = resp.GetGetQueueAttributesBatch();
                            for (const auto& entry : result.GetEntries()) {
                                if (entry.HasError()) {
                                    XML_ELEM("BatchResultErrorEntry") {
                                        XML_ELEM_CONT("Code", entry.GetError().GetErrorCode());
                                        XML_ELEM_CONT("Id", entry.GetId());
                                        XML_ELEM_CONT("Message", entry.GetError().GetMessage());
                                    }
                                } else {
                                    XML_ELEM("GetQueueAttributesBatchResultEntry") {
                                        XML_ELEM_CONT("Id", entry.GetId());
                                        WriteQueueAttributesToXml(entry, xmlBuilder);
                                    }
                                }
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetGetQueueAttributesBatch().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kGetQueueUrl: {
            HANDLE_ERROR(GetQueueUrl);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("GetQueueUrlResponse") {
                        XML_ELEM("GetQueueUrlResult") {
                            XML_ELEM_CONT("QueueUrl", resp.GetGetQueueUrl().GetQueueUrl());
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetGetQueueUrl().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kListQueues: {
            HANDLE_ERROR(ListQueues);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("ListQueuesResponse") {
                        XML_ELEM("ListQueuesResult") {
                            for (const auto& item : resp.GetListQueues().queues()) {
                                XML_ELEM_CONT("QueueUrl", item.GetQueueUrl());
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetListQueues().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kCountQueues: {
            HANDLE_ERROR(CountQueues);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("CountQueuesResponse") {
                        XML_ELEM("CountQueuesResult") {
                            XML_ELEM_CONT("Count", ::ToString(resp.GetCountQueues().GetCount()));
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetCountQueues().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kListUsers: {
            HANDLE_ERROR(ListUsers);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("ListUsersResponse") {
                        XML_ELEM("ListUsersResult") {
                            for (const auto& item : resp.GetListUsers().usernames()) {
                                XML_ELEM_CONT("UserName", item);
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetListUsers().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kPurgeQueue: {
            HANDLE_ERROR(PurgeQueue);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("PurgeQueueResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetPurgeQueue().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kPurgeQueueBatch: {
            HANDLE_ERROR(PurgeQueueBatch);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("PurgeQueueBatchResponse") {
                        XML_ELEM("PurgeQueueBatchResult") {
                            for (size_t i = 0; i < resp.GetPurgeQueueBatch().EntriesSize(); ++i) {
                                const auto& entry = resp.GetPurgeQueueBatch().GetEntries(i);
                                if (entry.HasError()) {
                                    XML_ELEM("BatchResultErrorEntry") {
                                        XML_ELEM_CONT("Code", entry.GetError().GetErrorCode());
                                        XML_ELEM_CONT("Id", entry.GetId());
                                        XML_ELEM_CONT("Message", entry.GetError().GetMessage());
                                    }
                                } else {
                                    XML_ELEM("PurgeQueueBatchResultEntry") {
                                        XML_ELEM_CONT("Id", entry.GetId());
                                    }
                                }
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetPurgeQueueBatch().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kReceiveMessage: {
            HANDLE_ERROR(ReceiveMessage);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("ReceiveMessageResponse") {
                        XML_ELEM("ReceiveMessageResult") {
                            for (size_t i = 0; i < resp.GetReceiveMessage().MessagesSize(); ++i) {
                                const auto& message = resp.GetReceiveMessage().GetMessages(i);
#define ATTRIBUTE(name, value)                                                    \
            XML_ELEM_IMPL("Attribute", Y_CAT(__LINE__, 1)) {                      \
                XML_ELEM_CONT_IMPL("Name", name, Y_CAT(__LINE__, 2));             \
                XML_ELEM_CONT_IMPL("Value", ToString(value), Y_CAT(__LINE__, 3)); \
            }

                                XML_ELEM("Message") {
                                    XML_ELEM_CONT("MessageId", message.GetMessageId());
                                    XML_ELEM_CONT("ReceiptHandle", message.GetReceiptHandle());
                                    XML_ELEM_CONT("MD5OfBody", message.GetMD5OfMessageBody());
                                    if (message.HasMD5OfMessageAttributes()) {
                                        XML_ELEM_CONT("MD5OfMessageAttributes", message.GetMD5OfMessageAttributes());
                                    }
                                    XML_ELEM_CONT("Body", message.GetData());

                                    // attributes
                                    if (message.HasSequenceNumber()) {
                                        ATTRIBUTE("SequenceNumber", message.GetSequenceNumber());
                                    }
                                    if (message.HasMessageDeduplicationId()) {
                                        ATTRIBUTE("MessageDeduplicationId", message.GetMessageDeduplicationId());
                                    }
                                    if (message.HasMessageGroupId()) {
                                        ATTRIBUTE("MessageGroupId", message.GetMessageGroupId());
                                    }
                                    if (message.HasApproximateFirstReceiveTimestamp()) {
                                        ATTRIBUTE("ApproximateFirstReceiveTimestamp", message.GetApproximateFirstReceiveTimestamp());
                                    }
                                    if (message.HasApproximateReceiveCount()) {
                                        ATTRIBUTE("ApproximateReceiveCount", message.GetApproximateReceiveCount());
                                    }
                                    if (message.HasSentTimestamp()) {
                                        ATTRIBUTE("SentTimestamp", message.GetSentTimestamp());
                                    }
                                    if (message.HasSenderId()) {
                                        ATTRIBUTE("SenderId", message.GetSenderId());
                                    }

                                    // message attributes
                                    for (const auto& attr : message.messageattributes()) {
                                        XML_ELEM("MessageAttribute") {
                                            XML_ELEM_CONT("Name", attr.GetName());
                                            XML_ELEM("Value") {
                                                if (attr.HasDataType()) {
                                                    XML_ELEM_CONT("DataType", attr.GetDataType());
                                                }
                                                if (attr.HasStringValue()) {
                                                    XML_ELEM_CONT("StringValue", attr.GetStringValue());
                                                }
                                                if (attr.HasBinaryValue()) {
                                                    XML_ELEM_CONT("BinaryValue", Base64Encode(attr.GetBinaryValue()));
                                                }
                                            }
                                        }
                                    }
                                }
#undef ATTRIBUTE
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetReceiveMessage().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kSendMessage: {
            HANDLE_ERROR(SendMessage);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("SendMessageResponse") {
                        XML_ELEM("SendMessageResult") {
                            XML_ELEM_CONT("MD5OfMessageBody", resp.GetSendMessage().GetMD5OfMessageBody());
                            if (resp.GetSendMessage().HasMD5OfMessageAttributes()) {
                                XML_ELEM_CONT("MD5OfMessageAttributes", resp.GetSendMessage().GetMD5OfMessageAttributes());
                            }
                            XML_ELEM_CONT("MessageId", resp.GetSendMessage().GetMessageId());
                            if (resp.GetSendMessage().HasSequenceNumber()) {
                                XML_ELEM_CONT("SequenceNumber", ToString(resp.GetSendMessage().GetSequenceNumber()));
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetSendMessage().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kSendMessageBatch: {
            HANDLE_ERROR(SendMessageBatch);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("SendMessageBatchResponse") {
                        XML_ELEM("SendMessageBatchResult") {
                            for (size_t i = 0; i < resp.GetSendMessageBatch().EntriesSize(); ++i) {
                                const auto& entry = resp.GetSendMessageBatch().GetEntries(i);
                                if (entry.HasError()) {
                                    XML_ELEM("BatchResultErrorEntry") {
                                        XML_ELEM_CONT("Code", entry.GetError().GetErrorCode());
                                        XML_ELEM_CONT("Id", entry.GetId());
                                        XML_ELEM_CONT("Message", entry.GetError().GetMessage());
                                    }
                                } else {
                                    XML_ELEM("SendMessageBatchResultEntry") {
                                        XML_ELEM_CONT("Id", entry.GetId());
                                        XML_ELEM_CONT("MD5OfMessageBody", entry.GetMD5OfMessageBody());
                                        XML_ELEM_CONT("MessageId", entry.GetMessageId());
                                        if (entry.HasSequenceNumber()) {
                                            XML_ELEM_CONT("SequenceNumber", ToString(entry.GetSequenceNumber()));
                                        }
                                        if (entry.HasMD5OfMessageAttributes()) {
                                            XML_ELEM_CONT("MD5OfMessageAttributes", entry.GetMD5OfMessageAttributes());
                                        }
                                    }
                                }
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetSendMessageBatch().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kSetQueueAttributes: {
            HANDLE_ERROR(SetQueueAttributes);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("SetQueueAttributesResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetSetQueueAttributes().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kCreateUser: {
            HANDLE_ERROR(CreateUser);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("CreateUserResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetCreateUser().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kDeleteUser: {
            HANDLE_ERROR(DeleteUser);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("DeleteUserResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetCreateUser().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kModifyPermissions: {
            HANDLE_ERROR(ModifyPermissions);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("ModifyPermissionsResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetModifyPermissions().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kListPermissions: {
            HANDLE_ERROR(ListPermissions);

            const auto& listPermissions = resp.GetListPermissions();

#define SERIALIZE_PERMISSIONS(resource, permission)                     \
    for (size_t i = 0; i < listPermissions.Y_CAT(Get, Y_CAT(resource, Permissions))().Y_CAT(Y_CAT(permission, s), Size)(); ++i) {     \
        XML_ELEM_IMPL("Ya" Y_STRINGIZE(permission), Y_CAT(__LINE__, a)) {                                                             \
            const auto& permissions = listPermissions.Y_CAT(Get, Y_CAT(resource, Permissions))().Y_CAT(Get, Y_CAT(permission, s))(i); \
            XML_ELEM_CONT_IMPL("Subject", permissions.GetSubject(),  Y_CAT(__LINE__, b));                                             \
            for (size_t j = 0; j < permissions.PermissionNamesSize(); ++j) {                                                          \
                XML_ELEM_CONT_IMPL("Permission", permissions.GetPermissionNames(j), Y_CAT(__LINE__, c));                              \
            }                                                                                                                         \
        }                                                                                                                             \
    }

#define SERIALIZE_PERMISSIONS_FOR_RESOURCE(resource)       \
    SERIALIZE_PERMISSIONS(resource, EffectivePermission);  \
    SERIALIZE_PERMISSIONS(resource, Permission);           \
    XML_ELEM_CONT("ResourceType", Y_STRINGIZE(resource));

            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("ListPermissionsResponse") {
                        XML_ELEM("YaListPermissionsResult") {
                            if (listPermissions.HasQueuePermissions()) {
                                SERIALIZE_PERMISSIONS_FOR_RESOURCE(Queue);
                            } else if (listPermissions.HasAccountPermissions()) {
                                SERIALIZE_PERMISSIONS_FOR_RESOURCE(Account);
                            }
                            XML_ELEM("ResponseMetadata") {
                                XML_ELEM_CONT("RequestId", listPermissions.GetRequestId());
                            }
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }
#undef SERIALIZE_PERMISSIONS_FOR_RESOURCE
#undef SERIALIZE_PERMISSIONS
        case TSqsResponse::kListDeadLetterSourceQueues: {
            HANDLE_ERROR(ListDeadLetterSourceQueues);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("ListDeadLetterSourceQueuesResponse") {
                        XML_ELEM("ListDeadLetterSourceQueuesResult") {
                            for (const auto& item : resp.GetListDeadLetterSourceQueues().queues()) {
                                XML_ELEM_CONT("QueueUrl", item.GetQueueUrl());
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetListDeadLetterSourceQueues().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kListQueueTags: {
            HANDLE_ERROR(ListQueueTags);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("ListQueueTagsResponse") {
                        XML_ELEM("ListQueueTagsResult") {
                            const auto& rec = resp.GetListQueueTags();
                            WriteQueueTagsToXml(rec, xmlBuilder);
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetListQueueTags().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kTagQueue: {
            HANDLE_ERROR(TagQueue);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("TagQueueResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetTagQueue().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::kUntagQueue: {
            HANDLE_ERROR(UntagQueue);
            XML_BUILDER() {
                XML_DOC() {
                    XML_ELEM("UntagQueueResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", resp.GetUntagQueue().GetRequestId());
                        }
                    }
                }
            }
            result << XML_RESULT();
            break;
        }

        case TSqsResponse::RESPONSE_NOT_SET: {
            return MakeErrorXmlResponse(NErrors::INTERNAL_FAILURE, nullptr, "Not implemented.");
        }
    }

    return TSqsHttpResponse(result, 200);

#undef HANDLE_ERROR
}

} // namespace NKikimr::NSQS
