#include "sqs_serialization.h"
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <ydb/core/http_proxy/sqs_xml/params.h>
#include <ydb/core/http_proxy/sqs_xml/xml_builder.h>

#include <util/string/builder.h>
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NHttpProxy::NSQS {

    void DeserializeXml(Ydb::Ymq::V1::ChangeMessageVisibilityRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
        if (params.ReceiptHandle) {
            value.set_receipt_handle(CGIEscapeRet(*params.ReceiptHandle));
        }
        if (params.VisibilityTimeout) {
            value.set_visibility_timeout(*params.VisibilityTimeout);
        }
    }

    void DeserializeXml(Ydb::Ymq::V1::ChangeMessageVisibilityBatchRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
        for (const auto& [_, item] : params.BatchEntries) {
            auto* entry = value.add_entries();
            if (item.Id) {
                entry->set_id(*item.Id);
            }
            if (item.ReceiptHandle) {
                entry->set_receipt_handle(CGIEscapeRet(*item.ReceiptHandle));
            }
            if (item.VisibilityTimeout) {
                entry->set_visibility_timeout(*item.VisibilityTimeout);
            }
        }
    }

    void DeserializeXml(Ydb::Ymq::V1::CreateQueueRequest& value, const TParameters& params) {
        value.set_queue_name(params.QueueName.GetOrElse(""));
        for (const auto& [_, attr] : params.Attributes) {
            value.mutable_attributes()->insert({attr.Name.GetOrElse(""), attr.Value.GetOrElse("")});
        }
        for (const auto& [_, tag] : params.Tags) {
            value.mutable_tags()->insert({tag.Key.GetOrElse(""), tag.Value.GetOrElse("")});
        }
    }

    void DeserializeXml(Ydb::Ymq::V1::DeleteMessageRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
        if (params.ReceiptHandle) {
            value.set_receipt_handle(CGIEscapeRet(*params.ReceiptHandle));
        }
    }

    void DeserializeXml(Ydb::Ymq::V1::DeleteMessageBatchRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
        for (const auto& [_, item] : params.BatchEntries) {
            auto* entry = value.add_entries();
            if (item.Id) {
                entry->set_id(*item.Id);
            }
            if (item.ReceiptHandle) {
                entry->set_receipt_handle(CGIEscapeRet(*item.ReceiptHandle));
            }
        }
    }

    void DeserializeXml(Ydb::Ymq::V1::DeleteQueueRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
    }

    void DeserializeXml(Ydb::Ymq::V1::GetQueueAttributesRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
        for (const auto& name : params.AttributeNames) {
            value.add_attribute_names(name.second);
        }
    }

    void DeserializeXml(Ydb::Ymq::V1::GetQueueUrlRequest& value, const TParameters& params) {
        value.set_queue_name(params.QueueName.GetOrElse(""));
    }

    void DeserializeXml(Ydb::Ymq::V1::ListDeadLetterSourceQueuesRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
        if (params.MaxResults) {
            value.set_max_results(*params.MaxResults);
        }
        if (params.NextToken) {
            value.set_next_token(*params.NextToken);
        }
    }

    void DeserializeXml(Ydb::Ymq::V1::ListQueuesRequest& value, const TParameters& params) {
        if (params.QueueNamePrefix) {
            value.set_queue_name_prefix(*params.QueueNamePrefix);
        }
        if (params.MaxResults) {
            value.set_max_results(*params.MaxResults);
        }
        if (params.NextToken) {
            value.set_next_token(*params.NextToken);
        }
    }

    void DeserializeXml(Ydb::Ymq::V1::ListQueueTagsRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
    }

    void DeserializeXml(Ydb::Ymq::V1::PurgeQueueRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
    }

    void DeserializeXml(Ydb::Ymq::V1::ReceiveMessageRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
        value.set_max_number_of_messages(params.MaxNumberOfMessages.GetOrElse(1));
        if (params.ReceiveRequestAttemptId) {
            value.set_receive_request_attempt_id(*params.ReceiveRequestAttemptId);
        }
        if (params.VisibilityTimeout) {
            value.set_visibility_timeout(*params.VisibilityTimeout);
        }
        if (params.WaitTimeSeconds) {
            value.set_wait_time_seconds(*params.WaitTimeSeconds);
        }
        for (const auto& name : params.AttributeNames) {
            value.add_attribute_names(name.second);
        }
        for (const auto& [_, item] : params.MessageAttributes) {
            value.add_message_attribute_names(item.Name);
        }
    }

    void DeserializeXml(Ydb::Ymq::V1::SendMessageRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
        if (params.DelaySeconds) {
            value.set_delay_seconds(*params.DelaySeconds);
        }
        if (params.MessageBody) {
            value.set_message_body(*params.MessageBody);
        }
        if (params.MessageDeduplicationId) {
            value.set_message_deduplication_id(*params.MessageDeduplicationId);
        }
        if (params.MessageGroupId) {
            value.set_message_group_id(*params.MessageGroupId);
        }
        for (const auto& [_, item] : params.Attributes) {
            Ydb::Ymq::V1::MessageAttribute attr;
            attr.set_string_value(item.Value.GetOrElse(""));
            value.mutable_message_system_attributes()->insert({item.Name.GetOrElse(""), attr});
        }
        for (const auto& [_, item] : params.MessageAttributes) {
            Ydb::Ymq::V1::MessageAttribute attr;
            if (item.DataType) {
                attr.set_data_type(*item.DataType);
            }
            if (item.StringValue) {
                attr.set_string_value(*item.StringValue);
            }
            if (item.BinaryValue) {
                attr.set_binary_value(*item.BinaryValue);
            }
            value.mutable_message_attributes()->insert({item.Name, attr});
        }
    }

    void DeserializeXml(Ydb::Ymq::V1::SendMessageBatchRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
        for (const auto& [_, paramsEntry] : params.BatchEntries) {
            auto& entry = *value.add_entries();
            if (paramsEntry.Id) {
                entry.set_id(*paramsEntry.Id);
            }
            if (paramsEntry.DelaySeconds) {
                entry.set_delay_seconds(*paramsEntry.DelaySeconds);
            }
            if (paramsEntry.MessageBody) {
                entry.set_message_body(*paramsEntry.MessageBody);
            }
            if (paramsEntry.MessageDeduplicationId) {
                entry.set_message_deduplication_id(*paramsEntry.MessageDeduplicationId);
            }
            if (paramsEntry.MessageGroupId) {
                entry.set_message_group_id(*paramsEntry.MessageGroupId);
            }
            for (const auto& [_, item] : paramsEntry.Attributes) {
                Ydb::Ymq::V1::MessageAttribute attr;
                attr.set_string_value(item.Value.GetOrElse(""));
                entry.mutable_message_system_attributes()->insert({item.Name.GetOrElse(""), attr});
            }
            for (const auto& [_, item] : paramsEntry.MessageAttributes) {
                Ydb::Ymq::V1::MessageAttribute attr;
                if (item.DataType) {
                    attr.set_data_type(*item.DataType);
                }
                if (item.StringValue) {
                    attr.set_string_value(*item.StringValue);
                }
                if (item.BinaryValue) {
                    attr.set_binary_value(*item.BinaryValue);
                }
                entry.mutable_message_attributes()->insert({item.Name, attr});
            }
        }
    }

    void DeserializeXml(Ydb::Ymq::V1::SetQueueAttributesRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
        for (const auto& [_, attr] : params.Attributes) {
            value.mutable_attributes()->insert({attr.Name.GetOrElse(""), attr.Value.GetOrElse("")});
        }
    }

    void DeserializeXml(Ydb::Ymq::V1::TagQueueRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
        for (const auto& [_, tag] : params.Tags) {
            value.mutable_tags()->insert({tag.Key.GetOrElse(""), tag.Value.GetOrElse("")});
        }
    }

    void DeserializeXml(Ydb::Ymq::V1::UntagQueueRequest& value, const TParameters& params) {
        value.set_queue_url(params.QueueUrl.GetOrElse(""));
        for (const auto& key : params.TagKeys) {
            value.add_tag_keys(key.second);
        }
    }

    TString SerializeXml(const THttpRequestContext& httpContext, const NProtoBuf::Message& value) {
        auto name = value.GetDescriptor()->name();
        TStringBuilder result;

        XML_BUILDER() {
            XML_DOC() {
                if (name == "ChangeMessageVisibilityResult") {
                    XML_ELEM("ChangeMessageVisibilityResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId",  httpContext.RequestId);
                        }
                    }
                } else if (name == "ChangeMessageVisibilityBatchResult") {
                    const auto* r = dynamic_cast<const Ydb::Ymq::V1::ChangeMessageVisibilityBatchResult*>(&value);
                    XML_ELEM("ChangeMessageVisibilityBatchResponse") {
                        XML_ELEM("ChangeMessageVisibilityBatchResult") {
                            for (const auto& entry : r->failed()) {
                                XML_ELEM("BatchResultErrorEntry") {
                                    XML_ELEM_CONT("Code", entry.code());
                                    XML_ELEM_CONT("Id", entry.id());
                                    XML_ELEM_CONT("Message", entry.message());
                                }
                            }
                            for (const auto& entry : r->successful()) {
                                XML_ELEM("ChangeMessageVisibilityBatchResultEntry") {
                                    XML_ELEM_CONT("Id", entry.id());
                                }
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "CreateQueueResult") {
                    const auto* r = dynamic_cast<const Ydb::Ymq::V1::CreateQueueResult*>(&value);
                    XML_ELEM("CreateQueueResponse") {
                        XML_ELEM("CreateQueueResult") {
                            XML_ELEM_CONT("QueueUrl", r->queue_url());
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "DeleteMessageResult") {
                    XML_ELEM("DeleteMessageResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "DeleteMessageBatchResult") {
                    const auto* r = dynamic_cast<const Ydb::Ymq::V1::DeleteMessageBatchResult*>(&value);
                    XML_ELEM("DeleteMessageBatchResponse") {
                        XML_ELEM("DeleteMessageBatchResult") {
                            for (const auto& entry : r->failed()) {
                                XML_ELEM("BatchResultErrorEntry") {
                                    XML_ELEM_CONT("Code", entry.code());
                                    XML_ELEM_CONT("Id", entry.id());
                                    XML_ELEM_CONT("Message", entry.message());
                                }
                            }
                            for (const auto& entry : r->successful()) {
                                XML_ELEM("DeleteMessageBatchResultEntry") {
                                    XML_ELEM_CONT("Id", entry.id());
                                }
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "DeleteQueueResult") {
                    XML_ELEM("DeleteQueueResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "GetQueueAttributesResult") {
                    const auto* r = dynamic_cast<const Ydb::Ymq::V1::GetQueueAttributesResult*>(&value);
                    XML_ELEM("GetQueueAttributesResponse") {
                        XML_ELEM("GetQueueAttributesResult") {
                            for (const auto& [key, value] : r->attributes()) {
                                XML_ELEM("Attribute") {
                                    XML_ELEM_CONT("Name", key);
                                    XML_ELEM_CONT("Value", value);
                                }
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "GetQueueUrlResult") {
                    const auto* r = dynamic_cast<const Ydb::Ymq::V1::GetQueueUrlResult*>(&value);
                    XML_ELEM("GetQueueUrlResponse") {
                        XML_ELEM("GetQueueUrlResult") {
                            XML_ELEM_CONT("QueueUrl", r->queue_url());
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "ListDeadLetterSourceQueuesResult") {
                    const auto* r = dynamic_cast<const Ydb::Ymq::V1::ListDeadLetterSourceQueuesResult*>(&value);
                    XML_ELEM("ListDeadLetterSourceQueuesResponse") {
                        XML_ELEM("ListDeadLetterSourceQueuesResult") {
                            for (const auto& queueUrl : r->queue_urls()) {
                                XML_ELEM_CONT("QueueUrl", queueUrl);
                            }
                            if (!r->next_token().empty()) {
                                XML_ELEM_CONT("NextToken", r->next_token());
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "ListQueuesResult") {
                    const auto* r = dynamic_cast<const Ydb::Ymq::V1::ListQueuesResult*>(&value);
                    XML_ELEM("ListQueuesResponse") {
                        XML_ELEM("ListQueuesResult") {
                            for (const auto& queueUrl : r->queue_urls()) {
                                XML_ELEM_CONT("QueueUrl", queueUrl);
                            }
                            if (!r->next_token().empty()) {
                                XML_ELEM_CONT("NextToken", r->next_token());
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "ListQueueTagsResult") {
                    const auto* r = dynamic_cast<const Ydb::Ymq::V1::ListQueueTagsResult*>(&value);
                    XML_ELEM("ListQueueTagsResponse") {
                        XML_ELEM("ListQueueTagsResult") {
                            for (const auto& [key, value] : r->tags()) {
                                XML_ELEM("Tag") {
                                    XML_ELEM_CONT("Key", key);
                                    XML_ELEM_CONT("Value", value);
                                }
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "PurgeQueueResult") {
                    XML_ELEM("PurgeQueueResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "ReceiveMessageResult") {
                    const auto* r = dynamic_cast<const Ydb::Ymq::V1::ReceiveMessageResult*>(&value);
                    XML_ELEM("ReceiveMessageResponse") {
                        XML_ELEM("ReceiveMessageResult") {
                            for (const auto& message : r->messages()) {
                                XML_ELEM("Message") {
                                    XML_ELEM_CONT("MessageId", message.message_id());
                                    XML_ELEM_CONT("ReceiptHandle", message.receipt_handle());
                                    if (!message.m_d_5_of_body().empty()) {
                                        XML_ELEM_CONT("MD5OfBody", message.m_d_5_of_body());
                                    }
                                    if (!message.m_d_5_of_message_attributes().empty()) {
                                        XML_ELEM_CONT("MD5OfMessageAttributes", message.m_d_5_of_message_attributes());
                                    }
                                    XML_ELEM_CONT("Body", message.body());
                                    for (const auto& [key, value] : message.attributes()) {
                                        XML_ELEM("Attribute") {
                                            XML_ELEM_CONT("Name", key);
                                            XML_ELEM_CONT("Value", value);
                                        }
                                    }
                                    for (const auto& [key, value] : message.message_attributes()) {
                                        XML_ELEM("MessageAttribute") {
                                            XML_ELEM_CONT("Name", key);
                                            XML_ELEM("Value") {
                                                if (!value.data_type().empty()) {
                                                    XML_ELEM_CONT("DataType", value.data_type());
                                                }
                                                if (!value.string_value().empty()) {
                                                    XML_ELEM_CONT("StringValue", value.string_value());
                                                } else if (!value.binary_value().empty()) {
                                                    XML_ELEM_CONT("BinaryValue", Base64Encode(value.binary_value()));
                                                } else if (value.string_list_values_size()) {
                                                    for (const auto& item : value.string_list_values()) {
                                                        XML_ELEM_CONT("StringListValue", item);
                                                    }
                                                } else if (value.binary_list_values_size()) {
                                                    for (const auto& item : value.binary_list_values()) {
                                                        XML_ELEM_CONT("BinaryListValue", Base64Encode(item));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "SendMessageResult") {
                    const auto* r = dynamic_cast<const Ydb::Ymq::V1::SendMessageResult*>(&value);
                    XML_ELEM("SendMessageResponse") {
                        XML_ELEM("SendMessageResult") {
                            XML_ELEM_CONT("MD5OfMessageBody", r->m_d_5_of_message_body());
                            if (!r->m_d_5_of_message_attributes().empty()) {
                                XML_ELEM_CONT("MD5OfMessageAttributes", r->m_d_5_of_message_attributes());
                            }
                            XML_ELEM_CONT("MessageId", r->message_id());
                            if (!r->sequence_number().empty()) {
                                XML_ELEM_CONT("SequenceNumber", ToString(r->sequence_number()));
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "SendMessageBatchResult") {
                    const auto* r = dynamic_cast<const Ydb::Ymq::V1::SendMessageBatchResult*>(&value);
                    XML_ELEM("SendMessageBatchResponse") {
                        XML_ELEM("SendMessageBatchResult") {
                            for (const auto& entry : r->failed()) {
                                XML_ELEM("BatchResultErrorEntry") {
                                    XML_ELEM_CONT("Code", entry.code());
                                    XML_ELEM_CONT("Id", entry.id());
                                    XML_ELEM_CONT("Message", entry.message());
                                }
                            }
                            for (const auto& entry : r->successful()) {
                                XML_ELEM("SendMessageBatchResultEntry") {
                                    XML_ELEM_CONT("Id", entry.id());
                                    if (!entry.m_d_5_of_message_body().empty()) {
                                        XML_ELEM_CONT("MD5OfMessageBody", entry.m_d_5_of_message_body());
                                    }
                                    if (!entry.m_d_5_of_message_attributes().empty()) {
                                        XML_ELEM_CONT("MD5OfMessageAttributes", entry.m_d_5_of_message_attributes());
                                    }
                                    XML_ELEM_CONT("MessageId", entry.message_id());
                                    XML_ELEM_CONT("SequenceNumber", entry.sequence_number());
                                }
                            }
                        }
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "SetQueueAttributesResult") {
                    XML_ELEM("SetQueueAttributesResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "TagQueueResult") {
                    XML_ELEM("TagQueueResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else if (name == "UntagQueueResult") {
                    XML_ELEM("UntagQueueResponse") {
                        XML_ELEM("ResponseMetadata") {
                            XML_ELEM_CONT("RequestId", httpContext.RequestId);
                        }
                    }
                } else {
                    Y_VERIFY_DEBUG_S(false, name.c_str());
                }
            }
        }

        result << XML_RESULT();

        return result;
    }

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

    // https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
    // https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/CommonErrors.html - list of error codes
    TString Serialize(const THttpRequestContext& httpContext, const NProtoBuf::Message& value) {
        switch (httpContext.ContentType) {
        case MIME_XML:
            return SerializeXml(httpContext, value);
        case MIME_CBOR:
            return SerializeCbor(value);
        case MIME_JSON:
            [[fallthrough]];
        default:
            return SerializeJson(value);
        }
    }

    TString Serialize(const THttpRequestContext& httpContext, TErrorResponse&& value) {
        auto makeJson = [&]() {
            NJson::TJsonValue json;
            json.SetType(NJson::JSON_MAP);
            json["message"] = value.ErrorText;
            json["__type"] = value.StatusCode;
            return json;
        };

        switch (httpContext.ContentType) {
        case MIME_XML:
            return BuildErrorXmlString(value.ErrorText, value.StatusCode, httpContext.RequestId);
        case MIME_CBOR:
            return SerializeCbor(makeJson());
        case MIME_JSON:
            [[fallthrough]];
        default:
            return SerializeJson(makeJson());
        }
    }

} // namespace NKikimr::NHttpProxy::NSQS
