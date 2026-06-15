#include "sqs_serialization.h"
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/core/http_proxy/sqs_xml/params.h>
#include <ydb/core/http_proxy/sqs_xml/xml_builder.h>

#include <util/string/builder.h>
#include <ydb/public/api/protos/draft/ymq.pb.h>

namespace NKikimr::NHttpProxy::NSQS {

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
                } else if (name == "DeleteQueueBatchResult") {
                    const auto* r = dynamic_cast<const Ydb::Ymq::V1::DeleteMessageBatchResult*>(&value);
                    XML_ELEM("DeleteQueueBatchResponse") {
                        XML_ELEM("DeleteQueueBatchResult") {
                            for (const auto& entry : r->failed()) {
                                XML_ELEM("BatchResultErrorEntry") {
                                    XML_ELEM_CONT("Code", entry.code());
                                    XML_ELEM_CONT("Id", entry.id());
                                    XML_ELEM_CONT("Message", entry.message());
                                }
                            }
                            for (const auto& entry : r->successful()) {
                                XML_ELEM("DeleteQueueBatchResultEntry") {
                                    XML_ELEM_CONT("Id", entry.id());
                                }
                            }
                        }
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
                } else if (name == "ListQueuesResult") {
                    const auto* r = dynamic_cast<const Ydb::Ymq::V1::ListQueuesResult*>(&value);
                    XML_ELEM("ListQueuesResponse") {
                        XML_ELEM("ListQueuesResult") {
                            for (const auto& queueUrl : r->queue_urls()) {
                                XML_ELEM_CONT("QueueUrl", queueUrl);
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
                } else {
                    Y_ENSURE(false);
                }
            }
        }

        result << XML_RESULT();
        return result;
    }

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


    TString Serialize(const MimeTypes mimeType, TErrorResponse&& value) {
        NJson::TJsonValue json;
        json.SetType(NJson::JSON_MAP);
        json["message"] = value.ErrorText;
        json["__type"] = value.StatusCode;

        switch (mimeType) {
        case MIME_CBOR:
            return SerializeCbor(json);
        case MIME_JSON:
            [[fallthrough]];
        default:
            return SerializeJson(json);
        }
    }

} // namespace NKikimr::NHttpProxy::NSQS
