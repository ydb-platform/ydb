#pragma once

#include "http_req.h"
#include "serialization.h"

#include <ydb/core/http_proxy/sqs_xml/params.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/public/api/protos/draft/ymq.pb.h>


namespace NKikimr::NHttpProxy::NSQS {

    template<typename TValue>
    void PrepareValue(TValue& value) {
        Y_UNUSED(value);
    }

    void DeserializeXml(Ydb::Ymq::V1::ChangeMessageVisibilityRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::ChangeMessageVisibilityBatchRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::CreateQueueRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::DeleteMessageRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::DeleteMessageBatchRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::DeleteQueueRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::GetQueueAttributesRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::GetQueueUrlRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::ListDeadLetterSourceQueuesRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::ListQueuesRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::ListQueueTagsRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::PurgeQueueRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::ReceiveMessageRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::SendMessageRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::SendMessageBatchRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::SetQueueAttributesRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::TagQueueRequest& value, const TParameters& params);
    void DeserializeXml(Ydb::Ymq::V1::UntagQueueRequest& value, const TParameters& params);

    template<typename TValue>
    void DeserializeXml(TValue& message, const TStringBuf& input) {
        DeserializeXml(message, ParseParameters(input));
    }

    template<typename TValue>
    void Deserialize(const MimeTypes mimeType, TValue& value, const TStringBuf& input)
        requires std::is_base_of_v<NProtoBuf::Message, TValue> {

        if (input.empty()) {
            throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) << "Empty body";
        }

        PrepareValue(value);

        switch (mimeType) {
        case MIME_CBOR:
            DeserializeCbor(value, input);
            break;
        case MIME_JSON:
            DeserializeJson(value, input);
            break;
        case MIME_XML:
            DeserializeXml(value, input);
            break;
        default:
            throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) <<
                "Unknown ContentType";
        }
    };

    TString Serialize(const THttpRequestContext& httpContext, const NProtoBuf::Message& value);

    struct TErrorResponse {
        TString StatusCode;
        TString ErrorText;
    };
    TString Serialize(const THttpRequestContext& httpContext, TErrorResponse&& value);

} // namespace NKikimr::NHttpProxy::NSQS
