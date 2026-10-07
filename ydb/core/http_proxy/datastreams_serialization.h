#pragma once

#include "exceptions_mapping.h"
#include "json_proto_conversion.h"
#include "serialization.h"

#include <ydb/library/http_proxy/error/error.h>
#include <ydb/public/api/protos/draft/datastreams.pb.h>

namespace NKikimr::NHttpProxy::NDataStreams {

    template<typename TValue>
    void PrepareValue(TValue& value) {
        Y_UNUSED(value);
    }

    template<>
    void PrepareValue<Ydb::DataStreams::V1::ListStreamsRequest>(Ydb::DataStreams::V1::ListStreamsRequest& value);

    template<typename TValue>
    void Deserialize(const MimeTypes mimeType, TValue& value, const TStringBuf& input)
        requires std::is_base_of_v<NProtoBuf::Message, TValue> {

        if (input.empty()) {
            throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) << "Empty body";
        }
    
        switch (mimeType) {
        case MIME_CBOR:
            PrepareValue(value);
            DeserializeCbor(value, input);
            break;
        case MIME_JSON:
            PrepareValue(value);
            DeserializeJson(value, input);
            break;
        default:
            throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) <<
                "Unknown ContentType";
        }
    };
                
    TString Serialize(const MimeTypes mimeType, const NProtoBuf::Message& value);

    struct TErrorResponse {
        TException Exception;
        TString ErrorText;
    };
    TString Serialize(const MimeTypes mimeType, TErrorResponse&& value);

} // namespace NKikimr::NHttpProxy::NDataStreams
