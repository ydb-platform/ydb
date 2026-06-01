#pragma once

#include "serialization.h"

#include <ydb/library/http_proxy/error/error.h>

namespace NKikimr::NHttpProxy::NSQS {

    template<typename TValue>
    void PrepareValue(TValue& value) {
        Y_UNUSED(value);
    }

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
        TString StatusCode;
        TString ErrorText;
    };
    TString Serialize(const MimeTypes mimeType, TErrorResponse&& value);

} // namespace NKikimr::NHttpProxy::NSQS
