#include "sqs_serialization.h"

namespace NKikimr::NHttpProxy::NSQS {

    TString Serialize(const MimeTypes mimeType, const NProtoBuf::Message& value) {
        switch (mimeType) {
        case MIME_XML:
            // TODO implement
            Y_ENSURE(false);
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
