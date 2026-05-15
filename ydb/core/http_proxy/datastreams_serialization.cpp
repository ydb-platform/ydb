#include "datastreams_serialization.h"

namespace NKikimr::NHttpProxy::NDataStreams {

    template<>
    void PrepareValue<Ydb::DataStreams::V1::ListStreamsRequest>(Ydb::DataStreams::V1::ListStreamsRequest& value) {
        value.set_recurse(true);
    }

    TString Serialize(const MimeTypes mimeType, const NProtoBuf::Message& value) {
        switch (mimeType) {
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
        json["__type"] = value.Exception.first;

        switch (mimeType) {
        case MIME_CBOR:
            return SerializeCbor(json);
        case MIME_JSON:
            [[fallthrough]];
        default:
            return SerializeJson(json);
        }
    }

} // namespace NKikimr::NHttpProxy::NDataStreams

namespace NKikimr::NHttpProxy {

    TString BuildError(MimeTypes mimeType, HttpCodes httpCode, const TString& errorName, const TString& errorText) {
        return Serialize(mimeType, NDataStreams::TErrorResponse{
            .Exception = TException{errorName, httpCode},
            .ErrorText = errorText
        });
    }

} // namespace NKikimr::NHttpProxy
