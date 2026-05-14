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

} // namespace NKikimr::NHttpProxy::NDataStreams