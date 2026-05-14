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

} // namespace NKikimr::NHttpProxy::NSQS
