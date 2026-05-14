#include "serialization.h"
#include "json_proto_conversion.h"

#include <nlohmann/json.hpp>


namespace NKikimr::NHttpProxy {

void DeserializeCbor(NProtoBuf::Message& message, const TStringBuf& input) {
    auto fromCbor = nlohmann::json::from_cbor(
        input.begin(),
        input.end(),
        true, // strict mode
        false, // allow exceptions
        nlohmann::json::cbor_tag_handler_t::ignore
    );
    if (fromCbor.is_discarded()) {
        throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) <<
            "Can not parse request body from CBOR";
    } else {
        NlohmannJsonToProto(fromCbor, &message);
    }
}

void DeserializeJson(NProtoBuf::Message& message, const TStringBuf& input) {
    auto fromJson = nlohmann::json::parse(input, nullptr, false);
    if (fromJson.is_discarded()) {
        throw NKikimr::NSQS::TSQSException(NKikimr::NSQS::NErrors::MALFORMED_QUERY_STRING) <<
            "Can not parse request body from JSON";
    } else {
        NlohmannJsonToProto(fromJson, &message);
    }
}

} // namespace NKikimr::NHttpProxy