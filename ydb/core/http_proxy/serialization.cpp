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

TString SerializeCbor(const NProtoBuf::Message& message) {
    NJson::TJsonValue result;
    ProtoToJson(message, result, true);
    return SerializeCbor(result);
}

TString SerializeJson(const NProtoBuf::Message& message) {
    NJson::TJsonValue result;
    ProtoToJson(message, result, false);
    return SerializeJson(result);
}

TString SerializeCbor(const NJson::TJsonValue& message) {
    // according to https://json.nlohmann.me/features/binary_formats/cbor/#serialization
    auto cborBinaryTagBySize = [](size_t size) -> ui8 {
        if (size <= 23) {
            return 0x40 + static_cast<ui32>(size);
        } else if (size <= 255) {
            return 0x58;
        } else if (size <= 65535) {
            return 0x59;
        }

        return 0x5A;
    };

    bool gotData = false;
    std::function<bool(int, nlohmann::json::parse_event_t, nlohmann::basic_json<>&)> bz =
        [&gotData, &cborBinaryTagBySize](int, nlohmann::json::parse_event_t event, nlohmann::json& parsed) {
            if (event == nlohmann::json::parse_event_t::key and parsed == nlohmann::json("Data")) {
                gotData = true;
                return true;
            }
            if (event == nlohmann::json::parse_event_t::value and gotData) {
                gotData = false;
                std::string data = parsed.get<std::string>();
                parsed = nlohmann::json::binary({data.begin(), data.end()},
                                                cborBinaryTagBySize(data.size()));
                return true;
            }
            return true;
        };

    auto toCborStr = NJson::WriteJson(message, false);
    TStringBuf toCborBuf(toCborStr);

    auto json = nlohmann::json::parse(toCborBuf.begin(), toCborBuf.end(), bz, false);
    auto toCbor = nlohmann::json::to_cbor(json);

    return {(char*)&toCbor[0], toCbor.size()};
}

TString SerializeJson(const NJson::TJsonValue& message) {
    return NJson::WriteJson(message, false);
}

} // namespace NKikimr::NHttpProxy
