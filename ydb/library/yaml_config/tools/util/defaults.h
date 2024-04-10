#pragma once

#include <library/cpp/protobuf/json/proto2json.h>

#include <google/protobuf/message.h>

#include <util/generic/string.h>

namespace NKikimr::NYaml::NInternal {

inline TString ProtoToJson(const google::protobuf::Message &resp) {
    auto config = NProtobufJson::TProto2JsonConfig()
        .SetFormatOutput(true)
        .SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumName);
    return NProtobufJson::Proto2Json(resp, config);
}

} // namespace NKikimr::NYaml::NInternal
