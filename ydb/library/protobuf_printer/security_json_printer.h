#pragma once

#include <library/cpp/protobuf/json/config.h>

#include <google/protobuf/message.h>

#include <util/generic/string.h>

namespace NJson {
    class TJsonWriter;
}

namespace NKikimr {

void SecureProto2Json(const NProtoBuf::Message& proto,
                      NJson::TJsonWriter& writer,
                      const NProtobufJson::TProto2JsonConfig& config = NProtobufJson::TProto2JsonConfig());

TString SecureProto2JsonString(const NProtoBuf::Message& proto,
                               const NProtobufJson::TProto2JsonConfig& config = NProtobufJson::TProto2JsonConfig());

} // namespace NKikimr
