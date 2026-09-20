#pragma once

#include <contrib/libs/protobuf/src/google/protobuf/message.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/mime/types/mime.h>

namespace NKikimr::NHttpProxy {

void DeserializeCbor(NProtoBuf::Message& message, const TStringBuf& input);
void DeserializeJson(NProtoBuf::Message& message, const TStringBuf& input);

TString SerializeCbor(const NProtoBuf::Message& message);
TString SerializeJson(const NProtoBuf::Message& message);

TString SerializeCbor(const NJson::TJsonValue& message);
TString SerializeJson(const NJson::TJsonValue& message);

} // namespace NKikimr::NHttpProxy
