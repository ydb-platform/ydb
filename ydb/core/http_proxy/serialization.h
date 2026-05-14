#pragma once

#include <contrib/libs/protobuf/src/google/protobuf/message.h>
#include <library/cpp/mime/types/mime.h>

namespace NKikimr::NHttpProxy {

void DeserializeCbor(NProtoBuf::Message& message, const TStringBuf& input);
void DeserializeJson(NProtoBuf::Message& message, const TStringBuf& input);

} // namespace NKikimr::NHttpProxy
