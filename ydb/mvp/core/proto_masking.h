#pragma once

#include <google/protobuf/message.h>

namespace NKikimr {

std::string SecureShortDebugStringMasked(const google::protobuf::Message& msg);

} // namespace NKikimr
