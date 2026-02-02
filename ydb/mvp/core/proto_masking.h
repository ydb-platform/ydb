#pragma once

#include <google/protobuf/message.h>

namespace NMVP {

std::string MaskedShortDebugString(const google::protobuf::Message& msg);

} // namespace NMVP
