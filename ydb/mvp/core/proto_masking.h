#pragma once

#include <google/protobuf/message.h>

namespace NMVP {

// Mask sensitive fields in-place on a Message instance.
void MaskMessageRecursively(google::protobuf::Message* m);

// Return a ShortDebugString of a masked copy of `msg`.
std::string MaskedShortDebugString(const google::protobuf::Message& msg);

} // namespace NMVP
