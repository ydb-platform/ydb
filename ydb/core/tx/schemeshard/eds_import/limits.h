#pragma once

#include <util/system/types.h>

namespace NKikimr::NSchemeShard::NExternalDataSource {

// Shared by creation validation and import rebinding; this is the producer's existing limit.
inline constexpr ui32 MAX_PROTOBUF_SIZE = 2 * 1024 * 1024;

} // namespace NKikimr::NSchemeShard::NExternalDataSource
