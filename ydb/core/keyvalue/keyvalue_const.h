#pragma once
#include "defs.h"

namespace NKikimr {
namespace NKeyValue {

constexpr ui32 BLOB_CHANNEL = 2;
constexpr ui64 KEYVALUE_VERSION = 1ull;

constexpr ui64 InlineStorageChannelInPublicApi = 1;
constexpr ui64 MainStorageChannelInPublicApi = 2;

} // NKeyValue
} // NKikimr
