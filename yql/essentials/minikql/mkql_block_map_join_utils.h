#pragma once

#include <util/system/types.h>

namespace NKikimr {
namespace NMiniKQL {

constexpr ui64 BlockMapJoinIndexEntrySize = 20;

ui64 EstimateBlockMapJoinIndexSize(ui64 rowsCount);

} // namespace NMiniKQL
} // namespace NKikimr
