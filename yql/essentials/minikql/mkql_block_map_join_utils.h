#pragma once

#include <util/system/types.h>

namespace NKikimr::NMiniKQL {

constexpr ui64 BlockMapJoinIndexEntrySize = 20;

ui64 EstimateBlockMapJoinIndexSize(ui64 rowsCount);

} // namespace NKikimr::NMiniKQL
