#pragma once

#include <util/system/types.h>

namespace NKikimr {
namespace NMiniKQL {

ui64 RHHashTableNeedsGrow(ui64 size, ui64 capacity);
ui64 CalculateRHHashTableGrowFactor(ui64 currentCapacity);
ui64 CalculateRHHashTableCapacity(ui64 targetSize);

} // namespace NMiniKQL
} // namespace NKikimr
