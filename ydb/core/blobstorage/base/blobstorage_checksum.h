#pragma once

#include "defs.h"

#include <ydb/core/base/logoblob.h>
#include <ydb/library/actors/util/rope.h>

#include <utility>

namespace NKikimr {

constexpr ui32 LogoBlobCrcModeXxh3WholePart = 1;

inline bool LogoBlobCrcModeHasXxh3WholePartChecksum(const TLogoBlobID& id) {
    return id.CrcMode() == LogoBlobCrcModeXxh3WholePart;
}

inline bool LogoBlobCrcModeHasXxh3WholePartChecksum(ui32 crcMode) {
    return crcMode == LogoBlobCrcModeXxh3WholePart;
}

std::pair<TRope::TConstIterator, ui64> CalculateXxh3Hash(TRope::TConstIterator it, size_t numBytes);

} // namespace NKikimr
