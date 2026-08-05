#pragma once

#include "defs.h"

#include <ydb/library/actors/util/rope.h>

#include <utility>

namespace NKikimr {

std::pair<TRope::TConstIterator, ui64> CalculateXxh3Hash(TRope::TConstIterator it, size_t numBytes);

} // namespace NKikimr
