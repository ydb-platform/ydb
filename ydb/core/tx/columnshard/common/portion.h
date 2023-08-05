#pragma once
#include <util/system/types.h>

namespace NKikimr::NOlap::NPortion {
// NOTE: These values are persisted in LocalDB so they must be stable
enum EProduced: ui32 {
    UNSPECIFIED = 0,
    INSERTED,
    COMPACTED,
    SPLIT_COMPACTED,
    INACTIVE,
    EVICTED
};

}
