#include "read.h"
#include <library/cpp/actors/core/log.h>

namespace NKikimr::NOlap {

void IBlobsReadingAction::StartReading(THashMap<TUnifiedBlobId, THashSet<TBlobRange>>&& ranges) {
    AFL_VERIFY(ranges.size());
    for (auto&& i : ranges) {
        AFL_VERIFY(i.second.size());
    }
    return DoStartReading(ranges);
}

}
