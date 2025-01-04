#include "compaction_info.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

bool TPlanCompactionInfo::Finish() {
    if (Count > 0) {
        return --Count == 0;
    } else {
        AFL_VERIFY(false);
        return false;
    }
}

}   // namespace NKikimr::NOlap
