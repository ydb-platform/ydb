#include "compaction_info.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

bool TPlanCompactionInfo::Finish() {
    return --Count == 0;
}

}   // namespace NKikimr::NOlap
