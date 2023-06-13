#include "compaction_info.h"
#include "storage/granule.h"

namespace NKikimr::NOlap {

NKikimr::NOlap::TPlanCompactionInfo TCompactionInfo::GetPlanCompaction() const {
    auto& granuleMeta = GetObject<TGranuleMeta>();
    return TPlanCompactionInfo(granuleMeta.GetPathId(), InGranule());
}

}
