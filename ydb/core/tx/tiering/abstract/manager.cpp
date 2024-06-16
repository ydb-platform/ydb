#include "manager.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NColumnShard {

const NTiers::TManager& ITiersManager::GetManagerVerified(const TString& tierId) const {
    auto* result = GetManagerOptional(tierId);
    AFL_VERIFY(result)("tier_id", tierId);
    return *result;
}

}
