#include "manager.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NColumnShard {

const NTiers::TManager& ITiersManager::GetManagerVerified(const NTiers::TExternalStorageId& tierId) const {
    auto* result = GetManagerOptional(tierId);
    YDB_LOG_VERIFY(result,
        {"tierId", tierId});
    return *result;
}

}
