#include "manager.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NDataLocks {

void TManager::RegisterLock(const TString& processId, const std::shared_ptr<ILock>& lock) {
    AFL_VERIFY(ProcessLocks.emplace(processId, lock).second)("process_id", processId);
}

void TManager::UnregisterLock(const TString& processId) {
    AFL_VERIFY(ProcessLocks.erase(processId))("process_id", processId);
}

std::optional<TString> TManager::IsLocked(const TPortionInfo& portion) const {
    for (auto&& i : ProcessLocks) {
        if (i.second->IsLocked(portion)) {
            return i.first;
        }
    }
    return {};
}

std::optional<TString> TManager::IsLocked(const TGranuleMeta& granule) const {
    for (auto&& i : ProcessLocks) {
        if (i.second->IsLocked(granule)) {
            return i.first;
        }
    }
    return {};
}

}