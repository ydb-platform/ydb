#include "manager.h"

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NDataLocks {

std::shared_ptr<TManager::TGuard> TManager::RegisterLock(const std::shared_ptr<ILock>& lock) {
    AFL_VERIFY(lock);
    AFL_VERIFY(ProcessLocks.emplace(lock->GetLockName(), lock).second)("process_id", lock->GetLockName());
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "lock")("process_id", lock->GetLockName());
    return std::make_shared<TGuard>(lock->GetLockName(), StopFlag);
}

void TManager::UnregisterLock(const TString& processId) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "unlock")("process_id", processId);
    AFL_VERIFY(ProcessLocks.erase(processId))("process_id", processId);
}

std::optional<TString> TManager::IsLocked(
    const TPortionInfo& portion, const ELockCategory lockCategory, const THashSet<TString>& excludedLocks) const {
    return IsLockedImpl(portion, lockCategory, excludedLocks);
}

std::optional<TString> TManager::IsLocked(
    const TGranuleMeta& granule, const ELockCategory lockCategory, const THashSet<TString>& excludedLocks) const {
    return IsLockedImpl(granule, lockCategory, excludedLocks);
}

std::optional<TString> TManager::IsLocked(const std::shared_ptr<const TPortionInfo>& portion, const ELockCategory lockCategory,
    const THashSet<TString>& excludedLocks /*= {}*/) const {
    AFL_VERIFY(!!portion);
    return IsLocked(*portion, lockCategory, excludedLocks);
}

void TManager::Stop() {
    AFL_VERIFY(StopFlag->Inc() == 1);
}

TManager::TGuard::~TGuard() {
    AFL_VERIFY(Released || !NActors::TlsActivationContext || StopFlag->Val() == 1);
}

void TManager::TGuard::Release(TManager& manager) {
    AFL_VERIFY(!Released);
    manager.UnregisterLock(ProcessId);
    Released = true;
}

void TManager::TGuard::AbortLock() {
    if (!Released) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("message", "aborted data locks manager");
    }
    Released = true;
}

}