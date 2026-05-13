#include "manager.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap::NDataLocks {

std::shared_ptr<TManager::TGuard> TManager::RegisterLock(const std::shared_ptr<ILock>& lock) {
    AFL_VERIFY(lock);
    AFL_VERIFY(ProcessLocks.emplace(lock->GetLockName(), lock).second)("process_id", lock->GetLockName());
    YDB_LOG_DEBUG("",
        {"event", "lock"},
        {"process_id", lock->GetLockName()});
    return std::make_shared<TGuard>(lock->GetLockName(), StopFlag);
}

void TManager::UnregisterLock(const TString& processId) {
    YDB_LOG_DEBUG("",
        {"event", "unlock"},
        {"process_id", processId});
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
        YDB_LOG_WARN("",
            {"message", "aborted data locks manager"});
    }
    Released = true;
}

}   // namespace NKikimr::NOlap::NDataLocks
