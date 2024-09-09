#include "manager.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NDataLocks {

std::optional<TManager::TGuard> TManager::Lock(ILock::TPtr&& lock, const ELockType lockType, ILockAcquired::TPtr&& onAcquired) {
    AFL_VERIFY(lock);
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "lock")("name", lock->GetLockName())("try", onAcquired ? "yes" : "no");
    for (const auto& awaiting: Awaiting) {
        if (!lock->IsCompatibleWith(*awaiting.Lock)) {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "lock")("name", lock->GetLockName())("incompatible", awaiting.Lock->GetLockName());
            if (onAcquired) {
                Awaiting.emplace_back(TLockInfo{
                    .Lock = std::move(lock),
                    .LockType = lockType,
                    .LockCount = 0
                });
            }
            return std::nullopt;
        }
    }
    for (auto&[id, existing]: Locks) {
        if (existing.LockType == ELockType::Shared && existing.LockType == ELockType::Shared && lock->IsEqualTo(*existing.Lock)) {
            ++existing.LockCount;
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "lock")("name", lock->GetLockName())("reuse", existing.Lock->GetLockName())("count", existing.LockCount);
            return TGuard(id, StopFlag);
        }
        if (lockType == ELockType::Exclusive || existing.LockType == ELockType::Exclusive) {
            AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "lock")("name", lock->GetLockName())("incompatible", existing.Lock->GetLockName());
            if (!lock->IsCompatibleWith(*existing.Lock)) {
                if (onAcquired) {
                    Awaiting.emplace_back(TLockInfo{
                        .Lock = std::move(lock),
                        .LockType = lockType,
                        .LockCount = 0
                    });
                }
                return std::nullopt;
            }
        }
    }
    ++LastLockId;
    AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "lock")("name", lock->GetLockName())("registered", LastLockId);
    AFL_VERIFY(Locks.emplace(
        LastLockId, 
        TLockInfo {
            .Lock = std::move(lock),
            .LockType = lockType,
            .LockCount = 1
        }
    ).second);
    return TGuard(LastLockId, StopFlag);
}

void TManager::ReleaseLock(const size_t lockId) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "unlock")("lock_id", lockId);
    const auto lockInfo = Locks.FindPtr(lockId);
    AFL_VERIFY(lockInfo);
    AFL_VERIFY(lockInfo->LockCount != 0);
    if (0 == --lockInfo->LockCount) {
        AFL_VERIFY(Locks.erase(lockId))("lock_id", lockId);
    }
}

std::optional<TString> TManager::IsLocked(const TPortionInfo& portion, const TLockScope& scope, const std::optional<TGuard>& ignored) const {
    for (const auto& [id, lockInfo] : Locks) {
        if (ignored && ignored->GetLockId() == id) {
            continue;
        }
        if (auto lockName = lockInfo.Lock->IsLocked(portion, scope)) {
            return lockName;
        }
    }
    return {};
}

std::optional<TString> TManager::IsLocked(const TGranuleMeta& granule, const TLockScope& scope, const std::optional<TGuard>& ignored) const {
    for (const auto& [id, lockInfo] : Locks) {
        if (ignored && ignored->GetLockId() == id) {
            continue;
        }
        if (auto lockName = lockInfo.Lock->IsLocked(granule, scope)) {
            return lockName;
        }
    }
    return {};
}

void TManager::Stop() {
    AFL_VERIFY(StopFlag->Inc() == 1);
}

TManager::TGuard::~TGuard() {
    AFL_VERIFY(Released || !NActors::TlsActivationContext || StopFlag->Val() == 1);
}

void TManager::TGuard::Release(TManager& manager) {
    AFL_VERIFY(!Released);
    manager.ReleaseLock(LockId);
    Released = true;
}

void TManager::TGuard::AbortLock() {
    if (!Released) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("message", "aborted data locks manager");
    }
    Released = true;
}

} //namespace NKikimr::NOlap::NDataLocks