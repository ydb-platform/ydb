#pragma once
#include <ydb/core/tx/columnshard/data_locks/locks/abstract.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <optional>
#include <deque>

namespace NKikimr::NOlap::NDataLocks {


enum class ELockType {
    Shared,
    Exclusive
};

class TManager {
private:
    struct TLockInfo {
        std::unique_ptr<ILock> Lock;
        ELockType LockType;
        size_t LockCount;
    };
    THashMap<size_t, TLockInfo> Locks;
    std::deque<TLockInfo> Awaiting;
    size_t LastLockId = 0;
    std::shared_ptr<TAtomicCounter> StopFlag = std::make_shared<TAtomicCounter>(0);
    void ReleaseLock(const size_t lockId);
public:
    TManager() = default;

    void Stop();

    class TGuard {
    private:
        size_t LockId;
        std::shared_ptr<TAtomicCounter> StopFlag;
        bool Released = false;
    public:
        TGuard(const size_t lockId, const std::shared_ptr<TAtomicCounter>& stopFlag)
            : LockId(lockId)
            , StopFlag(stopFlag)
        {
        }
        TGuard(const TGuard&) = delete;
        TGuard(TGuard&& other) {
            LockId = other.LockId;
            StopFlag = std::move(other.StopFlag);
            Released = other.Released;
            other.Released = true;
        }
        TGuard& operator=(const TGuard&) = delete;
        TGuard& operator=(TGuard&& other) {
            if (this == &other) {
                return *this;
            }
            LockId = other.LockId;
            StopFlag = std::move(other.StopFlag);
            Released = other.Released;
            other.Released = true;
            return *this;
        }

        size_t GetLockId() const {
            return LockId;
        }

        void AbortLock();

        ~TGuard();

        void Release(TManager& manager);
    };

    struct ILockAcquired {
        using TPtr = std::unique_ptr<ILockAcquired>;
        virtual void OnLockAcquired(TGuard&& guard) = 0;
        virtual ~ILockAcquired() = default;
    };

    std::optional<TGuard> Lock(ILock::TPtr&& lock,  const ELockType type, ILockAcquired::TPtr&& onAcquired);
    std::optional<TGuard> TryLock(ILock::TPtr&& lock,  const ELockType type) {
        return Lock(std::move(lock), type, ILockAcquired::TPtr{});
    }
    
    std::optional<TString> IsLocked(const TPortionInfo& portion, const TLockScope& scope = TLockScope{.Action = EAction::Modify, .Originator = EOriginator::Bg}, const std::optional<TGuard>& ignored = std::nullopt) const;
    std::optional<TString> IsLocked(const TGranuleMeta& granule, const TLockScope& scope = TLockScope{.Action = EAction::Modify, .Originator = EOriginator::Bg}, const std::optional<TGuard>& ignored = std::nullopt) const;
    std::optional<TString> IsLockedTableSchema(const ui64 pathId, const TLockScope& scope = TLockScope{.Action = EAction::Modify, .Originator = EOriginator::Bg});
    //std::optional<TString> IsLockedTableDataCommitted(const ui64 pathId, const TLockScope& scope = TLockScope{.Action = EAction::Modify, .Originator = EOriginator::Bg});
};

}