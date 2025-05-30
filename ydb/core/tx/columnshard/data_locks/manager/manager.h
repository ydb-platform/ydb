#pragma once
#include <ydb/core/tx/columnshard/data_locks/locks/abstract.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

#include <optional>

namespace NKikimr::NOlap::NDataLocks {

class TManager {
private:
    THashMap<TString, std::shared_ptr<ILock>> ProcessLocks;
    std::shared_ptr<TAtomicCounter> StopFlag = std::make_shared<TAtomicCounter>(0);
    void UnregisterLock(const TString& processId);

private:
    template <typename TObject>
    std::optional<TString> IsLockedImpl(const TObject& portion, const ELockCategory lockCategory, const THashSet<TString>& excludedLocks) const {
        const auto& isLocked = [&](const TString& name, const std::shared_ptr<ILock>& lock) -> std::optional<TString> {
            if (excludedLocks.contains(name)) {
                return std::nullopt;
            }
            if (auto lockName = lock->IsLocked(portion, lockCategory, excludedLocks)) {
                return lockName;
            }
            return std::nullopt;
        };
        for (auto&& [name, lock] : ProcessLocks) {
            if (auto locked = isLocked(name, lock)) {
                return locked;
            }
        }
        for (auto&& [name, lock] : NYDBTest::TControllers::GetColumnShardController()->GetExternalDataLocks()) {
            if (auto locked = isLocked(name, lock)) {
                return locked;
            }
        }
        return {};
    }

public:
    TManager() = default;

    void Stop();

    class TGuard {
    private:
        const TString ProcessId;
        std::shared_ptr<TAtomicCounter> StopFlag;
        bool Released = false;
    public:
        TGuard(const TString& processId, const std::shared_ptr<TAtomicCounter>& stopFlag)
            : ProcessId(processId)
            , StopFlag(stopFlag)
        {

        }

        void AbortLock();

        ~TGuard();

        void Release(TManager& manager);
    };

    [[nodiscard]] std::shared_ptr<TGuard> RegisterLock(const std::shared_ptr<ILock>& lock);
    template <class TLock, class ...Args>
    [[nodiscard]] std::shared_ptr<TGuard> RegisterLock(Args&&... args) {
        return RegisterLock(std::make_shared<TLock>(args...));
    }
    std::optional<TString> IsLocked(
        const TPortionInfo& portion, const ELockCategory lockCategory, const THashSet<TString>& excludedLocks = {}) const;
    std::optional<TString> IsLocked(
        const std::shared_ptr<const TPortionInfo>& portion, const ELockCategory lockCategory, const THashSet<TString>& excludedLocks = {}) const;
    std::optional<TString> IsLocked(
        const TGranuleMeta& granule, const ELockCategory lockCategory, const THashSet<TString>& excludedLocks = {}) const;

};

}