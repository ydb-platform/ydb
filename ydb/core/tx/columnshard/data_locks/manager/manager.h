#pragma once
#include <ydb/core/tx/columnshard/data_locks/locks/abstract.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <optional>

namespace NKikimr::NOlap::NDataLocks {

class TManager {
private:
    THashMap<TString, std::shared_ptr<ILock>> ProcessLocks;
    std::shared_ptr<TAtomicCounter> StopFlag = std::make_shared<TAtomicCounter>(0);
    void UnregisterLock(const TString& processId);
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
    std::optional<TString> IsLocked(const TPortionInfo& portion, const THashSet<TString>& excludedLocks = {}) const;
    std::optional<TString> IsLocked(const TGranuleMeta& granule, const THashSet<TString>& excludedLocks = {}) const;

};

}