#pragma once
#include <ydb/core/tx/columnshard/data_locks/locks/abstract.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <optional>

namespace NKikimr::NOlap::NDataLocks {

class TManager {
private:
    THashMap<TString, std::shared_ptr<ILock>> ProcessLocks;
    bool UseTestController;
    std::shared_ptr<TAtomicCounter> StopFlag = std::make_shared<TAtomicCounter>(0);
    void UnregisterLock(const TString& processId);
public:
    TManager(bool useTestController = false)
        : UseTestController(useTestController)
    {}

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
    template<IsLocable T>
    std::optional<TString> IsLocked(const T& obj, const TLockFilter& filter) const {
        for (const auto& [name, lock] : ProcessLocks) {
            if (!filter(name, lock->GetLockCategory())) {
                continue;
            }
            if (auto lockName = lock->IsLocked(obj, filter)) {
                return lockName;
            }
        }
        if (UseTestController) {
            if (const auto manager = NYDBTest::TControllers::GetColumnShardController()->GetLockManager()) {
                return manager->IsLocked(obj, filter);
            }
        }
        return std::nullopt;
    }

    static TString GetSharingSessionLockName(const TStringBuf sessionId) {
        static const TString prefix = "sharing_session:";
        return prefix + sessionId;
    }

    static TString GetNewDataTxLockName(const ui64 pathId) {
        static const TString prefix = "new_data_tx:";
        return prefix + std::to_string(pathId);
    }

};

}