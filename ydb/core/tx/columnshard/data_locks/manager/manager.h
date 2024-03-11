#pragma once
#include <ydb/core/tx/columnshard/data_locks/locks/abstract.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <optional>

namespace NKikimr::NOlap::NDataLocks {

class TManager {
private:
    THashMap<TString, std::shared_ptr<ILock>> ProcessLocks;
public:
    TManager() = default;

    void RegisterLock(const TString& processId, const std::shared_ptr<ILock>& lock);
    template <class TLock, class ...Args>
    void RegisterLock(const TString& processId, Args&&... args) {
        RegisterLock(processId, std::make_shared<TLock>(args...));
    }
    void UnregisterLock(const TString& processId);
    std::optional<TString> IsLocked(const TPortionInfo& portion) const;
    std::optional<TString> IsLocked(const TGranuleMeta& granule) const;

};

}