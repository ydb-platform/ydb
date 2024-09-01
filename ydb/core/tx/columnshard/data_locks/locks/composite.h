#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NDataLocks {

class TCompositeLock: public ILock {
private:
    using TBase = ILock;
    std::vector<std::shared_ptr<ILock>> Locks;
private:
    template <IsLocable T> 
    std::optional<TString> DoIsLockedImpl(const T& obj, const TLockFilter& filter) const {
        for (const auto& lock : Locks) {
            if (!filter(lock->GetLockName(), lock->GetLockCategory())) {
                continue;
            }
            if (auto lockName = lock->IsLocked(obj, filter)) {
                return lockName;
            }
        }
        return std::nullopt;
    }
protected:
    virtual std::optional<TString> DoIsLocked(const TPortionInfo& portion, const TLockFilter& filter) const override {
        return DoIsLockedImpl(portion, filter);
    }
    virtual std::optional<TString> DoIsLocked(const TGranuleMeta& granule, const TLockFilter& filter) const override {
        return DoIsLockedImpl(granule, filter);
    }
    virtual std::optional<TString> DoIsLocked(const ui64 pathId, const TLockFilter& filter) const override {
        return DoIsLockedImpl(pathId, filter);
    }
    bool DoIsEmpty() const override {
        return Locks.empty();
    }
public:
    TCompositeLock(const TString& lockName, const std::vector<std::shared_ptr<ILock>>& locks)
        : TBase(lockName, ELockCategory::Generic)
    {
        for (auto&& l : locks) {
            if (!l || l->IsEmpty()) {
                continue;
            }
            Locks.emplace_back(l);
        }
    }

    TCompositeLock(const TString& lockName, std::initializer_list<std::shared_ptr<ILock>> locks)
        : TBase(lockName, ELockCategory::Generic)
    {
        for (auto&& l : locks) {
            if (!l || l->IsEmpty()) {
                continue;
            }
            Locks.emplace_back(l);
        }
    }
};

}