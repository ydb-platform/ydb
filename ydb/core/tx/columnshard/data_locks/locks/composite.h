#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NDataLocks {

class TCompositeLock: public ILock {
private:
    using TBase = ILock;
    std::vector<std::shared_ptr<ILock>> Locks;
private:
    template <IsLocable T>
    std::optional<TString> DoIsLockedImpl(const T& obj, const THashSet<TString>& excludedLocks) const {
        for (auto&& i : Locks) {
            if (excludedLocks.contains(i->GetLockName())) {
                continue;
            }
            if (auto lockName = i->IsLocked(obj)) {
                return lockName;
            }
        }
        return {};
    }
protected:
    virtual std::optional<TString> DoIsLocked(const TPortionInfo& portion, const THashSet<TString>& excludedLocks) const override {
        return DoIsLockedImpl(portion, excludedLocks);
    }
    virtual std::optional<TString> DoIsLocked(const TGranuleMeta& granule, const THashSet<TString>& excludedLocks) const override {
        return DoIsLockedImpl(granule, excludedLocks);
    }
    virtual std::optional<TString> DoIsLocked(const ui64 pathId, const THashSet<TString>& excludedLocks) const override {
        return DoIsLockedImpl(pathId, excludedLocks);
    }
    bool DoIsEmpty() const override {
        return Locks.empty();
    }
public:
    TCompositeLock(const TString& lockName, const std::vector<std::shared_ptr<ILock>>& locks, const bool readOnly = false)
        : TBase(lockName, readOnly)
    {
        for (auto&& l : locks) {
            if (!l || l->IsEmpty()) {
                continue;
            }
            Locks.emplace_back(l);
        }
    }

    TCompositeLock(const TString& lockName, std::initializer_list<std::shared_ptr<ILock>> locks, const bool readOnly = false)
        : TBase(lockName, readOnly)
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