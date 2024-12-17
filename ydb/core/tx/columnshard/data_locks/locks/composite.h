#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NDataLocks {

class TCompositeLock: public ILock {
private:
    using TBase = ILock;
    std::vector<std::shared_ptr<ILock>> Locks;
protected:
    virtual std::optional<TString> DoIsLocked(const TPortionInfo& portion, const ELockCategory category, const THashSet<TString>& excludedLocks) const override {
        for (auto&& i : Locks) {
            if (excludedLocks.contains(i->GetLockName())) {
                continue;
            }
            if (auto lockName = i->IsLocked(portion, category)) {
                return lockName;
            }
        }
        return {};
    }
    virtual std::optional<TString> DoIsLocked(
        const TGranuleMeta& granule, const ELockCategory category, const THashSet<TString>& excludedLocks) const override {
        for (auto&& i : Locks) {
            if (excludedLocks.contains(i->GetLockName())) {
                continue;
            }
            if (auto lockName = i->IsLocked(granule, category)) {
                return lockName;
            }
        }
        return {};
    }
    bool DoIsEmpty() const override {
        return Locks.empty();
    }
public:
    static std::shared_ptr<ILock> Build(const TString& lockName, const std::initializer_list<std::shared_ptr<ILock>>& locks) {
        std::vector<std::shared_ptr<ILock>> locksUseful;
        for (auto&& i : locks) {
            if (i && !i->IsEmpty()) {
                locksUseful.emplace_back(i);
            }
        }
        if (locksUseful.size() == 1) {
            return locksUseful.front();
        } else {
            return std::make_shared<TCompositeLock>(lockName, locksUseful);
        }
    }

    TCompositeLock(const TString& lockName, const std::vector<std::shared_ptr<ILock>>& locks,
        const ELockCategory category = NDataLocks::ELockCategory::Any, const bool readOnly = false)
        : TBase(lockName, category, readOnly)
    {
        for (auto&& l : locks) {
            if (!l || l->IsEmpty()) {
                continue;
            }
            Locks.emplace_back(l);
        }
    }

    TCompositeLock(const TString& lockName, std::initializer_list<std::shared_ptr<ILock>> locks,
        const ELockCategory category = NDataLocks::ELockCategory::Any, const bool readOnly = false)
        : TBase(lockName, category, readOnly)
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