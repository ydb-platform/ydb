#pragma once
#include "abstract.h"

namespace NKikimr::NOlap::NDataLocks {

class TCompositeLock: public ILock {
private:
    using TBase = ILock;
    std::vector<std::shared_ptr<ILock>> Locks;
protected:
    virtual bool DoIsLocked(const TPortionInfo& portion) const override {
        for (auto&& i : Locks) {
            if (i->IsLocked(portion)) {
                return true;
            }
        }
        return false;
    }
    virtual bool DoIsLocked(const TGranuleMeta& granule) const override {
        for (auto&& i : Locks) {
            if (i->IsLocked(granule)) {
                return true;
            }
        }
        return false;
    }
    bool DoIsEmpty() const override {
        return Locks.empty();
    }
public:
    TCompositeLock(const std::vector<std::shared_ptr<ILock>>& locks, const bool readOnly = false)
        : TBase(readOnly)
    {
        for (auto&& l : locks) {
            if (!l || l->IsEmpty()) {
                continue;
            }
            Locks.emplace_back(l);
        }
    }

    TCompositeLock(std::initializer_list<std::shared_ptr<ILock>> locks, const bool readOnly = false)
        : TBase(readOnly)
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