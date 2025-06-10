#pragma once

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/system/spinlock.h>

namespace NKikimr::NOlap::NBlobOperations::NTier {

class TErrorCollector {
public:
    TErrorCollector() = default;

    void Add(const TString& tier, const TString& message) {
        TGuard<TSpinLock> g(Lock);
        Errors[tier] = message;
    }

    THashMap<TString, TString> GetAll() const {
        TGuard<TSpinLock> g(Lock);
        return Errors;
    }

private:
    mutable TSpinLock Lock;
    THashMap<TString, TString> Errors;
};

} // namespace NKikimr::NOlap::NBlobOperations::NTier