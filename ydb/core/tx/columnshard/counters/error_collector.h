#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

#include <queue>

namespace NKikimr::NColumnShard {

class TPerTierErrorCollector {
public:
    struct TError {
        TString Reason;
        TInstant Time;
    };

    TPerTierErrorCollector() {
    }

    void Add(const TString& tier, const TString& reason) {
        TGuard<TSpinLock> lock(Lock);
        auto& q = ErrorsCollector[tier];
        if (q.size() == QUEUE_MAX_SIZE) {
            q.pop();
        }

        q.push({ reason, TInstant::Now() });
    }

    THashMap<TString, std::queue<TError>> GetAll() const {
        TGuard<TSpinLock> lock(Lock);
        return ErrorsCollector;
    }

private:
    static constexpr size_t QUEUE_MAX_SIZE = 10;
    mutable TSpinLock Lock;
    THashMap<TString, std::queue<TError>> ErrorsCollector;
};

class TErrorCollector {
public:
    void OnReadError(const TString& tier, const TString& message) {
        Read.Add(tier, message);
    }

    void OnWriteError(const TString& tier, const TString& message) {
        Write.Add(tier, message);
    }

    THashMap<TString, std::queue<TPerTierErrorCollector::TError>> GetAllReadErrors() const {
        return Read.GetAll();
    }

    THashMap<TString, std::queue<TPerTierErrorCollector::TError>> GetAllWriteErrors() const {
        return Write.GetAll();
    }

private:
    TPerTierErrorCollector Read;
    TPerTierErrorCollector Write;
};

}   // namespace NKikimr::NColumnShard
