#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/system/spinlock.h>

#include <queue>

namespace NKikimr::NColumnShard {

class TErrorCollector {
public:
    struct Error {
        TString Tier;
        TString ErrorReason;
        TInstant Time;
    };

    TErrorCollector() = default;

    void AddReadError(const TString& tier, const TString& message, const TInstant& time) {
        TGuard<TSpinLock> lock(Lock);
        if (ErrorReadCollector.size() == QUEUE_MAX_SIZE) {
            ErrorReadCollector.pop();
        }

        ErrorReadCollector.push({ tier, message, time });
    }

    void AddWriteError(const TString& tier, const TString& message, const TInstant& time) {
        TGuard<TSpinLock> lock(Lock);
        if (ErrorWriteCollector.size() == QUEUE_MAX_SIZE) {
            ErrorWriteCollector.pop();
        }

        ErrorWriteCollector.push({ tier, message, time });
    }

    std::queue<Error> GetAllReadErrors() const {
        TGuard<TSpinLock> lock(Lock);
        return ErrorReadCollector;
    }

    std::queue<Error> GetAllWriteErrors() const {
        TGuard<TSpinLock> lock(Lock);
        return ErrorWriteCollector;
    }

private:
    static constexpr size_t QUEUE_MAX_SIZE = 10;
    mutable TSpinLock Lock;
    std::queue<Error> ErrorReadCollector;
    std::queue<Error> ErrorWriteCollector;
};

}   // namespace NKikimr::NColumnShard
