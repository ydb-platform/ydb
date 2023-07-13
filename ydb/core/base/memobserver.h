#pragma once

#include "defs.h"

namespace NKikimr {

class TMemObserver : public TThrRefBase {
public:
    struct TMemStat {
        ui64 Used;
        ui64 HardLimit;
        ui64 SoftLimit;
    };

    using TCallback = std::function<void()>;

    void Subscribe(TCallback callback) {
        auto guard = Guard(Mutex);
        Callbacks.push_back(std::move(callback));
    }

    void SetStat(TMemStat stat) {
        auto guard = Guard(Mutex);
        Stat = stat;
    }

    void NotifyStat(TMemStat stat) {
        TVector<TCallback> copy;
        {
            auto guard = Guard(Mutex);
            copy = Callbacks;
            Stat = stat;
        }

        for (const auto &c : copy) {
            c();
        }
    }

    TMemStat GetStat() {
        auto guard = Guard(Mutex);
        return Stat;
    }

private:
    TVector<TCallback> Callbacks;
    TMutex Mutex;
    TMemStat Stat;
};

}
