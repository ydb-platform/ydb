#pragma once

#include <library/cpp/time_provider/monotonic.h>

namespace NYql::NDq {

class IS3ActorStats {
public:
    enum class EState {
        Initializing,
        WaitingCpu,
        WaitingHttp,
        WaitingConsumer,
        Processing,
        Finished
    };

    struct TSnapshot {
        EState State;
        TMonotonic StateSince;
        bool CpuHeld;
    };

    virtual ~IS3ActorStats() = default;
    virtual TSnapshot GetSnapshot() const = 0;
};

} // namespace NYql::NDq
