#pragma once

#include "../defs.h"

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/subsystem.h>

#include <util/generic/string.h>

#include <memory>
#include <optional>

namespace NActors {

    enum class ECGroupVersion : ui8 {
        Unknown = 0,
        V1 = 1,
        V2 = 2,
    };

    struct TCGroupMemoryLimit {
        bool Unlimited = false;
        ui64 Value = 0;
    };

    // Version-neutral memory snapshot consumed by the OOM controller.
    struct TCGroupMemoryStats {
        ECGroupVersion Version = ECGroupVersion::Unknown;
        TString CGroupPath;

        ui64 CurrentBytes = 0;
        std::optional<ui64> PeakBytes;
        std::optional<TCGroupMemoryLimit> MaxBytes;
        std::optional<TCGroupMemoryLimit> HighBytes;
        std::optional<ui64> SwapCurrentBytes;
        std::optional<TCGroupMemoryLimit> SwapMaxBytes;

        ui64 AnonBytes = 0;
        ui64 FileBytes = 0;
        ui64 KernelBytes = 0;

        ui64 HighEvents = 0;
        ui64 MaxEvents = 0;

        std::optional<double> PressureFullAvg10;
    };

    using TCGroupMemoryStatsPtr = std::shared_ptr<const TCGroupMemoryStats>;

    class ICGroupMemoryStatsProvider : public ISubSystem {
    public:
        virtual ~ICGroupMemoryStatsProvider() = default;

        // The actor system must be running. Sends TEvCGroupMemoryStats to
        // the local recipient actor and preserves cookie in
        // IEventHandle::Cookie. A null Stats pointer means that the provider is
        // unavailable or failed to read statistics.
        virtual void ReadMemoryStats(const TActorId& recipient, ui64 cookie = 0) const = 0;
    };

} // namespace NActors
