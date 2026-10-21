#pragma once

#include "line_types.h"

#include <optional>
#include <span>

namespace NActors {
    struct TWritableChunkMemory {
        std::span<char> Payload;
        ui32 UsedPayloadBytes = 0;
        NHPTimer::STime FirstTs = 0;
        NHPTimer::STime LastTs = 0;
    };

    using TAccessChunkMemoryFn = bool (*)(void*, TWritableChunkMemory&) noexcept;

    // Single-writer endpoint used by line frontends. Implementations own their
    // registration state and storage; consumers never depend on a concrete pool.
    // The system must outlive its line handles. Close is called once by TLine.
    class IMetricLine {
    public:
        virtual ~IMetricLine() = default;
        virtual bool IsValid() const noexcept = 0;
        virtual void Close() noexcept = 0;
        virtual ui32 GetLineId() const noexcept = 0;
        virtual NHPTimer::STime CurrentTimestampTs() const noexcept = 0;
        // Invoke synchronously; publish changes only on success. A false result
        // means the frontend did not append. Never retain opaque or the callback.
        virtual bool AccessChunkMemory(void* opaque, TAccessChunkMemoryFn access) noexcept = 0;
        virtual std::optional<ui64> GetLastMaterializedValue() const noexcept = 0;
        virtual void MarkMaterialized(ui64 value) noexcept = 0;
    };
} // namespace NActors
