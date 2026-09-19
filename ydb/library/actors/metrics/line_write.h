#pragma once

#include "line_types.h"

#include <atomic>
#include <span>
#include <memory>

namespace NActors {
    class TLineReader;
    struct TChunk;
    class TInMemoryMetricsBackend;

    enum class ELineWriterStatus : ui8 {
        Pending,
        Ready,
        Closed,
        Rejected,
    };

    class TLineWriterState {
    public:
        std::atomic<ELineWriterStatus> Status = ELineWriterStatus::Pending;
        // Published by the single writer through Status=Closed.
        NHPTimer::STime ClosedAtTs = 0;
        // Published by Status=Ready; never dereferenced while Pending/Rejected.
        TLineReader* Reader = nullptr;
        std::atomic<TChunk*> LastMaterializedChunk = nullptr;
        std::atomic<ui64> LastMaterializedGeneration = 0;
        std::atomic<bool> HasLastMaterialized = false;
        std::atomic<ui64> LastMaterializedValue = 0;
    };

    struct TWritableChunkMemory {
        std::span<char> Payload;
        ui32 UsedPayloadBytes = 0;
        NHPTimer::STime FirstTs = 0;
        NHPTimer::STime LastTs = 0;
    };

    using TAccessChunkMemoryFn = bool (*)(void*, TWritableChunkMemory&) noexcept;

} // namespace NActors
