#pragma once

#include "line_types.h"

#include <atomic>
#include <span>

namespace NActors {
    class TLineReader;
    struct TChunk;
    class TInMemoryMetricsBackend;

    class TLineWriterState {
    public:
        TLineReader* Reader = nullptr;
        std::atomic<bool> HasLastMaterialized = false;
        std::atomic<ui64> LastMaterializedValue = 0;
    };

    struct TWritableChunkMemory {
        std::span<char> Payload;
        NHPTimer::STime FirstTs = 0;
        NHPTimer::STime LastTs = 0;
    };

    using TAccessChunkMemoryFn = bool (*)(void*, TWritableChunkMemory&) noexcept;

} // namespace NActors
