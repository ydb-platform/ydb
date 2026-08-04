#pragma once

#include "line_read.h"
#include "line_types.h"
#include "line_write.h"
#include "line_storage.h"

#include <functional>
#include <memory>

namespace NActors {
    template<class TFrontend>
    class TLine;

    template<class TValue>
    struct TRawLineFrontend;
    template<class TValue>
    struct TOnChangeLineFrontend;

    class TInMemoryMetricsBackend {
    public:
        explicit TInMemoryMetricsBackend(TInMemoryMetricsConfig config);
        ~TInMemoryMetricsBackend();

        template<class TFrontend = TRawLineFrontend<ui64>>
        TLine<TFrontend> CreateLine(TStringBuf name, std::span<const TLabel> labels, const typename TFrontend::TConfig& config = {});

        void SetCommonLabels(std::span<const TLabel> labels);
        TVector<TLabel> GetCommonLabels() const;
        TInMemoryMetricsStats GetStats() const;
        void UpdateSelfMetrics();
        // Common labels and line views are borrowing objects and are valid only during cb().
        void ReadSnapshot(const TReadSnapshotCallback& cb) const;

        ui64 GetReuseWatermark() const noexcept;
        const TInMemoryMetricsConfig& GetConfig() const noexcept;

        void CloseLine(TLineWriterState* state) noexcept;
        bool AccessChunkMemory(
            TLineWriterState* state,
            void* opaque,
            TAccessChunkMemoryFn accessChunkMemory) noexcept;
        NHPTimer::STime CurrentTimestampTs() const noexcept;
        std::optional<ui64> GetLastMaterializedValue(const TLineWriterState* state) const noexcept;
        ui32 GetLineId(const TLineWriterState* state) const noexcept;
        void MarkMaterialized(TLineWriterState* state, ui64 value) noexcept;
        void ReleasePinnedChunk(TChunk* chunk) noexcept;

    private:
        class TImpl;

        TLineWriterState* CreateLineWithMeta(TStringBuf name, std::span<const TLabel> labels, const TLineMeta& meta);
        TChunk* TryAcquireFreeChunk();
        TChunk* TryStealOldestChunk();
        void PublishSealedChunk(TChunk* chunk);
        void RetireChunk(TChunk* chunk);
        void ReturnChunkToFree(TChunk* chunk);
        void MaybeDropClosedLine(TLineReader* line);
        bool IsMetricAllowed(TStringBuf name) const noexcept;

    private:
        std::unique_ptr<TImpl> Impl;
    };

} // namespace NActors

#include "line.h"
