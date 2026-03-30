#pragma once

#include "line_base.h"
#include "line.h"
#include "line_storage.h"
#include "lines/on_change_line_frontend.h"
#include "lines/raw_line_frontend.h"

#include <functional>
#include <memory>

namespace NActors {

    class TInMemoryMetricsBackend : public TLineWriteBackend {
    public:
        explicit TInMemoryMetricsBackend(TInMemoryMetricsConfig config);
        ~TInMemoryMetricsBackend() override;

        TLine<TRawLineFrontend<>> CreateLine(TStringBuf name, std::span<const TLabel> labels);
        template<class TFrontend>
        TLine<TFrontend> CreateLine(TStringBuf name, std::span<const TLabel> labels, const typename TFrontend::TConfig& config = {}) {
            return TLine<TFrontend>(this, CreateLineWithMeta(name, labels, TFrontend::MakeMeta(config)));
        }

        void SetCommonLabels(std::span<const TLabel> labels);
        TVector<TLabel> GetCommonLabels() const;
        TInMemoryMetricsStats GetStats() const;
        void UpdateSelfMetrics();
        // Snapshot and line views are borrowing objects and are valid only during cb().
        template<class TCallback>
        void ReadSnapshot(TCallback&& cb) const {
            ReadSnapshotImpl([&](const TSnapshot& snapshot) {
                cb(snapshot);
            });
        }

        ui64 GetReuseWatermark() const noexcept;
        const TInMemoryMetricsConfig& GetConfig() const noexcept;
        void Shutdown() noexcept;

        void CloseLine(TLineWriterState* writer) noexcept override;
        bool AppendChunkData(
            TLineWriterState* writer,
            std::span<const char> data,
            NHPTimer::STime firstTs,
            NHPTimer::STime lastTs) noexcept override;
        NHPTimer::STime CurrentTimestampTs() const noexcept override;
        TLinePublishState GetPublishState(const TLineWriterState* writer) const noexcept override;
        ui32 GetLineId(const TLineWriterState* writer) const noexcept override;
        void MarkObserved(TLineWriterState* writer, NHPTimer::STime nowTs) noexcept override;
        void MarkPublished(TLineWriterState* writer, ui64 value, NHPTimer::STime nowTs) noexcept override;
        void ReleasePinnedChunk(TChunk* chunk) noexcept override;

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
        void ReadSnapshotImpl(const std::function<void(const TSnapshot&)>& cb) const;

    private:
        std::unique_ptr<TImpl> Impl;
    };

} // namespace NActors
