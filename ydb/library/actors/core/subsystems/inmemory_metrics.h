#pragma once

#include <ydb/library/actors/core/subsystem.h>
#include <ydb/library/actors/metrics/line_base.h>
#include <ydb/library/actors/metrics/line.h>
#include <ydb/library/actors/metrics/lines/raw_line_frontend.h>
#include <ydb/library/actors/metrics/lines/on_change_line_frontend.h>
#include <util/generic/function.h>

#include <memory>
#include <utility>

namespace NActors {
    class TActorSystem;
    class TInMemoryMetricsRegistry;
    class IActor;
    class TLineReader;
    struct TChunk;
    struct TSnapshotData;

    std::unique_ptr<TInMemoryMetricsRegistry> MakeInMemoryMetricsRegistry(TInMemoryMetricsConfig config);
    IActor* CreateInMemoryMetricsStatsActor(TDuration interval = TDuration::Seconds(1));

    class TInMemoryMetricsRegistry : public ISubSystem, public TLineWriteBackend {
    public:
        explicit TInMemoryMetricsRegistry(TInMemoryMetricsConfig config);
        ~TInMemoryMetricsRegistry() override;

        // Single-writer contract: a line can be created only once for a key.
        // Duplicate CreateLine() calls return a noop line.
        // Common labels are registry-wide mutable state and are not part of line identity.
        TLine<TRawLineFrontend<>> CreateLine(TStringBuf name, std::span<const TLabel> labels);
        template<class TFrontend>
        TLine<TFrontend> CreateLine(TStringBuf name, std::span<const TLabel> labels, const typename TFrontend::TConfig& config = {}) {
            return TLine<TFrontend>(this, CreateLineWithMeta(name, labels, TFrontend::MakeMeta(config)));
        }
        void SetCommonLabels(std::span<const TLabel> labels);
        TVector<TLabel> GetCommonLabels() const;
        TInMemoryMetricsStats GetStats() const;
        void UpdateSelfMetrics();
        TSnapshot Snapshot() const;

        ui64 GetReuseWatermark() const noexcept;
        const TInMemoryMetricsConfig& GetConfig() const noexcept;

    private:
        template<class TFrontend>
        friend class TLine;
        friend struct TSnapshotData;

        class TImpl;

        void OnAfterStart(TActorSystem&) override;
        void OnBeforeStop(TActorSystem&) override;
        TLineWriterState* CreateLineWithMeta(TStringBuf name, std::span<const TLabel> labels, const TLineMeta& meta);
        void CloseLine(TLineWriterState* writer) noexcept override;
        bool AppendStoredRecord(TLineWriterState* writer, const TStoredRecord& record) noexcept override;
        NHPTimer::STime CurrentTimestampTs() const noexcept override;
        TLinePublishState GetPublishState(const TLineWriterState* writer) const noexcept override;
        ui32 GetLineId(const TLineWriterState* writer) const noexcept override;
        void MarkObserved(TLineWriterState* writer, NHPTimer::STime nowTs) noexcept override;
        void MarkPublished(TLineWriterState* writer, ui64 value, NHPTimer::STime nowTs) noexcept override;
        TChunk* TryAcquireFreeChunk();
        TChunk* TryStealOldestChunk();
        void PublishSealedChunk(TChunk* chunk);
        void RetireChunk(TChunk* chunk);
        void ReturnChunkToFree(TChunk* chunk);
        void ReleasePinnedChunk(TChunk* chunk);
        void MaybeDropClosedLine(TLineReader* line);
        bool IsMetricAllowed(TStringBuf name) const noexcept;

    private:
        std::unique_ptr<TImpl> Impl;
    };

    TInMemoryMetricsRegistry* GetInMemoryMetrics(TActorSystem& actorSystem);
    const TInMemoryMetricsRegistry* GetInMemoryMetrics(const TActorSystem& actorSystem);
    TInMemoryMetricsRegistry* GetInMemoryMetrics();

} // namespace NActors
