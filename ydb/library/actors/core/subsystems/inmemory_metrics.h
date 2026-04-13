#pragma once

#include <ydb/library/actors/core/subsystem.h>

#include <util/datetime/base.h>
#include <util/generic/function.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/hp_timer.h>

#include <memory>
#include <span>

namespace NActors {
    class TActorSystem;
    class TInMemoryMetricsRegistry;
    class IActor;
    class TLine;
    struct TChunk;
    struct TSnapshotData;

    struct TLabel {
        TString Name;
        TString Value;

        bool operator==(const TLabel& rhs) const noexcept;
    };

    struct TInMemoryMetricsConfig {
        ui64 MemoryBytes = 0;
        ui32 ChunkSizeBytes = 4096;
        ui32 MaxLines = 0;
        TVector<TLabel> CommonLabels;
    };

    struct TLineKey {
        TString Name;
        TVector<TLabel> Labels;

        bool operator==(const TLineKey& rhs) const noexcept;
    };

    struct TLineKeyHash {
        size_t operator()(const TLineKey& key) const noexcept;
    };

    struct TRecordView {
        TInstant Timestamp;
        ui64 Value = 0;
    };

    struct TChunkView {
        ui32 ChunkId = 0;
        TInstant FirstTs;
        TInstant LastTs;
        ui32 CommittedBytes = 0;
    };

    enum class EMetricKind : ui8 {
        Gauge,
        Counter,
    };

    enum class EPublishPolicy : ui8 {
        Raw,
        OnChange,
        OnChangeWithHeartbeat,
    };

    enum class EStorageEncoding : ui8 {
        RawPoints,
        RunLengthEncoded,
    };

    struct TLineMeta {
        EMetricKind MetricKind = EMetricKind::Gauge;
        EPublishPolicy PublishPolicy = EPublishPolicy::Raw;
        EStorageEncoding StorageEncoding = EStorageEncoding::RawPoints;
        TDuration Heartbeat;
    };

    class TLineWriter {
    public:
        TLineWriter() noexcept = default;
        TLineWriter(TInMemoryMetricsRegistry* registry, TLine* line) noexcept;
        TLineWriter(TLineWriter&& rhs) noexcept;
        TLineWriter& operator=(TLineWriter&& rhs) noexcept;
        TLineWriter(const TLineWriter&) = delete;
        TLineWriter& operator=(const TLineWriter&) = delete;
        ~TLineWriter();

        explicit operator bool() const noexcept;
        bool Append(ui64 value) noexcept;
        void Close() noexcept;
        ui32 GetLineId() const noexcept;
        ui32 ReleaseLineId() noexcept;

    private:
        TInMemoryMetricsRegistry* Registry = nullptr;
        TLine* Line = nullptr;
    };

    class TLineSnapshot {
    public:
        TLineSnapshot();
        TLineSnapshot(const TLineSnapshot&);
        TLineSnapshot(TLineSnapshot&&) noexcept;
        TLineSnapshot& operator=(const TLineSnapshot&);
        TLineSnapshot& operator=(TLineSnapshot&&) noexcept;
        ~TLineSnapshot();

        void ForEachRecord(const std::function<void(const TRecordView&)>& cb) const;

    public:
        ui32 LineId = 0;
        TString Name;
        TVector<TLabel> Labels;
        TLineMeta Meta;
        bool Closed = false;
        TVector<TChunkView> Chunks;

    private:
        friend class TInMemoryMetricsRegistry;
        std::shared_ptr<TSnapshotData> Data;
        TVector<size_t> ChunkIndexes;
        bool HasObservedTail = false;
        TInstant ObservedTailTimestamp;
    };

    class TSnapshot {
    public:
        TSnapshot();
        TSnapshot(const TSnapshot&);
        TSnapshot(TSnapshot&&) noexcept;
        TSnapshot& operator=(const TSnapshot&);
        TSnapshot& operator=(TSnapshot&&) noexcept;
        ~TSnapshot();

        TVector<TLineSnapshot> Lines() const;

    public:
        TVector<TLabel> CommonLabels;

    private:
        friend class TInMemoryMetricsRegistry;
        TVector<TLineSnapshot> SnapshotLines;
    };

    struct TStoredRecord {
        NHPTimer::STime TimestampTs = 0;
        ui64 Value = 0;
    };

    struct TInMemoryMetricsStats {
        ui64 MemoryUsedBytes = 0;
        ui64 CommittedBytes = 0;
        ui64 FreeChunks = 0;
        ui64 UsedChunks = 0;
        ui64 SealedChunks = 0;
        ui64 WritableChunks = 0;
        ui64 RetiringChunks = 0;
        ui64 Lines = 0;
        ui64 ClosedLines = 0;
        ui64 ReuseWatermark = 0;
        ui64 AppendFailuresTotal = 0;
    };

    std::unique_ptr<TInMemoryMetricsRegistry> MakeInMemoryMetricsRegistry(TInMemoryMetricsConfig config);
    IActor* CreateInMemoryMetricsStatsActor(TDuration interval = TDuration::Seconds(1));

    class TInMemoryMetricsRegistry : public ISubSystem {
    public:
        explicit TInMemoryMetricsRegistry(TInMemoryMetricsConfig config);
        ~TInMemoryMetricsRegistry() override;

        // Single-writer contract: a line can be created only once for a key.
        // Duplicate CreateLine() calls return a noop writer.
        // Common labels are registry-wide mutable state and are not part of line identity.
        TLineWriter CreateLine(TStringBuf name, std::span<const TLabel> labels);
        TLineWriter CreateLine(TStringBuf name, std::span<const TLabel> labels, const TLineMeta& meta);
        void SetCommonLabels(std::span<const TLabel> labels);
        TVector<TLabel> GetCommonLabels() const;
        TInMemoryMetricsStats GetStats() const;
        void UpdateSelfMetrics();
        TSnapshot Snapshot() const;

        ui64 GetReuseWatermark() const noexcept;
        const TInMemoryMetricsConfig& GetConfig() const noexcept;

    private:
        friend class TLineWriter;
        friend struct TSnapshotData;

        class TImpl;

        void OnBeforeStop(TActorSystem&) override;
        void CloseLine(TLine* line) noexcept;
        bool Append(TLine* line, ui64 value) noexcept;
        TChunk* TryAcquireFreeChunk();
        TChunk* TryStealOldestChunk();
        void PublishSealedChunk(TChunk* chunk);
        void RetireChunk(TChunk* chunk);
        void ReturnChunkToFree(TChunk* chunk);
        void ReleasePinnedChunk(TChunk* chunk);
        void MaybeDropClosedLine(TLine* line);

    private:
        std::unique_ptr<TImpl> Impl;
    };

    TInMemoryMetricsRegistry& GetInMemoryMetrics(TActorSystem& actorSystem);
    const TInMemoryMetricsRegistry& GetInMemoryMetrics(const TActorSystem& actorSystem);
    TInMemoryMetricsRegistry& GetInMemoryMetrics();

} // namespace NActors
