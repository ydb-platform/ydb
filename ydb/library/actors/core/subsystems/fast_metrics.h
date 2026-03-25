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
    class TFastMetricsRegistry;
    class TLine;
    struct TChunk;
    struct TSnapshotData;

    struct TFastMetricsConfig {
        ui64 MemoryBytes = 0;
        ui32 ChunkSizeBytes = 4096;
        ui32 MaxLines = 0;
    };

    struct TLabel {
        TString Name;
        TString Value;

        bool operator==(const TLabel& rhs) const noexcept;
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

    class TLineWriter {
    public:
        TLineWriter() noexcept = default;
        TLineWriter(TFastMetricsRegistry* registry, TLine* line) noexcept;
        TLineWriter(TLineWriter&& rhs) noexcept;
        TLineWriter& operator=(TLineWriter&& rhs) noexcept;
        TLineWriter(const TLineWriter&) = delete;
        TLineWriter& operator=(const TLineWriter&) = delete;
        ~TLineWriter();

        explicit operator bool() const noexcept;
        bool Append(ui64 value) noexcept;
        void Close() noexcept;
        ui32 GetLineId() const noexcept;

    private:
        TFastMetricsRegistry* Registry = nullptr;
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
        bool Closed = false;
        TVector<TChunkView> Chunks;

    private:
        friend class TFastMetricsRegistry;
        std::shared_ptr<TSnapshotData> Data;
        TVector<size_t> ChunkIndexes;
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

    private:
        friend class TFastMetricsRegistry;
        TVector<TLineSnapshot> SnapshotLines;
    };

    struct TStoredRecord {
        NHPTimer::STime TimestampTs = 0;
        ui64 Value = 0;
    };

    std::unique_ptr<TFastMetricsRegistry> MakeFastMetricsRegistry(TFastMetricsConfig config);

    class TFastMetricsRegistry : public ISubSystem {
    public:
        explicit TFastMetricsRegistry(TFastMetricsConfig config);
        ~TFastMetricsRegistry() override;

        TLineWriter GetOrCreateLine(TStringBuf name, std::span<const TLabel> labels);
        TSnapshot Snapshot() const;

        ui64 GetReuseWatermark() const noexcept;
        const TFastMetricsConfig& GetConfig() const noexcept;

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

    TFastMetricsRegistry& GetFastMetrics(TActorSystem& actorSystem);
    const TFastMetricsRegistry& GetFastMetrics(const TActorSystem& actorSystem);
    TFastMetricsRegistry& GetFastMetrics();

} // namespace NActors
