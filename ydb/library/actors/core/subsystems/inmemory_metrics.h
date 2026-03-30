#pragma once

#include <ydb/library/actors/core/subsystem.h>
#include <ydb/library/actors/metrics/inmemory.h>
#include <util/generic/function.h>

#include <memory>
#include <utility>

namespace NActors {
    class TActorSystem;
    class TInMemoryMetricsRegistry;
    class IActor;
    class TLineReader;
    class TLineWriterState;
    struct TChunk;
    struct TSnapshotData;

    template<class TFrontend>
    class TLine {
    public:
        using TValueType = typename TFrontend::TValueType;

        TLine() noexcept = default;
        TLine(TInMemoryMetricsRegistry* registry, TLineWriterState* writer) noexcept;
        TLine(TLine&& rhs) noexcept;
        TLine& operator=(TLine&& rhs) noexcept;
        TLine(const TLine&) = delete;
        TLine& operator=(const TLine&) = delete;
        ~TLine();

        explicit operator bool() const noexcept;
        bool Append(const TValueType& value) noexcept;
        void Close() noexcept;
        ui32 GetLineId() const noexcept;
        ui32 ReleaseLineId() noexcept;

    private:
        TInMemoryMetricsRegistry* Registry = nullptr;
        TLineWriterState* Writer = nullptr;
    };

    std::unique_ptr<TInMemoryMetricsRegistry> MakeInMemoryMetricsRegistry(TInMemoryMetricsConfig config);
    IActor* CreateInMemoryMetricsStatsActor(TDuration interval = TDuration::Seconds(1));

    class TInMemoryMetricsRegistry : public ISubSystem {
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
        template<class TValue>
        friend struct TRawLineFrontend;
        template<class TValue>
        friend struct TOnChangeLineFrontend;
        friend struct TSnapshotData;

        class TImpl;

        void OnAfterStart(TActorSystem&) override;
        void OnBeforeStop(TActorSystem&) override;
        TLineWriterState* CreateLineWithMeta(TStringBuf name, std::span<const TLabel> labels, const TLineMeta& meta);
        void CloseLine(TLineWriterState* writer) noexcept;
        bool AppendStoredRecord(TLineWriterState* writer, const TStoredRecord& record) noexcept;
        NHPTimer::STime CurrentTimestampTs() const noexcept;
        TLinePublishState GetPublishState(const TLineWriterState* writer) const noexcept;
        ui32 GetLineId(const TLineWriterState* writer) const noexcept;
        void MarkObserved(TLineWriterState* writer, NHPTimer::STime nowTs) noexcept;
        void MarkPublished(TLineWriterState* writer, ui64 value, NHPTimer::STime nowTs) noexcept;
        TChunk* TryAcquireFreeChunk();
        TChunk* TryStealOldestChunk();
        void PublishSealedChunk(TChunk* chunk);
        void RetireChunk(TChunk* chunk);
        void ReturnChunkToFree(TChunk* chunk);
        void ReleasePinnedChunk(TChunk* chunk);
        void MaybeDropClosedLine(TLineReader* line);

    private:
        std::unique_ptr<TImpl> Impl;
    };

    TInMemoryMetricsRegistry* GetInMemoryMetrics(TActorSystem& actorSystem);
    const TInMemoryMetricsRegistry* GetInMemoryMetrics(const TActorSystem& actorSystem);
    TInMemoryMetricsRegistry* GetInMemoryMetrics();

    template<class TFrontend>
    TLine<TFrontend>::TLine(TInMemoryMetricsRegistry* registry, TLineWriterState* writer) noexcept
        : Registry(registry)
        , Writer(writer)
    {
    }

    template<class TFrontend>
    TLine<TFrontend>::TLine(TLine&& rhs) noexcept
        : Registry(rhs.Registry)
        , Writer(rhs.Writer)
    {
        rhs.Registry = nullptr;
        rhs.Writer = nullptr;
    }

    template<class TFrontend>
    TLine<TFrontend>& TLine<TFrontend>::operator=(TLine&& rhs) noexcept {
        if (this != &rhs) {
            Close();
            Registry = rhs.Registry;
            Writer = rhs.Writer;
            rhs.Registry = nullptr;
            rhs.Writer = nullptr;
        }
        return *this;
    }

    template<class TFrontend>
    TLine<TFrontend>::~TLine() {
        Close();
    }

    template<class TFrontend>
    TLine<TFrontend>::operator bool() const noexcept {
        return Registry && Writer;
    }

    template<class TFrontend>
    bool TLine<TFrontend>::Append(const TValueType& value) noexcept {
        if (!Registry || !Writer) {
            return false;
        }
        return TFrontend::Append(*Registry, Writer, value);
    }

    template<class TFrontend>
    void TLine<TFrontend>::Close() noexcept {
        if (Registry && Writer) {
            Registry->CloseLine(Writer);
            Registry = nullptr;
            Writer = nullptr;
        }
    }

    template<class TFrontend>
    ui32 TLine<TFrontend>::GetLineId() const noexcept {
        return Registry && Writer ? Registry->GetLineId(Writer) : 0;
    }

    template<class TFrontend>
    ui32 TLine<TFrontend>::ReleaseLineId() noexcept {
        const ui32 lineId = GetLineId();
        Registry = nullptr;
        Writer = nullptr;
        return lineId;
    }

    template<class TValue>
    bool TRawLineFrontend<TValue>::Append(TInMemoryMetricsRegistry& registry, TLineWriterState* writer, const TValue& value) noexcept {
        const ui64 encoded = NInMemoryMetricsPrivate::EncodeLineValue(value);
        const NHPTimer::STime nowTs = registry.CurrentTimestampTs();

        TStoredRecord record{
            .TimestampTs = nowTs,
            .Value = encoded,
        };
        if (!registry.AppendStoredRecord(writer, record)) {
            return false;
        }
        registry.MarkPublished(writer, encoded, nowTs);
        return true;
    }

    template<class TValue>
    bool TOnChangeLineFrontend<TValue>::Append(TInMemoryMetricsRegistry& registry, TLineWriterState* writer, const TValue& value) noexcept {
        const ui64 encoded = NInMemoryMetricsPrivate::EncodeLineValue(value);
        const NHPTimer::STime nowTs = registry.CurrentTimestampTs();
        const TLinePublishState state = registry.GetPublishState(writer);

        if (state.HasLastPublished && state.LastPublishedValue == encoded) {
            registry.MarkObserved(writer, nowTs);
            return true;
        } else if (state.HasLastPublished && state.LastObservedTs > state.LastPublishedTs) {
            TStoredRecord previousRecord{
                .TimestampTs = state.LastPublishedTs,
                .Value = state.LastPublishedValue,
            };
            if (!registry.AppendStoredRecord(writer, previousRecord)) {
                return false;
            }
            registry.MarkPublished(writer, state.LastPublishedValue, previousRecord.TimestampTs);
        }

        TStoredRecord record{
            .TimestampTs = nowTs,
            .Value = encoded,
        };
        if (!registry.AppendStoredRecord(writer, record)) {
            return false;
        }
        registry.MarkPublished(writer, encoded, nowTs);
        return true;
    }

} // namespace NActors
