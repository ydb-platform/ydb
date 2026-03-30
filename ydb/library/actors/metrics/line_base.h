#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/hp_timer.h>

#include <cstring>
#include <atomic>
#include <memory>
#include <span>
#include <type_traits>
#include <utility>
namespace NActors {
    class TLineReader;
    struct TChunk;

    template<class TValue>
    struct TGenericRecordView {
        TInstant Timestamp;
        TValue Value;
    };

    class TLineSnapshot;
    class TSnapshot;
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
        TVector<TString> AllowedMetricPrefixes;
    };

    struct TLineKey {
        TString Name;
        // Line identity is order-independent by labels.
        // Labels stored in TLineKey must be canonically sorted by (Name, Value).
        TVector<TLabel> Labels;

        bool operator==(const TLineKey& rhs) const noexcept;
    };

    struct TLineKeyHash {
        size_t operator()(const TLineKey& key) const noexcept;
    };

    struct TChunkView {
        ui32 ChunkId = 0;
        TInstant FirstTs;
        TInstant LastTs;
        ui32 CommittedBytes = 0;
    };

    struct TChunkSnapshotView {
        TChunkView Meta;
        std::span<const char> Payload;
    };

    struct TLinePublishState {
        bool HasLastPublished = false;
        ui64 LastPublishedValue = 0;
        NHPTimer::STime LastPublishedTs = 0;
        NHPTimer::STime LastObservedTs = 0;
    };

    class TLineWriterState {
    public:
        TLineReader* Reader = nullptr;
        std::atomic<bool> HasLastPublished = false;
        std::atomic<ui64> LastPublishedValue = 0;
        std::atomic<NHPTimer::STime> LastPublishedTs = 0;
        std::atomic<NHPTimer::STime> LastObservedTs = 0;
    };

    class ILineWriteBackend {
    public:
        virtual ~ILineWriteBackend() = default;

        virtual void CloseLine(TLineWriterState* state) noexcept = 0;
        // Backend only manages raw chunk memory and lifetime. Physical record layout
        // is defined by a concrete line frontend and passed here as opaque bytes.
        virtual bool AppendChunkData(
            TLineWriterState* state,
            std::span<const char> data,
            NHPTimer::STime firstTs,
            NHPTimer::STime lastTs) noexcept = 0;
        virtual NHPTimer::STime CurrentTimestampTs() const noexcept = 0;
        virtual TLinePublishState GetPublishState(const TLineWriterState* state) const noexcept = 0;
        virtual ui32 GetLineId(const TLineWriterState* state) const noexcept = 0;
        virtual void MarkObserved(TLineWriterState* state, NHPTimer::STime nowTs) noexcept = 0;
        virtual void MarkPublished(TLineWriterState* state, ui64 value, NHPTimer::STime nowTs) noexcept = 0;
        virtual void ReleasePinnedChunk(TChunk* chunk) noexcept = 0;
    };

    namespace NInMemoryMetricsPrivate {
        template<class TValue>
        ui64 EncodeLineValue(const TValue& value) noexcept {
            static_assert(std::is_trivially_copyable_v<TValue>);
            static_assert(sizeof(TValue) <= sizeof(ui64));

            ui64 encoded = 0;
            std::memcpy(&encoded, &value, sizeof(TValue));
            return encoded;
        }

        template<class TValue>
        TValue DecodeLineValue(ui64 value) noexcept {
            static_assert(std::is_trivially_copyable_v<TValue>);
            static_assert(sizeof(TValue) <= sizeof(ui64));

            TValue decoded{};
            std::memcpy(&decoded, &value, sizeof(TValue));
            return decoded;
        }
    } // namespace NInMemoryMetricsPrivate

    struct TLineFrontendOps {
        using TInvokeValue = void (*)(void*, TInstant, const void*);
        using TReadRange = void (*)(const TLineSnapshot&, TInstant, TInstant, void*, TInvokeValue);

        TStringBuf Name;
        TReadRange ReadRange = nullptr;
    };

    struct TLineMeta {
        const TLineFrontendOps* Frontend = nullptr;

        TLineMeta() noexcept;
        explicit TLineMeta(const TLineFrontendOps* frontend) noexcept;

        TStringBuf FrontendName() const noexcept;
    };

    // Borrowing snapshot view of a single line. Instances are owned by TSnapshot
    // and must only be used during the enclosing ReadSnapshot() callback.
    class TLineSnapshot {
    public:
        TLineSnapshot();
        TLineSnapshot(const TLineSnapshot&) = delete;
        TLineSnapshot(TLineSnapshot&&) noexcept;
        TLineSnapshot& operator=(const TLineSnapshot&) = delete;
        TLineSnapshot& operator=(TLineSnapshot&&) noexcept;
        ~TLineSnapshot();

        template<class TCallback>
        void ForEachChunk(TCallback&& cb) const;
        TInstant DecodeTimestampTs(NHPTimer::STime ts) const noexcept;
        template<class TValueType, class TCallback>
        void ForEachRecordAs(TCallback&& cb) const {
            ForEachRecordAsInRange<TValueType>(TInstant::Zero(), TInstant::Max(), std::forward<TCallback>(cb));
        }
        template<class TValueType, class TCallback>
        void ForEachRecordAsInRange(TInstant beginTs, TInstant endTs, TCallback&& cb) const {
            const auto* frontend = Meta.Frontend;
            auto* callback = std::addressof(cb);
            auto invoker = [](void* opaque, TInstant timestamp, const void* valuePtr) {
                auto* callback = static_cast<std::remove_reference_t<TCallback>*>(opaque);
                (*callback)(TGenericRecordView<TValueType>{
                    .Timestamp = timestamp,
                    .Value = *static_cast<const TValueType*>(valuePtr),
                });
            };
            if (frontend && frontend->ReadRange) {
                frontend->ReadRange(*this, beginTs, endTs, callback, invoker);
            }
        }

        TInstant GetLastPublishedTimestamp() const noexcept {
            return LastPublishedTimestamp;
        }

        TInstant GetLastObservedTimestamp() const noexcept {
            return LastObservedTimestamp;
        }

    public:
        ui32 LineId = 0;
        TString Name;
        TVector<TLabel> Labels;
        TLineMeta Meta;
        bool Closed = false;
        TVector<TChunkView> Chunks;

    private:
        template<class TFrontend>
        friend class TLine;
        friend class TInMemoryMetricsBackend;
        friend class TInMemoryMetricsRegistry;
        std::shared_ptr<TSnapshotData> Data;
        TVector<size_t> ChunkIndexes;
        TInstant LastPublishedTimestamp;
        TInstant LastObservedTimestamp;
    };

    // Borrowing snapshot view owned by ReadSnapshot(). It cannot be created or
    // stored by value outside backend internals and is only valid during cb().
    class TSnapshot {
    public:
        TSnapshot(const TSnapshot&) = delete;
        TSnapshot(TSnapshot&&) = delete;
        TSnapshot& operator=(const TSnapshot&) = delete;
        TSnapshot& operator=(TSnapshot&&) = delete;

        template<class TCallback>
        void ForEachLine(TCallback&& cb) const {
            for (const auto& line : SnapshotLines) {
                cb(line);
            }
        }

    public:
        TVector<TLabel> CommonLabels;

    private:
        friend class TInMemoryMetricsBackend;
        friend class TInMemoryMetricsRegistry;
        TSnapshot();
        ~TSnapshot();
        TVector<TLineSnapshot> SnapshotLines;
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

    TLineKey MakeLineKey(TStringBuf name, std::span<const TLabel> labels);
    TVector<TLabel> NormalizeCommonLabels(std::span<const TLabel> labels);

} // namespace NActors
