#pragma once

#include "line_types.h"

#include <util/generic/deque.h>

#include <cstddef>
#include <memory>
#include <span>
#include <type_traits>
#include <utility>

namespace NActors {
    struct TChunk;
    template<class TFrontend> class TLine;
    class TLineSnapshot;
    class TSnapshot;
    class TInMemoryMetricsBackend;
    class TInMemoryMetricsRegistry;

    struct TLineSnapshotAccess {
        template<class TCallback>
        static void ForEachChunk(const TLineSnapshot& snapshot, TCallback&& cb);

        static TInstant DecodeTimestampTs(const TLineSnapshot& snapshot, NHPTimer::STime ts) noexcept;
    };

    template<class TValue>
    struct TGenericRecordView {
        TInstant Timestamp;
        TValue Value;
    };

    struct TTimeAnchor {
        NHPTimer::STime BaseCycles = 0;
        TInstant BaseWallClock;
    };

    struct TChunkSnapshotView {
        TChunkView Meta;
        std::span<const char> Payload;
    };

    struct TSnapshotPinnedChunk {
        TInMemoryMetricsBackend* Backend = nullptr;
        TChunk* Chunk = nullptr;
        TChunkView View;
    };

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

        template<class TValueType>
        TDeque<TGenericRecordView<TValueType>> ReadRecordsAs() const {
            return ReadRecordsAsInRange<TValueType>(TInstant::Zero(), TInstant::Max());
        }

        template<class TValueType>
        TDeque<TGenericRecordView<TValueType>> ReadRecordsAsInRange(TInstant beginTs, TInstant endTs) const {
            TDeque<TGenericRecordView<TValueType>> records;
            const auto* frontend = Meta.Frontend;
            auto* output = std::addressof(records);
            auto invoker = [](void* opaque, TInstant timestamp, const void* valuePtr) {
                auto* output = static_cast<TDeque<TGenericRecordView<TValueType>>*>(opaque);
                output->push_back(TGenericRecordView<TValueType>{
                    .Timestamp = timestamp,
                    .Value = *static_cast<const TValueType*>(valuePtr),
                });
            };
            if (frontend && frontend->ReadRange) {
                frontend->ReadRange(*this, beginTs, endTs, output, invoker);
            }
            return records;
        }

        template<class TValueType>
        TDeque<TValueType> ReadValuesAs() const {
            return ReadValuesAsInRange<TValueType>(TInstant::Zero(), TInstant::Max());
        }

        template<class TValueType>
        TDeque<TValueType> ReadValuesAsInRange(TInstant beginTs, TInstant endTs) const {
            TDeque<TValueType> values;
            const auto* frontend = Meta.Frontend;
            auto* output = std::addressof(values);
            auto invoker = [](void* opaque, TInstant, const void* valuePtr) {
                auto* output = static_cast<TDeque<TValueType>*>(opaque);
                output->push_back(*static_cast<const TValueType*>(valuePtr));
            };
            if (frontend && frontend->ReadRange) {
                frontend->ReadRange(*this, beginTs, endTs, output, invoker);
            }
            return values;
        }

    public:
        ui32 LineId = 0;
        TString Name;
        TVector<TLabel> Labels;
        TLineMeta Meta;
        bool Closed = false;

    private:
        template<class TFrontend>
        friend class TLine;
        friend struct TLineSnapshotAccess;
        friend class TInMemoryMetricsBackend;
        friend class TInMemoryMetricsRegistry;

        template<class TCallback>
        void ForEachChunk(TCallback&& cb) const;
        TInstant DecodeTimestampTs(NHPTimer::STime ts) const noexcept;

        const TSnapshot* Owner = nullptr;
        size_t ChunkBegin = 0;
        size_t ChunkCount = 0;
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
        friend class TLineSnapshot;

        TSnapshot();
        ~TSnapshot();

        TTimeAnchor Anchor;
        TVector<TSnapshotPinnedChunk> SnapshotChunks;
        TVector<TLineSnapshot> SnapshotLines;
    };

} // namespace NActors
