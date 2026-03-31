#pragma once

#include "line_types.h"

#include <util/generic/deque.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <type_traits>
#include <utility>

namespace NActors {
    struct TChunk;
    template<class TFrontend> class TLine;
    class TLineSnapshot;
    class TInMemoryMetricsBackend;
    class TInMemoryMetricsRegistry;

    using TReadSnapshotCallback = std::function<void(std::span<const TLabel>, std::span<const TLineSnapshot>)>;

    namespace NInMemoryMetricsPrivate {
        struct TChunkSnapshotView;
        class TSnapshot;

        struct TLineSnapshotAccess {
            template<class TCallback>
            static void ForEachChunk(const TLineSnapshot& snapshot, TCallback&& cb);

            static TInstant DecodeTimestampTs(const TLineSnapshot& snapshot, NHPTimer::STime ts) noexcept;
        };
    } // namespace NInMemoryMetricsPrivate

    template<class TValue>
    struct TGenericRecordView {
        TInstant Timestamp;
        TValue Value;
    };

    struct TTimeAnchor {
        NHPTimer::STime BaseCycles = 0;
        TInstant BaseWallClock;
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

    // Borrowing snapshot view of a single line. Instances are owned by backend
    // internals and must only be used during the enclosing ReadSnapshot() callback.
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
        friend struct NInMemoryMetricsPrivate::TLineSnapshotAccess;
        friend class TInMemoryMetricsBackend;
        friend class TInMemoryMetricsRegistry;

        template<class TCallback>
        void ForEachChunk(TCallback&& cb) const;
        TInstant DecodeTimestampTs(NHPTimer::STime ts) const noexcept;

        const NInMemoryMetricsPrivate::TSnapshot* Owner = nullptr;
        size_t ChunkBegin = 0;
        size_t ChunkCount = 0;
    };

} // namespace NActors
