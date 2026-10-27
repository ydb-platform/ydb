#pragma once

#include "metric_line.h"
#include <ydb/library/actors/util/intrusive_funnel_queue.h>

#include <atomic>
#include <span>
#include <memory>

namespace NActors {
    class TLineReader;
    struct TChunk;
    class TInMemoryMetricsBackend;

    enum class ELineWriterStatus : ui8 {
        Pending,
        Ready,
        Closed,
        Rejected,
    };

    class TLineWriterState : public IMetricLine, public TIntrusiveFunnelQueueItem<TLineWriterState>,
        public std::enable_shared_from_this<TLineWriterState> {
    public:
        explicit TLineWriterState(TInMemoryMetricsBackend* backend) noexcept;
        bool IsValid() const noexcept override;
        void Close() noexcept override;
        ui32 GetLineId() const noexcept override;
        NHPTimer::STime CurrentTimestampTs() const noexcept override;
        bool AccessChunkMemory(void* opaque, TAccessChunkMemoryFn access) noexcept override;
        std::optional<ui64> GetLastMaterializedValue() const noexcept override;
        void MarkMaterialized(ui64 value) noexcept override;

        std::atomic<bool> MaintenanceQueued = false;
        // Producer owns this slot until Push; consumer moves it before clearing
        // MaintenanceQueued. Keeps the intrusive node alive through detachment.
        std::shared_ptr<TLineWriterState> QueuedOwner;
        // Metadata-owner only. A queued Close may outlive registry membership.
        std::weak_ptr<TLineReader> RegisteredReader;
        std::atomic<ELineWriterStatus> Status = ELineWriterStatus::Pending;
        // Published by the single writer through Status=Closed.
        NHPTimer::STime ClosedAtTs = 0;
        // Published by Status=Ready; never dereferenced while Pending/Rejected.
        TLineReader* Reader = nullptr;
        std::atomic<TChunk*> LastMaterializedChunk = nullptr;
        std::atomic<ui64> LastMaterializedGeneration = 0;
        std::atomic<bool> HasLastMaterialized = false;
        std::atomic<ui64> LastMaterializedValue = 0;

    private:
        TInMemoryMetricsBackend* const Backend;
    };

} // namespace NActors
