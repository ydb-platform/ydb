#include "line_write.h"
#include "inmemory_backend.h"

namespace NActors {
    TLineWriterState::TLineWriterState(TInMemoryMetricsBackend* backend) noexcept
        : Backend(backend)
    {}

    bool TLineWriterState::IsValid() const noexcept {
        return Status.load(std::memory_order_acquire) != ELineWriterStatus::Rejected;
    }

    void TLineWriterState::Close() noexcept {
        Backend->CloseLine(this);
    }

    ui32 TLineWriterState::GetLineId() const noexcept {
        return Backend->GetLineId(this);
    }

    NHPTimer::STime TLineWriterState::CurrentTimestampTs() const noexcept {
        return Backend->CurrentTimestampTs();
    }

    bool TLineWriterState::AccessChunkMemory(void* opaque, TAccessChunkMemoryFn access) noexcept {
        return Backend->AccessChunkMemory(this, opaque, access);
    }

    std::optional<ui64> TLineWriterState::GetLastMaterializedValue() const noexcept {
        return Backend->GetLastMaterializedValue(this);
    }

    void TLineWriterState::MarkMaterialized(ui64 value) noexcept {
        Backend->MarkMaterialized(this, value);
    }
} // namespace NActors
