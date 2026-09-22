#pragma once

#include "line_write.h"

namespace NActors {
    class TInMemoryMetricsBackend;

    template<class TFrontend>
    class TLine {
    public:
        TLine() noexcept = default;
        TLine(TInMemoryMetricsBackend* backend, std::shared_ptr<TLineWriterState> state) noexcept;

        TLine(TLine&& rhs) noexcept;
        TLine& operator=(TLine&& rhs) noexcept;

        TLine(const TLine&) = delete;
        TLine& operator=(const TLine&) = delete;

        ~TLine();

        explicit operator bool() const noexcept {
            return Backend && State && State->Status.load(std::memory_order_acquire) != ELineWriterStatus::Rejected;
        }

        // Pending handles are valid but Append returns false until registration
        // and chunk delivery. Capacity exhaustion also returns false.
        bool Append(const typename TFrontend::TValueType& value) noexcept;
        void Close() noexcept;
        ui32 GetLineId() const noexcept;

    private:
        TInMemoryMetricsBackend* Backend = nullptr;
        std::shared_ptr<TLineWriterState> State;
    };

} // namespace NActors

#include "line_impl.h"
