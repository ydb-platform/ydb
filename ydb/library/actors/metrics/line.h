#pragma once

#include "line_write.h"

namespace NActors {
    class TInMemoryMetricsBackend;

    template<class TFrontend>
    class TLine {
    public:
        TLine() noexcept = default;
        TLine(TInMemoryMetricsBackend* backend, TLineWriterState* state) noexcept;

        TLine(TLine&& rhs) noexcept;
        TLine& operator=(TLine&& rhs) noexcept;

        TLine(const TLine&) = delete;
        TLine& operator=(const TLine&) = delete;

        ~TLine();

        explicit operator bool() const noexcept {
            return Backend && State;
        }

        bool Append(const typename TFrontend::TValueType& value) noexcept;
        void Close() noexcept;
        ui32 GetLineId() const noexcept;

    private:
        TInMemoryMetricsBackend* Backend = nullptr;
        TLineWriterState* State = nullptr;
    };

} // namespace NActors

#include "line_impl.h"
