#pragma once

#include "metric_line.h"
#include <memory>

namespace NActors {
    template<class TFrontend>
    class TLine {
    public:
        TLine() noexcept = default;
        explicit TLine(std::shared_ptr<IMetricLine> state) noexcept;

        TLine(TLine&& rhs) noexcept;
        TLine& operator=(TLine&& rhs) noexcept;

        TLine(const TLine&) = delete;
        TLine& operator=(const TLine&) = delete;

        ~TLine();

        explicit operator bool() const noexcept {
            return State && State->IsValid();
        }

        // Pending handles are valid but Append returns false until registration
        // and chunk delivery. Capacity exhaustion also returns false.
        bool Append(const typename TFrontend::TValueType& value) noexcept;
        void Close() noexcept;
        ui32 GetLineId() const noexcept;

    private:
        std::shared_ptr<IMetricLine> State;
    };

} // namespace NActors

#include "line_impl.h"
