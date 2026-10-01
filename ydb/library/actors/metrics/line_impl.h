#pragma once

#include "line.h"

namespace NActors {

    template<class TFrontend>
    TLine<TFrontend>::TLine(std::shared_ptr<IMetricLine> state) noexcept
        : State(std::move(state))
    {
    }

    template<class TFrontend>
    TLine<TFrontend>::TLine(TLine&& rhs) noexcept
        : State(std::move(rhs.State))
    {
        rhs.State = nullptr;
    }

    template<class TFrontend>
    TLine<TFrontend>& TLine<TFrontend>::operator=(TLine&& rhs) noexcept {
        if (this != &rhs) {
            Close();
            State = std::move(rhs.State);
            rhs.State = nullptr;
        }
        return *this;
    }

    template<class TFrontend>
    TLine<TFrontend>::~TLine() {
        Close();
    }

    template<class TFrontend>
    bool TLine<TFrontend>::Append(const typename TFrontend::TValueType& value) noexcept {
        if (!State) {
            return false;
        }
        return TFrontend::Append(*State, value);
    }

    template<class TFrontend>
    void TLine<TFrontend>::Close() noexcept {
        if (State) {
            State->Close();
            State = nullptr;
        }
    }

    template<class TFrontend>
    ui32 TLine<TFrontend>::GetLineId() const noexcept {
        return State ? State->GetLineId() : 0;
    }

} // namespace NActors
