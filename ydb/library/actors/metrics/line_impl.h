#pragma once

#include "inmemory_backend.h"

namespace NActors {

    template<class TFrontend>
    TLine<TFrontend>::TLine(TInMemoryMetricsBackend* backend, TLineWriterState* state) noexcept
        : Backend(backend)
        , State(state)
    {
    }

    template<class TFrontend>
    TLine<TFrontend>::TLine(TLine&& rhs) noexcept
        : Backend(rhs.Backend)
        , State(rhs.State)
    {
        rhs.Backend = nullptr;
        rhs.State = nullptr;
    }

    template<class TFrontend>
    TLine<TFrontend>& TLine<TFrontend>::operator=(TLine&& rhs) noexcept {
        if (this != &rhs) {
            Close();
            Backend = rhs.Backend;
            State = rhs.State;
            rhs.Backend = nullptr;
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
        if (!Backend || !State) {
            return false;
        }
        return TFrontend::Append(*Backend, State, value);
    }

    template<class TFrontend>
    void TLine<TFrontend>::Close() noexcept {
        if (Backend && State) {
            Backend->CloseLine(State);
            Backend = nullptr;
            State = nullptr;
        }
    }

    template<class TFrontend>
    ui32 TLine<TFrontend>::GetLineId() const noexcept {
        return Backend && State ? Backend->GetLineId(State) : 0;
    }

    template<class TFrontend>
    TLine<TFrontend> TInMemoryMetricsBackend::CreateLine(TStringBuf name, std::span<const TLabel> labels, const typename TFrontend::TConfig& config) {
        return TLine<TFrontend>(this, CreateLineWithMeta(name, labels, TFrontend::MakeMeta(config)));
    }

} // namespace NActors
