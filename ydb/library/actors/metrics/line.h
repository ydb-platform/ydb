#pragma once

#include "line_base.h"

namespace NActors {

    template<class TFrontend>
    class TLine {
    public:
        using TValueType = typename TFrontend::TValueType;

        TLine() noexcept = default;
        TLine(ILineWriteBackend* backend, TLineWriterState* state) noexcept
            : Backend(backend)
            , State(state)
        {
        }

        TLine(TLine&& rhs) noexcept
            : Backend(rhs.Backend)
            , State(rhs.State)
        {
            rhs.Backend = nullptr;
            rhs.State = nullptr;
        }

        TLine& operator=(TLine&& rhs) noexcept {
            if (this != &rhs) {
                Close();
                Backend = rhs.Backend;
                State = rhs.State;
                rhs.Backend = nullptr;
                rhs.State = nullptr;
            }
            return *this;
        }

        TLine(const TLine&) = delete;
        TLine& operator=(const TLine&) = delete;

        ~TLine() {
            Close();
        }

        explicit operator bool() const noexcept {
            return Backend && State;
        }

        bool Append(const TValueType& value) noexcept {
            if (!Backend || !State) {
                return false;
            }
            return TFrontend::Append(*Backend, State, value);
        }

        void Close() noexcept {
            if (Backend && State) {
                Backend->CloseLine(State);
                Backend = nullptr;
                State = nullptr;
            }
        }

        ui32 GetLineId() const noexcept {
            return Backend && State ? Backend->GetLineId(State) : 0;
        }

    private:
        ILineWriteBackend* Backend = nullptr;
        TLineWriterState* State = nullptr;
    };

} // namespace NActors
