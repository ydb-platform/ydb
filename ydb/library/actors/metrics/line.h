#pragma once

#include "line_base.h"

namespace NActors {

    template<class TFrontend>
    class TLine {
    public:
        using TValueType = typename TFrontend::TValueType;

        TLine() noexcept = default;
        TLine(ILineWriteBackend* backend, TLineWriterState* writer) noexcept
            : Backend(backend)
            , Writer(writer)
        {
        }

        TLine(TLine&& rhs) noexcept
            : Backend(rhs.Backend)
            , Writer(rhs.Writer)
        {
            rhs.Backend = nullptr;
            rhs.Writer = nullptr;
        }

        TLine& operator=(TLine&& rhs) noexcept {
            if (this != &rhs) {
                Close();
                Backend = rhs.Backend;
                Writer = rhs.Writer;
                rhs.Backend = nullptr;
                rhs.Writer = nullptr;
            }
            return *this;
        }

        TLine(const TLine&) = delete;
        TLine& operator=(const TLine&) = delete;

        ~TLine() {
            Close();
        }

        explicit operator bool() const noexcept {
            return Backend && Writer;
        }

        bool Append(const TValueType& value) noexcept {
            if (!Backend || !Writer) {
                return false;
            }
            return TFrontend::Append(*Backend, Writer, value);
        }

        void Close() noexcept {
            if (Backend && Writer) {
                Backend->CloseLine(Writer);
                Backend = nullptr;
                Writer = nullptr;
            }
        }

        ui32 GetLineId() const noexcept {
            return Backend && Writer ? Backend->GetLineId(Writer) : 0;
        }

        ui32 ReleaseLineId() noexcept {
            const ui32 lineId = GetLineId();
            Backend = nullptr;
            Writer = nullptr;
            return lineId;
        }

    private:
        ILineWriteBackend* Backend = nullptr;
        TLineWriterState* Writer = nullptr;
    };

} // namespace NActors
