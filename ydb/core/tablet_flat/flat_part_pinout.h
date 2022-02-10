#pragma once

#include <util/generic/array_ref.h>
#include <util/generic/vector.h>
#include <util/generic/ptr.h>

namespace NKikimr {
namespace NTable {

    class TPinout {
    public:
        struct TPin {
            TPin() = default;

            TPin(ui32 from, ui32 to)
                : From(from)
                , To(to)
            {

            }

            ui32 From;
            ui32 To;
        };

        using TPins = TVector<TPin>;
        using const_iterator = TPins::const_iterator;

        TPinout() = delete;

        TPinout(TVector<TPin>&& pins, TVector<ui32>&& altGroups)
            : Pins_(std::move(pins))
            , AltGroups_(std::move(altGroups))
        {

        }

        explicit operator bool() const noexcept
        {
            return bool(Pins_);
        }

        const_iterator begin() const noexcept
        {
            return Pins_.begin();
        }

        const_iterator end() const noexcept
        {
            return Pins_.end();
        }

        size_t size() const noexcept
        {
            return Pins_.size();
        }

        TArrayRef<const TPin> Pins() const noexcept
        {
            return Pins_;
        }

        /**
         * Returns ids of required non-main groups
         */
        TArrayRef<const ui32> AltGroups() const noexcept
        {
            return AltGroups_;
        }

    private:
        TVector<TPin> Pins_;
        TVector<ui32> AltGroups_;
    };
}
}
