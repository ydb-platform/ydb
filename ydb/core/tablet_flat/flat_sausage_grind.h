#pragma once

#include "util_fmt_desc.h"
#include "util_fmt_abort.h"
#include "flat_sausage_solid.h"

namespace NKikimr {
namespace NPageCollection {

    struct TSlot {
        TSlot(ui8 cnl, ui32 group) : Channel(cnl), Group(group) { }

        ui8 Channel = Max<ui8>();
        ui32 Group = NPageCollection::TLargeGlobId::InvalidGroup;
    };

    struct TCookieRange {
        constexpr bool Has(ui32 ref) const noexcept
        {
            return Head <= ref && ref <= Tail;
        }

        ui32 Head, Tail; /* [ Head, Tail], but Tail is used as a gap */
    };

    class TCookieAllocator {
        static constexpr ui32 Mask = Max<ui32>() >> 8;
        static constexpr ui32 Spacer = ui32(1) << 31;

    public:
        TCookieAllocator(ui64 tablet, ui64 stamp, TCookieRange cookieRange, TArrayRef<const TSlot> row)
            : Tablet(tablet)
            , Gen(stamp >> 32)
            , CookieRange(cookieRange)
            , Span(CookieRange.Tail - CookieRange.Head)
            , Step(stamp)
            , Slots(Max<ui8>(), Max<ui8>())
        {
            if ((cookieRange.Head & ~Mask) || (cookieRange.Tail & ~Mask)) {
                Y_ABORT("CookieRange range can use only lower 24 bits");
            } else if (cookieRange.Head > cookieRange.Tail) {
                Y_ABORT("Invalid TLogoBlobID cookieRange capacity range");
            }

            for (auto &one: row) {
                auto &place = Slots[one.Channel];

                Y_ABORT_UNLESS(one.Channel != Max<ui8>(),
                    "Channel cannot be set to Max<ui8>() value");

                if (place != Max<ui8>()) {
                    /* Channel already associated with a group */
                    Y_ABORT_UNLESS(Group[place] == one.Group,
                        "Channel assigned to different groups");
                    continue;
                }

                place = Group.size();
                Group.emplace_back(one.Group);
            }

            State.resize(Group.size(), 0);
        }

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "Cookies{" << Tablet << ":" << Gen << ":" << Step
                << " cookieRange[" << CookieRange.Head << " +" << Span << "]}";
        }

        ui32 GroupBy(ui8 channel) const noexcept
        {
            return Group[Locate(channel)];
        }

        ui64 Stamp() const noexcept
        {
            return (ui64(Gen) << 32) | Step;
        }

        TGlobId Do(ui8 channel, ui32 bytes) noexcept
        {
            const auto slot = Locate(channel);

            auto first = Allocate(slot, 1, false);

            TLogoBlobID lead(Tablet, Gen, Step, channel, bytes, first);

            return { lead, Group[slot] };
        }

        TLargeGlobId Do(ui8 cnl, ui32 bytes, ui32 lim) noexcept
        {
            const auto slot = Locate(cnl);

            /* Eeach TLargeGlobId have to be gapped with a single unused TLogoBlobID
                items for ability to chop them with NPageCollection::TGroupBlobsByCookie tool. */

            auto first = Allocate(slot, bytes / lim + (bytes % lim > 0), true);

            TLogoBlobID lead(Tablet, Gen, Step, cnl, Min(lim, bytes), first);

            return { Group[slot], lead, bytes };
        }

    private:
        ui8 Locate(ui8 channel) const noexcept
        {
            auto slot = Slots[channel];

            Y_ABORT_UNLESS(slot != Max<ui8>(), "Requested unknown channel");

            return slot;
        }

        ui32 Allocate(ui8 slot, ui32 num, bool gap) noexcept
        {
            auto &value = State.at(slot);

            const ui32 left = value && (gap || (value & Spacer) ? 1 : 0);

            num += left;

            if (num > Span || Span - num <= (Mask & value))
                Y_Fail(NFmt::Do(*this) << " #" << slot << " was exhausted");

            ui32 to = ((Mask & value) + num) | (gap ? Spacer : 0);

            return CookieRange.Head + (Mask & std::exchange(value, to)) + left;
        }

    public:
        const ui64 Tablet = 0;
        const ui32 Gen = Max<ui32>();
        const TCookieRange CookieRange;       /* Cookie range row over channels slices*/

    protected:
        const ui32 Span = 0;

        ui32 Step = Max<ui32>();
        TVector<ui8> Slots;     /* Mapping { channel -> slot (slice) }  */
        TVector<ui32> Group;    /* BS group for each channel slice      */
        TVector<ui32> State;
    };

    class TSteppedCookieAllocator : public TCookieAllocator {
    public:
        using TCookieAllocator::TCookieAllocator;

        void Switch(const ui32 step, bool strict) noexcept
        {
            if (step == Max<ui32>()) {
                Y_Fail(NFmt::Do(*this) << " is out of steps capacity");
            } else if (Step + (strict ? 1 : 0) > step && Step != Max<ui32>()) {
                Y_Fail(
                    NFmt::Do(*this) << " got stuck on step switch to "
                    <<  step << ", " << (strict ? "strict" : "weak"));
            } else if (std::exchange(Step, step) != step) {
                std::fill(State.begin(), State.end(), 0);
            }
        }
    };

}
}
