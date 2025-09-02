#pragma once
#include "flat_sausage_grind.h"

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

    struct TCookie {
        const static ui32 OffType = 20;
        const static ui32 OffIdx = 12;

        const static ui32 MaskType  = 0xc00000;
        const static ui32 MaskIdx   = 0x3ff000;
        const static ui32 MaskSub   = 0x000fff;

        enum class EType : ui8 {
            Log     = 0,    /* Regular executor blob    */
        };

        enum class EIdx : ui32 {
            Snap    = 0,    /* Legacy, now is unused    */
            Redo    = 1,    /* DBase redo log flow      */
            Alter   = 2,    /* DBase scheme TAlter log  */
            Pack    = 3,    /* NPageCollection metablobs       */
            Turn    = 5,    /* Data transmutations log  */
            RedoLz4 = 6,
            SnapLz4 = 7,    /* TExecutor state snapshot */
            TurnLz4 = 8,
            Loan    = 9,    /* Borrow logic changes log */
            GCExt   = 10,   /* Extended GC logic log    */

            Raw     = 17,   /* Uniform space [Raw, End] */
            End     = 0x3ff
        };

        static_assert(MaskType ^ MaskIdx ^ MaskSub == 0xffffff, "");
        static_assert((MaskIdx >> OffIdx) == ui32(EIdx::End), "");

        TCookie(ui32 raw) noexcept : Raw(raw) { }

        TCookie(EType type, EIdx index, ui32 sub)
            : Raw((ui32(type) << OffType) | (ui32(index) << OffIdx) | sub)
        {
            Y_ENSURE(sub <= MaskSub, "TCookue sub value is out of capacity");
        }

        EType Type() const { return EType((Raw & MaskType) >> OffType); }
        EIdx Index() const { return EIdx((Raw & MaskIdx) >> OffIdx); }
        ui32 Sub() const { return (Raw & MaskSub); }

        static NPageCollection::TCookieRange CookieRange(EIdx index)
        {
            return { RawFor(index, 0), RawFor(index, Max<ui32>()) };
        }

        static NPageCollection::TCookieRange CookieRangeRaw()
        {
            return { RawFor(EIdx::Raw, 0), RawFor(EIdx::End, Max<ui32>()) };
        }

        static ui32 RawFor(EIdx type, ui32 offset)
        {
            offset = Min(offset, ui32(MaskSub));

            return TCookie(EType::Log, type, offset).Raw;
        }

        const ui32 Raw; /* only lower 24 bits are used */
    };

    enum class ELogCommitMeta : ui32 {
        LeaseInfo = 1,
    };

}
}
}
