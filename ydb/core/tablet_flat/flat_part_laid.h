#pragma once

#include "flat_table_part.h"
#include "flat_part_screen.h"
#include "flat_part_slice.h"
#include "util_basics.h"

namespace NKikimr {
namespace NTable {

    using TBundleSlicesMap = THashMap<TLogoBlobID, TIntrusiveConstPtr<TSlices>>;

    struct TPartView {
        explicit operator bool() const
        {
            return bool(Part);
        }

        const TPart& operator*() const
        {
            return *Part;
        }

        const TPart* operator->() const
        {
            return Part.Get();
        }

        template<typename TSub>
        const TSub* As() const
        {
            return dynamic_cast<const TSub*>(Part.Get());
        }

        template<typename TSub>
        TIntrusiveConstPtr<TSub> Ref() const
        {
            return const_cast<TSub*>(As<TSub>());
        }

        TEpoch Epoch() const
        {
            return Part ? Part->Epoch : TEpoch::Max();
        }

        TPartView CloneWithEpoch(TEpoch epoch) const {
            return TPartView{ Part->CloneWithEpoch(epoch), Screen, Slices };
        }

        TIntrusiveConstPtr<TPart> Part;
        TIntrusiveConstPtr<TScreen> Screen;
        TIntrusiveConstPtr<TSlices> Slices;
    };

    struct TWriteStats {
        /*_ Some writer stats collection    */

        ui64 Rows   = 0;
        ui64 Bytes  = 0;    /* Raw (unencoded) data pages size  */
        ui64 Coded  = 0;    /* Data pages size after encoding   */
        ui64 Drops  = 0;    /* Total rows with ERowOp::Erase code */
        ui64 Parts  = 0;    /* Total number of produced parts   */
        ui64 HiddenRows = 0; /* Hidden (non-head) total rows */
        ui64 HiddenDrops = 0; /* Hidden (non-head) rows with ERowOp::Erase */
    };

}
}
