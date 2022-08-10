#pragma once

#include "flat_table_subset.h"
#include "flat_sausage_solid.h"
#include "util_fmt_flat.h"

#include <ydb/core/base/row_version.h>

namespace NKikimr {
namespace NTable {

    struct TChange {
        using TMemGlobs = TVector<NPageCollection::TMemGlob>;

        struct TStats {
            ui64 ChargeSieved = 0;
            ui64 ChargeWeeded = 0;
            ui64 SelectSieved = 0;
            ui64 SelectWeeded = 0;
            ui64 SelectNoKey = 0;
            ui64 SelectInvisible = 0;
        };

        struct TRemovedRowVersions {
            TRowVersion Lower;
            TRowVersion Upper;
        };

        TChange(TTxStamp stamp, ui64 serial)
            : Stamp(stamp), Serial(serial) { }

        bool HasAny() const noexcept
        {
            return Scheme || Redo || RemovedRowVersions;
        }

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "Change{" << Serial
                << ", redo " << Redo.size() << "b"
                << " alter " << Scheme.size() << "b"
                << " annex " << Annex.size()
                << ", ~" << NFmt::Arr(Affects) << " -" << NFmt::Arr(Deleted)
                << ", " << Garbage.size() << " gb}";
        }

        const TTxStamp Stamp{  Max<ui64>() };
        const ui64 Serial =  Max<ui64>();

        TString Scheme;         /* Serialized sheme delta       */
        TString Redo;           /* Serialized db redo log       */
        TMemGlobs Annex;        /* Enum of blobs used in redo   */

        TVector<ui32> Affects;  /* This tables touched in redo  */
        TVector<ui32> Deleted;  /* Tables deleted in some alter */
        TGarbage Garbage;       /* Wiped tables, ids in Deleted */

        ui32 Snapshots = 0;

        TMap<ui32, TVector<TRemovedRowVersions>> RemovedRowVersions;

        TStats Stats;
    };

}
}
