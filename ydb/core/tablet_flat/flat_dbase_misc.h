#pragma once
#include "flat_table_stats.h"

#include <ydb/core/scheme_types/scheme_raw_type_value.h>
#include <util/generic/hash.h>
#include <util/system/types.h>

namespace NKikimr {
namespace NTable {

    using TKeys = TArrayRef<const TRawTypeValue>;

    struct TDbStats {

        void Describe(IOutputStream &out) const noexcept
        {
            const ui64 sys = Parts.FlatIndexBytes + Parts.BTreeIndexBytes + Parts.ByKeyBytes + Parts.OtherBytes;

            out
                << "DBase{" << Tables << "t " << Parts.PartsCount << "p"
                << " " << Parts.RowsTotal << "r" << ", (" << MemTableBytes << " mem, "
                << sys << " sys, " << (Parts.CodedBytes + Parts.LargeBytes) << ")b}";
        }

        ui32 Tables = 0;
        ui64 TxCommited = 0;
        ui64 MemTableWaste = 0;
        ui64 MemTableBytes = 0;
        ui64 MemTableOps = 0;
        TPartStats Parts;
        THashMap<ui64, TPartStats> PartsPerTablet;
    };

}
}
