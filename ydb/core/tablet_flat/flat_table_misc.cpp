#include "flat_table_misc.h"
#include "flat_mem_warm.h"

namespace NKikimr {
namespace NTable {

    IPages::TResult MemTableRefLookup(const TMemTable *memTable, ui64 ref, ui32) noexcept
    {
        const auto &data = memTable->GetBlobs()->Get(ref).Data;

        Y_ABORT_UNLESS(data, "Got external blob in NMem::TBlobs with no data");

        return { true, &data };
    }

}
}
