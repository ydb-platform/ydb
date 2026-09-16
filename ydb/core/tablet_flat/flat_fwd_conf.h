#pragma once

#include <util/system/types.h>
#include <util/generic/array_ref.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/stream/format.h>

#include <memory>

namespace NKikimr {
namespace NTable {
namespace NFwd {

    struct TConf {
        void Describe(IOutputStream &out) const
        {
            out
                << "TConf{"
                << "high=" << HumanReadableSize(AheadHi, SF_BYTES)
                << ",low=" << HumanReadableSize(AheadLo, SF_BYTES)
                << ",edge=" << Edge
                << ",tablet=" << Tablet
                << ",trace=" << Trace
                << ",moveGroups=" << (ForceMaterializeGroups ? ForceMaterializeGroups->size() : 0)
                << "}";
        }

        /*_ Cache lines read ahead settings     */

        ui64 AheadHi = 1;
        ui64 AheadLo = 8 * 1024 * 1024;

        /*_ Outline blobs materialization conf  */

        ui32 Edge = Max<ui32>();    /* Outlined blob materialization edge    */
        ui64 Tablet = 0;            /* Use Edge only for this tablet if set  */
        TVector<ui32> Keys;         /* Always materialize these tag values   */

        /* Extern blobs left in these BlobStorage groups are materialized
            regardless of Edge, so a compaction rewrites them into the groups
            channels currently point at.
         */
        std::shared_ptr<const THashSet<ui32>> ForceMaterializeGroups;

        /*_ Misc features configuration          */

        bool Trace = false;         /* Track seen blobs used by reference    */
    };

}
}
}
