#pragma once

#include <util/system/types.h>
#include <util/generic/array_ref.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NTable {
namespace NFwd {

    struct TConf {
        /*_ Cache lines read ahead settings     */

        ui64 AheadHi = 1;
        ui64 AheadLo = 8 * 1024 * 1024;

        /*_ Outline blobs materialization conf  */

        ui32 Edge = Max<ui32>();    /* Outlined blob materialization edge    */
        ui64 Tablet = 0;            /* Use Edge only for this tablet if set  */
        TVector<ui32> Keys;         /* Always materialize these tag values   */

        /*_ Misc features configuration          */

        bool Trace = false;         /* Track seen blobs used by reference    */
    };

}
}
}
