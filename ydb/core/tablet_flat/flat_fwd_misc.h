#pragma once

#include <util/stream/output.h>
#include <util/stream/format.h>

namespace NKikimr {
namespace NTable {
namespace NFwd {

    struct TStat {
        /*
            Fetch >= Saved >= Usage + After;
            Fetch >= Before + Usage + After;

            eff = Usage * [ /Fetch; /(Before + Usage + After) ];
        */

        void Describe(IOutputStream &out) const
        {
            out
                << "TFwd{"
                << "fetch=" << HumanReadableSize(Fetch, SF_BYTES)
                << ",saved=" << HumanReadableSize(Saved, SF_BYTES)
                << ",usage=" << HumanReadableSize(Usage, SF_BYTES)
                << ",after=" << HumanReadableSize(After, SF_BYTES)
                << ",before=" << HumanReadableSize(Before, SF_BYTES)
                << "}";
        }

        TStat& operator +=(const TStat &stat)
        {
            Fetch += stat.Fetch;
            Saved += stat.Saved;
            Usage += stat.Usage;
            After += stat.After;
            Before += stat.Before;

            return *this;
        }

        auto operator<=>(const TStat&) const = default;

        ui64 Fetch = 0;     /* Requested to load by cache       */
        ui64 Saved = 0;     /* Obtained by cache with DoSave()  */
        ui64 Usage = 0;     /* Actually was used by client      */
        ui64 After = 0;     /* Dropped after fetch completed    */
        ui64 Before = 0;    /* Dropped before fetch completed   */
    };

}
}
}
