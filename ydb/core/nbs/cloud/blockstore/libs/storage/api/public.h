#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/random/fast.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf IntermediateWriteBufferTagName =
    "use-intermediate-write-buffer";

////////////////////////////////////////////////////////////////////////////////
// BackpressureReport event descriptor

struct TBackpressureReport
{
    double FreshIndexScore = 0;
    double CompactionScore = 0;
    double DiskSpaceScore = 0;
    double CleanupScore = 0;
};

////////////////////////////////////////////////////////////////////////////////
// Broken data marker

inline TStringBuf GetBrokenDataMarker()
{
    struct TBuf
    {
        TString Magic;

        TBuf()
            : Magic(4096, '0')
        {
            TFastRng64 rng(1337);
            for (ui32 i = 0; i < Magic.size(); i += 8) {
                ui64 x = rng.GenRand();
                ui8* bytes = reinterpret_cast<ui8*>(&x);
                memcpy(Magic.begin() + i, bytes, 8);
            }
        }
    };

    static TBuf buf;
    return buf.Magic;
}

}   // namespace NYdb::NBS::NStorage
