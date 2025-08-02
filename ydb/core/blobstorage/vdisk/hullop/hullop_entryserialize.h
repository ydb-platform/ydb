#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKikimrVDiskData {
    class THullDbEntryPoint;
}

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // THullDbSignatureRoutines
    ////////////////////////////////////////////////////////////////////////////
    struct THullDbSignatureRoutines {
        static const ui32 CurSignature;

        static TString Serialize(const NKikimrVDiskData::THullDbEntryPoint &pb);
        static bool ParseArray(NKikimrVDiskData::THullDbEntryPoint &pb,
                          const char* data,
                          size_t size,
                          TString &explanation);
        static bool Parse(NKikimrVDiskData::THullDbEntryPoint &pb,
                          const TString &source,
                          TString &explanation);
    };

} // NKikimr
