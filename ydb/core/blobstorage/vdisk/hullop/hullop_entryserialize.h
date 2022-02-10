#pragma once

#include "defs.h"
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // THullDbSignatureRoutines
    ////////////////////////////////////////////////////////////////////////////
    struct THullDbSignatureRoutines {
        static const ui32 CurSignature;

        static TString Serialize(const NKikimrVDiskData::THullDbEntryPoint &pb); 
        static bool Parse(NKikimrVDiskData::THullDbEntryPoint &pb,
                          const TString &source, 
                          TString &explanation); 
    };

} // NKikimr
