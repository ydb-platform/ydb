#pragma once
#include "defs.h"

#include <ydb/core/blobstorage/base/blobstorage_vdiskid.h>

namespace NKikimr {

struct TTestConfig : public TThrRefBase {
    TVDiskID VDiskID;
    TActorId YardActorID;

    TTestConfig(const TVDiskID &vDiskID, const TActorId &yardActorID)
        : VDiskID(vDiskID)
        , YardActorID(yardActorID)
    {}
};


} // NKikimr
