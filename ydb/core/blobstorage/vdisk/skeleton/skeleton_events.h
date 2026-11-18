#pragma once
#include "defs.h"
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    //////////////////////////////////////////////////////////////////////////
    // TEvTimeToUpdateStats
    //////////////////////////////////////////////////////////////////////////
    class TEvTimeToUpdateStats : public TEventLocal<
        TEvTimeToUpdateStats,
        TEvBlobStorage::EvTimeToUpdateStats>
    {};

} // NKikimr
