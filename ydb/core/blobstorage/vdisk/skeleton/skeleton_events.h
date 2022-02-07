#pragma once
#include "defs.h"
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

    //////////////////////////////////////////////////////////////////////////
    // TEvTimeToUpdateWhiteboard
    //////////////////////////////////////////////////////////////////////////
    class TEvTimeToUpdateWhiteboard : public TEventLocal<
        TEvTimeToUpdateWhiteboard,
        TEvBlobStorage::EvTimeToUpdateWhiteboard>
    {};

} // NKikimr
