#pragma once
#include "defs.h"

namespace NKikimr {

struct TInterconnectChannels {
    enum EInterconnectChannels {
        IC_COMMON,
        IC_BLOBSTORAGE,
        IC_BLOBSTORAGE_ASYNC_DATA,
        IC_BLOBSTORAGE_SYNCER,
        IC_BLOBSTORAGE_DISCOVER,
        IC_BLOBSTORAGE_SMALL_MSG,
        IC_TABLETS_SMALL,   // < 1 KB
        IC_TABLETS_MEDIUM,  // < 1 MB
        IC_TABLETS_LARGE,   // > 1 MB
    };

    enum ETabletBorders : ui64 {
        EChannelSmall = 1024,
        EChannelMedium = 1024 * 1024,
    };

    static EInterconnectChannels GetTabletChannel(ui64 msgSize) {
        if (msgSize < EChannelSmall)
            return IC_TABLETS_SMALL;
        if (msgSize < EChannelMedium)
            return IC_TABLETS_MEDIUM;
        return IC_TABLETS_LARGE;
    }
};

}
