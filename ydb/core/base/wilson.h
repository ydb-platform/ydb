#pragma once

namespace NKikimr {

    struct TWilson {
        enum {
            BlobStorage = 8, // DS proxy and lower levels
            DsProxyInternals = 7,
            VDiskTopLevel = 6,
            VDiskInternals = 5,
        };
    };

} // NKikimr
