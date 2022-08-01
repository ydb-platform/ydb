#pragma once

namespace NKikimr {

    struct TWilson {
        enum {
            BlobStorage = 8, // DS proxy and lower levels
            DsProxyInternals = 9,
            VDiskTopLevel = 12,
            VDiskInternals = 13,
        };
    };

} // NKikimr
