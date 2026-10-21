#pragma once

#include <util/generic/string.h>

namespace NKikimr::NPDisk {

struct TDeviceMode {
    using TFlags = ui32;

    enum EFlags : TFlags {
        None = 0,
        LockFile = 1 << 0,
        UseSpdk = 1 << 1,
        UseShmem = 1 << 2,
        UseSubmitGetThread = 1 << 3,
    };

    static TString Validate(TFlags flags) {
        if ((flags & UseSpdk) && (flags & UseShmem)) {
            return "Spdk cannot be used on top of shmem device";
        } else if (flags & UseShmem) {
            return "PDisk on shared memory is not supported now";
        } else {
            return "";
        }
    }
};

} // NKikimr::NPDisk
