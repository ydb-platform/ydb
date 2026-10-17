#pragma once
#include "blobstorage_pdisk_impl.h"

namespace NKikimr::NPDisk {
class TPDiskTestPeer {
public:
    // Caller holds StateMutex; installation precedes every creation attempt.
    static void ConfigureRouter(TPDisk& pdisk, std::function<void(TUringRouter&)> configure) {
        Y_ABORT_UNLESS(!pdisk.SharedUringCreateAttempted);
        pdisk.ConfigureRouterForTest = std::move(configure);
    }
};
}
