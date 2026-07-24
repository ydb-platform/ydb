#include "local_kikhouse_snapshot_id.h"

#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    try {
        TString uri(reinterpret_cast<const char*>(data), size);
        if (uri.empty()) {
            uri = "snapshot:///1/3";
        }

        NKikimr::NGRpcService::TKikhouseSnapshotId id;
        const bool ok = id.Parse(uri);
        if (ok) {
            TString back = id.ToUri();
            Y_UNUSED(back);
        }
    } catch (...) {
    }
    return 0;
}
