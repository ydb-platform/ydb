#include <ydb/core/ydb_convert/ydb_convert.h>

#include <cstddef>
#include <cstdint>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    TString permissionName(reinterpret_cast<const char*>(data), size);
    if (permissionName.empty()) {
        return 0;
    }

    try {
        NKikimr::TACLAttrs attrs = NKikimr::ConvertYdbPermissionNameToACLAttrs(permissionName);
        (void)attrs;
    } catch (...) {
    }

    return 0;
}
