#include <ydb/core/ydb_convert/ydb_convert.h>

#include <cstddef>
#include <cstdint>
#include <util/generic/string.h>

using namespace NKikimr;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    TString permissionName(reinterpret_cast<const char*>(data), size);
    if (permissionName.empty()) {
        return 0;
    }
    try {
        TString fullName = ConvertShortYdbPermissionNameToFullYdbPermissionName(permissionName);
        (void)fullName;
    } catch (...) {
    }

    return 0;
}
