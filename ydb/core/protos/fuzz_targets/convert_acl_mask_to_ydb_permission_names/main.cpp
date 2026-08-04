#include <ydb/core/ydb_convert/ydb_convert.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    ui32 mask = 0;
    std::memcpy(&mask, data, std::min(size, sizeof(mask)));

    try {
        TVector<TString> names = NKikimr::ConvertACLMaskToYdbPermissionNames(mask);
        (void)names;
    } catch (...) {
    }

    return 0;
}
