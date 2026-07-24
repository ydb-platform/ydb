#include <ydb/core/util/ulid.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 16 * 1024) {
        return 0;
    }

    const TString input(reinterpret_cast<const char*>(data), size);
    try {
        NKikimr::TULID ulid;
        ulid.ParseString(input);
        (void)ulid;
    } catch (...) {
    }
    return 0;
}
