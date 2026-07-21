#include <ydb/core/ymq/base/acl.h>
#include <util/generic/string.h>

#include <cstddef>
#include <cstdint>

using namespace NKikimr::NSQS;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    constexpr size_t kMaxInputSize = 16 * 1024;
    if (size == 0 || size > kMaxInputSize) {
        return 0;
    }

    const TString input(reinterpret_cast<const char*>(data), size);
    try {
        ui32 access = GetActionRequiredAccess(input);
        (void)access;
    } catch (...) {
    }
    return 0;
}
