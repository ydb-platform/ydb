#include <ydb/core/ymq/base/acl.h>

#include <cstddef>
#include <cstdint>

using namespace NKikimr::NSQS;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    constexpr size_t kMaxInputSize = 16 * 1024;
    if (size > kMaxInputSize) {
        return 0;
    }

    ui32 access = 0;
    for (size_t i = 0; i < size; ++i) {
        access = access * 131u + data[i];
    }

    try {
        TVector<TStringBuf> aceNames = GetAccessMatchingACE(access);
        (void)aceNames;
    } catch (...) {
    }
    return 0;
}
