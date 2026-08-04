#include <ydb/core/ymq/base/helpers.h>

#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    try {
        const TStringBuf input(reinterpret_cast<const char*>(data), size);
        bool result = NKikimr::NSQS::ValidateQueueNameOrUserName(input);
        (void)result;
    } catch (...) {
    }

    return 0;
}
