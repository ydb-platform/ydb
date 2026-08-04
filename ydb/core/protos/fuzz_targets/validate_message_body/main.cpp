#include <ydb/core/ymq/base/helpers.h>

#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    try {
        TString errorDescription;
        const TStringBuf input(reinterpret_cast<const char*>(data), size);
        bool result = NKikimr::NSQS::ValidateMessageBody(input, errorDescription);
        (void)result;
        (void)errorDescription;
    } catch (...) {
    }

    return 0;
}
