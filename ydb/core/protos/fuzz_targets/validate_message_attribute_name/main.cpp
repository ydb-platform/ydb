#include <ydb/core/ymq/base/helpers.h>

#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    try {
        const bool allowYandexPrefix = size > 0 && (data[0] & 1u);
        const size_t offset = size > 0 ? 1 : 0;
        const TStringBuf input(reinterpret_cast<const char*>(data + offset), size - offset);
        bool hasYandexPrefix = false;
        bool result = NKikimr::NSQS::ValidateMessageAttributeName(input, hasYandexPrefix, allowYandexPrefix);
        (void)result;
        (void)hasYandexPrefix;
    } catch (...) {
    }

    return 0;
}
