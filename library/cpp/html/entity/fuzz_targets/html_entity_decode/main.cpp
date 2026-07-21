#include <library/cpp/html/entity/htmlentity.h>
#include <library/cpp/charset/doccodes.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size == 0 || size > 65536) return 0;
    const char* str = (const char*)data;
    if (str[0] == '&') {
        TEntity entity;
        HtTryDecodeEntity(str, size, &entity);
    }
    try {
        wchar32 buffer[4096];
        size_t buflen = sizeof(buffer) / sizeof(buffer[0]);
        size_t safeLen = size < 8192 ? size : 8192;
        HtEntDecode(CODES_UTF8, str, safeLen, buffer, buflen, nullptr);
    } catch (...) {}
    return 0;
}
