#include <ydb/library/actors/http/http.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TStringBuf input((const char*)data, size);
    try {
        NHttp::TCookies cookies(input);
    } catch (...) {}
    return 0;
}
