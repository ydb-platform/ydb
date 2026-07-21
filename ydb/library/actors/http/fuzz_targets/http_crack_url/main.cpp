#include <ydb/library/actors/http/http.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TStringBuf input((const char*)data, size);
    TStringBuf scheme, host, uri;
    try {
        NHttp::CrackURL(input, scheme, host, uri);
    } catch (...) {}
    return 0;
}
