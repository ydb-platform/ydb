#include <ydb/library/actors/http/http.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);
    TString hostname;
    TIpPort port = 0;
    try {
        NHttp::CrackAddress(input, hostname, port);
    } catch (...) {}
    return 0;
}
