#include <library/cpp/neh/location.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TStringBuf input((const char*)data, size);
    try {
        NNeh::TParsedLocation loc(input);
        auto port = loc.GetPort();
        (void)port;
        (void)loc.Scheme;
        (void)loc.Host;
        (void)loc.Service;
    } catch (...) {}
    return 0;
}
