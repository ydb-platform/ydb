#include <library/cpp/http/io/headers.h>
#include <util/generic/string.h>
#include <util/stream/str.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);
    try {
        TStringInput stream(input);
        THttpHeaders headers(&stream);
        for (auto it = headers.Begin(); it != headers.End(); ++it) {
            (void)it->Name();
            (void)it->Value();
        }
    } catch (...) {}
    try {
        TStringBuf buf(input);
        THttpInputHeader header(buf);
        (void)header.Name();
        (void)header.Value();
    } catch (...) {}
    return 0;
}
