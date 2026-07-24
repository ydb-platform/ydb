#include <library/cpp/http/push_parser/http_parser.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    try {
        THttpParser parser(THttpParser::Response);
        parser.DisableCollectingHeaders();
        parser.Parse((const char*)data, size);
    } catch (...) {}
    return 0;
}
