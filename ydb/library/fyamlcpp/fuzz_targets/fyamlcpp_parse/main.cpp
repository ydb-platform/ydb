#include <ydb/library/fyamlcpp/fyamlcpp.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);
    try {
        NKikimr::NFyaml::TDocument::Parse(input);
    } catch (...) {}
    return 0;
}
