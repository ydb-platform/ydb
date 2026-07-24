#include <ydb/library/naming_conventions/naming_conventions.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);
    try {
        NKikimr::NNaming::SnakeToCamelCase(input);
    } catch (...) {}
    return 0;
}
