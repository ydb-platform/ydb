#include <ydb/library/actors/util/cpumask.h>

#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);
    try {
        TCpuMask mask(input);
    } catch (...) {}
    return 0;
}
