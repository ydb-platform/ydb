#include <library/cpp/scheme/tests/fuzz_ops/lib/fuzz_ops.h>

extern "C" int LLVMFuzzerTestOneInput(const ui8* wireData, const size_t wireSize) {
    NSc::NUt::FuzzOps({(const char*)wireData, wireSize}, false);
    return 0;
}
