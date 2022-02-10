#include <library/cpp/scheme/tests/fuzz_json/lib/fuzz_json.h>

extern "C" int LLVMFuzzerTestOneInput(const ui8* wireData, const size_t wireSize) {
    NSc::NUt::FuzzJson({(const char*)wireData, wireSize});
    return 0;
}
