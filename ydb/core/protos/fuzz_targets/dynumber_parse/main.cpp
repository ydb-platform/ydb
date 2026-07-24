#include <yql/essentials/types/dynumber/dynumber.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size == 0) {
        return 0;
    }

    const TString strToParse(reinterpret_cast<const char*>(data), size);

    try {
        auto parsed = NKikimr::NDyNumber::ParseDyNumberString(strToParse);
        (void)parsed;
    } catch (...) {
        // We only care about undefined behavior or crashes. ParseDyNumberString might throw if invalid.
    }

    return 0;
}
