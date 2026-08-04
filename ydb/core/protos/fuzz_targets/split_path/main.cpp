#include <ydb/core/base/path.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 16 * 1024) {
        return 0;
    }

    const TString input(reinterpret_cast<const char*>(data), size);
    try {
        TVector<TString> pathParts = NKikimr::SplitPath(input);
        (void)pathParts;
    } catch (...) {
    }
    return 0;
}
