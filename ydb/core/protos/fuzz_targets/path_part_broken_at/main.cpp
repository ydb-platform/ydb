#include <ydb/core/base/path.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    const TString part = TString(fdp.ConsumeRandomLengthString(256));
    const TString extraSymbols = TString(fdp.ConsumeRandomLengthString(32));

    try {
        const auto brokenAt = NKikimr::PathPartBrokenAt(part, extraSymbols);
        (void)brokenAt;
    } catch (...) {
    }

    return 0;
}
