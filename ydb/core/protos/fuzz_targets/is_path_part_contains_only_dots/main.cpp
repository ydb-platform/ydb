#include <ydb/core/base/path.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    const TString part = TString(fdp.ConsumeRandomLengthString(256));

    try {
        const auto result = NKikimr::IsPathPartContainsOnlyDots(part);
        (void)result;
    } catch (...) {
    }

    return 0;
}
