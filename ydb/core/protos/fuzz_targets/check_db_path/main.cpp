#include <ydb/core/base/path.h>
#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    try {
        const TString path = TString(fdp.ConsumeRandomLengthString(256));
        const TString domain = TString(fdp.ConsumeRandomLengthString(64));
        TString error;
        const bool result = NKikimr::CheckDbPath(path, domain, error);
        (void)result;
        (void)error;
    } catch (...) {
    }
    return 0;
}
