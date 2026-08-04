#include <ydb/core/base/path.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <utility>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    const TString path = TString(fdp.ConsumeRandomLengthString(256));
    const TString database = TString(fdp.ConsumeRandomLengthString(256));

    try {
        std::pair<TString, TString> result;
        TString error;
        (void)NKikimr::TrySplitPathByDb(path, database, result, error);
    } catch (...) {
    }

    return 0;
}
