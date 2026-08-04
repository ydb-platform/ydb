#include <ydb/core/base/path.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

namespace {

TVector<TString> MakePathParts(FuzzedDataProvider& fdp) {
    const size_t partsCount = fdp.ConsumeIntegralInRange<size_t>(0, 16);
    TVector<TString> pathParts;
    pathParts.reserve(partsCount);
    for (size_t i = 0; i < partsCount; ++i) {
        pathParts.emplace_back(fdp.ConsumeRandomLengthString(64));
    }
    return pathParts;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    const auto pathParts = MakePathParts(fdp);

    try {
        const auto joined = NKikimr::JoinPath(pathParts);
        (void)joined;
    } catch (...) {
    }

    return 0;
}
