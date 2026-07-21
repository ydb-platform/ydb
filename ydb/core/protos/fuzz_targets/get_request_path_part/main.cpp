#include <ydb/core/ymq/base/utils.h>

#include <cstddef>
#include <cstdint>
#include <util/generic/string.h>

using namespace NKikimr::NSQS;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    try {
        TString path(reinterpret_cast<const char*>(data), size);
        bool isPrivate = size != 0 && ((data[0] & 1u) != 0);
        size_t partIdx = size > 1 ? data[size / 2] % 10 : 0;
        TString part = GetRequestPathPart(path, partIdx, isPrivate);
        (void)part;
    } catch (...) {
    }

    return 0;
}
