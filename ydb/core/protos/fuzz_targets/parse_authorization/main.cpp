#include <ydb/library/http_proxy/authorization/auth_helpers.h>
#include <util/generic/strbuf.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 16 * 1024) {
        return 0;
    }

    const TStringBuf input(reinterpret_cast<const char*>(data), size);
    try {
        auto params = NKikimr::NSQS::ParseAuthorizationParams(input);
        (void)params;
    } catch (...) {
    }
    return 0;
}
