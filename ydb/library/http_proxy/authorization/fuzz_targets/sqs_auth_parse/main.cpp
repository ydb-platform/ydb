// Fuzzer for NKikimr::NSQS::ParseAuthorizationParams — the SQS/AWS SigV4
// Authorization header parser. This header arrives in every SQS-compatible
// HTTP request and is parsed before any authentication takes place.
// The parser splits on ',' and '=' characters and is reachable without auth.
#include <ydb/library/http_proxy/authorization/auth_helpers.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > 16 * 1024) return 0;
    TStringBuf input(reinterpret_cast<const char*>(data), size);
    try {
        auto params = NKikimr::NSQS::ParseAuthorizationParams(input);
        (void)params;
    } catch (...) {}
    return 0;
}
