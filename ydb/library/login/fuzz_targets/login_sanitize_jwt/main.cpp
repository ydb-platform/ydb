#include <ydb/library/login/login.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);
    try {
        TString sanitized = NLogin::TLoginProvider::SanitizeJwtToken(input);
        (void)sanitized;
    } catch (...) {}
    try {
        TString audience = NLogin::TLoginProvider::GetTokenAudience(input);
        (void)audience;
    } catch (...) {}
    try {
        auto expiresAt = NLogin::TLoginProvider::GetTokenExpiresAt(input);
        (void)expiresAt;
    } catch (...) {}
    return 0;
}
