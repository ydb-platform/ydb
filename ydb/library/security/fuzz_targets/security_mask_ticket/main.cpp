#include <ydb/library/security/util.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);
    try {
        TString masked = NKikimr::MaskTicket(input);
        (void)masked;
    } catch (...) {}
    try {
        TString masked = NKikimr::MaskIAMTicket(input);
        (void)masked;
    } catch (...) {}
    try {
        TString masked = NKikimr::MaskNebiusTicket(input);
        (void)masked;
    } catch (...) {}
    try {
        TString sanitized = NKikimr::SanitizeNebiusTicket(input);
        (void)sanitized;
    } catch (...) {}
    return 0;
}
