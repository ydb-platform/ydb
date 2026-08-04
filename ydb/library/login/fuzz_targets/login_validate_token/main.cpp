#include <ydb/library/login/login.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);
    NLogin::TLoginProvider provider;
    NLogin::TLoginProvider::TValidateTokenRequest request;
    request.Token = input;
    try {
        auto response = provider.ValidateToken(request);
        (void)response;
    } catch (...) {}
    return 0;
}
