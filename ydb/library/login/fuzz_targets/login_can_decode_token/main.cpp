#include <ydb/library/login/login.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TString input((const char*)data, size);
    try {
        NLogin::TLoginProvider::CanDecodeToken(input);
    } catch (...) {}
    return 0;
}
