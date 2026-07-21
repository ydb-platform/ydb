#include <ydb/library/aclib/aclib.h>

#include <util/generic/string.h>

#include <cstring>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size < 4) return 0;

    ui32 rights = 0;
    std::memcpy(&rights, data, 4);

    try {
        TString str = NACLib::AccessRightsToString(rights);
        (void)str;
    } catch (...) {}

    if (size > 4) {
        TString input(reinterpret_cast<const char*>(data + 4), size - 4);
        try {
            NACLib::TACL acl;
            acl.FromString(input);
            TString out = acl.ToString();
            (void)out;
            NACLib::TACL acl2;
            acl2.FromString(out);
            (void)acl2.ToString();
        } catch (...) {}
    }
    return 0;
}
