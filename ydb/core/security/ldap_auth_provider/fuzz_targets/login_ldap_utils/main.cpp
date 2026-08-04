#include <ydb/library/login/fuzz_targets/login_fuzz_common.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>

#include <ydb/core/security/ldap_auth_provider/ldap_utils.h>

namespace {

NKikimrProto::TLdapAuthentication BuildSettings(FuzzedDataProvider& fdp) {
    NKikimrProto::TLdapAuthentication settings;
    settings.SetHost(NAuthSecurityFuzz::ConsumeToken(fdp, 32));
    settings.SetPort(fdp.ConsumeIntegral<ui32>());
    settings.SetSearchFilter(fdp.ConsumeRandomLengthString(128));
    settings.SetSearchAttribute(NAuthSecurityFuzz::ConsumeToken(fdp, 16));
    settings.SetScheme(NAuthSecurityFuzz::ConsumeToken(fdp, 8));

    for (size_t i = 0, count = fdp.ConsumeIntegralInRange<size_t>(0, 6); i < count; ++i) {
        *settings.AddHosts() = NAuthSecurityFuzz::ConsumeToken(fdp, 48);
    }

    return settings;
}

void RunCreators(const NKikimrProto::TLdapAuthentication& settings, const TString& userName, ui32 port) {
    NKikimr::TSearchFilterCreator filterCreator(settings);
    (void)filterCreator.GetFilter(userName);

    NKikimr::TLdapUrisCreator urisCreator(settings, port);
    (void)urisCreator.GetUris();
    (void)urisCreator.GetUris();
    (void)urisCreator.GetConfiguredPort();
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    try {
        const auto settings = BuildSettings(fdp);
        RunCreators(settings, NAuthSecurityFuzz::ConsumeToken(fdp, 32), fdp.ConsumeIntegral<ui32>());
    } catch (...) {
    }

    NKikimrProto::TLdapAuthentication parsed;
    if (parsed.ParseFromArray(data, static_cast<int>(size))) {
        try {
            RunCreators(parsed, NAuthSecurityFuzz::ConsumeToken(fdp, 32), fdp.ConsumeIntegral<ui32>());
        } catch (...) {
        }
    }

    return 0;
}
