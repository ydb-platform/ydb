#include <ydb/library/login/fuzz_targets/login_fuzz_common.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/string.h>

#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/security/login_shared_func.h>

namespace {

NKikimrProto::TAuthConfig BuildAuthConfig(FuzzedDataProvider& fdp) {
    NKikimrProto::TAuthConfig config;
    config.SetDomainLoginOnly(fdp.ConsumeBool());
    config.SetLdapAuthenticationDomain(NAuthSecurityFuzz::ConsumeToken(fdp, 16));
    if (fdp.ConsumeBool()) {
        config.MutableLdapAuthentication();
    }
    if (config.HasLdapAuthentication()) {
        auto* ldap = config.MutableLdapAuthentication();
        ldap->SetHost(NAuthSecurityFuzz::ConsumeToken(fdp, 24));
        ldap->SetPort(fdp.ConsumeIntegral<ui32>());
        ldap->SetSearchFilter(fdp.ConsumeRandomLengthString(128));
        ldap->SetSearchAttribute(NAuthSecurityFuzz::ConsumeToken(fdp, 16));
        ldap->SetScheme(NAuthSecurityFuzz::ConsumeToken(fdp, 8));
        for (size_t i = 0, count = fdp.ConsumeIntegralInRange<size_t>(0, 4); i < count; ++i) {
            *ldap->AddHosts() = NAuthSecurityFuzz::ConsumeToken(fdp, 32);
        }
    }
    if (fdp.ConsumeBool()) {
        static const TStringBuf durations[] = {"0s", "1s", "1m", "1h", "12h"};
        if (fdp.ConsumeBool()) {
            const TStringBuf duration = durations[fdp.ConsumeIntegralInRange<size_t>(0, sizeof(durations) / sizeof(durations[0]) - 1)];
            config.SetLoginTokenExpireTime(duration.data(), duration.size());
        } else {
            config.SetLoginTokenExpireTime(fdp.ConsumeRandomLengthString(64));
        }
    }
    return config;
}

NLoginProto::EHashType::HashType ConsumeHashType(FuzzedDataProvider& fdp) {
    return fdp.ConsumeBool() ? NLoginProto::EHashType::Argon : NLoginProto::EHashType::ScramSha256;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    const NKikimrProto::TAuthConfig config = BuildAuthConfig(fdp);
    const TString username = fdp.ConsumeRandomLengthString(128);
    const TString peerName = NAuthSecurityFuzz::ConsumeToken(fdp, 32);
    const TString hashToValidate = fdp.ConsumeRandomLengthString(256);
    const TString authMessage = fdp.ConsumeRandomLengthString(256);

    try {
        (void)NKikimr::IsDomainLoginOnlyEnabled(config);
    } catch (...) {
    }
    try {
        (void)NKikimr::IsLdapAuthenticationEnabled(config);
    } catch (...) {
    }
    try {
        (void)NKikimr::IsUsernameFromLdapAuthDomain(username, config);
    } catch (...) {
    }
    try {
        (void)NKikimr::PrepareLdapUsername(username, config);
    } catch (...) {
    }
    try {
        (void)NKikimr::CreatePlainLoginRequest(username, ConsumeHashType(fdp), hashToValidate, peerName, config);
    } catch (...) {
    }
    try {
        (void)NKikimr::CreatePlainLdapLoginRequest(username, peerName, config);
    } catch (...) {
    }
    try {
        (void)NKikimr::CreateScramLoginRequest(username, ConsumeHashType(fdp), hashToValidate, authMessage, peerName, config);
    } catch (...) {
    }

    return 0;
}
