#include <util/generic/string.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/path.h>
#include <ydb/core/security/ldap_auth_provider/ldap_auth_provider.h>
#include "login_shared_func.h"

namespace NKikimr {

THolder<NSchemeCache::TSchemeCacheNavigate> CreateNavigateKeySetRequest(const TString& pathToDatabase) {
    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    request->DatabaseName = pathToDatabase;
    auto& entry = request->ResultSet.emplace_back();
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
    entry.Path = ::NKikimr::SplitPath(pathToDatabase);
    entry.RedirectRequired = false;
    return request;
}

TAuthCredentials PrepareCredentials(const TString& login, const TString& password, const NKikimrProto::TAuthConfig& config) {
    if (config.HasLdapAuthentication() && !config.GetLdapAuthenticationDomain().empty()) {
        size_t n = login.find("@" + config.GetLdapAuthenticationDomain());
        if (n != TString::npos) {
            return {.AuthType = TAuthCredentials::EAuthType::Ldap, .Login = login.substr(0, n), .Password = password};
        }
    }
    return {.AuthType = TAuthCredentials::EAuthType::Internal, .Login = login, .Password = password};
}

NKikimrScheme::TEvLogin CreateLoginRequest(const TAuthCredentials& credentials, const NKikimrProto::TAuthConfig& config) {
    NKikimrScheme::TEvLogin record;
    record.SetUser(credentials.Login);
    record.SetPassword(credentials.Password);
    switch (credentials.AuthType) {
        case TAuthCredentials::EAuthType::Ldap: {
            record.SetExternalAuth(config.GetLdapAuthenticationDomain());
            break;
        }
        default: {}
    }
    if (config.HasLoginTokenExpireTime()) {
        record.SetExpiresAfterMs(TDuration::Parse(config.GetLoginTokenExpireTime()).MilliSeconds());
    }
    return record;
}

TSendParameters GetSendParameters(const TAuthCredentials& credentials, const TString& pathToDatabase) {
    switch (credentials.AuthType) {
        case TAuthCredentials::EAuthType::Internal: {
            return {
                .Recipient = MakeSchemeCacheID(),
                .Event = MakeHolder<TEvTxProxySchemeCache::TEvNavigateKeySet>(CreateNavigateKeySetRequest(pathToDatabase).Release())
            };
        }
        case TAuthCredentials::EAuthType::Ldap: {
            return {
                .Recipient = MakeLdapAuthProviderID(),
                .Event = MakeHolder<TEvLdapAuthProvider::TEvAuthenticateRequest>(credentials.Login, credentials.Password)
            };
        }
    }
}

} // namespace NKikimr
