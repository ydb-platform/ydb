#include "login_shared_func.h"

#include <util/generic/string.h>

#include <ydb/core/protos/auth.pb.h>


namespace NKikimr {

bool IsLdapAuthenticationEnabled(const NKikimrProto::TAuthConfig& config) {
    return config.HasLdapAuthentication();
}

bool IsUsernameFromLdapAuthDomain(const TString& username, const NKikimrProto::TAuthConfig& config) {
    if (config.HasLdapAuthentication() && !config.GetLdapAuthenticationDomain().empty()) {
        const TString ldapDomain = "@" + config.GetLdapAuthenticationDomain();
        return username.EndsWith(ldapDomain);
    }

    return false;
}

TString PrepareLdapUsername(const TString& username, const NKikimrProto::TAuthConfig& config) {
    const TString ldapDomain = "@" + config.GetLdapAuthenticationDomain();
    return username.substr(0, username.size() - ldapDomain.size());
}

NKikimrScheme::TEvLogin CreatePlainLoginRequest(const TString& username, NLoginProto::EHashType::HashType hashType,
    const TString& hashToValidate, const TString& peerName, const NKikimrProto::TAuthConfig& config)
{
    NKikimrScheme::TEvLogin record;
    record.SetUser(username);
    record.MutableHashToValidate()->SetAuthMech(NLoginProto::ESaslAuthMech::Plain);
    record.MutableHashToValidate()->SetHashType(hashType);
    record.MutableHashToValidate()->SetHash(hashToValidate);

    if (config.HasLoginTokenExpireTime()) {
        record.SetExpiresAfterMs(TDuration::Parse(config.GetLoginTokenExpireTime()).MilliSeconds());
    }

    record.SetPeerName(peerName);
    return record;
}

NKikimrScheme::TEvLogin CreatePlainLoginRequestOldFormat(const TString& username, const TString& password,
    const TString& peerName, const NKikimrProto::TAuthConfig& config)
{
    NKikimrScheme::TEvLogin record;
    record.SetUser(username);
    record.SetPassword(password);

    if (config.HasLoginTokenExpireTime()) {
        record.SetExpiresAfterMs(TDuration::Parse(config.GetLoginTokenExpireTime()).MilliSeconds());
    }

    record.SetPeerName(peerName);
    return record;
}

NKikimrScheme::TEvLogin CreatePlainLdapLoginRequest(const TString& username, const TString& peerName,
    const NKikimrProto::TAuthConfig& config)
{
    NKikimrScheme::TEvLogin record;
    record.SetUser(username);
    record.SetExternalAuth(config.GetLdapAuthenticationDomain());

    if (config.HasLoginTokenExpireTime()) {
        record.SetExpiresAfterMs(TDuration::Parse(config.GetLoginTokenExpireTime()).MilliSeconds());
    }

    record.SetPeerName(peerName);
    return record;
}

NKikimrScheme::TEvLogin CreateScramLoginRequest(const TString& username, NLoginProto::EHashType::HashType hashType,
    const TString& clientProof, const TString& authMessage,  const TString& peerName, const NKikimrProto::TAuthConfig& config)
{
    NKikimrScheme::TEvLogin record;
    record.SetUser(username);
    record.MutableHashToValidate()->SetAuthMech(NLoginProto::ESaslAuthMech::Scram);
    record.MutableHashToValidate()->SetHashType(hashType);
    record.MutableHashToValidate()->SetHash(clientProof);
    record.MutableHashToValidate()->SetAuthMessage(authMessage);

    if (config.HasLoginTokenExpireTime()) {
        record.SetExpiresAfterMs(TDuration::Parse(config.GetLoginTokenExpireTime()).MilliSeconds());
    }

    record.SetPeerName(peerName);
    return record;
}

} // namespace NKikimr
