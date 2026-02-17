#pragma once
#include <ydb/core/security/certificate_check/cert_auth_utils.h>

#include <util/system/tempfile.h>
#include <util/generic/string.h>

namespace NKikimrProto {

class TLdapAuthentication;

} // NKikimrProto

namespace NKikimr {

enum class ESecurityConnectionType {
    NON_SECURE,
    START_TLS,
    LDAPS_SCHEME,
};

struct TLdapClientOptions {
    TString CaCertFile;
    TString CertFile;
    TString KeyFile;
    ESecurityConnectionType Type = ESecurityConnectionType::NON_SECURE;
    bool IsLoginAuthenticationEnabled = true;
};

class TCertStorage {
public:
    TCertStorage();
    TString GetCaCertFileName() const;
    TString GetServerCertFileName() const;
    TString GetServerKeyFileName() const;
    TString GetClientCertFileName() const;
    TString GetClientKeyFileName() const;

private:
    TCertAndKey CaCertAndKey;
    TCertAndKey ServerCertAndKey;
    TCertAndKey ClientCertAndKey;
    TTempFileHandle CaCertFile;
    TTempFileHandle ServerCertFile;
    TTempFileHandle ServerKeyFile;
    TTempFileHandle ClientCertFile;
    TTempFileHandle ClientKeyFile;
};

void InitLdapSettings(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions);
void InitLdapSettingsWithInvalidRobotUserLogin(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions);
void InitLdapSettingsWithInvalidRobotUserPassword(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions);
void InitLdapSettingsWithInvalidFilter(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions);
void InitLdapSettingsWithUnavailableHost(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions);
void InitLdapSettingsWithEmptyHost(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions);
void InitLdapSettingsWithEmptyBaseDn(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions);
void InitLdapSettingsWithEmptyBindDn(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions);
void InitLdapSettingsWithEmptyBindPassword(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions);
void InitLdapSettingsWithCustomGroupAttribute(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions);
void InitLdapSettingsWithListOfHosts(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions);
void InitLdapSettingsDisableSearchNestedGroups(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions);
void InitLdapSettingsWithSaslExternalBind(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions);

} // NKikimr
