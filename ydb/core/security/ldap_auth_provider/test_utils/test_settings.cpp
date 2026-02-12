#include "test_settings.h"

#include <ydb/core/protos/auth.grpc.pb.h>

namespace NKikimr {

TCertStorage::TCertStorage()
    : CaCertAndKey(GenerateCA(TProps::AsCA()))
    , ServerCertAndKey(GenerateSignedCert(CaCertAndKey, TProps::AsServer()))
    , ClientCertAndKey(GenerateSignedCert(CaCertAndKey, TProps::AsClientServer()))
{
    CaCertFile.Write(CaCertAndKey.Certificate.c_str(), CaCertAndKey.Certificate.size());
    ServerCertFile.Write(ServerCertAndKey.Certificate.c_str(), ServerCertAndKey.Certificate.size());
    ServerKeyFile.Write(ServerCertAndKey.PrivateKey.c_str(), ServerCertAndKey.PrivateKey.size());
    ClientCertFile.Write(ClientCertAndKey.Certificate.c_str(), ClientCertAndKey.Certificate.size());
    ClientKeyFile.Write(ClientCertAndKey.PrivateKey.c_str(), ClientCertAndKey.PrivateKey.size());
}

TString TCertStorage::GetCaCertFileName() const {
    return CaCertFile.Name();
}

TString TCertStorage::GetServerCertFileName() const {
    return ServerCertFile.Name();
}

TString TCertStorage::GetServerKeyFileName() const {
    return ServerKeyFile.Name();
}

TString TCertStorage::GetClientCertFileName() const {
    return ClientCertFile.Name();
}

TString TCertStorage::GetClientKeyFileName() const {
    return ClientKeyFile.Name();
}

void InitLdapSettings(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions) {
    ldapSettings->SetHost("127.0.0.1");
    ldapSettings->SetPort(ldapPort);
    ldapSettings->SetBaseDn("dc=search,dc=yandex,dc=net");
    ldapSettings->SetBindDn("cn=robouser,dc=search,dc=yandex,dc=net");
    ldapSettings->SetBindPassword("robouserPassword");
    ldapSettings->SetSearchFilter("uid=$username");
    auto extendedSettings = ldapSettings->MutableExtendedSettings();
    extendedSettings->SetEnableNestedGroupsSearch(true);

    const auto setCertificate = [&ldapSettings] (bool useStartTls, const TLdapClientOptions& ldapClientOptions) {
        auto useTls = ldapSettings->MutableUseTls();
        useTls->SetEnable(useStartTls);
        useTls->SetCaCertFile(ldapClientOptions.CaCertFile);
        useTls->SetCertRequire(NKikimrProto::TLdapAuthentication::TUseTls::DEMAND);
        useTls->SetCertFile(ldapClientOptions.CertFile);
        useTls->SetKeyFile(ldapClientOptions.KeyFile);
    };

    switch (ldapClientOptions.Type) {
        case ESecurityConnectionType::NON_SECURE:
            break;
        case ESecurityConnectionType::START_TLS:
            setCertificate(true, ldapClientOptions);
            break;
        case ESecurityConnectionType::LDAPS_SCHEME:
            ldapSettings->SetScheme("ldaps");
            setCertificate(false, ldapClientOptions);
            break;
    }
}

void InitLdapSettingsWithInvalidRobotUserLogin(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions) {
    InitLdapSettings(ldapSettings, ldapPort, ldapClientOptions);
    ldapSettings->SetBindDn("cn=invalidRobouser,dc=search,dc=yandex,dc=net");
}

void InitLdapSettingsWithInvalidRobotUserPassword(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions) {
    InitLdapSettings(ldapSettings, ldapPort, ldapClientOptions);
    ldapSettings->SetBindPassword("invalidPassword");
}

void InitLdapSettingsWithInvalidFilter(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions) {
    InitLdapSettings(ldapSettings, ldapPort, ldapClientOptions);
    ldapSettings->SetSearchFilter("&(uid=$username)()");
}

void InitLdapSettingsWithUnavailableHost(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions) {
    InitLdapSettings(ldapSettings, ldapPort, ldapClientOptions);
    ldapSettings->SetHost("unavailablehost");
}

void InitLdapSettingsWithEmptyHost(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions) {
    InitLdapSettings(ldapSettings, ldapPort, ldapClientOptions);
    ldapSettings->SetHost("");
}

void InitLdapSettingsWithEmptyBaseDn(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions) {
    InitLdapSettings(ldapSettings, ldapPort, ldapClientOptions);
    ldapSettings->SetBaseDn("");
}

void InitLdapSettingsWithEmptyBindDn(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions) {
    InitLdapSettings(ldapSettings, ldapPort, ldapClientOptions);
    ldapSettings->SetBindDn("");
}

void InitLdapSettingsWithEmptyBindPassword(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions) {
    InitLdapSettings(ldapSettings, ldapPort, ldapClientOptions);
    ldapSettings->SetBindPassword("");
}

void InitLdapSettingsWithCustomGroupAttribute(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions) {
    InitLdapSettings(ldapSettings, ldapPort, ldapClientOptions);
    ldapSettings->SetRequestedGroupAttribute("groupDN");
}

void InitLdapSettingsWithListOfHosts(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions) {
    InitLdapSettings(ldapSettings, ldapPort, ldapClientOptions);
    ldapSettings->AddHosts("qqq");
    ldapSettings->AddHosts("localhost");
    ldapSettings->AddHosts("localhost:11111");
}

void InitLdapSettingsDisableSearchNestedGroups(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions) {
    InitLdapSettings(ldapSettings, ldapPort, ldapClientOptions);
    auto extendedSettings = ldapSettings->MutableExtendedSettings();
    extendedSettings->SetEnableNestedGroupsSearch(false);
}

void InitLdapSettingsWithSaslExternalBind(NKikimrProto::TLdapAuthentication* ldapSettings, ui16 ldapPort, const TLdapClientOptions& ldapClientOptions) {
    InitLdapSettings(ldapSettings, ldapPort, ldapClientOptions);
    ldapSettings->SetBindDn("");
    ldapSettings->SetBindPassword("");
    ldapSettings->MutableExtendedSettings()->SetEnableSaslExternalBind(true);
}

} // NKikimr
