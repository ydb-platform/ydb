#include "ut_common.h"

#include <ydb/core/base/backtrace.h>
#include <ydb/core/security/ldap_auth_provider/ldap_auth_provider.h>
#include <ydb/core/security/login_page.h>

namespace NKikimr {

TTestEnv::TTestEnv(ui32 staticNodes, ui32 dynamicNodes, const TTestEnvSettings& settings) {
    EnableYDBBacktraceFormat();

    auto mbusPort = PortManager.GetPort();
    auto grpcPort = PortManager.GetPort();

    TVector<NKikimrKqp::TKqpSetting> kqpSettings;

    NKikimrProto::TAuthConfig authConfig = settings.AuthConfig;
    authConfig.SetUseBuiltinDomain(true);
    authConfig.SetUseBlackBox(false);
    authConfig.SetUseLoginProvider(true);

    if (settings.EnableLDAP) {
        auto ldapPort = PortManager.GetPort();

        auto& ldapSettings = *authConfig.MutableLdapAuthentication();
        ldapSettings.MutableUseTls()->SetCertRequire(NKikimrProto::TLdapAuthentication::TUseTls::NEVER);
        ldapSettings.SetPort(ldapPort);
        ldapSettings.AddHosts("localhost");
        ldapSettings.SetBaseDn("dc=search,dc=yandex,dc=net");
        ldapSettings.SetBindDn("cn=robouser,dc=search,dc=yandex,dc=net");
        ldapSettings.SetBindPassword("robouserPassword");
        ldapSettings.SetSearchFilter("uid=$username");
    }

    Settings = new Tests::TServerSettings(mbusPort, std::move(authConfig));
    Settings->SetDomainName("Root");
    Settings->SetGrpcPort(grpcPort);
    Settings->SetNodeCount(staticNodes);
    Settings->SetDynamicNodeCount(dynamicNodes);
    Settings->SetKqpSettings(kqpSettings);

    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableResourcePools(false);

    Settings->SetFeatureFlags(featureFlags);

    NKikimrConfig::TAppConfig appConfig;
    *appConfig.MutableFeatureFlags() = Settings->FeatureFlags;

    Settings->SetAppConfig(appConfig);

    AuditLogLines = std::make_shared<std::vector<std::string>>();
    Settings->AuditLogBackendLines = AuditLogLines;

    Server = new Tests::TServer(*Settings);
    Server->EnableGRpc(grpcPort);

    Client = MakeHolder<Tests::TClient>(*Settings);
    Client->InitRootScheme("Root");

    Endpoint = "localhost:" + ToString(grpcPort);
    DriverConfig = NYdb::TDriverConfig().SetEndpoint(Endpoint).SetDatabase("/Root");
    Driver = MakeHolder<NYdb::TDriver>(DriverConfig);

    WebLoginService = Server->GetRuntime()->Register(CreateWebLoginService());

    // start ldap auth provider service
    if (settings.EnableLDAP) {
        Server->GetRuntime()->SetLogPriority(NKikimrServices::LDAP_AUTH_PROVIDER, NActors::NLog::PRI_DEBUG);

        const auto& appData = Server->GetRuntime()->GetAppData();
        IActor* ldapAuthProvider = NKikimr::CreateLdapAuthProvider(appData.AuthConfig.GetLdapAuthentication());
        TActorId ldapAuthProviderId = Server->GetRuntime()->Register(ldapAuthProvider);
        Server->GetRuntime()->RegisterService(MakeLdapAuthProviderID(), ldapAuthProviderId);

        LdapMock::TSimpleServer::TOptions options;
        options.Port = appData.AuthConfig.GetLdapAuthentication().GetPort();
        LdapServer = std::make_unique<LdapMock::TSimpleServer>(options, settings.LDAPResponses);
        LdapServer->Start();
    }
}

TTestEnv::~TTestEnv() {
    Driver->Stop(true);

    if (LdapServer) {
        LdapServer->Stop();
    }
}

} // NKikimr
