#pragma once

#include <ydb/core/testlib/test_client.h>

#include <ydb/library/testlib/service_mocks/ldap_mock/ldap_defines.h>
#include <ydb/library/testlib/service_mocks/ldap_mock/simple_server.h>

namespace NKikimr {

struct TTestEnvSettings {
    NKikimrProto::TAuthConfig AuthConfig = {};
    bool EnableLDAP = false;
    LdapMock::TLdapMockResponses LDAPResponses = {};
};

class TTestEnv {
public:
    TTestEnv(ui32 staticNodes = 1, ui32 dynamicNodes = 4, const TTestEnvSettings& settings = {});

    TTestEnv(const TTestEnvSettings& settings) : TTestEnv(1, 4, settings)
    {
    }

    ~TTestEnv();

    Tests::TServer& GetServer() const {
        return *Server;
    }

    Tests::TClient& GetClient() const {
        return *Client;
    }

    NYdb::TDriver& GetDriver() const {
        return *Driver;
    }

    const TString& GetEndpoint() const {
        return Endpoint;
    }

    const Tests::TServerSettings::TPtr GetSettings() const {
        return Settings;
    }

    const std::vector<std::string>& GetAuditLogLines() const {
        return *AuditLogLines;
    }

    TActorId GetWebLoginService() const {
        return WebLoginService;
    }


private:
    TPortManager PortManager;

    Tests::TServerSettings::TPtr Settings;
    Tests::TServer::TPtr Server;
    THolder<Tests::TClient> Client;

    TString Endpoint;
    NYdb::TDriverConfig DriverConfig;
    THolder<NYdb::TDriver> Driver;

    std::shared_ptr<std::vector<std::string>> AuditLogLines;
    TActorId WebLoginService;

    std::unique_ptr<LdapMock::TSimpleServer> LdapServer;
};

} // NKikimr
