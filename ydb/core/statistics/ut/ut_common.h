#pragma once

#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NStat {

NKikimrSubDomains::TSubDomainSettings GetSubDomainDeclareSettings(
    const TString &name, const TStoragePools &pools = {});

NKikimrSubDomains::TSubDomainSettings GetSubDomainDefaultSettings(
    const TString &name, const TStoragePools &pools = {});
    
class TTestEnv {
public:
    TTestEnv(ui32 staticNodes = 1, ui32 dynamicNodes = 1, ui32 storagePools = 0);
    ~TTestEnv();

    Tests::TServer& GetServer() const {
        return *Server;
    }

    Tests::TClient& GetClient() const {
        return *Client;
    }

    Tests::TTenants& GetTenants() const {
        return *Tenants;
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

    TStoragePools GetPools() const;

private:
    TPortManager PortManager;

    Tests::TServerSettings::TPtr Settings;
    Tests::TServer::TPtr Server;
    THolder<Tests::TClient> Client;
    THolder<Tests::TTenants> Tenants;

    TString Endpoint;
    NYdb::TDriverConfig DriverConfig;
    THolder<NYdb::TDriver> Driver;
};

} // namespace NStat
} // namespace NKikimr
