#pragma once

#include <ydb/core/testlib/test_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NSysView {

NKikimrSubDomains::TSubDomainSettings GetSubDomainDeclareSettings(
    const TString &name, const TStoragePools &pools = {});

NKikimrSubDomains::TSubDomainSettings GetSubDomainDefaultSettings(
    const TString &name, const TStoragePools &pools = {});

struct TTestEnvSettings {
    ui32 StoragePools = 0;
    ui32 PqTabletsN = 0;
    bool EnableSVP = false;
    bool EnableForceFollowers = false;
    bool ShowCreateTable = false;
};

class TTestEnv {
public:
    class TDisableSourcesTag {};
    static TDisableSourcesTag DisableSourcesTag;

public:
    TTestEnv(ui32 staticNodes = 1, ui32 dynamicNodes = 4, const TTestEnvSettings& settings = {});

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

    const TVector<ui64>& GetPqTabletIds() const {
        return PqTabletIds;
    }

    TStoragePools GetPools() const;
    TStoragePools CreatePoolsForTenant(const TString& tenant);


private:
    TPortManager PortManager;

    Tests::TServerSettings::TPtr Settings;
    Tests::TServer::TPtr Server;
    THolder<Tests::TClient> Client;
    THolder<Tests::TTenants> Tenants;

    TString Endpoint;
    NYdb::TDriverConfig DriverConfig;
    THolder<NYdb::TDriver> Driver;
    TVector<ui64> PqTabletIds;
};

} // NSysView
} // NKikimr
