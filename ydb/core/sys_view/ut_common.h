#pragma once

#include <ydb/core/testlib/test_client.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NSysView {

NKikimrSubDomains::TSubDomainSettings GetSubDomainDeclareSettings(
    const TString &name, const TStoragePools &pools = {});

NKikimrSubDomains::TSubDomainSettings GetSubDomainDefaultSettings(
    const TString &name, const TStoragePools &pools = {});

class TTestEnv {
public:
    class TDisableSourcesTag {};
    static TDisableSourcesTag DisableSourcesTag;

public:
    TTestEnv(ui32 staticNodes = 1, ui32 dynamicNodes = 4, ui32 storagePools = 0,
        ui32 pqTabletsN = 0, bool enableSVP = false);

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
