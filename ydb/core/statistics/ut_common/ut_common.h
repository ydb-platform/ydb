#pragma once

#include <ydb/core/statistics/events.h>

#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

#include <ydb/core/testlib/test_client.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimrStat {
    class TTable;
}

namespace NKikimr {
namespace NStat {

static constexpr ui32 ColumnTableRowsNumber = 1000;

NKikimrSubDomains::TSubDomainSettings GetSubDomainDeclareSettings(
    const TString &name, const TStoragePools &pools = {});

NKikimrSubDomains::TSubDomainSettings GetSubDomainDefaultSettings(
    const TString &name, const TStoragePools &pools = {});

class TTestEnv {
public:
    TTestEnv(ui32 staticNodes = 1, ui32 dynamicNodes = 1, ui32 storagePools = 1, bool useRealThreads = false);
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

    auto& GetController() {
        return CSController;
    }

private:
    TPortManager PortManager;

    Tests::TServerSettings::TPtr Settings;
    Tests::TServer::TPtr Server;
    THolder<Tests::TClient> Client;
    THolder<Tests::TTenants> Tenants;

    TString Endpoint;
    NYdb::TDriverConfig DriverConfig;
    THolder<NYdb::TDriver> Driver;
    NYDBTest::TControllers::TGuard<NYDBTest::NColumnShard::TController> CSController;
};

void CreateDatabase(TTestEnv& env, const TString& databaseName, size_t nodeCount = 1);

void CreateServerlessDatabase(TTestEnv& env, const TString& databaseName, TPathId resourcesDomainKey);

struct TTableInfo {
    std::vector<ui64> ShardIds;
    ui64 SaTabletId;
    TPathId DomainKey;
    TPathId PathId;
};
std::vector<TTableInfo> CreateDatabaseColumnTables(TTestEnv& env, ui8 tableCount, ui8 shardCount);

TPathId ResolvePathId(TTestActorRuntime& runtime, const TString& path, TPathId* domainKey = nullptr, ui64* saTabletId = nullptr);


TVector<ui64> GetTableShards(TTestActorRuntime& runtime, TActorId sender, const TString &path);
TVector<ui64> GetColumnTableShards(TTestActorRuntime& runtime, TActorId sender,const TString &path);

void CreateUniformTable(TTestEnv& env, const TString& databaseName, const TString& tableName);
void CreateColumnStoreTable(TTestEnv& env, const TString& databaseName, const TString& tableName, int shardCount);
void DropTable(TTestEnv& env, const TString& databaseName, const TString& tableName);

std::shared_ptr<TCountMinSketch> ExtractCountMin(TTestActorRuntime& runtime, const TPathId& pathId, ui64 columnTag = 1);
void ValidateCountMinColumnshard(TTestActorRuntime& runtime, const TPathId& pathId, ui64 expectedProbe);

void ValidateCountMinDatashard(TTestActorRuntime& runtime, TPathId pathId);
void ValidateCountMinDatashardAbsense(TTestActorRuntime& runtime, TPathId pathId);

struct TAnalyzedTable {
    TPathId PathId;
    std::vector<ui32> ColumnTags;

    TAnalyzedTable(const TPathId& pathId);
    TAnalyzedTable(const TPathId& pathId, const std::vector<ui32>& columnTags);
    void ToProto(NKikimrStat::TTable& tableProto) const;
};

std::unique_ptr<TEvStatistics::TEvAnalyze> MakeAnalyzeRequest(const std::vector<TAnalyzedTable>& tables, const TString operationId = "operationId");

void Analyze(TTestActorRuntime& runtime, ui64 saTabletId, const std::vector<TAnalyzedTable>& table, const TString operationId = "operationId");
void AnalyzeTable(TTestActorRuntime& runtime, ui64 shardTabletId, const TAnalyzedTable& table);
void AnalyzeStatus(TTestActorRuntime& runtime, TActorId sender, ui64 saTabletId, const TString operationId, const NKikimrStat::TEvAnalyzeStatusResponse::EStatus expectedStatus);

void WaitForSavedStatistics(TTestActorRuntime& runtime, const TPathId& pathId);

} // namespace NStat
} // namespace NKikimr
