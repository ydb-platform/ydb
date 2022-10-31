#include "columnshard_ut_common.h"
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/services/metadata/service.h>

#include <library/cpp/actors/core/av_bootstrapped.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/system/hostname.h>

namespace NKikimr {

using namespace NColumnShard;

class TLocalHelper: public Tests::NCS::THelper {
private:
    using TBase = Tests::NCS::THelper;
public:
    using TBase::TBase;
    void CreateTestOlapTable(TString tableName = "olapTable", TString storeName = "olapStore",
        ui32 storeShardsCount = 4, ui32 tableShardsCount = 3,
        TString shardingFunction = "HASH_FUNCTION_CLOUD_LOGS") {
        TActorId sender = Server.GetRuntime()->AllocateEdgeActor();
        CreateTestOlapStore(sender, Sprintf(R"(
             Name: "%s"
             ColumnShardCount: %d
             SchemaPresets {
                 Name: "default"
                 Schema {
                     Columns { Name: "timestamp" Type: "Timestamp" }
                     #Columns { Name: "resource_type" Type: "Utf8" }
                     Columns { Name: "resource_id" Type: "Utf8" }
                     Columns { Name: "uid" Type: "Utf8" }
                     Columns { Name: "level" Type: "Int32" }
                     Columns { Name: "message" Type: "Utf8" }
                     #Columns { Name: "json_payload" Type: "Json" }
                     #Columns { Name: "ingested_at" Type: "Timestamp" }
                     #Columns { Name: "saved_at" Type: "Timestamp" }
                     #Columns { Name: "request_id" Type: "Utf8" }
                     KeyColumnNames: "timestamp"
                     Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                     EnableTiering : true
                 }
             }
        )", storeName.c_str(), storeShardsCount));

        TString shardingColumns = "[\"timestamp\", \"uid\"]";
        if (shardingFunction != "HASH_FUNCTION_CLOUD_LOGS") {
            shardingColumns = "[\"uid\"]";
        }

        TBase::CreateTestOlapTable(sender, storeName, Sprintf(R"(
            Name: "%s"
            ColumnShardCount: %d
            Sharding {
                HashSharding {
                    Function: %s
                    Columns: %s
                }
            })", tableName.c_str(), tableShardsCount, shardingFunction.c_str(), shardingColumns.c_str()));
    }
};


Y_UNIT_TEST_SUITE(ColumnShardTiers) {

    const TString ConfigProtoStr = "Name : \"abc\"";

    class TJsonChecker {
    private:
        YDB_ACCESSOR_DEF(TString, Path);
        YDB_ACCESSOR_DEF(TString, Expectation);
    public:
        TJsonChecker(const TString& path, const TString& expectation)
            : Path(path)
            , Expectation(expectation)
        {

        }
        bool Check(const NJson::TJsonValue& jsonInfo) const {
            auto* jsonPathValue = jsonInfo.GetValueByPath(Path);
            if (!jsonPathValue) {
                return Expectation == "__NULL";
            }
            return jsonPathValue->GetStringRobust() == Expectation;
        }

        TString GetDebugString() const {
            TStringBuilder sb;
            sb << "path=" << Path << ";"
                << "expectation=" << Expectation << ";";
            return sb;
        }
    };

    class TTestCSEmulator: public NActors::TActorBootstrapped<TTestCSEmulator> {
    private:
        using TBase = NActors::TActorBootstrapped<TTestCSEmulator>;
        std::shared_ptr<NTiers::TSnapshotConstructor> ExternalDataManipulation;
        TActorId ProviderId;
        TInstant Start;
        YDB_READONLY_FLAG(Found, false);
        YDB_ACCESSOR(ui32, TieringsCount, 1);
        using TKeyCheckers = TMap<TString, TJsonChecker>;
        YDB_ACCESSOR_DEF(TKeyCheckers, Checkers);
    public:
        STATEFN(StateInit) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NMetadataProvider::TEvRefreshSubscriberData, Handle);
                default:
                    Y_VERIFY(false);
            }
        }

        void CheckRuntime(TTestActorRuntime& runtime) {
            const auto pred = [this](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)->TTestActorRuntimeBase::EEventAction {
                if (event->HasBuffer() && !event->HasEvent()) {
                } else if (!event->GetBase()) {
                } else {
                    auto ptr = dynamic_cast<NMetadataProvider::TEvRefreshSubscriberData*>(event->GetBase());
                    if (ptr) {
                        CheckFound(ptr);
                    }
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };

            runtime.SetObserverFunc(pred);

            for (const TInstant start = Now(); !IsFound() && Now() - start < TDuration::Seconds(10); ) {
                runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
            }
            Y_VERIFY(IsFound());
        }

        void CheckFound(NMetadataProvider::TEvRefreshSubscriberData* event) {
            auto snapshot = event->GetSnapshotAs<NTiers::TConfigsSnapshot>();
            Y_VERIFY(!!snapshot);
            auto* tInfo = snapshot->GetTableTiering("/Root/olapStore/olapTable");
            if (TieringsCount) {
                if (!tInfo) {
                    Cerr << "tiering not found: " << snapshot->SerializeToString() << Endl;
                    return ;
                }
                if (tInfo->GetRules().size() != TieringsCount) {
                    Cerr << "TieringsCount incorrect: " << snapshot->SerializeToString() << Endl;
                    return;
                }
            } else if (tInfo) {
                Cerr << "tiering found but its incorrect: " << snapshot->SerializeToString() << Endl;
                return;
            }
            for (auto&& [_, tiering] : snapshot->GetTableTierings()) {
                if (tiering.GetTablePathId() == 0) {
                    Cerr << "PathId not initialized: " << snapshot->SerializeToString() << Endl;
                    return;
                } else if (tiering.GetTablePathId() == InvalidLocalPathId) {
                    Cerr << "PathId invalid: " << snapshot->SerializeToString() << Endl;
                    return;
                } else {
                    Cerr << "PathId: " << tiering.GetTablePathId() << Endl;
                }
            }
            for (auto&& i : Checkers) {
                auto value = snapshot->GetValue(i.first);
                NJson::TJsonValue jsonData;
                NProtobufJson::Proto2Json(value->GetProtoConfig(), jsonData);
                if (!i.second.Check(jsonData)) {
                    Cerr << "config value incorrect:" << snapshot->SerializeToString() << ";snapshot_check_path=" << i.first << Endl;
                    Cerr << "json path incorrect:" << jsonData << ";" << i.second.GetDebugString() << Endl;
                    return;
                }
            }
            FoundFlag = true;
        }

        void Handle(NMetadataProvider::TEvRefreshSubscriberData::TPtr& ev) {
            CheckFound(ev->Get());
        }

        void Bootstrap() {
            ProviderId = NMetadataProvider::MakeServiceId(1);
            ExternalDataManipulation = std::make_shared<NTiers::TSnapshotConstructor>();
            ExternalDataManipulation->Start(ExternalDataManipulation);
            Become(&TThis::StateInit);
            Sender<NMetadataProvider::TEvSubscribeExternal>(ExternalDataManipulation).SendTo(ProviderId);
            Start = Now();
        }
    };

    Y_UNIT_TEST(DSConfigsStub) {
        TPortManager pm;

        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
            .SetEnableOlapSchemaOperations(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        //        server->SetupDefaultProfiles();

        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);
        TLocalHelper lHelper(*server);
        lHelper.CreateTestOlapTable();
        {
            TTestCSEmulator* emulator = new TTestCSEmulator;
            emulator->MutableCheckers().emplace("/Root/olapStore.tier1", TJsonChecker("Name", "abc"));
            runtime.Register(emulator);
            {
                const TInstant start = Now();
                while (Now() - start < TDuration::Seconds(10)) {
                    runtime.WaitForEdgeEvents([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                        Cerr << "Step " << event->Type << Endl;
                        return false;
                        }, {}, TDuration::Seconds(1));
                    Sleep(TDuration::Seconds(1));
                    Cerr << "Step finished" << Endl;
                }
            }

            {
                NYdb::NTable::TTableClient tClient(server->GetDriver(), NYdb::NTable::TClientSettings().UseQueryCache(false));
                tClient.CreateSession().Subscribe([](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
                    auto session = f.GetValueSync().GetSession();
                    session.ExecuteDataQuery(
                        "INSERT INTO `/Root/.external_data/tiers` (ownerPath, tierName, tierConfig) "
                        "VALUES ('/Root/olapStore', 'tier1', '" + ConfigProtoStr + "')"
                        , NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx());
                    });
            }
            {
                NYdb::NTable::TTableClient tClient(server->GetDriver(), NYdb::NTable::TClientSettings().UseQueryCache(false));
                tClient.CreateSession().Subscribe([](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
                    auto session = f.GetValueSync().GetSession();
                    session.ExecuteDataQuery(
                        "INSERT INTO `/Root/.external_data/tiering` (ownerPath, tierName, tablePath, column, durationForEvict) "
                        "VALUES ('/Root/olapStore', 'tier1', '/Root/olapStore/olapTable', 'timestamp', '10d')"
                        , NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx());
                    });
            }
            const TInstant start = Now();
            while (!emulator->IsFound() && Now() - start < TDuration::Seconds(20)) {
                runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(10));
            }
            Y_VERIFY(emulator->IsFound());
        }
        //runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
        //runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_TRACE);
    }

    Y_UNIT_TEST(DSConfigs) {
        TPortManager pm;

        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
            .SetEnableOlapSchemaOperations(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);
        TLocalHelper lHelper(*server);
        lHelper.CreateTestOlapTable("olapTable");

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_NOTICE);
        runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NLog::PRI_INFO);
//        runtime.SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NLog::PRI_DEBUG);
        for (const TInstant start = Now(); Now() - start < TDuration::Seconds(10); ) {
            runtime.WaitForEdgeEvents([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                Cerr << "Step " << event->Type << Endl;
                return false;
                }, {}, TDuration::Seconds(1));
            Sleep(TDuration::Seconds(1));
            Cerr << "Step finished" << Endl;
        }

        {
            NYdb::NTable::TTableClient tClient(server->GetDriver(), NYdb::NTable::TClientSettings().UseQueryCache(false));
            tClient.CreateSession().Subscribe([](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
                auto session = f.GetValueSync().GetSession();
                session.ExecuteDataQuery(
                    "INSERT INTO `/Root/.external_data/tiers` (ownerPath, tierName, tierConfig) "
                    "VALUES ('/Root/olapStore', 'tier1', '" + ConfigProtoStr + "')"
                    , NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx());
                });
        }
        {
            NYdb::NTable::TTableClient tClient(server->GetDriver(), NYdb::NTable::TClientSettings().UseQueryCache(false));
            tClient.CreateSession().Subscribe([](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
                auto session = f.GetValueSync().GetSession();
                session.ExecuteDataQuery(
                    "INSERT INTO `/Root/.external_data/tiering` (ownerPath, tierName, tablePath, column, durationForEvict) "
                    "VALUES ('/Root/olapStore', 'tier1', '/Root/olapStore/olapTable', 'timestamp', '10d')"
                    , NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx());
                });
        }
        {
            TTestCSEmulator emulator;
            emulator.MutableCheckers().emplace("/Root/olapStore.tier1", TJsonChecker("Name", "abc"));
            emulator.SetTieringsCount(1);
            emulator.CheckRuntime(runtime);
        }

        {
            NYdb::NTable::TTableClient tClient(server->GetDriver(), NYdb::NTable::TClientSettings().UseQueryCache(false));
            tClient.CreateSession().Subscribe([](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
                auto session = f.GetValueSync().GetSession();
                session.ExecuteDataQuery(
                    "INSERT INTO `/Root/.external_data/tiers` (ownerPath, tierName, tierConfig) "
                    "VALUES ('/Root/olapStore', 'tier2', '" + ConfigProtoStr + "')"
                    , NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx());
                });
        }
        {
            NYdb::NTable::TTableClient tClient(server->GetDriver(), NYdb::NTable::TClientSettings().UseQueryCache(false));
            tClient.CreateSession().Subscribe([](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
                auto session = f.GetValueSync().GetSession();
                session.ExecuteDataQuery(
                    "INSERT INTO `/Root/.external_data/tiering` (ownerPath, tierName, tablePath, column, durationForEvict) "
                    "VALUES ('/Root/olapStore', 'tier2', '/Root/olapStore/olapTable', 'timestamp', '20d')"
                    , NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx());
                });
        }

        {
            TTestCSEmulator emulator;
            emulator.MutableCheckers().emplace("/Root/olapStore.tier1", TJsonChecker("Name", "abc"));
            emulator.MutableCheckers().emplace("/Root/olapStore.tier2", TJsonChecker("Name", "abc"));
            emulator.SetTieringsCount(2);
            emulator.CheckRuntime(runtime);
        }

        {
            NYdb::NTable::TTableClient tClient(server->GetDriver(), NYdb::NTable::TClientSettings().UseQueryCache(false));
            tClient.CreateSession().Subscribe([](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
                auto session = f.GetValueSync().GetSession();
                session.ExecuteDataQuery(
                    "DELETE FROM `/Root/.external_data/tiers`"
                    , NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx());
                });
        }
        {
            NYdb::NTable::TTableClient tClient(server->GetDriver(), NYdb::NTable::TClientSettings().UseQueryCache(false));
            tClient.CreateSession().Subscribe([](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
                auto session = f.GetValueSync().GetSession();
                session.ExecuteDataQuery(
                    "DELETE FROM `/Root/.external_data/tiering`"
                    , NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx());
                });
        }
        {
            TTestCSEmulator emulator;
            emulator.SetTieringsCount(0);
            emulator.CheckRuntime(runtime);
        }

        //runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
        //runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_TRACE);
    }

}
}
