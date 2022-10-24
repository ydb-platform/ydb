#include "columnshard_ut_common.h"
#include "external_data.h"
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/services/metadata/service.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/actors/core/av_bootstrapped.h>

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
                     StorageTiers {
                         Name: "tier1"
                         ObjectStorage {
                             Endpoint: "fake"
                             AccessKey: "$a"
                         }
                     }
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

    class TTestCSEmulator: public NActors::TActorBootstrapped<TTestCSEmulator> {
    private:
        using TBase = NActors::TActorBootstrapped<TTestCSEmulator>;
        std::shared_ptr<NTiers::TSnapshotConstructor> ExternalDataManipulation;
        TActorId ProviderId;
        TInstant Start;
        YDB_READONLY_FLAG(Found, false);
    public:
        STFUNC(StateInit) {
            switch (ev->GetTypeRewrite()) {
                HFunc(NMetadataProvider::TEvRefreshSubscriberData, Handle);
                default:
                    Y_VERIFY(false);
            }
        }

        void Handle(NMetadataProvider::TEvRefreshSubscriberData::TPtr& ev, const TActorContext&) {
            auto value = ev->Get()->GetSnapshotAs<NTiers::TConfigsSnapshot>()->GetValue("/Root/olapStore.tier1");
            if (value && value->GetProtoConfig().GetName() == "abc") {
                FoundFlag = true;
            } else {
                Cerr << ev->Get()->GetSnapshot()->SerializeToString() << Endl;
            }
        }

        void Bootstrap() {
            ProviderId = NMetadataProvider::MakeServiceId(1);
            ExternalDataManipulation = std::make_shared<NTiers::TSnapshotConstructor>("/Root/.external_data/tiers");
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
            .SetEnableKqpSessionActor(true)
            .SetEnableOlapSchemaOperations(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        //        server->SetupDefaultProfiles();

        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);
        {
            TTestCSEmulator* emulator = new TTestCSEmulator;
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

            NYdb::NTable::TTableClient tClient(server->GetDriver(), NYdb::NTable::TClientSettings().UseQueryCache(false));
            tClient.CreateSession().Subscribe([](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
                auto sResult = f.GetValueSync();
                Cerr << sResult.GetIssues().ToString() << Endl;
                auto session = sResult.GetSession();
                session.ExecuteDataQuery("INSERT INTO `/Root/.external_data/tiers` (ownerPath, tierName, tierConfig) VALUES ('/Root/olapStore', 'tier1', '" + ConfigProtoStr + "')"
                    , NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx());
                });

            const TInstant start = Now();
            while (!emulator->IsFound() && Now() - start < TDuration::Seconds(20)) {
                runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(10));
            }
            Y_VERIFY(emulator->IsFound());
        }
        //runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
        //runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_TRACE);
    }
/*
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
            .SetEnableKqpSessionActor(true)
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

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        const TInstant start = Now();
        while (Now() - start < TDuration::Seconds(10)) {
            runtime.WaitForEdgeEvents([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                Cerr << "Step " << event->Type << Endl;
                return false;
                }, {}, TDuration::Seconds(1));
            Sleep(TDuration::Seconds(1));
            Cerr << "Step finished" << Endl;
        }

        NYdb::NTable::TTableClient tClient(server->GetDriver(), NYdb::NTable::TClientSettings().UseQueryCache(false));
        tClient.CreateSession().Subscribe([](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
            auto sResult = f.GetValueSync();
            Cerr << sResult.GetIssues().ToString() << Endl;
            auto session = sResult.GetSession();
            session.ExecuteDataQuery("INSERT INTO `/Root/.external_data/tiers` (ownerPath, tierName, tierConfig) VALUES ('/Root/olapStore', 'tier1', '" + ConfigProtoStr + "')"
                , NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx());
            });

        bool found = false;
        bool* foundPtr = &found;
        const auto pred = [foundPtr](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)->TTestActorRuntimeBase::EEventAction {
            if (event->HasBuffer() && !event->HasEvent()) {
            } else if (!event->GetBase()) {
                Cerr << "Type nullptr" << Endl;
            } else {
                Cerr << "Step " << event->GetBase()->Type() << Endl;
                auto ptr = dynamic_cast<NMetadataProvider::TEvRefreshSubscriberData*>(event->GetBase());
                if (ptr) {
                    auto value = ptr->GetSnapshotAs<NTiers::TConfigsSnapshot>()->GetValue("/Root/olapStore.tier1");
                    if (value && value->GetProtoConfig().GetName() == "abc") {
                        *foundPtr = true;
                    } else {
                        Cerr << ptr->GetSnapshot()->SerializeToString() << Endl;
                    }
                }
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        };

        runtime.SetObserverFunc(pred);

        while (!found) {
            runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(10));
        }
        //runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
        //runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_TRACE);
    }
*/
}
}
