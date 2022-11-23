#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/fake_storage.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/services/metadata/manager/alter.h>
#include <ydb/services/metadata/manager/common.h>
#include <ydb/services/metadata/manager/table_record.h>
#include <ydb/services/metadata/manager/ydb_value_operator.h>
#include <ydb/services/metadata/service.h>

#include <library/cpp/actors/core/av_bootstrapped.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/testing/unittest/registar.h>

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
            TtlSettings: {
                Tiering: {
                    EnableTiering: true
                }
            }
            Sharding {
                HashSharding {
                    Function: %s
                    Columns: %s
                }
            }
        )", tableName.c_str(), tableShardsCount, shardingFunction.c_str(), shardingColumns.c_str()));
    }
};


Y_UNIT_TEST_SUITE(ColumnShardTiers) {

    const TString ConfigProtoStr = "Name : \"abc\"";
    const TString ConfigProtoStr1 = "Name : \"abc1\"";
    const TString ConfigProtoStr2 = "Name : \"abc2\"";

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
        YDB_ACCESSOR(ui32, ExpectedTieringsCount, 1);
        YDB_ACCESSOR(ui32, ExpectedTiersCount, 1);
        using TKeyCheckers = TMap<NTiers::TGlobalTierId, TJsonChecker>;
        YDB_ACCESSOR_DEF(TKeyCheckers, Checkers);
    public:
        void ResetConditions() {
            FoundFlag = false;
            Checkers.clear();
        }

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
                runtime.SimulateSleep(TDuration::Seconds(1));
            }
            runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
            Y_VERIFY(IsFound());
        }

        void CheckFound(NMetadataProvider::TEvRefreshSubscriberData* event) {
            auto snapshot = event->GetSnapshotAs<NTiers::TConfigsSnapshot>();
            Y_VERIFY(!!snapshot);
            auto* tInfo = snapshot->GetTableTiering("/Root/olapStore/olapTable");
            if (ExpectedTieringsCount) {
                if (!tInfo) {
                    Cerr << "tiering not found: " << snapshot->SerializeToString() << Endl;
                    return ;
                }
                if (tInfo->GetRules().size() != ExpectedTieringsCount) {
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
            if (ExpectedTiersCount != snapshot->GetTierConfigs().size()) {
                Cerr << "TieringsCount incorrect: " << snapshot->SerializeToString() << Endl;
                return;
            }
            for (auto&& i : Checkers) {
                auto value = snapshot->GetValue(i.first);
                NJson::TJsonValue jsonData;
                NProtobufJson::Proto2Json(value->GetProtoConfig(), jsonData);
                if (!i.second.Check(jsonData)) {
                    Cerr << "config value incorrect:" << snapshot->SerializeToString() << ";snapshot_check_path=" << i.first.ToString() << Endl;
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
            Become(&TThis::StateInit);
            Sender<NMetadataProvider::TEvSubscribeExternal>(ExternalDataManipulation).SendTo(ProviderId);
            Start = Now();
        }
    };

    class TEmulatorAlterController: public NMetadataManager::IAlterController {
    private:
        YDB_READONLY_FLAG(Finished, false);
    public:
        virtual void AlterProblem(const TString& errorMessage) override {
            Cerr << errorMessage << Endl;
            Y_VERIFY(false);
        }
        virtual void AlterFinished() override {
            FinishedFlag = true;
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
            emulator->MutableCheckers().emplace(NTiers::TGlobalTierId("/Root/olapStore", "tier1"), TJsonChecker("Name", "abc"));
            runtime.Register(emulator);
            runtime.SimulateSleep(TDuration::Seconds(10));
            Cerr << "Initialization finished" << Endl;

            lHelper.StartSchemaRequest("CREATE OBJECT tier1 ( "
                "TYPE TIER) WITH (tierConfig = `" + ConfigProtoStr + "`, ownerPath = `/Root/olapStore`)");
            lHelper.StartSchemaRequest("CREATE OBJECT tier1 ("
                "TYPE TIERING_RULE) WITH (tierName = tier1, tablePath = `/Root/olapStore/olapTable`, "
                "ownerPath = `/Root/olapStore`, column = timestamp, durationForEvict = `10d` )");
            {
                const TInstant start = Now();
                while (!emulator->IsFound() && Now() - start < TDuration::Seconds(2000)) {
                    runtime.SimulateSleep(TDuration::Seconds(1));
                }
                Y_VERIFY(emulator->IsFound());
            }
            {
                lHelper.StartSchemaRequest("ALTER OBJECT tier1 ( "
                    "TYPE TIER) SET tierConfig = `" + ConfigProtoStr1 + "`");

                emulator->ResetConditions();
                emulator->MutableCheckers().emplace(NTiers::TGlobalTierId("/Root/olapStore", "tier1"), TJsonChecker("Name", "abc1"));
                {
                    const TInstant start = Now();
                    while (!emulator->IsFound() && Now() - start < TDuration::Seconds(2000)) {
                        runtime.SimulateSleep(TDuration::Seconds(1));
                    }
                    Y_VERIFY(emulator->IsFound());
                }
            }
            {
                std::vector<NMetadataManager::TTableRecord> patches;
                {
                    NMetadataManager::TTableRecord patch;
                    patch.SetColumn("ownerPath", NMetadataManager::TYDBValue::Bytes("/Root/olapStore"));
                    patch.SetColumn("tierName", NMetadataManager::TYDBValue::Bytes("tier1"));
                    patches.emplace_back(std::move(patch));
                }

                lHelper.StartSchemaRequest("DROP OBJECT tier1(TYPE TIER)");
                lHelper.StartSchemaRequest("DROP OBJECT tier1(TYPE TIERING_RULE)");

                emulator->ResetConditions();
                emulator->SetExpectedTieringsCount(0);
                emulator->SetExpectedTiersCount(0);
                {
                    const TInstant start = Now();
                    while (!emulator->IsFound() && Now() - start < TDuration::Seconds(20)) {
                        runtime.SimulateSleep(TDuration::Seconds(1));
                    }
                    Y_VERIFY(emulator->IsFound());
                }
            }
        }
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
        runtime.SimulateSleep(TDuration::Seconds(10));
        Cerr << "Initialization finished" << Endl;

        lHelper.StartSchemaRequest("CREATE OBJECT tier1 ( "
            "TYPE TIER) WITH (tierConfig = `" + ConfigProtoStr1 + "`, ownerPath = `/Root/olapStore`)");
        lHelper.StartSchemaRequest("CREATE OBJECT tier1 ("
            "TYPE TIERING_RULE) "
            "WITH (ownerPath = `/Root/olapStore`, tierName = tier1, tablePath = `/Root/olapStore/olapTable`, column = timestamp, durationForEvict = `10d` "
            ")");
        {
            TTestCSEmulator emulator;
            emulator.MutableCheckers().emplace(NTiers::TGlobalTierId("/Root/olapStore", "tier1"), TJsonChecker("Name", "abc1"));
            emulator.SetExpectedTieringsCount(1);
            emulator.SetExpectedTiersCount(1);
            emulator.CheckRuntime(runtime);
        }

        lHelper.StartSchemaRequest("CREATE OBJECT tier2 ( "
            "TYPE TIER) WITH (ownerPath = `/Root/olapStore`, tierConfig = `" + ConfigProtoStr2 + "`)");
        lHelper.StartSchemaRequest("CREATE OBJECT tier2 ("
            "TYPE TIERING_RULE) WITH (tierName = tier2, tablePath = `/Root/olapStore/olapTable`, ownerPath = `/Root/olapStore`, column = timestamp, durationForEvict = `20d` )");
        {
            TTestCSEmulator emulator;
            emulator.MutableCheckers().emplace(NTiers::TGlobalTierId("/Root/olapStore", "tier1"), TJsonChecker("Name", "abc1"));
            emulator.MutableCheckers().emplace(NTiers::TGlobalTierId("/Root/olapStore", "tier2"), TJsonChecker("Name", "abc2"));
            emulator.SetExpectedTieringsCount(2);
            emulator.SetExpectedTiersCount(2);
            emulator.CheckRuntime(runtime);
        }

        lHelper.StartSchemaRequest("DROP OBJECT tier2 (TYPE TIER)");
        lHelper.StartSchemaRequest("DROP OBJECT tier2 (TYPE TIERING_RULE)");
        {
            TTestCSEmulator emulator;
            emulator.SetExpectedTieringsCount(1);
            emulator.SetExpectedTiersCount(1);
            emulator.CheckRuntime(runtime);
        }
        lHelper.StartSchemaRequest("DROP OBJECT tier1 (TYPE TIER)");
        lHelper.StartSchemaRequest("DROP OBJECT tier1 (TYPE TIERING_RULE)");
        {
            TTestCSEmulator emulator;
            emulator.SetExpectedTieringsCount(0);
            emulator.SetExpectedTiersCount(0);
            emulator.CheckRuntime(runtime);
        }

        //runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
        //runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_TRACE);
    }
//#define S3_TEST_USAGE
#ifdef S3_TEST_USAGE
    const TString TierConfigProtoStr =
        R"(
        Name : "fakeTier"
        ObjectStorage : {
            Scheme: HTTP
            VerifySSL: false
            Endpoint: "storage.cloud-preprod.yandex.net"
            Bucket: "tiering-test-01"
            AccessKey: "..."
            SecretKey: "..."
            ProxyHost: "localhost"
            ProxyPort: 8080
            ProxyScheme: HTTP
        }
    )";
    const TString TierEndpoint = "storage.cloud-preprod.yandex.net";
#else
    const TString TierConfigProtoStr =
        R"(
        Name : "fakeTier"
        ObjectStorage : {
            Endpoint: "fake"
            Bucket: "fake"
        }
    )";
    const TString TierEndpoint = "fake";
#endif

    Y_UNIT_TEST(TieringUsage) {
        TPortManager pm;

        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
            .SetEnableBackgroundTasks(true)
            .SetEnableOlapSchemaOperations(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_TRACE);

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);
        TLocalHelper lHelper(*server);
        lHelper.CreateTestOlapTable("olapTable");

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_NOTICE);
        runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BG_TASKS, NLog::PRI_DEBUG);
        //        runtime.SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NLog::PRI_DEBUG);
        Cerr << "Wait initialization" << Endl;
        runtime.SimulateSleep(TDuration::Seconds(20));
        Cerr << "Initialization finished" << Endl;

        lHelper.StartSchemaRequest("CREATE OBJECT fakeTier1 ( "
            "TYPE TIER) WITH (tierConfig = `" + TierConfigProtoStr + "`, ownerPath = `/Root/olapStore`)");
        lHelper.StartSchemaRequest("CREATE OBJECT fakeTier1 ("
            "TYPE TIERING_RULE) WITH (tierName = fakeTier1, ownerPath = `/Root/olapStore`, tablePath = `/Root/olapStore/olapTable`, column = timestamp, durationForEvict = `10d` )");

        lHelper.StartSchemaRequest("CREATE OBJECT fakeTier2 ( "
            "TYPE TIER) WITH (tierConfig = `" + TierConfigProtoStr + "`, ownerPath = `/Root/olapStore`)");
        lHelper.StartSchemaRequest("CREATE OBJECT fakeTier2 ("
            "TYPE TIERING_RULE) WITH (tierName = fakeTier2, ownerPath = `/Root/olapStore`, tablePath = `/Root/olapStore/olapTable`, column = timestamp, durationForEvict = `20d` )");
        {
            TTestCSEmulator emulator;
            emulator.MutableCheckers().emplace(NTiers::TGlobalTierId("/Root/olapStore", "fakeTier1"), TJsonChecker("Name", "fakeTier"));
            emulator.MutableCheckers().emplace(NTiers::TGlobalTierId("/Root/olapStore", "fakeTier2"), TJsonChecker("ObjectStorage.Endpoint", TierEndpoint));
            emulator.SetExpectedTieringsCount(2);
            emulator.SetExpectedTiersCount(2);
            emulator.CheckRuntime(runtime);
        }
        Cerr << "Insert..." << Endl;
        const TInstant pkStart = Now() - TDuration::Days(15);
        ui32 idx = 0;
        lHelper.SendDataViaActorSystem("/Root/olapStore/olapTable", 0, (pkStart + TDuration::Seconds(2 * idx++)).GetValue(), 2000);
        {
            const TInstant start = Now();
            bool check = false;
            while (Now() - start < TDuration::Seconds(60)) {
                Cerr << "Waiting..." << Endl;
#ifndef S3_TEST_USAGE
                if (Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize()) {
                    check = true;
                    Cerr << "Fake storage filled" << Endl;
                    break;
                }
#else
                check = true;
#endif
                runtime.SimulateSleep(TDuration::Seconds(1));
            }
            Y_VERIFY(check);
        }
#ifdef S3_TEST_USAGE
        Cerr << "storage initialized..." << Endl;
#endif

        lHelper.DropTable("/Root/olapStore/olapTable");
        {
            const TInstant start = Now();
            bool check = false;
            while (Now() - start < TDuration::Seconds(60)) {
                Cerr << "Cleaning waiting..." << Endl;
#ifndef S3_TEST_USAGE
                if (!Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetSize()) {
                    check = true;
                    Cerr << "Fake storage clean" << Endl;
                    break;
                }
#else
                check = true;
#endif
                runtime.SimulateSleep(TDuration::Seconds(1));
            }
            Y_VERIFY(check);
        }
#ifndef S3_TEST_USAGE
        Y_VERIFY(Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucketsCount() == 1);
#endif
    }

}
}
