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
    void CreateTestOlapTable(TString tableName = "olapTable", ui32 tableShardsCount = 3,
        TString storeName = "olapStore", ui32 storeShardsCount = 4,
        TString shardingFunction = "HASH_FUNCTION_CLOUD_LOGS") {
        TActorId sender = Server.GetRuntime()->AllocateEdgeActor();
        CreateTestOlapStore(sender, Sprintf(R"(
             Name: "%s"
             ColumnShardCount: %d
             SchemaPresets {
                 Name: "default"
                 Schema {
                     %s
                 }
             }
        )", storeName.c_str(), storeShardsCount, GetTestTableSchema().data()));

        TString shardingColumns = "[\"timestamp\", \"uid\"]";
        if (shardingFunction != "HASH_FUNCTION_CLOUD_LOGS") {
            shardingColumns = "[\"uid\"]";
        }

        TBase::CreateTestOlapTable(sender, storeName, Sprintf(R"(
            Name: "%s"
            ColumnShardCount: %d
            TtlSettings: {
                UseTiering: "tiering1"
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

    const TString ConfigTiering1Str = R"({
        "rules" : [
            {
                "tierName" : "tier1",
                "durationForEvict" : "10d"
            },
            {
                "tierName" : "tier2",
                "durationForEvict" : "20d"
            }
        ]
    })";

    const TString ConfigTiering2Str = R"({
        "rules" : [
            {
                "tierName" : "tier1",
                "durationForEvict" : "10d"
            }
        ]
    })";

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

        using TKeyCheckers = TMap<TString, TJsonChecker>;
        YDB_ACCESSOR_DEF(TKeyCheckers, Checkers);
    public:
        void ResetConditions() {
            FoundFlag = false;
            Checkers.clear();
        }

        STATEFN(StateInit) {
            switch (ev->GetTypeRewrite()) {
                hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle);
                default:
                    Y_VERIFY(false);
            }
        }

        void CheckRuntime(TTestActorRuntime& runtime) {
            const auto pred = [this](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)->TTestActorRuntimeBase::EEventAction {
                if (event->HasBuffer() && !event->HasEvent()) {
                } else if (!event->HasEvent()) {
                } else {
                    auto ptr = event->CastAsLocal<NMetadata::NProvider::TEvRefreshSubscriberData>();
                    if (ptr) {
                        CheckFound(ptr);
                    }
                }
                return TTestActorRuntimeBase::EEventAction::PROCESS;
            };

            runtime.SetObserverFunc(pred);

            for (const TInstant start = Now(); !IsFound() && Now() - start < TDuration::Seconds(30); ) {
                runtime.SimulateSleep(TDuration::Seconds(1));
            }
            runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);
            Y_VERIFY(IsFound());
        }

        void CheckFound(NMetadata::NProvider::TEvRefreshSubscriberData* event) {
            auto snapshot = event->GetSnapshotAs<NTiers::TConfigsSnapshot>();
            if (!snapshot) {
                Cerr << "incorrect snapshot" << Endl;
                return;
            }
            Cerr << "SNAPSHOT: " << snapshot->SerializeToString() << Endl;
            const auto& tierings = snapshot->GetTableTierings();
            if (tierings.size() != ExpectedTieringsCount) {
                Cerr << "TieringsCount incorrect: " << snapshot->SerializeToString() << ";expectation=" << ExpectedTieringsCount << Endl;
                return;
            }
            if (ExpectedTiersCount != snapshot->GetTierConfigs().size()) {
                Cerr << "TiersCount incorrect: " << snapshot->SerializeToString() << ";expectation=" << ExpectedTiersCount << Endl;
                return;
            }
            for (auto&& i : Checkers) {
                NJson::TJsonValue jsonData;
                if (i.first.StartsWith("TIER.")) {
                    auto value = snapshot->GetTierById(i.first.substr(5));
                    jsonData = value->SerializeConfigToJson();
                } else if (i.first.StartsWith("TIERING_RULE.")) {
                    auto value = snapshot->GetTierById(i.first.substr(13));
                    jsonData = value->SerializeConfigToJson();
                } else {
                    Y_VERIFY(false);
                }
                if (!i.second.Check(jsonData)) {
                    Cerr << "config value incorrect:" << snapshot->SerializeToString() << ";snapshot_check_path=" << i.first << Endl;
                    Cerr << "json path incorrect:" << jsonData << ";" << i.second.GetDebugString() << Endl;
                    return;
                }
            }
            FoundFlag = true;
        }

        void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
            CheckFound(ev->Get());
        }

        void Bootstrap() {
            ProviderId = NMetadata::NProvider::MakeServiceId(SelfId().NodeId());
            ExternalDataManipulation = std::make_shared<NTiers::TSnapshotConstructor>();
            Become(&TThis::StateInit);
            Sender<NMetadata::NProvider::TEvSubscribeExternal>(ExternalDataManipulation).SendTo(ProviderId);
            Start = Now();
        }
    };

    class TEmulatorAlterController: public NMetadata::NModifications::IAlterController {
    private:
        YDB_READONLY_FLAG(Finished, false);
    public:
        virtual void OnAlteringProblem(const TString& errorMessage) override {
            Cerr << errorMessage << Endl;
            Y_VERIFY(false);
        }
        virtual void OnAlteringFinished() override {
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
            .SetForceColumnTablesCompositeMarks(true);
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
            emulator->MutableCheckers().emplace("TIER.tier1", TJsonChecker("Name", "abc"));
            emulator->SetExpectedTiersCount(2);
            runtime.Register(emulator);
            runtime.SimulateSleep(TDuration::Seconds(10));
            Cerr << "Initialization finished" << Endl;

            lHelper.StartSchemaRequest("CREATE OBJECT tier1 (TYPE TIER) WITH tierConfig = `" + ConfigProtoStr + "`");
            lHelper.StartSchemaRequest("CREATE OBJECT tiering1 ("
                "TYPE TIERING_RULE) WITH (defaultColumn = timestamp, description = `" + ConfigTiering1Str + "` )", false);
            lHelper.StartSchemaRequest("CREATE OBJECT tier2 (TYPE TIER) WITH tierConfig = `" + ConfigProtoStr + "`");
            lHelper.StartSchemaRequest("CREATE OBJECT tiering1 ("
                "TYPE TIERING_RULE) WITH (defaultColumn = timestamp, description = `" + ConfigTiering1Str + "` )");
            {
                const TInstant start = Now();
                while (!emulator->IsFound() && Now() - start < TDuration::Seconds(2000)) {
                    runtime.SimulateSleep(TDuration::Seconds(1));
                }
                Y_VERIFY(emulator->IsFound());
            }
            {
                emulator->ResetConditions();
                emulator->SetExpectedTiersCount(2);
                emulator->MutableCheckers().emplace("TIER.tier1", TJsonChecker("Name", "abc1"));

                lHelper.StartSchemaRequest("ALTER OBJECT tier1 (TYPE TIER) SET tierConfig = `" + ConfigProtoStr1 + "`");

                {
                    const TInstant start = Now();
                    while (!emulator->IsFound() && Now() - start < TDuration::Seconds(2000)) {
                        runtime.SimulateSleep(TDuration::Seconds(1));
                    }
                    Y_VERIFY(emulator->IsFound());
                }
            }
            {
                emulator->ResetConditions();
                emulator->SetExpectedTieringsCount(0);
                emulator->SetExpectedTiersCount(0);

                lHelper.StartSchemaRequest("DROP OBJECT tier1(TYPE TIER)", false);
                lHelper.StartSchemaRequest("DROP OBJECT tiering1(TYPE TIERING_RULE)", false);
                lHelper.StartSchemaRequest("DROP TABLE `/Root/olapStore/olapTable`");
                lHelper.StartSchemaRequest("DROP OBJECT tiering1(TYPE TIERING_RULE)");
                lHelper.StartSchemaRequest("DROP OBJECT tier1(TYPE TIER)");
                lHelper.StartSchemaRequest("DROP OBJECT tier2(TYPE TIER)");

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
            .SetForceColumnTablesCompositeMarks(true);

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

        lHelper.StartSchemaRequest("CREATE OBJECT tier1 (TYPE TIER) WITH tierConfig = `" + ConfigProtoStr1 + "`", true, false);
        {
            TTestCSEmulator emulator;
            emulator.MutableCheckers().emplace("TIER.tier1", TJsonChecker("Name", "abc1"));
            emulator.SetExpectedTieringsCount(0);
            emulator.SetExpectedTiersCount(1);
            emulator.CheckRuntime(runtime);
        }

        lHelper.StartSchemaRequest("CREATE OBJECT tier2 (TYPE TIER) WITH tierConfig = `" + ConfigProtoStr2 + "`");
        lHelper.StartSchemaRequest("CREATE OBJECT tiering1 (TYPE TIERING_RULE) "
            "WITH (defaultColumn = timestamp, description = `" + ConfigTiering1Str + "`)");
        lHelper.StartSchemaRequest("CREATE OBJECT tiering2 (TYPE TIERING_RULE) "
            "WITH (defaultColumn = timestamp, description = `" + ConfigTiering2Str + "` )", true, false);
        {
            TTestCSEmulator emulator;
            emulator.MutableCheckers().emplace("TIER.tier1", TJsonChecker("Name", "abc1"));
            emulator.MutableCheckers().emplace("TIER.tier2", TJsonChecker("Name", "abc2"));
            emulator.SetExpectedTieringsCount(2);
            emulator.SetExpectedTiersCount(2);
            emulator.CheckRuntime(runtime);
        }

        lHelper.StartSchemaRequest("DROP OBJECT tier2 (TYPE TIER)", false);
        lHelper.StartSchemaRequest("DROP OBJECT tier1 (TYPE TIER)", false);
        lHelper.StartSchemaRequest("DROP OBJECT tiering2 (TYPE TIERING_RULE)");
        lHelper.StartSchemaRequest("DROP OBJECT tiering1 (TYPE TIERING_RULE)", false);
        lHelper.StartSchemaRequest("DROP TABLE `/Root/olapStore/olapTable`");
        lHelper.StartSchemaRequest("DROP OBJECT tiering1 (TYPE TIERING_RULE)", true, false);
        {
            TTestCSEmulator emulator;
            emulator.SetExpectedTieringsCount(0);
            emulator.SetExpectedTiersCount(2);
            emulator.CheckRuntime(runtime);
        }
        lHelper.StartSchemaRequest("DROP OBJECT tier2 (TYPE TIER)");
        lHelper.StartSchemaRequest("DROP OBJECT tier1 (TYPE TIER)", true, false);
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
            AccessKey: "SId:secretAccessKey"
            SecretKey: "USId:root@builtin:secretSecretKey"
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
            SecretableAccessKey: {
                SecretId: {
                    Id: "secretAccessKey"
                    OwnerId: "root@builtin"
                }
            }
            SecretKey: "SId:secretSecretKey"
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
            .SetForceColumnTablesCompositeMarks(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();
        runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_TRACE);

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_NOTICE);
        runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BG_TASKS, NLog::PRI_DEBUG);
        //        runtime.SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NLog::PRI_DEBUG);

        TLocalHelper lHelper(*server);
        lHelper.StartSchemaRequest("CREATE OBJECT secretAccessKey ( "
            "TYPE SECRET) WITH (value = ak)");
        lHelper.StartSchemaRequest("CREATE OBJECT secretSecretKey ( "
            "TYPE SECRET) WITH (value = sk)");
        Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->SetSecretKey("sk");

        lHelper.StartSchemaRequest("CREATE OBJECT tier1 ( "
            "TYPE TIER) WITH (tierConfig = `" + TierConfigProtoStr + "`)");
        lHelper.StartSchemaRequest("CREATE OBJECT tier2 ( "
            "TYPE TIER) WITH (tierConfig = `" + TierConfigProtoStr + "`)");

        lHelper.StartSchemaRequest("CREATE OBJECT tiering1 ("
            "TYPE TIERING_RULE) WITH (defaultColumn = timestamp, description = `" + ConfigTiering1Str + "` )");
        lHelper.StartSchemaRequest("CREATE OBJECT tiering2 ("
            "TYPE TIERING_RULE) WITH (defaultColumn = timestamp, description = `" + ConfigTiering2Str + "` )");
        {
            TTestCSEmulator* emulator = new TTestCSEmulator;
            runtime.Register(emulator);
            emulator->MutableCheckers().emplace("TIER.tier1", TJsonChecker("Name", "fakeTier"));
            emulator->MutableCheckers().emplace("TIER.tier2", TJsonChecker("ObjectStorage.Endpoint", TierEndpoint));
            emulator->SetExpectedTieringsCount(2);
            emulator->SetExpectedTiersCount(2);
            emulator->CheckRuntime(runtime);
        }
        lHelper.CreateTestOlapTable("olapTable", 2);
        Cerr << "Wait tables" << Endl;
        runtime.SimulateSleep(TDuration::Seconds(20));
        Cerr << "Initialization tables" << Endl;
        const TInstant pkStart = Now() - TDuration::Days(15);
        ui32 idx = 0;

        auto batch = lHelper.TestArrowBatch(0, (pkStart + TDuration::Seconds(2 * idx++)).GetValue(), 6000);
        auto batchSize = NArrow::GetBatchDataSize(batch);
        Cerr << "Inserting " << batchSize << " bytes..." << Endl;
        UNIT_ASSERT(batchSize > 4 * 1024 * 1024); // NColumnShard::TLimits::MIN_BYTES_TO_INSERT
        UNIT_ASSERT(batchSize < 8 * 1024 * 1024);

        for (ui32 i = 0; i < 4; ++i) {
            lHelper.SendDataViaActorSystem("/Root/olapStore/olapTable", batch);
        }
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
            UNIT_ASSERT(check);
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
            UNIT_ASSERT(check);
        }
#ifndef S3_TEST_USAGE
        UNIT_ASSERT_EQUAL(Singleton<NKikimr::NWrappers::NExternalStorage::TFakeExternalStorage>()->GetBucketsCount(), 1);
#endif
    }

}
}
