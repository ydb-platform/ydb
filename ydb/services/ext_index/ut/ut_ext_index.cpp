#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/tiering/external_data.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/core/wrappers/fake_storage.h>
#include <ydb/core/formats/arrow/hash/xx_hash.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/services/metadata/manager/alter.h>
#include <ydb/services/metadata/manager/common.h>
#include <ydb/services/metadata/manager/table_record.h>
#include <ydb/services/metadata/manager/ydb_value_operator.h>
#include <ydb/services/metadata/service.h>

#include <ydb/library/actors/core/av_bootstrapped.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/hostname.h>
#include <contrib/libs/xxhash/xxhash.h>

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
                Enabled: {
                    ColumnName: "timestamp"
                    ExpireAfterSeconds : 60
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


Y_UNIT_TEST_SUITE(ExternalIndex) {

    Y_UNIT_TEST(Simple) {
        TPortManager pm;

        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
            .SetEnableExternalIndex(true)
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

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_NOTICE);
        runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::BG_TASKS, NLog::PRI_DEBUG);
        //        runtime.SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NLog::PRI_DEBUG);

        TLocalHelper lHelper(*server);
        lHelper.SetWithJsonDocument(true);
        lHelper.CreateTestOlapTable("olapTable", 2);
        lHelper.StartSchemaRequest("CREATE OBJECT `/Root/olapStore/olapTable:ext_index_simple` ( "
            "TYPE CS_EXT_INDEX) WITH (extractor = `{\"class_name\" : \"city64\", \"object\" :"
            "{\"fields\" : [{\"id\":\"uid\"}, {\"id\":\"level\"}, {\"id\":\"json_payload\", \"path\" : \"strict $.a.b\"}]}}`)");
        lHelper.StartSchemaRequest("CREATE OBJECT `/Root/olapStore/olapTable:ext_index_simple1` ( "
            "TYPE CS_EXT_INDEX) WITH (extractor = `{\"class_name\" : \"city64\", \"object\" :"
            "{\"fields\" : [{\"id\":\"uid\"}]}}`)");
        Cerr << "Wait tables" << Endl;
        runtime.SimulateSleep(TDuration::Seconds(20));
        Cerr << "Initialization tables" << Endl;
        const TInstant pkStart = Now() - TDuration::Days(15);
        ui32 idx = 0;

        const ui32 tsStart = (pkStart + TDuration::Seconds(2 * idx++)).GetValue();
        auto batch = lHelper.TestArrowBatch(0, tsStart, 6000);
        auto batchSize = NArrow::GetBatchDataSize(batch);
        Cerr << "Inserting " << batchSize << " bytes..." << Endl;
        UNIT_ASSERT(batchSize > 4 * 1024 * 1024); // NColumnShard::TLimits::MIN_BYTES_TO_INSERT
        UNIT_ASSERT(batchSize < 8 * 1024 * 1024);

        for (ui32 i = 0; i < 4; ++i) {
            lHelper.SendDataViaActorSystem("/Root/olapStore/olapTable", batch);
        }

        {
            TString resultData;
            lHelper.StartDataRequest("SELECT COUNT(*) FROM `/Root/.metadata/cs_index/Root/olapStore/olapTable/ext_index_simple`", true, &resultData);
            Cerr << resultData << Endl;
            UNIT_ASSERT_EQUAL(resultData, "[6000u]");
        }
        {
            NArrow::NHash::NXX64::TStreamStringHashCalcer calcer(0);
            calcer.Start();
            TString resultData;
            TString uid = "uid_" + ::ToString(tsStart + 2);
            ui32 level = 2;
            TString id = "2";
            calcer.Update((const ui8*)uid.data(), uid.size());
            calcer.Update((const ui8*)&level, sizeof(level));
            calcer.Update((const ui8*)id.data(), id.size());
            const ui64 hash = calcer.Finish();
            lHelper.StartDataRequest("SELECT index_hash, pk_timestamp FROM `/Root/.metadata/cs_index/Root/olapStore/olapTable/ext_index_simple` WHERE index_hash = " +
                ::ToString(hash), true, &resultData);
            Cerr << resultData << "/" << tsStart << Endl;
            UNIT_ASSERT_EQUAL(resultData, "[[" + ::ToString(hash) + "u];[" + ::ToString(tsStart + 2) + "u]]");
        }
        {
            TString resultData;
            lHelper.StartDataRequest("SELECT COUNT(*) FROM `/Root/.metadata/cs_index/Root/olapStore/olapTable/ext_index_simple1`", true, &resultData);
            Cerr << resultData << Endl;
            UNIT_ASSERT_EQUAL(resultData, "[6000u]");
        }
        lHelper.StartSchemaRequest("DROP OBJECT `/Root/olapStore/olapTable:ext_index_simple` (TYPE CS_EXT_INDEX)");
        lHelper.StartSchemaRequest("DROP OBJECT `/Root/olapStore/olapTable:ext_index_simple1` (TYPE CS_EXT_INDEX)");
        for (ui32 i = 0; i < 10; ++i) {
            server->GetRuntime()->SimulateSleep(TDuration::Seconds(10));
        }
        lHelper.StartDataRequest("SELECT COUNT(*) FROM `/Root/.metadata/cs_index/Root/olapStore/olapTable/ext_index_simple`", false);
        lHelper.StartDataRequest("SELECT COUNT(*) FROM `/Root/.metadata/cs_index/Root/olapStore/olapTable/ext_index_simple1`", false);
        {
            TString resultData;
            lHelper.StartDataRequest("SELECT COUNT(*) FROM `/Root/.metadata/cs_index/external`", true, &resultData);
            UNIT_ASSERT_EQUAL(resultData, "[0u]");
        }
        {
            TString resultData;
            lHelper.StartDataRequest("SELECT COUNT(*) FROM `/Root/.metadata/initialization/migrations`", true, &resultData);
            UNIT_ASSERT_EQUAL(resultData, "[6u]");
        }
    }

}
}
