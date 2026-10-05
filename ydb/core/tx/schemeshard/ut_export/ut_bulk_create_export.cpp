#include <ydb/core/base/hive.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/public/lib/value/value.h>

#include <library/cpp/testing/common/network.h>

#include <util/string/printf.h>

using namespace NKikimr;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::NWrappers::NTestHelpers;

namespace {

class TBulkExportTest {
    TPortManager Ports;
    const ui16 S3Port = Ports.GetPort();
    TS3Mock S3{TS3Mock::TSettings(S3Port)};
    Tests::TServer::TPtr Server;

public:
    ui64 SchemeShard;
    ui64 Hive;

    TBulkExportTest() {
        UNIT_ASSERT(S3.Start());
        Tests::TServerSettings settings(Ports.GetPort());
        settings.SetDomainName("Root").SetUseRealThreads(false);
        settings.SetDataShardExportFactory(std::make_shared<TDataShardExportFactory>());
        settings.FeatureFlags.SetEnableHiveBulkCreate(true);
        settings.FeatureFlags.SetEnableExportAutoDropping(true);
        Server = new Tests::TServer(settings);
        SchemeShard = Tests::ChangeStateStorage(Tests::SchemeRoot, settings.Domain);
        Hive = Tests::ChangeStateStorage(Tests::Hive, settings.Domain);
        Server->SetupRootStoragePools(Runtime().AllocateEdgeActor());
    }

    TTestActorRuntime& Runtime() {
        return *Server->GetRuntime();
    }

    void Wait(ui64 txId) {
        TestWaitNotification(Runtime(), {txId}, CreateNotificationSubscriber(Runtime(), SchemeShard));
    }

    TMap<ui32, TString> ReadRows(const TString& name) {
        const auto description = DescribePath(Runtime(), SchemeShard, "/Root/" + name, true);
        TMap<ui32, TString> rows;
        for (const auto& partition : description.GetPathDescription().GetTablePartitions()) {
            const auto result = ReadTable(Runtime(), partition.GetDatashardId(), name, {"key"}, {"key", "value"});
            const auto list = NClient::TValue::Create(result)["Result"]["List"];
            for (size_t i = 0; i < list.Size(); ++i) {
                const auto row = list[static_cast<int>(i)];
                UNIT_ASSERT(rows.emplace(static_cast<ui32>(row["key"]), TString(row["value"])).second);
            }
        }
        return rows;
    }

    void CreateSource() {
        TestCreateTable(Runtime(), SchemeShard, 100, "/Root", R"(
            Name: "Source"
            Columns { Name: "key" Type: "Uint32" }
            Columns { Name: "value" Type: "Utf8" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 4
            PartitionConfig { PartitioningPolicy { MinPartitionsCount: 4 MaxPartitionsCount: 4 } }
        )");
        Wait(100);
        const auto description = DescribePath(Runtime(), SchemeShard, "/Root/Source", true);
        const auto& partitions = description.GetPathDescription().GetTablePartitions();
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 4);
        for (ui32 i = 0; i < 4; ++i) {
            for (ui32 j = 1; j <= 10; ++j) {
                const ui32 key = (i << 30) + j;
                UpdateRow(Runtime(), "Source", key, Sprintf("payload-%u", key), partitions[i].GetDatashardId());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(ReadRows("Source").size(), 40);
    }

    void StartExport() {
        TestExport(Runtime(), SchemeShard, 101, "/Root", Sprintf(R"(
            ExportToS3Settings {
                endpoint: "localhost:%u"
                scheme: HTTP
                items { source_path: "/Root/Source" destination_prefix: "backup" }
            }
        )", S3Port));
    }

    void Import() {
        TestImport(Runtime(), SchemeShard, 102, "/Root", Sprintf(R"(
            ImportFromS3Settings {
                endpoint: "localhost:%u"
                scheme: HTTP
                items { source_prefix: "backup" destination_path: "/Root/Restored" }
            }
        )", S3Port));
        Wait(102);
        TestGetImport(Runtime(), SchemeShard, 102, "/Root", Ydb::StatusIds::SUCCESS);
    }

    void WaitForBackupCleanup() {
        const auto edge = Runtime().AllocateEdgeActor();
        Runtime().WaitFor("backup DataShards are removed from Hive", [&] {
            auto request = MakeHolder<TEvHive::TEvRequestHiveInfo>();
            request->Record.SetTabletType(TTabletTypes::DataShard);
            Runtime().SendToPipe(Hive, edge, request.Release(), 0, GetPipeConfigWithRetries());
            const auto info = Runtime().GrabEdgeEventRethrow<TEvHive::TEvResponseHiveInfo>(edge);
            for (const auto& tablet : info->Get()->Record.GetTablets()) {
                if (tablet.GetTabletOwner().GetOwner() == SchemeShard && tablet.GetIsBackup()) {
                    return false;
                }
            }
            return true;
        });
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TBulkCreateExport) {
    Y_UNIT_TEST_FLAG(ExportImportAfterLostReplyAndReboot, rebootHive) {
        TBulkExportTest env;
        auto& runtime = env.Runtime();
        env.CreateSource();
        const auto expected = env.ReadRows("Source");
        size_t backupRequests = 0;
        auto requestObserver = runtime.AddObserver<TEvHive::TEvCreateTablet>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (record.GetOwner() == env.SchemeShard && record.GetIsBackup()) {
                UNIT_ASSERT(TEvHive::TEvCreateTablet::IsBatch(record));
                UNIT_ASSERT_VALUES_EQUAL(TEvHive::TEvCreateTablet::GetBatchOwnerIdxs(record).size(), 4);
                ++backupRequests;
            }
        });
        TBlockEvents<TEvHive::TEvCreateTabletReply> replies(runtime, [&](const auto& ev) {
            return ev->Get()->Record.GetIsBatch() && ev->Get()->Record.GetOwner() == env.SchemeShard;
        });
        env.StartExport();
        runtime.WaitFor("committed backup tablet batch", [&] { return !replies.empty(); });
        THashMap<ui64, ui64> committed;
        for (const auto& result : replies.front()->Get()->Record.GetResults()) {
            UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrProto::OK);
            UNIT_ASSERT(committed.emplace(result.GetOwnerIdx(), result.GetTabletID()).second);
        }
        UNIT_ASSERT_VALUES_EQUAL(committed.size(), 4);
        replies.Stop().clear(); // Lose the reply after real Hive committed the batch.

        size_t successfulRetries = 0;
        auto replyObserver = runtime.AddObserver<TEvHive::TEvCreateTabletReply>([&](auto& ev) {
            const auto& record = ev->Get()->Record;
            if (!record.GetIsBatch() || record.GetOwner() != env.SchemeShard
                || record.GetResults().empty() || !committed.contains(record.GetResults(0).GetOwnerIdx())) {
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL(record.GetOrigin(), env.Hive);
            UNIT_ASSERT_VALUES_EQUAL(record.ResultsSize(), committed.size());
            for (const auto& result : record.GetResults()) {
                UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), NKikimrProto::OK);
                UNIT_ASSERT_VALUES_EQUAL(result.GetTabletID(), committed.at(result.GetOwnerIdx()));
            }
            ++successfulRetries;
        });
        if (rebootHive) {
            RebootTablet(runtime, env.Hive, runtime.AllocateEdgeActor());
        }
        RebootTablet(runtime, env.SchemeShard, runtime.AllocateEdgeActor());
        env.Wait(101);
        TestGetExport(runtime, env.SchemeShard, 101, "/Root", Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT(backupRequests >= 2);
        UNIT_ASSERT(successfulRetries > 0);
        UNIT_ASSERT(env.ReadRows("Source") == expected);
        env.WaitForBackupCleanup();

        // A drained operation must remain readable when subsequent creates use
        // the legacy path. This is flag-off coverage, not an old-binary test.
        runtime.GetAppData().FeatureFlags.SetEnableHiveBulkCreate(false);
        env.Import();
        UNIT_ASSERT(env.ReadRows("Restored") == expected);
    }
}
