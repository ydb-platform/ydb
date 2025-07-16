#include "datashard_ut_common_kqp.h"

#include <ydb/core/base/path.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/change_exchange/change_sender.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/user_info.h>
#include <ydb/core/persqueue/write_meta.h>
#include <ydb/core/tx/scheme_board/events.h>
#include <ydb/core/tx/scheme_board/events_internal.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <ydb/public/lib/value/value.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/generic/size_literals.h>
#include <util/string/join.h>
#include <util/string/printf.h>
#include <util/string/strip.h>

namespace NKikimr {

using namespace NDataShard;
using namespace NDataShard::NKqpHelpers;
using namespace Tests;

Y_UNIT_TEST_SUITE(IncrementalBackup) {
    using namespace NYdb::NPersQueue;
    using namespace NYdb::NDataStreams::V1;

    using TCdcStream = TShardedTableOptions::TCdcStream;

    static NKikimrPQ::TPQConfig DefaultPQConfig() {
        NKikimrPQ::TPQConfig pqConfig;
        pqConfig.SetEnabled(true);
        pqConfig.SetEnableProtoSourceIdInfo(true);
        pqConfig.SetTopicsAreFirstClassCitizen(true);
        pqConfig.SetMaxReadCookies(10);
        pqConfig.AddClientServiceType()->SetName("data-streams");
        pqConfig.SetCheckACL(false);
        pqConfig.SetRequireCredentialsInNewProtocol(false);
        pqConfig.MutableQuotingConfig()->SetEnableQuoting(false);
        return pqConfig;
    }

    static void SetupLogging(TTestActorRuntime& runtime) {
        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::CHANGE_EXCHANGE, NLog::PRI_TRACE);
        runtime.SetLogPriority(NKikimrServices::PERSQUEUE, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::PQ_READ_PROXY, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::PQ_METACACHE, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::CONTINUOUS_BACKUP, NLog::PRI_DEBUG);
        runtime.SetLogPriority(NKikimrServices::REPLICATION_SERVICE, NLog::PRI_DEBUG);
    }

    TShardedTableOptions SimpleTable() {
        return TShardedTableOptions();
    }

    ui64 ResolvePqTablet(TTestActorRuntime& runtime, const TActorId& sender, const TString& path, ui32 partitionId) {
        auto streamDesc = Ls(runtime, sender, path);

        const auto& streamEntry = streamDesc->ResultSet.at(0);
        UNIT_ASSERT(streamEntry.ListNodeEntry);

        const auto& children = streamEntry.ListNodeEntry->Children;
        UNIT_ASSERT_VALUES_EQUAL(children.size(), 1);

        auto topicDesc = Navigate(runtime, sender, JoinPath(ChildPath(SplitPath(path), children.at(0).Name)),
            NSchemeCache::TSchemeCacheNavigate::EOp::OpTopic);

        const auto& topicEntry = topicDesc->ResultSet.at(0);
        UNIT_ASSERT(topicEntry.PQGroupInfo);

        const auto& pqDesc = topicEntry.PQGroupInfo->Description;
        for (const auto& partition : pqDesc.GetPartitions()) {
            if (partitionId == partition.GetPartitionId()) {
                return partition.GetTabletId();
            }
        }

        UNIT_ASSERT_C(false, "Cannot find partition: " << partitionId);
        return 0;
    }

    auto GetRecords(TTestActorRuntime& runtime, const TActorId& sender, const TString& path, ui32 partitionId) {
        NKikimrClient::TPersQueueRequest request;
        request.MutablePartitionRequest()->SetTopic(path);
        request.MutablePartitionRequest()->SetPartition(partitionId);

        auto& cmd = *request.MutablePartitionRequest()->MutableCmdRead();
        cmd.SetClientId(NKikimr::NPQ::CLIENTID_WITHOUT_CONSUMER);
        cmd.SetCount(10000);
        cmd.SetOffset(0);
        cmd.SetReadTimestampMs(0);
        cmd.SetExternalOperation(true);

        auto req = MakeHolder<TEvPersQueue::TEvRequest>();
        req->Record = std::move(request);
        ForwardToTablet(runtime, ResolvePqTablet(runtime, sender, path, partitionId), sender, req.Release());

        auto resp = runtime.GrabEdgeEventRethrow<TEvPersQueue::TEvResponse>(sender);
        UNIT_ASSERT(resp);

        TVector<std::pair<TString, TString>> result;
        for (const auto& r : resp->Get()->Record.GetPartitionResponse().GetCmdReadResult().GetResult()) {
            const auto data = NKikimr::GetDeserializedData(r.GetData());
            result.emplace_back(r.GetPartitionKey(), data.GetData());
        }

        return result;
    }

    void WaitForContent(TServer::TPtr server, const TActorId& sender, const TString& path, const TVector<NKikimrChangeExchange::TChangeRecord>& expected) {
        while (true) {
            const auto records = GetRecords(*server->GetRuntime(), sender, path, 0);
            for (ui32 i = 0; i < std::min(records.size(), expected.size()); ++i) {
                NKikimrChangeExchange::TChangeRecord proto;
                UNIT_ASSERT(proto.ParseFromString(records.at(i).second));
                google::protobuf::util::MessageDifferencer md;
                auto fieldComparator = google::protobuf::util::DefaultFieldComparator();
                md.set_field_comparator(&fieldComparator);
                UNIT_ASSERT(md.Compare(proto.GetCdcDataChange(), expected.at(i).GetCdcDataChange()));
            }

            if (records.size() >= expected.size()) {
                UNIT_ASSERT_VALUES_EQUAL_C(records.size(), expected.size(),
                    "Unexpected record: " << records.at(expected.size()).second);
                break;
            }

            SimulateSleep(server, TDuration::Seconds(1));
        }
    }

    NKikimrChangeExchange::TChangeRecord MakeUpsert(ui32 key, ui32 value) {
        auto keyCell = TCell::Make<ui32>(key);
        auto valueCell = TCell::Make<ui32>(value);
        NKikimrChangeExchange::TChangeRecord proto;

        auto& dc = *proto.MutableCdcDataChange();
        auto& dcKey = *dc.MutableKey();
        dcKey.AddTags(1);
        dcKey.SetData(TSerializedCellVec::Serialize({keyCell}));
        auto& upsert = *dc.MutableUpsert();
        upsert.AddTags(2);
        upsert.SetData(TSerializedCellVec::Serialize({valueCell}));

        return proto;
    }

    NKikimrChangeExchange::TChangeRecord MakeErase(ui32 key) {
        auto keyCell = TCell::Make<ui32>(key);
        NKikimrChangeExchange::TChangeRecord proto;

        auto& dc = *proto.MutableCdcDataChange();
        auto& dcKey = *dc.MutableKey();
        dcKey.AddTags(1);
        dcKey.SetData(TSerializedCellVec::Serialize({keyCell}));
        dc.MutableErase();

        return proto;
    }

    Y_UNIT_TEST(SimpleBackup) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        WaitTxNotification(server, edgeActor, AsyncCreateContinuousBackup(server, "/Root", "Table"));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 100),
            (5, 200),
            (6, 300);
        )");

        ExecSQL(server, edgeActor, "DELETE FROM `/Root/Table` ON (key) VALUES (2), (6);");

        WaitForContent(server, edgeActor, "/Root/Table/0_continuousBackupImpl", {
            MakeUpsert(1, 100),
            MakeUpsert(5, 200),
            MakeUpsert(6, 300),
            MakeErase(2),
            MakeErase(6),
        });

        WaitTxNotification(server, edgeActor,
            AsyncAlterTakeIncrementalBackup(server, "/Root", "Table", "IncrBackupImpl", "1_continuousBackupImpl"));

        // Actions below mustn't affect the result

        SimulateSleep(server, TDuration::Seconds(1));

        const char* const result = "{ items { uint32_value: 1 } items { uint32_value: 100 } }, "
            "{ items { uint32_value: 2 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 5 } items { uint32_value: 200 } }, "
            "{ items { uint32_value: 6 } items { null_flag_value: NULL_VALUE } }";

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/IncrBackupImpl`
                ORDER BY key
                )"),
            result);

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (2, 2000),
            (5, 5000),
            (7, 7000);
        )");

        ExecSQL(server, edgeActor, "DELETE FROM `/Root/Table` ON (key) VALUES (3), (5), (8);");

        SimulateSleep(server, TDuration::Seconds(1));

        // Unchanged

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/IncrBackupImpl`
                ORDER BY key
                )"),
            result);
    }

    Y_UNIT_TEST(SimpleRestore) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (2, 2),
            (3, 3);
        )");

        CreateShardedTable(
            server,
            edgeActor,
            "/Root",
            "IncrBackupImpl",
            SimpleTable()
                .AllowSystemColumnNames(true)
                .Columns({
                    {"key", "Uint32", true, false},
                    {"value", "Uint32", false, false},
                    {"__ydb_incrBackupImpl_deleted", "Bool", false, false}}));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/IncrBackupImpl` (key, value, __ydb_incrBackupImpl_deleted) VALUES
            (1, 10, NULL),
            (2, NULL, true),
            (3, 30, NULL),
            (5, NULL, true);
        )");

        WaitTxNotification(server, edgeActor, AsyncAlterRestoreIncrementalBackup(server, "/Root", "/Root/IncrBackupImpl", "/Root/Table"));

        SimulateSleep(server, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/Table`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 30 } }");
    }

    Y_UNIT_TEST(MultiBackup) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        WaitTxNotification(server, edgeActor,
            AsyncCreateContinuousBackup(server, "/Root", "Table", "0_continuousBackupImpl"));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 100),
            (5, 200),
            (6, 300);
        )");

        ExecSQL(server, edgeActor, "DELETE FROM `/Root/Table` ON (key) VALUES (2), (6);");

        WaitForContent(server, edgeActor, "/Root/Table/0_continuousBackupImpl", {
            MakeUpsert(1, 100),
            MakeUpsert(5, 200),
            MakeUpsert(6, 300),
            MakeErase(2),
            MakeErase(6),
        });

        WaitTxNotification(server, edgeActor,
            AsyncAlterTakeIncrementalBackup(server, "/Root", "Table", "1_IncrBackupImpl", "1_continuousBackupImpl"));

        SimulateSleep(server, TDuration::Seconds(1));

        const char* const result1 = "{ items { uint32_value: 1 } items { uint32_value: 100 } }, "
            "{ items { uint32_value: 2 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 5 } items { uint32_value: 200 } }, "
            "{ items { uint32_value: 6 } items { null_flag_value: NULL_VALUE } }";

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/1_IncrBackupImpl`
                ORDER BY key
                )"),
            result1);

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (2, 2000),
            (5, 5000),
            (7, 7000);
        )");

        ExecSQL(server, edgeActor, "DELETE FROM `/Root/Table` ON (key) VALUES (3), (5), (8);");

        WaitForContent(server, edgeActor, "/Root/Table/1_continuousBackupImpl", {
            MakeUpsert(2, 2000),
            MakeUpsert(5, 5000),
            MakeUpsert(7, 7000),
            MakeErase(3),
            MakeErase(5),
            MakeErase(8),
        });

        WaitTxNotification(server, edgeActor,
            AsyncAlterTakeIncrementalBackup(server, "/Root", "Table", "2_IncrBackupImpl", "2_continuousBackupImpl"));

        SimulateSleep(server, TDuration::Seconds(1));

        const char* const result2 = "{ items { uint32_value: 2 } items { uint32_value: 2000 } }, "
            "{ items { uint32_value: 3 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 5 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 7 } items { uint32_value: 7000 } }, "
            "{ items { uint32_value: 8 } items { null_flag_value: NULL_VALUE } }";

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/2_IncrBackupImpl`
                ORDER BY key
                )"),
            result2);

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (20, 2000),
            (50, 5000),
            (70, 7000);
        )");

        ExecSQL(server, edgeActor, "DELETE FROM `/Root/Table` ON (key) VALUES (1), (2), (3);");

        WaitForContent(server, edgeActor, "/Root/Table/2_continuousBackupImpl", {
            MakeUpsert(20, 2000),
            MakeUpsert(50, 5000),
            MakeUpsert(70, 7000),
            MakeErase(1),
            MakeErase(2),
            MakeErase(3),
        });

        WaitTxNotification(server, edgeActor,
            AsyncAlterTakeIncrementalBackup(server, "/Root", "Table", "3_IncrBackupImpl", "3_continuousBackupImpl"));

        SimulateSleep(server, TDuration::Seconds(1));

        const char* const result3 = "{ items { uint32_value: 1 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 2 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 3 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 20 } items { uint32_value: 2000 } }, "
            "{ items { uint32_value: 50 } items { uint32_value: 5000 } }, "
            "{ items { uint32_value: 70 } items { uint32_value: 7000 } }";

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/3_IncrBackupImpl`
                ORDER BY key
                )"),
            result3);

        // Check increments are unchanged

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES (10, 10000)
        )");

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/1_IncrBackupImpl`
                ORDER BY key
                )"),
            result1);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/2_IncrBackupImpl`
                ORDER BY key
                )"),
            result2);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/3_IncrBackupImpl`
                ORDER BY key
                )"),
            result3);
    }

    // TODO(innokentii): test actual state of MultiRestore and probably rename it back to just restore

    Y_UNIT_TEST(BackupRestore) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());
        CreateShardedTable(server, edgeActor, "/Root", "RestoreTable", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/RestoreTable` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        WaitTxNotification(server, edgeActor, AsyncCreateContinuousBackup(server, "/Root", "Table", "0_continuousBackupImpl"));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 100),
            (5, 200),
            (6, 300);
        )");

        ExecSQL(server, edgeActor, "DELETE FROM `/Root/Table` ON (key) VALUES (2), (6);");

        SimulateSleep(server, TDuration::Seconds(1));

        WaitTxNotification(server, edgeActor,
            AsyncAlterTakeIncrementalBackup(server, "/Root", "Table", "IncrBackupImpl", "1_continuousBackupImpl"));

        SimulateSleep(server, TDuration::Seconds(1));

        WaitTxNotification(server, edgeActor, AsyncAlterRestoreIncrementalBackup(server, "/Root", "/Root/IncrBackupImpl", "/Root/RestoreTable"));

        SimulateSleep(server, TDuration::Seconds(5)); // wait longer until schema will be applied

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/Table`
                ORDER BY key
                )"),
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/RestoreTable`
                ORDER BY key
                )"));
    }

    Y_UNIT_TEST_TWIN(SimpleBackupBackupCollection, WithIncremental) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
            .SetEnableBackupService(true)
            .SetEnableRealSystemViewPaths(false)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
                (1, 10)
              , (2, 20)
              , (3, 30)
              ;
            )");

        ExecSQL(server, edgeActor, R"(
            CREATE BACKUP COLLECTION `MyCollection`
              ( TABLE `/Root/Table`
              )
            WITH
              ( STORAGE = 'cluster'
              , INCREMENTAL_BACKUP_ENABLED = ')" + TString(WithIncremental ? "true" : "false") +  R"('
              );
            )", false);

        ExecSQL(server, edgeActor, R"(BACKUP `MyCollection`;)", false);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                -- TODO: fix with navigate after proper scheme cache handling
                SELECT key, value FROM `/Root/.backups/collections/MyCollection/19700101000001Z_full/Table`
                ORDER BY key
                )"),
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/Table`
                ORDER BY key
                )"));


        if (WithIncremental) {
            ExecSQL(server, edgeActor, R"(
                UPSERT INTO `/Root/Table` (key, value) VALUES
                (2, 200);
            )");

            ExecSQL(server, edgeActor, R"(DELETE FROM `/Root/Table` WHERE key=1;)");

            ExecSQL(server, edgeActor, R"(BACKUP `MyCollection` INCREMENTAL;)", false);

            SimulateSleep(server, TDuration::Seconds(1));

            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    -- TODO: fix with navigate after proper scheme cache handling
                    SELECT key, value FROM `/Root/.backups/collections/MyCollection/19700101000002Z_incremental/Table`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 1 } items { null_flag_value: NULL_VALUE } }, "
                "{ items { uint32_value: 2 } items { uint32_value: 200 } }");
        }
    }

    Y_UNIT_TEST(ComplexBackupBackupCollection) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
            .SetEnableBackupService(true)
            .SetEnableRealSystemViewPaths(false)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
                (1, 10)
              , (2, 20)
              , (3, 30)
              ;
            )");

        ExecSQL(server, edgeActor, R"(
            CREATE BACKUP COLLECTION `MyCollection`
              ( TABLE `/Root/Table`
              )
            WITH
              ( STORAGE = 'cluster'
              , INCREMENTAL_BACKUP_ENABLED = 'true'
              );
            )", false);

        ExecSQL(server, edgeActor, R"(BACKUP `MyCollection`;)", false);

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                -- TODO: fix with navigate after proper scheme cache handling
                SELECT key, value FROM `/Root/.backups/collections/MyCollection/19700101000001Z_full/Table`
                ORDER BY key
                )"),
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/Table`
                ORDER BY key
                )"));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (2, 200);
        )");

        ExecSQL(server, edgeActor, R"(DELETE FROM `/Root/Table` WHERE key=1;)");

        ExecSQL(server, edgeActor, R"(BACKUP `MyCollection` INCREMENTAL;)", false);

        SimulateSleep(server, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                -- TODO: fix with navigate after proper scheme cache handling
                SELECT key, value FROM `/Root/.backups/collections/MyCollection/19700101000002Z_incremental/Table`
                ORDER BY key
                )"),
            "{ items { uint32_value: 1 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 2 } items { uint32_value: 200 } }");

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (3, 300);
        )");

        ExecSQL(server, edgeActor, R"(DELETE FROM `/Root/Table` WHERE key=2;)");

        ExecSQL(server, edgeActor, R"(BACKUP `MyCollection` INCREMENTAL;)", false);

        SimulateSleep(server, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                -- TODO: fix with navigate after proper scheme cache handling
                SELECT key, value FROM `/Root/.backups/collections/MyCollection/19700101000003Z_incremental/Table`
                ORDER BY key
                )"),
            "{ items { uint32_value: 2 } items { null_flag_value: NULL_VALUE } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 300 } }");
    }

    Y_UNIT_TEST_TWIN(SimpleRestoreBackupCollection, WithIncremental) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
            .SetEnableBackupService(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);

        ExecSQL(server, edgeActor, R"(
            CREATE BACKUP COLLECTION `MyCollection`
              ( TABLE `/Root/Table`
              )
            WITH
              ( STORAGE = 'cluster'
              , INCREMENTAL_BACKUP_ENABLED = ')" + TString(WithIncremental ? "true" : "false") +  R"('
              );
            )", false);

        CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000001Z_full", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000001Z_full/Table` (key, value) VALUES
                (1, 10)
              , (2, 20)
              , (3, 30)
              , (4, 40)
              , (5, 50)
              ;
            )");

        if (WithIncremental) {
            auto opts = TShardedTableOptions()
                .AllowSystemColumnNames(true)
                .Columns({
                    {"key", "Uint32", true, false},
                    {"value", "Uint32", false, false},
                    {"__ydb_incrBackupImpl_deleted", "Bool", false, false}});

            CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000002Z_incremental", "Table", opts);

            ExecSQL(server, edgeActor, R"(
                UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000002Z_incremental/Table` (key, value, __ydb_incrBackupImpl_deleted) VALUES
                  (2, 200, NULL)
                , (1, NULL, true)
                ;
            )");

            CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000003Z_incremental", "Table", opts);

            ExecSQL(server, edgeActor, R"(
                UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000003Z_incremental/Table` (key, value, __ydb_incrBackupImpl_deleted) VALUES
                  (2, 2000, NULL)
                , (5, NULL, true)
                ;
            )");
        }

        ExecSQL(server, edgeActor, R"(RESTORE `MyCollection`;)", false);

        // Add sleep to ensure restore operation completes
        runtime.SimulateSleep(TDuration::Seconds(10));

        if (!WithIncremental) {
            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/Table`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
                "{ items { uint32_value: 2 } items { uint32_value: 20 } }, "
                "{ items { uint32_value: 3 } items { uint32_value: 30 } }, "
                "{ items { uint32_value: 4 } items { uint32_value: 40 } }, "
                "{ items { uint32_value: 5 } items { uint32_value: 50 } }"
                );
        } else {
            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/Table`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 2 } items { uint32_value: 2000 } }, "
                "{ items { uint32_value: 3 } items { uint32_value: 30 } }, "
                "{ items { uint32_value: 4 } items { uint32_value: 40 } }"
                );
        }
    }

    Y_UNIT_TEST_TWIN(ComplexRestoreBackupCollection, WithIncremental) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
            .SetEnableBackupService(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);

        ExecSQL(server, edgeActor, R"(
            CREATE BACKUP COLLECTION `MyCollection`
              ( TABLE `/Root/Table`
              , TABLE `/Root/DirA/TableA`
              , TABLE `/Root/DirA/TableB`
              , TABLE `/Root/DirB/DirC/TableC`
              )
            WITH
              ( STORAGE = 'cluster'
              , INCREMENTAL_BACKUP_ENABLED = ')" + TString(WithIncremental ? "true" : "false") +  R"('
              );
            )", false);

        {
            CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000001Z_full", "Table", SimpleTable());

            ExecSQL(server, edgeActor, R"(
                UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000001Z_full/Table` (key, value) VALUES
                  (1, 10)
                , (2, 20)
                , (3, 30)
                , (4, 40)
                , (5, 50)
                ;
                )");
        }

        {
            CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000001Z_full/DirA", "TableA", SimpleTable());

            ExecSQL(server, edgeActor, R"(
                UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000001Z_full/DirA/TableA` (key, value) VALUES
                  (11, 101)
                , (21, 201)
                , (31, 301)
                , (41, 401)
                , (51, 501)
                ;
                )");
        }

        {
            CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000001Z_full/DirA", "TableB", SimpleTable());

            ExecSQL(server, edgeActor, R"(
                UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000001Z_full/DirA/TableB` (key, value) VALUES
                  (12, 102)
                , (22, 202)
                , (32, 302)
                , (42, 402)
                , (52, 502)
                ;
                )");
        }

        {
            CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000001Z_full/DirB/DirC", "TableC", SimpleTable());

            ExecSQL(server, edgeActor, R"(
                UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000001Z_full/DirB/DirC/TableC` (key, value) VALUES
                  (13, 103)
                , (23, 203)
                , (33, 303)
                , (43, 403)
                , (53, 503)
                ;
                )");
        }

        if (WithIncremental) {
            auto opts = TShardedTableOptions()
                .AllowSystemColumnNames(true)
                .Columns({
                    {"key", "Uint32", true, false},
                    {"value", "Uint32", false, false},
                    {"__ydb_incrBackupImpl_deleted", "Bool", false, false}});

            {
                CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000002Z_incremental", "Table", opts);

                ExecSQL(server, edgeActor, R"(
                    UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000002Z_incremental/Table` (key, value, __ydb_incrBackupImpl_deleted) VALUES
                      (2, 200, NULL)
                    , (1, NULL, true)
                    ;
                )");

                CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000003Z_incremental", "Table", opts);

                ExecSQL(server, edgeActor, R"(
                    UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000003Z_incremental/Table` (key, value, __ydb_incrBackupImpl_deleted) VALUES
                      (2, 2000, NULL)
                    , (5, NULL, true)
                    ;
                )");
            }

            {
                CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000002Z_incremental/DirA", "TableA", opts);
                CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000003Z_incremental/DirA", "TableA", opts);

                ExecSQL(server, edgeActor, R"(
                    UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000003Z_incremental/DirA/TableA` (key, value, __ydb_incrBackupImpl_deleted) VALUES
                      (21, 20001, NULL)
                    , (51, NULL, true)
                    ;
                )");
            }

            {
                CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000002Z_incremental/DirA", "TableB", opts);

                ExecSQL(server, edgeActor, R"(
                    UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000002Z_incremental/DirA/TableB` (key, value, __ydb_incrBackupImpl_deleted) VALUES
                      (22, 2002, NULL)
                    , (12, NULL, true)
                    ;
                )");

                CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000003Z_incremental/DirA", "TableB", opts);
            }

            {
                CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000002Z_incremental/DirB/DirC", "TableC", opts);
                CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000003Z_incremental/DirB/DirC", "TableC", opts);
            }
        }

        ExecSQL(server, edgeActor, R"(RESTORE `MyCollection`;)", false);

        // Add sleep to ensure restore operation completes
        runtime.SimulateSleep(TDuration::Seconds(5));

        if (!WithIncremental) {
            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/Table`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
                "{ items { uint32_value: 2 } items { uint32_value: 20 } }, "
                "{ items { uint32_value: 3 } items { uint32_value: 30 } }, "
                "{ items { uint32_value: 4 } items { uint32_value: 40 } }, "
                "{ items { uint32_value: 5 } items { uint32_value: 50 } }"
                );

            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/DirA/TableA`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 11 } items { uint32_value: 101 } }, "
                "{ items { uint32_value: 21 } items { uint32_value: 201 } }, "
                "{ items { uint32_value: 31 } items { uint32_value: 301 } }, "
                "{ items { uint32_value: 41 } items { uint32_value: 401 } }, "
                "{ items { uint32_value: 51 } items { uint32_value: 501 } }"
                );

            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/DirA/TableB`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 12 } items { uint32_value: 102 } }, "
                "{ items { uint32_value: 22 } items { uint32_value: 202 } }, "
                "{ items { uint32_value: 32 } items { uint32_value: 302 } }, "
                "{ items { uint32_value: 42 } items { uint32_value: 402 } }, "
                "{ items { uint32_value: 52 } items { uint32_value: 502 } }"
                );

            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/DirB/DirC/TableC`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 13 } items { uint32_value: 103 } }, "
                "{ items { uint32_value: 23 } items { uint32_value: 203 } }, "
                "{ items { uint32_value: 33 } items { uint32_value: 303 } }, "
                "{ items { uint32_value: 43 } items { uint32_value: 403 } }, "
                "{ items { uint32_value: 53 } items { uint32_value: 503 } }"
                );
        } else {
            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/Table`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 2 } items { uint32_value: 2000 } }, "
                "{ items { uint32_value: 3 } items { uint32_value: 30 } }, "
                "{ items { uint32_value: 4 } items { uint32_value: 40 } }"
                );

            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/DirA/TableA`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 11 } items { uint32_value: 101 } }, "
                "{ items { uint32_value: 21 } items { uint32_value: 20001 } }, "
                "{ items { uint32_value: 31 } items { uint32_value: 301 } }, "
                "{ items { uint32_value: 41 } items { uint32_value: 401 } }"
                );

            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/DirA/TableB`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 22 } items { uint32_value: 2002 } }, "
                "{ items { uint32_value: 32 } items { uint32_value: 302 } }, "
                "{ items { uint32_value: 42 } items { uint32_value: 402 } }, "
                "{ items { uint32_value: 52 } items { uint32_value: 502 } }"
                );

            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/DirB/DirC/TableC`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 13 } items { uint32_value: 103 } }, "
                "{ items { uint32_value: 23 } items { uint32_value: 203 } }, "
                "{ items { uint32_value: 33 } items { uint32_value: 303 } }, "
                "{ items { uint32_value: 43 } items { uint32_value: 403 } }, "
                "{ items { uint32_value: 53 } items { uint32_value: 503 } }"
                );
        }
    }


    Y_UNIT_TEST(E2EBackupCollection) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
            .SetEnableBackupService(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
                (1, 10)
              , (2, 20)
              , (3, 30)
              ;
        )");

        ExecSQL(server, edgeActor, R"(
            CREATE BACKUP COLLECTION `MyCollection`
              ( TABLE `/Root/Table`
              )
            WITH
              ( STORAGE = 'cluster'
              , INCREMENTAL_BACKUP_ENABLED = 'true'
              );
            )", false);

        ExecSQL(server, edgeActor, R"(BACKUP `MyCollection`;)", false);

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (2, 200);
        )");

        ExecSQL(server, edgeActor, R"(DELETE FROM `/Root/Table` WHERE key=1;)");

        ExecSQL(server, edgeActor, R"(BACKUP `MyCollection` INCREMENTAL;)", false);

        SimulateSleep(server, TDuration::Seconds(1));

        auto expected = KqpSimpleExec(runtime, R"(SELECT key, value FROM `/Root/Table` ORDER BY key)");

        ExecSQL(server, edgeActor, R"(DROP TABLE `/Root/Table`;)", false);

        ExecSQL(server, edgeActor, R"(RESTORE `MyCollection`;)", false);

        // Add sleep to ensure restore operation completes
        runtime.SimulateSleep(TDuration::Seconds(5));

        auto actual = KqpSimpleExec(runtime, R"(SELECT key, value FROM `/Root/Table` ORDER BY key)");

        UNIT_ASSERT_VALUES_EQUAL(expected, actual);
    }

    Y_UNIT_TEST(MultiShardIncrementalRestore) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
            .SetEnableBackupService(true)
            .SetEnableRealSystemViewPaths(false)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);

        // Create a table with multiple shards by using 4 shards
        CreateShardedTable(server, edgeActor, "/Root", "MultiShardTable", 
            TShardedTableOptions()
                .Shards(4)
                .Columns({
                    {"key", "Uint32", true, false},
                    {"value", "Uint32", false, false}
                }));

        // Insert data across all shards
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/MultiShardTable` (key, value) VALUES
              (1, 10),   -- shard 1
              (2, 20),   -- shard 1
              (11, 110), -- shard 2
              (12, 120), -- shard 2
              (21, 210), -- shard 3
              (22, 220), -- shard 3
              (31, 310), -- shard 4
              (32, 320)  -- shard 4
            ;
        )");

        // Create backup collection
        ExecSQL(server, edgeActor, R"(
            CREATE BACKUP COLLECTION `MultiShardCollection`
              ( TABLE `/Root/MultiShardTable`
              )
            WITH
              ( STORAGE = 'cluster'
              , INCREMENTAL_BACKUP_ENABLED = 'true'
              );
            )", false);

        // Create full backup
        ExecSQL(server, edgeActor, R"(BACKUP `MultiShardCollection`;)", false);

        // Wait for backup to complete
        SimulateSleep(server, TDuration::Seconds(1));

        // Modify data in multiple shards
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/MultiShardTable` (key, value) VALUES
              (2, 200),   -- shard 1 - update
              (12, 1200), -- shard 2 - update
              (22, 2200), -- shard 3 - update
              (32, 3200); -- shard 4 - update
        )");

        // Delete data from multiple shards
        ExecSQL(server, edgeActor, R"(
            DELETE FROM `/Root/MultiShardTable` WHERE key IN (1, 11, 21, 31);
        )");

        // Create first incremental backup
        ExecSQL(server, edgeActor, R"(BACKUP `MultiShardCollection` INCREMENTAL;)", false);

        // Wait for incremental backup to complete
        SimulateSleep(server, TDuration::Seconds(1));

        // Capture expected state
        auto expected = KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/MultiShardTable` ORDER BY key
        )");

        // Drop table and restore from backups
        ExecSQL(server, edgeActor, R"(DROP TABLE `/Root/MultiShardTable`;)", false);

        ExecSQL(server, edgeActor, R"(RESTORE `MultiShardCollection`;)", false);

        // Wait for restore to complete
        runtime.SimulateSleep(TDuration::Seconds(10));

        auto actual = KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/MultiShardTable` ORDER BY key
        )");

        UNIT_ASSERT_VALUES_EQUAL(expected, actual);

        // Verify that we have the expected final state:
        // - Keys 1, 11, 21, 31 deleted by incremental backup
        // - Keys 2, 12, 22, 32 updated to 200, 1200, 2200, 3200 by incremental backup
        UNIT_ASSERT_VALUES_EQUAL(actual, 
            "{ items { uint32_value: 2 } items { uint32_value: 200 } }, "
            "{ items { uint32_value: 12 } items { uint32_value: 1200 } }, "
            "{ items { uint32_value: 22 } items { uint32_value: 2200 } }, "
            "{ items { uint32_value: 32 } items { uint32_value: 3200 } }");
    }

    Y_UNIT_TEST_TWIN(ForgedMultiShardIncrementalRestore, WithIncremental) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
            .SetEnableBackupService(true)
            .SetEnableRealSystemViewPaths(false)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);

        ExecSQL(server, edgeActor, R"(
            CREATE BACKUP COLLECTION `ForgedMultiShardCollection`
              ( TABLE `/Root/Table2Shard`
              , TABLE `/Root/Table3Shard`
              , TABLE `/Root/Table4Shard`
              )
            WITH
              ( STORAGE = 'cluster'
              , INCREMENTAL_BACKUP_ENABLED = ')" + TString(WithIncremental ? "true" : "false") +  R"('
              );
            )", false);

        // Create full backup tables with different sharding
        // Table with 2 shards
        CreateShardedTable(server, edgeActor, "/Root/.backups/collections/ForgedMultiShardCollection/19700101000001Z_full", "Table2Shard", 
            TShardedTableOptions().Shards(2));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/.backups/collections/ForgedMultiShardCollection/19700101000001Z_full/Table2Shard` (key, value) VALUES
                (1, 100), (2, 200), (11, 1100), (12, 1200), (21, 2100), (22, 2200)
              ;
            )");

        // Table with 3 shards
        CreateShardedTable(server, edgeActor, "/Root/.backups/collections/ForgedMultiShardCollection/19700101000001Z_full", "Table3Shard", 
            TShardedTableOptions().Shards(3));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/.backups/collections/ForgedMultiShardCollection/19700101000001Z_full/Table3Shard` (key, value) VALUES
                (1, 10), (2, 20), (3, 30), (11, 110), (12, 120), (13, 130), (21, 210), (22, 220), (23, 230)
              ;
            )");

        // Table with 4 shards
        CreateShardedTable(server, edgeActor, "/Root/.backups/collections/ForgedMultiShardCollection/19700101000001Z_full", "Table4Shard", 
            TShardedTableOptions().Shards(4));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/.backups/collections/ForgedMultiShardCollection/19700101000001Z_full/Table4Shard` (key, value) VALUES
                (1, 1), (2, 2), (3, 3), (4, 4), (11, 11), (12, 12), (13, 13), (14, 14), 
                (21, 21), (22, 22), (23, 23), (24, 24), (31, 31), (32, 32), (33, 33), (34, 34)
              ;
            )");

        if (WithIncremental) {
            auto opts = TShardedTableOptions()
                .AllowSystemColumnNames(true)
                .Columns({
                    {"key", "Uint32", true, false},
                    {"value", "Uint32", false, false},
                    {"__ydb_incrBackupImpl_deleted", "Bool", false, false}});

            // Create incremental backup tables with same sharding as full backup
            // Table2Shard - 2 shards: delete some keys, update others
            CreateShardedTable(server, edgeActor, "/Root/.backups/collections/ForgedMultiShardCollection/19700101000002Z_incremental", "Table2Shard", 
                opts.Shards(2));

            ExecSQL(server, edgeActor, R"(
                UPSERT INTO `/Root/.backups/collections/ForgedMultiShardCollection/19700101000002Z_incremental/Table2Shard` (key, value, __ydb_incrBackupImpl_deleted) VALUES
                  (2, 2000, NULL)
                , (12, 12000, NULL)
                , (1, NULL, true)
                , (21, NULL, true)
                ;
            )");

            // Table3Shard - 3 shards: more complex changes across all shards
            CreateShardedTable(server, edgeActor, "/Root/.backups/collections/ForgedMultiShardCollection/19700101000002Z_incremental", "Table3Shard", 
                opts.Shards(3));

            ExecSQL(server, edgeActor, R"(
                UPSERT INTO `/Root/.backups/collections/ForgedMultiShardCollection/19700101000002Z_incremental/Table3Shard` (key, value, __ydb_incrBackupImpl_deleted) VALUES
                  (1, 1000, NULL)
                , (11, 11000, NULL)
                , (21, 21000, NULL)
                , (3, NULL, true)
                , (13, NULL, true)
                , (23, NULL, true)
                ;
            )");

            // Table4Shard - 4 shards: changes in all shards
            CreateShardedTable(server, edgeActor, "/Root/.backups/collections/ForgedMultiShardCollection/19700101000002Z_incremental", "Table4Shard", 
                opts.Shards(4));

            ExecSQL(server, edgeActor, R"(
                UPSERT INTO `/Root/.backups/collections/ForgedMultiShardCollection/19700101000002Z_incremental/Table4Shard` (key, value, __ydb_incrBackupImpl_deleted) VALUES
                  (2, 200, NULL)
                , (12, 1200, NULL)
                , (22, 2200, NULL)
                , (32, 3200, NULL)
                , (1, NULL, true)
                , (11, NULL, true)
                , (21, NULL, true)
                , (31, NULL, true)
                ;
            )");
        }

        ExecSQL(server, edgeActor, R"(RESTORE `ForgedMultiShardCollection`;)", false);

        // Wait for restore to complete
        runtime.SimulateSleep(TDuration::Seconds(10));

        if (!WithIncremental) {
            // Verify full backup restore for all tables
            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/Table2Shard`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 1 } items { uint32_value: 100 } }, "
                "{ items { uint32_value: 2 } items { uint32_value: 200 } }, "
                "{ items { uint32_value: 11 } items { uint32_value: 1100 } }, "
                "{ items { uint32_value: 12 } items { uint32_value: 1200 } }, "
                "{ items { uint32_value: 21 } items { uint32_value: 2100 } }, "
                "{ items { uint32_value: 22 } items { uint32_value: 2200 } }");

            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/Table3Shard`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 1 } items { uint32_value: 10 } }, "
                "{ items { uint32_value: 2 } items { uint32_value: 20 } }, "
                "{ items { uint32_value: 3 } items { uint32_value: 30 } }, "
                "{ items { uint32_value: 11 } items { uint32_value: 110 } }, "
                "{ items { uint32_value: 12 } items { uint32_value: 120 } }, "
                "{ items { uint32_value: 13 } items { uint32_value: 130 } }, "
                "{ items { uint32_value: 21 } items { uint32_value: 210 } }, "
                "{ items { uint32_value: 22 } items { uint32_value: 220 } }, "
                "{ items { uint32_value: 23 } items { uint32_value: 230 } }");

            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/Table4Shard`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 1 } items { uint32_value: 1 } }, "
                "{ items { uint32_value: 2 } items { uint32_value: 2 } }, "
                "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
                "{ items { uint32_value: 4 } items { uint32_value: 4 } }, "
                "{ items { uint32_value: 11 } items { uint32_value: 11 } }, "
                "{ items { uint32_value: 12 } items { uint32_value: 12 } }, "
                "{ items { uint32_value: 13 } items { uint32_value: 13 } }, "
                "{ items { uint32_value: 14 } items { uint32_value: 14 } }, "
                "{ items { uint32_value: 21 } items { uint32_value: 21 } }, "
                "{ items { uint32_value: 22 } items { uint32_value: 22 } }, "
                "{ items { uint32_value: 23 } items { uint32_value: 23 } }, "
                "{ items { uint32_value: 24 } items { uint32_value: 24 } }, "
                "{ items { uint32_value: 31 } items { uint32_value: 31 } }, "
                "{ items { uint32_value: 32 } items { uint32_value: 32 } }, "
                "{ items { uint32_value: 33 } items { uint32_value: 33 } }, "
                "{ items { uint32_value: 34 } items { uint32_value: 34 } }");
        } else {
            // Verify incremental backup restore for all tables
            // Table2Shard: key 1,21 deleted, key 2,12 updated
            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/Table2Shard`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 2 } items { uint32_value: 2000 } }, "
                "{ items { uint32_value: 11 } items { uint32_value: 1100 } }, "
                "{ items { uint32_value: 12 } items { uint32_value: 12000 } }, "
                "{ items { uint32_value: 22 } items { uint32_value: 2200 } }");

            // Table3Shard: key 3,13,23 deleted, key 1,11,21 updated
            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/Table3Shard`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 1 } items { uint32_value: 1000 } }, "
                "{ items { uint32_value: 2 } items { uint32_value: 20 } }, "
                "{ items { uint32_value: 11 } items { uint32_value: 11000 } }, "
                "{ items { uint32_value: 12 } items { uint32_value: 120 } }, "
                "{ items { uint32_value: 21 } items { uint32_value: 21000 } }, "
                "{ items { uint32_value: 22 } items { uint32_value: 220 } }");

            // Table4Shard: key 1,11,21,31 deleted, key 2,12,22,32 updated
            UNIT_ASSERT_VALUES_EQUAL(
                KqpSimpleExec(runtime, R"(
                    SELECT key, value FROM `/Root/Table4Shard`
                    ORDER BY key
                    )"),
                "{ items { uint32_value: 2 } items { uint32_value: 200 } }, "
                "{ items { uint32_value: 3 } items { uint32_value: 3 } }, "
                "{ items { uint32_value: 4 } items { uint32_value: 4 } }, "
                "{ items { uint32_value: 12 } items { uint32_value: 1200 } }, "
                "{ items { uint32_value: 13 } items { uint32_value: 13 } }, "
                "{ items { uint32_value: 14 } items { uint32_value: 14 } }, "
                "{ items { uint32_value: 22 } items { uint32_value: 2200 } }, "
                "{ items { uint32_value: 23 } items { uint32_value: 23 } }, "
                "{ items { uint32_value: 24 } items { uint32_value: 24 } }, "
                "{ items { uint32_value: 32 } items { uint32_value: 3200 } }, "
                "{ items { uint32_value: 33 } items { uint32_value: 33 } }, "
                "{ items { uint32_value: 34 } items { uint32_value: 34 } }");
        }
    }

    Y_UNIT_TEST(E2EMultipleBackupRestoreCycles) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
            .SetEnableBackupService(true)
        );

        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        SetupLogging(runtime);
        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        // Step 1: Initial data
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
                (1, 100), (2, 200), (3, 300), (4, 400), (5, 500)
              ;
        )");

        // Step 2: Create backup collection and full backup
        ExecSQL(server, edgeActor, R"(
            CREATE BACKUP COLLECTION `TestCollection`
              ( TABLE `/Root/Table`
              )
            WITH
              ( STORAGE = 'cluster'
              , INCREMENTAL_BACKUP_ENABLED = 'true'
              );
            )", false);

        ExecSQL(server, edgeActor, R"(BACKUP `TestCollection`;)", false);
        SimulateSleep(server, TDuration::Seconds(5));

        // Step 3: Modify data and first incremental backup
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES (2, 2000), (6, 6000);
            DELETE FROM `/Root/Table` WHERE key = 1;
        )");

        ExecSQL(server, edgeActor, R"(BACKUP `TestCollection` INCREMENTAL;)", false);
        SimulateSleep(server, TDuration::Seconds(5));

        // Step 4: Modify data and second incremental backup
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES (3, 3000), (7, 7000);
            DELETE FROM `/Root/Table` WHERE key = 4;
        )");

        ExecSQL(server, edgeActor, R"(BACKUP `TestCollection` INCREMENTAL;)", false);
        SimulateSleep(server, TDuration::Seconds(5));

        // Step 5: Modify data and third incremental backup
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES (5, 5000), (8, 8000);
            DELETE FROM `/Root/Table` WHERE key = 2;
        )");

        ExecSQL(server, edgeActor, R"(BACKUP `TestCollection` INCREMENTAL;)", false);
        SimulateSleep(server, TDuration::Seconds(5));

        // Step 6: Capture expected state before restore
        auto expectedState = KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/Table` ORDER BY key
        )");

        // Step 7: Drop table and restore
        ExecSQL(server, edgeActor, R"(DROP TABLE `/Root/Table`;)", false);
        ExecSQL(server, edgeActor, R"(RESTORE `TestCollection`;)", false);
        runtime.SimulateSleep(TDuration::Seconds(10));

        // Step 8: Verify final result
        auto actualState = KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/Table` ORDER BY key
        )");

        UNIT_ASSERT_VALUES_EQUAL(expectedState, actualState);

        // Verify specific expected values after all operations
        UNIT_ASSERT_VALUES_EQUAL(actualState,
            "{ items { uint32_value: 3 } items { uint32_value: 3000 } }, "
            "{ items { uint32_value: 5 } items { uint32_value: 5000 } }, "
            "{ items { uint32_value: 6 } items { uint32_value: 6000 } }, "
            "{ items { uint32_value: 7 } items { uint32_value: 7000 } }, "
            "{ items { uint32_value: 8 } items { uint32_value: 8000 } }"
        );
    }

} // Y_UNIT_TEST_SUITE(IncrementalBackup)

} // NKikimr
