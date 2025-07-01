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

        WaitForContent(server, edgeActor, "/Root/Table/continuousBackupImpl", {
            MakeUpsert(1, 100),
            MakeUpsert(5, 200),
            MakeUpsert(6, 300),
            MakeErase(2),
            MakeErase(6),
        });

        WaitTxNotification(server, edgeActor, AsyncAlterTakeIncrementalBackup(server, "/Root", "Table", "IncrBackupImpl"));

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

    Y_UNIT_TEST(MultiRestore) {
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
            "IncrBackupImpl1",
            SimpleTable()
                .AllowSystemColumnNames(true)
                .Columns({
                    {"key", "Uint32", true, false},
                    {"value", "Uint32", false, false},
                    {"__ydb_incrBackupImpl_deleted", "Bool", false, false}}));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/IncrBackupImpl1` (key, value, __ydb_incrBackupImpl_deleted) VALUES
            (1, 10, NULL),
            (2, NULL, true),
            (3, 30, NULL),
            (5, NULL, true);
        )");

        CreateShardedTable(
            server,
            edgeActor,
            "/Root",
            "IncrBackupImpl2",
            SimpleTable()
                .AllowSystemColumnNames(true)
                .Columns({
                    {"key", "Uint32", true, false},
                    {"value", "Uint32", false, false},
                    {"__ydb_incrBackupImpl_deleted", "Bool", false, false}}));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/IncrBackupImpl2` (key, value, __ydb_incrBackupImpl_deleted) VALUES
            (1, NULL, true),
            (2, 100, NULL),
            (1000, 1000, NULL);
        )");

        CreateShardedTable(
            server,
            edgeActor,
            "/Root",
            "IncrBackupImpl3",
            SimpleTable()
                .AllowSystemColumnNames(true)
                .Columns({
                    {"key", "Uint32", true, false},
                    {"value", "Uint32", false, false},
                    {"__ydb_incrBackupImpl_deleted", "Bool", false, false}}));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/IncrBackupImpl3` (key, value, __ydb_incrBackupImpl_deleted) VALUES
            (5, 50000, NULL),
            (20000, 20000, NULL);
        )");


        WaitTxNotification(server, edgeActor, AsyncAlterRestoreMultipleIncrementalBackups(
                               server,
                               "/Root",
                               {"/Root/IncrBackupImpl1", "/Root/IncrBackupImpl2", "/Root/IncrBackupImpl3"},
                               "/Root/Table"));

        SimulateSleep(server, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/Table`
                )"),
            "{ items { uint32_value: 2 } items { uint32_value: 100 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 30 } }, "
            "{ items { uint32_value: 5 } items { uint32_value: 50000 } }, "
            "{ items { uint32_value: 1000 } items { uint32_value: 1000 } }, "
            "{ items { uint32_value: 20000 } items { uint32_value: 20000 } }");
    }

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

        WaitTxNotification(server, edgeActor, AsyncCreateContinuousBackup(server, "/Root", "Table"));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 100),
            (5, 200),
            (6, 300);
        )");

        ExecSQL(server, edgeActor, "DELETE FROM `/Root/Table` ON (key) VALUES (2), (6);");

        SimulateSleep(server, TDuration::Seconds(1));

        WaitTxNotification(server, edgeActor, AsyncAlterTakeIncrementalBackup(server, "/Root", "Table", "IncrBackupImpl"));

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
                "{ items { uint32_value: 41 } items { uint32_value: 401 } }, "
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

        SimulateSleep(server, TDuration::Seconds(1));

        auto actual = KqpSimpleExec(runtime, R"(SELECT key, value FROM `/Root/Table` ORDER BY key)");

        UNIT_ASSERT_VALUES_EQUAL(expected, actual);
    }

} // Y_UNIT_TEST_SUITE(IncrementalBackup)

} // NKikimr
