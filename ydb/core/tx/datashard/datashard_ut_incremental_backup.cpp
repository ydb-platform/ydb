#include "datashard_ut_common_kqp.h"

#include <ydb/core/base/path.h>
#include <ydb/core/change_exchange/change_sender.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/write_meta/write_meta.h>
#include <ydb/core/protos/datashard_backup.pb.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/scheme_board/events.h>
#include <ydb/core/tx/scheme_board/events_internal.h>
#include <ydb/public/lib/value/value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/datastreams/datastreams.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>

#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/size_literals.h>
#include <util/string/escape.h>
#include <util/string/hex.h>
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

    TShardedTableOptions ThreeColumnTable() {
        TShardedTableOptions opts;
        opts.Columns_ = {
            {"key",   "Uint32", true,  false},
            {"value", "Uint32", false, false},
            {"extra", "Uint32", false, false}
        };
        return opts;
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

                TString diff;
                google::protobuf::io::StringOutputStream diffStream(&diff);
                google::protobuf::util::MessageDifferencer::StreamReporter reporter(&diffStream);
                md.ReportDifferencesTo(&reporter);

                bool isEqual = md.Compare(proto.GetCdcDataChange(), expected.at(i).GetCdcDataChange());

                UNIT_ASSERT_C(isEqual,
                    "CDC data change mismatch at record " << i << ":\n" << diff
                    << "\nActual: " << proto.GetCdcDataChange().ShortDebugString()
                    << "\nExpected: " << expected.at(i).GetCdcDataChange().ShortDebugString());
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

    NKikimrChangeExchange::TChangeRecord MakeReset(ui32 key, ui32 value) {
        auto keyCell = TCell::Make<ui32>(key);
        auto valueCell = TCell::Make<ui32>(value);
        NKikimrChangeExchange::TChangeRecord proto;

        auto& dc = *proto.MutableCdcDataChange();
        auto& dcKey = *dc.MutableKey();
        dcKey.AddTags(1);
        dcKey.SetData(TSerializedCellVec::Serialize({keyCell}));

        auto& reset = *dc.MutableReset();
        reset.AddTags(2);
        reset.SetData(TSerializedCellVec::Serialize({valueCell}));

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

    NKikimrChangeExchange::TChangeRecord MakeUpsertPartial(ui32 key, ui32 value, const TVector<ui32>& tags = {2}) {
        auto keyCell = TCell::Make<ui32>(key);
        auto valueCell = TCell::Make<ui32>(value);
        NKikimrChangeExchange::TChangeRecord proto;

        auto& dc = *proto.MutableCdcDataChange();
        auto& dcKey = *dc.MutableKey();
        dcKey.AddTags(1);
        dcKey.SetData(TSerializedCellVec::Serialize({keyCell}));

        auto& upsert = *dc.MutableUpsert();
        for (auto tag : tags) {
            upsert.AddTags(tag);
        }
        upsert.SetData(TSerializedCellVec::Serialize({valueCell}));

        return proto;
    }

    NKikimrChangeExchange::TChangeRecord MakeResetPartial(ui32 key, ui32 value, const TVector<ui32>& tags = {2}) {
        auto keyCell = TCell::Make<ui32>(key);
        auto valueCell = TCell::Make<ui32>(value);
        NKikimrChangeExchange::TChangeRecord proto;

        auto& dc = *proto.MutableCdcDataChange();
        auto& dcKey = *dc.MutableKey();
        dcKey.AddTags(1);
        dcKey.SetData(TSerializedCellVec::Serialize({keyCell}));

        auto& reset = *dc.MutableReset();
        for (auto tag : tags) {
            reset.AddTags(tag);
        }
        reset.SetData(TSerializedCellVec::Serialize({valueCell}));

        return proto;
    }

    // Helper function to create serialized TChangeMetadata
    TString SerializeChangeMetadata(bool isDeleted = false, const TVector<std::pair<ui32, std::pair<bool, bool>>>& columnStates = {}) {
        NKikimrBackup::TChangeMetadata metadata;
        metadata.SetIsDeleted(isDeleted);

        for (const auto& [tag, state] : columnStates) {
            auto* columnState = metadata.AddColumnStates();
            columnState->SetTag(tag);
            columnState->SetIsNull(state.first);
            columnState->SetIsChanged(state.second);
        }

        TString binaryData;
        Y_PROTOBUF_SUPPRESS_NODISCARD metadata.SerializeToString(&binaryData);

        // Convert binary data to hex escape sequences for YDB SQL string literal
        TString result;
        for (unsigned char byte : binaryData) {
            result += TStringBuilder() << "\\x" << Sprintf("%02x", static_cast<int>(byte));
        }
        return result;
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
                    {"__ydb_incrBackupImpl_changeMetadata", "String", false, false}}));

        auto normalMetadata = SerializeChangeMetadata(false); // Not deleted
        auto deletedMetadata = SerializeChangeMetadata(true);  // Deleted

        ExecSQL(server, edgeActor, TStringBuilder() << R"(
            UPSERT INTO `/Root/IncrBackupImpl` (key, value, __ydb_incrBackupImpl_changeMetadata) VALUES
            (1, 10, ')" << normalMetadata << R"('),
            (2, NULL, ')" << deletedMetadata << R"('),
            (3, 30, ')" << normalMetadata << R"('),
            (5, NULL, ')" << deletedMetadata << R"(');
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

        SimulateSleep(server, TDuration::Seconds(5));

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

        SimulateSleep(server, TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL(
            KqpSimpleExec(runtime, R"(
                -- TODO: fix with navigate after proper scheme cache handling
                SELECT key, value FROM `/Root/.backups/collections/MyCollection/19700101000007Z_incremental/Table`
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
                    {"__ydb_incrBackupImpl_changeMetadata", "String", false, false}});

            CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000002Z_incremental", "Table", opts);

            auto normalMetadata = SerializeChangeMetadata(false); // Not deleted
            auto deletedMetadata = SerializeChangeMetadata(true);  // Deleted

            ExecSQL(server, edgeActor, TStringBuilder() << R"(
                UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000002Z_incremental/Table` (key, value, __ydb_incrBackupImpl_changeMetadata) VALUES
                  (2, 200, ')" << normalMetadata << R"(')
                , (1, NULL, ')" << deletedMetadata << R"(')
                ;
            )");

            CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000003Z_incremental", "Table", opts);

            ExecSQL(server, edgeActor, TStringBuilder() << R"(
                UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000003Z_incremental/Table` (key, value, __ydb_incrBackupImpl_changeMetadata) VALUES
                  (2, 2000, ')" << normalMetadata << R"(')
                , (5, NULL, ')" << deletedMetadata << R"(')
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
                    {"__ydb_incrBackupImpl_changeMetadata", "String", false, false}});

            auto normalMetadata = SerializeChangeMetadata(false); // Not deleted
            auto deletedMetadata = SerializeChangeMetadata(true);  // Deleted

            {
                CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000002Z_incremental", "Table", opts);

                ExecSQL(server, edgeActor, TStringBuilder() << R"(
                    UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000002Z_incremental/Table` (key, value, __ydb_incrBackupImpl_changeMetadata) VALUES
                      (2, 200, ')" << normalMetadata << R"(')
                    , (1, NULL, ')" << deletedMetadata << R"(')
                    ;
                )");

                CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000003Z_incremental", "Table", opts);

                ExecSQL(server, edgeActor, TStringBuilder() << R"(
                    UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000003Z_incremental/Table` (key, value, __ydb_incrBackupImpl_changeMetadata) VALUES
                      (2, 2000, ')" << normalMetadata << R"(')
                    , (5, NULL, ')" << deletedMetadata << R"(')
                    ;
                )");
            }

            {
                CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000002Z_incremental/DirA", "TableA", opts);
                CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000003Z_incremental/DirA", "TableA", opts);

                ExecSQL(server, edgeActor, TStringBuilder() << R"(
                    UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000003Z_incremental/DirA/TableA` (key, value, __ydb_incrBackupImpl_changeMetadata) VALUES
                      (21, 20001, ')" << normalMetadata << R"(')
                    , (51, NULL, ')" << deletedMetadata << R"(')
                    ;
                )");
            }

            {
                CreateShardedTable(server, edgeActor, "/Root/.backups/collections/MyCollection/19700101000002Z_incremental/DirA", "TableB", opts);

                ExecSQL(server, edgeActor, TStringBuilder() << R"(
                    UPSERT INTO `/Root/.backups/collections/MyCollection/19700101000002Z_incremental/DirA/TableB` (key, value, __ydb_incrBackupImpl_changeMetadata) VALUES
                      (22, 2002, ')" << normalMetadata << R"(')
                    , (12, NULL, ')" << deletedMetadata << R"(')
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

        SimulateSleep(server, TDuration::Seconds(5));

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
        SimulateSleep(server, TDuration::Seconds(5));

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
                    {"__ydb_incrBackupImpl_changeMetadata", "String", false, false}});

            // Create incremental backup tables with same sharding as full backup
            // Table2Shard - 2 shards: delete some keys, update others
            CreateShardedTable(server, edgeActor, "/Root/.backups/collections/ForgedMultiShardCollection/19700101000002Z_incremental", "Table2Shard",
                opts.Shards(2));

            auto normalMetadata = SerializeChangeMetadata(false); // Not deleted
            auto deletedMetadata = SerializeChangeMetadata(true);  // Deleted

            ExecSQL(server, edgeActor, TStringBuilder() << R"(
                UPSERT INTO `/Root/.backups/collections/ForgedMultiShardCollection/19700101000002Z_incremental/Table2Shard` (key, value, __ydb_incrBackupImpl_changeMetadata) VALUES
                  (2, 2000, ')" << normalMetadata << R"(')
                , (12, 12000, ')" << normalMetadata << R"(')
                , (1, NULL, ')" << deletedMetadata << R"(')
                , (21, NULL, ')" << deletedMetadata << R"(')
                ;
            )");

            // Table3Shard - 3 shards: more complex changes across all shards
            CreateShardedTable(server, edgeActor, "/Root/.backups/collections/ForgedMultiShardCollection/19700101000002Z_incremental", "Table3Shard",
                opts.Shards(3));

            ExecSQL(server, edgeActor, TStringBuilder() << R"(
                UPSERT INTO `/Root/.backups/collections/ForgedMultiShardCollection/19700101000002Z_incremental/Table3Shard` (key, value, __ydb_incrBackupImpl_changeMetadata) VALUES
                  (1, 1000, ')" << normalMetadata << R"(')
                , (11, 11000, ')" << normalMetadata << R"(')
                , (21, 21000, ')" << normalMetadata << R"(')
                , (3, NULL, ')" << deletedMetadata << R"(')
                , (13, NULL, ')" << deletedMetadata << R"(')
                , (23, NULL, ')" << deletedMetadata << R"(')
                ;
            )");

            // Table4Shard - 4 shards: changes in all shards
            CreateShardedTable(server, edgeActor, "/Root/.backups/collections/ForgedMultiShardCollection/19700101000002Z_incremental", "Table4Shard",
                opts.Shards(4));

            ExecSQL(server, edgeActor, TStringBuilder() << R"(
                UPSERT INTO `/Root/.backups/collections/ForgedMultiShardCollection/19700101000002Z_incremental/Table4Shard` (key, value, __ydb_incrBackupImpl_changeMetadata) VALUES
                  (2, 200, ')" << normalMetadata << R"(')
                , (12, 1200, ')" << normalMetadata << R"(')
                , (22, 2200, ')" << normalMetadata << R"(')
                , (32, 3200, ')" << normalMetadata << R"(')
                , (1, NULL, ')" << deletedMetadata << R"(')
                , (11, NULL, ')" << deletedMetadata << R"(')
                , (21, NULL, ')" << deletedMetadata << R"(')
                , (31, NULL, ')" << deletedMetadata << R"(')
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

    Y_UNIT_TEST(ShopDemoIncrementalBackupScenario) {
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

        // === DATABASE STRUCTURE CREATION ===
        // Create orders table
        CreateShardedTable(server, edgeActor, "/Root", "orders",
            TShardedTableOptions().Columns({
                {"order_id", "Uint32", true, false},
                {"customer_name", "Utf8", false, false},
                {"order_date", "Utf8", false, false},
                {"amount", "Uint32", false, false}
            }));

        // Create products table
        CreateShardedTable(server, edgeActor, "/Root", "products",
            TShardedTableOptions().Columns({
                {"product_id", "Uint32", true, false},
                {"product_name", "Utf8", false, false},
                {"price", "Uint32", false, false},
                {"last_updated", "Utf8", false, false}
            }));

        // === BACKUP COLLECTION CREATION ===
        ExecSQL(server, edgeActor, R"(
            CREATE BACKUP COLLECTION `shop_backups`
              ( TABLE `/Root/orders`
              , TABLE `/Root/products`
              )
            WITH
              ( STORAGE = 'cluster'
              , INCREMENTAL_BACKUP_ENABLED = 'true'
              );
            )", false);

        // === CHECKPOINT 1: Initial data + full backup ===
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/orders` (order_id, customer_name, order_date, amount) VALUES
                (1001, 'Иван Петров', '2024-01-01T10:00:00Z', 2500),
                (1002, 'Мария Сидорова', '2024-01-01T10:15:00Z', 1800),
                (1003, 'Алексей Иванов', '2024-01-01T10:30:00Z', 3200);
        )");

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/products` (product_id, product_name, price, last_updated) VALUES
                (501, 'Ноутбук Model A', 65000, '2024-01-01T09:00:00Z'),
                (502, 'Мышь YdbTech', 2500, '2024-01-01T09:00:00Z'),
                (503, 'Монитор 24"', 18000, '2024-01-01T09:00:00Z');
        )");

        // Check initial data
        auto initialOrdersCount = KqpSimpleExec(runtime, R"(SELECT COUNT(*) FROM `/Root/orders`)");
        auto initialProductsCount = KqpSimpleExec(runtime, R"(SELECT COUNT(*) FROM `/Root/products`)");

        UNIT_ASSERT_VALUES_EQUAL(initialOrdersCount, "{ items { uint64_value: 3 } }");
        UNIT_ASSERT_VALUES_EQUAL(initialProductsCount, "{ items { uint64_value: 3 } }");

        // Create full backup
        ExecSQL(server, edgeActor, R"(BACKUP `shop_backups`;)", false);
        SimulateSleep(server, TDuration::Seconds(5));

        // Additional delay to ensure CDC streams are fully initialized for incremental backup
        SimulateSleep(server, TDuration::Seconds(3));

        // === CHECKPOINT 2: Changes + first incremental backup ===

        // New orders
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/orders` (order_id, customer_name, order_date, amount) VALUES
                (1004, 'Елена Козлова', '2024-01-01T11:00:00Z', 4100),
                (1005, 'Дмитрий Волков', '2024-01-01T11:15:00Z', 2800);
        )");

        // Update product prices
        ExecSQL(server, edgeActor, R"(
            UPDATE `/Root/products` SET price = 63000, last_updated = '2024-01-01T11:30:00Z' WHERE product_id = 501;
        )");
        ExecSQL(server, edgeActor, R"(
            UPDATE `/Root/products` SET price = 2300, last_updated = '2024-01-01T11:30:00Z' WHERE product_id = 502;
        )");

        // Cancel order (delete)
        ExecSQL(server, edgeActor, R"(DELETE FROM `/Root/orders` WHERE order_id = 1002;)");

        // Check state after changes
        auto afterChanges1Count = KqpSimpleExec(runtime, R"(SELECT COUNT(*) FROM `/Root/orders`)");
        UNIT_ASSERT_VALUES_EQUAL(afterChanges1Count, "{ items { uint64_value: 4 } }"); // 3 + 2 - 1 = 4

        // Create first incremental backup
        ExecSQL(server, edgeActor, R"(BACKUP `shop_backups` INCREMENTAL;)", false);
        SimulateSleep(server, TDuration::Seconds(5));

        // === CHECKPOINT 3: More changes + second incremental backup ===

        // Add more orders
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/orders` (order_id, customer_name, order_date, amount) VALUES
                (1006, 'Анна Смирнова', '2024-01-01T12:00:00Z', 5200),
                (1007, 'Сергей Попов', '2024-01-01T12:15:00Z', 1950);
        )");

        // Add new product
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/products` (product_id, product_name, price, last_updated) VALUES
                (504, 'Клавиатура Kikimr', 8500, '2024-01-01T12:30:00Z');
        )");

        // Check state after second changes
        auto afterChanges2Count = KqpSimpleExec(runtime, R"(SELECT COUNT(*) FROM `/Root/orders`)");
        UNIT_ASSERT_VALUES_EQUAL(afterChanges2Count, "{ items { uint64_value: 6 } }"); // 4 + 2 = 6

        auto afterChanges2ProductsCount = KqpSimpleExec(runtime, R"(SELECT COUNT(*) FROM `/Root/products`)");
        UNIT_ASSERT_VALUES_EQUAL(afterChanges2ProductsCount, "{ items { uint64_value: 4 } }"); // 3 + 1 = 4

        // Create second incremental backup
        ExecSQL(server, edgeActor, R"(BACKUP `shop_backups` INCREMENTAL;)", false);
        SimulateSleep(server, TDuration::Seconds(5));

        // === FINAL STATE CHECK ===

        // Check final state of main tables
        auto finalOrdersState = KqpSimpleExec(runtime, R"(
            SELECT order_id, customer_name FROM `/Root/orders`
            ORDER BY order_id
        )");

        // Expected orders: 1001, 1003, 1004, 1005, 1006, 1007 (1002 was deleted)
        TString expectedFinalOrders =
            "{ items { uint32_value: 1001 } items { text_value: \"Иван Петров\" } }, "
            "{ items { uint32_value: 1003 } items { text_value: \"Алексей Иванов\" } }, "
            "{ items { uint32_value: 1004 } items { text_value: \"Елена Козлова\" } }, "
            "{ items { uint32_value: 1005 } items { text_value: \"Дмитрий Волков\" } }, "
            "{ items { uint32_value: 1006 } items { text_value: \"Анна Смирнова\" } }, "
            "{ items { uint32_value: 1007 } items { text_value: \"Сергей Попов\" } }";

        UNIT_ASSERT_VALUES_EQUAL(finalOrdersState, expectedFinalOrders);

        auto finalProductsState = KqpSimpleExec(runtime, R"(
            SELECT product_id, product_name FROM `/Root/products`
            ORDER BY product_id
        )");

        TString expectedFinalProducts =
            "{ items { uint32_value: 501 } items { text_value: \"Ноутбук Model A\" } }, "
            "{ items { uint32_value: 502 } items { text_value: \"Мышь YdbTech\" } }, "
            "{ items { uint32_value: 503 } items { text_value: \"Монитор 24\\\"\" } }, "
            "{ items { uint32_value: 504 } items { text_value: \"Клавиатура Kikimr\" } }";

        UNIT_ASSERT_VALUES_EQUAL(finalProductsState, expectedFinalProducts);

        // === RESTORE TEST ===

        // Save expected state before dropping tables
        auto expectedOrdersForRestore = KqpSimpleExec(runtime, R"(
            SELECT order_id, customer_name, amount FROM `/Root/orders` ORDER BY order_id
        )");
        auto expectedProductsForRestore = KqpSimpleExec(runtime, R"(
            SELECT product_id, product_name, price FROM `/Root/products` ORDER BY product_id
        )");

        // Drop tables
        ExecSQL(server, edgeActor, R"(DROP TABLE `/Root/orders`;)", false);
        ExecSQL(server, edgeActor, R"(DROP TABLE `/Root/products`;)", false);

        // Restore from backup
        ExecSQL(server, edgeActor, R"(RESTORE `shop_backups`;)", false);
        SimulateSleep(server, TDuration::Seconds(10)); // More time for restore

        // Check that data was restored correctly
        auto restoredOrders = KqpSimpleExec(runtime, R"(
            SELECT order_id, customer_name, amount FROM `/Root/orders` ORDER BY order_id
        )");
        auto restoredProducts = KqpSimpleExec(runtime, R"(
            SELECT product_id, product_name, price FROM `/Root/products` ORDER BY product_id
        )");

        UNIT_ASSERT_VALUES_EQUAL(restoredOrders, expectedOrdersForRestore);
        UNIT_ASSERT_VALUES_EQUAL(restoredProducts, expectedProductsForRestore);
    }

    Y_UNIT_TEST(DropBackupCollectionSqlPathResolution) {
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

        // Create backup collection using SQL
        ExecSQL(server, edgeActor, R"(
            CREATE BACKUP COLLECTION `TestCollection`
              ( TABLE `/Root/Table`
              )
            WITH
              ( STORAGE = 'cluster'
              , INCREMENTAL_BACKUP_ENABLED = 'false'
              );
            )", false);

        // Add sleep to ensure create operation completes
        runtime.SimulateSleep(TDuration::Seconds(1));

        ExecSQL(server, edgeActor, R"(DROP BACKUP COLLECTION `TestCollection`;)", false);

        // Verify collection was deleted by trying to drop it again (should fail)
        ExecSQL(server, edgeActor, R"(DROP BACKUP COLLECTION `TestCollection`;)",
                false, Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST(DropBackupCollectionSqlWithDatabaseLikeNames) {
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

        // Test with collection name that could be confused with database name
        const TString collectionName = "Root";

        ExecSQL(server, edgeActor, Sprintf(R"(
            CREATE BACKUP COLLECTION `%s`
              ( TABLE `/Root/Table`
              )
            WITH
              ( STORAGE = 'cluster'
              , INCREMENTAL_BACKUP_ENABLED = 'false'
              );
            )", collectionName.c_str()), false);

        // Add sleep to ensure create operation completes
        runtime.SimulateSleep(TDuration::Seconds(1));

        // Drop backup collection - should not be confused with database path
        ExecSQL(server, edgeActor, Sprintf(R"(DROP BACKUP COLLECTION `%s`;)", collectionName.c_str()), false);

        // Verify collection was deleted by trying to drop it again (should fail)
        ExecSQL(server, edgeActor, Sprintf(R"(DROP BACKUP COLLECTION `%s`;)", collectionName.c_str()),
                false, Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST(DropBackupCollectionSqlNonExistent) {
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

        ExecSQL(server, edgeActor, R"(DROP BACKUP COLLECTION `NonExistentCollection`;)",
                false, Ydb::StatusIds::SCHEME_ERROR);
    }

    Y_UNIT_TEST(VerifyIncrementalBackupTableAttributes) {
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

        // Insert some initial data
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
                (1, 10), (2, 20), (3, 30);
        )");

        // Create backup collection with incremental backup enabled
        ExecSQL(server, edgeActor, R"(
            CREATE BACKUP COLLECTION `TestCollection`
              ( TABLE `/Root/Table`
              )
            WITH
              ( STORAGE = 'cluster'
              , INCREMENTAL_BACKUP_ENABLED = 'true'
              );
            )", false);

        // Create full backup first
        ExecSQL(server, edgeActor, R"(BACKUP `TestCollection`;)", false);
        runtime.SimulateSleep(TDuration::Seconds(5));

        // Modify some data
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES (2, 200);
            DELETE FROM `/Root/Table` WHERE key = 1;
        )");

        // Create incremental backup - this should create the incremental backup implementation tables
        ExecSQL(server, edgeActor, R"(BACKUP `TestCollection` INCREMENTAL;)", false);
        runtime.SimulateSleep(TDuration::Seconds(10)); // More time for incremental backup to complete

        // Try to find the incremental backup table by iterating through possible timestamps
        TString foundIncrementalBackupPath;
        bool foundIncrementalBackupTable = false;

        // Common incremental backup paths to try (timestamp-based)
        TVector<TString> possiblePaths = {
            "/Root/.backups/collections/TestCollection/19700101000002Z_incremental/Table",
            "/Root/.backups/collections/TestCollection/19700101000003Z_incremental/Table",
            "/Root/.backups/collections/TestCollection/19700101000004Z_incremental/Table",
            "/Root/.backups/collections/TestCollection/19700101000005Z_incremental/Table",
            "/Root/.backups/collections/TestCollection/19700101000006Z_incremental/Table",
            "/Root/.backups/collections/TestCollection/19700101000007Z_incremental/Table",
            "/Root/.backups/collections/TestCollection/19700101000008Z_incremental/Table",
            "/Root/.backups/collections/TestCollection/19700101000009Z_incremental/Table",
            "/Root/.backups/collections/TestCollection/19700101000010Z_incremental/Table",
        };

        for (const auto& path : possiblePaths) {
            auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
            request->Record.MutableDescribePath()->SetPath(path);
            request->Record.MutableDescribePath()->MutableOptions()->SetShowPrivateTable(true);
            runtime.Send(new IEventHandle(MakeTxProxyID(), edgeActor, request.Release()));
            auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(edgeActor);

            if (reply->Get()->GetRecord().GetStatus() == NKikimrScheme::EStatus::StatusSuccess) {
                foundIncrementalBackupPath = path;
                foundIncrementalBackupTable = true;
                Cerr << "Found incremental backup table at: " << path << Endl;
                break;
            }
        }

        UNIT_ASSERT_C(foundIncrementalBackupTable, TStringBuilder() << "Could not find incremental backup table. Tried paths: " << JoinSeq(", ", possiblePaths));

        // Now check the found incremental backup table attributes
        auto request = MakeHolder<TEvTxUserProxy::TEvNavigate>();
        request->Record.MutableDescribePath()->SetPath(foundIncrementalBackupPath);
        request->Record.MutableDescribePath()->MutableOptions()->SetShowPrivateTable(true);
        runtime.Send(new IEventHandle(MakeTxProxyID(), edgeActor, request.Release()));
        auto reply = runtime.GrabEdgeEventRethrow<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(edgeActor);

        UNIT_ASSERT_EQUAL(reply->Get()->GetRecord().GetStatus(), NKikimrScheme::EStatus::StatusSuccess);

        const auto& pathDescription = reply->Get()->GetRecord().GetPathDescription();
        UNIT_ASSERT(pathDescription.HasTable());

        // Verify that incremental backup table has __incremental_backup attribute
        bool hasIncrementalBackupAttr = false;
        bool hasAsyncReplicaAttr = false;

        for (const auto& attr : pathDescription.GetUserAttributes()) {
            Cerr << "Found attribute: " << attr.GetKey() << " = " << attr.GetValue() << Endl;
            if (attr.GetKey() == "__incremental_backup") {
                hasIncrementalBackupAttr = true;
            }
            if (attr.GetKey() == "__async_replica") {
                hasAsyncReplicaAttr = true;
            }
        }

        // Verify that we have __incremental_backup but NOT __async_replica
        UNIT_ASSERT_C(hasIncrementalBackupAttr, TStringBuilder() << "Incremental backup table at " << foundIncrementalBackupPath << " must have __incremental_backup attribute");
        UNIT_ASSERT_C(!hasAsyncReplicaAttr, TStringBuilder() << "Incremental backup table at " << foundIncrementalBackupPath << " must NOT have __async_replica attribute");
    }

    Y_UNIT_TEST(ResetOperationIncrementalBackup) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
        );

        auto& runtime = *server->GetRuntime();
        const TActorId edgeActor = runtime.AllocateEdgeActor();

        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20);
        )");

        WaitTxNotification(server, edgeActor, AsyncCreateContinuousBackup(server, "/Root", "Table"));

        // Test kReset operation (REPLACE INTO)
        ExecSQL(server, edgeActor, R"(
            REPLACE INTO `/Root/Table` (key, value) VALUES
            (1, 100),
            (3, 300);
        )");

        WaitForContent(server, edgeActor, "/Root/Table/0_continuousBackupImpl", {
            MakeReset(1, 100),
            MakeReset(3, 300),
        });
    }

    Y_UNIT_TEST(ReplaceIntoIncrementalBackup) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2135), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
        );

        auto& runtime = *server->GetRuntime();
        const TActorId edgeActor = runtime.AllocateEdgeActor();

        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", SimpleTable());

        // Insert initial data
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES
            (1, 10),
            (2, 20),
            (3, 30);
        )");

        WaitTxNotification(server, edgeActor, AsyncCreateContinuousBackup(server, "/Root", "Table"));

        // Test multiple REPLACE operations
        ExecSQL(server, edgeActor, R"(
            REPLACE INTO `/Root/Table` (key, value) VALUES
            (1, 100),
            (4, 400);
        )");

        WaitForContent(server, edgeActor, "/Root/Table/0_continuousBackupImpl", {
            MakeReset(1, 100),
            MakeReset(4, 400),
        });
    }

    Y_UNIT_TEST(ResetVsUpsertMissingColumnsTest) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2136), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
        );

        auto& runtime = *server->GetRuntime();
        const TActorId edgeActor = runtime.AllocateEdgeActor();

        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", ThreeColumnTable());

        // Insert initial data with all three columns
        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value, extra) VALUES
            (1, 10, 100),
            (2, 20, 200);
        )");

        WaitTxNotification(server, edgeActor, AsyncCreateContinuousBackup(server, "/Root", "Table"));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES (1, 15);
        )");

        ExecSQL(server, edgeActor, R"(
            REPLACE INTO `/Root/Table` (key, value) VALUES (2, 25);
        )");

        SimulateSleep(server, TDuration::Seconds(1));

        auto records = GetRecords(runtime, edgeActor, "/Root/Table/0_continuousBackupImpl", 0);
        UNIT_ASSERT_VALUES_EQUAL(records.size(), 2);

        // Parse the first record (Upsert)
        NKikimrChangeExchange::TChangeRecord firstRecord;
        UNIT_ASSERT(firstRecord.ParseFromString(records[0].second));
        UNIT_ASSERT_C(firstRecord.GetCdcDataChange().HasUpsert(), "First record should be an upsert");

        // Parse the second record (Reset)
        NKikimrChangeExchange::TChangeRecord secondRecord;
        UNIT_ASSERT(secondRecord.ParseFromString(records[1].second));
        UNIT_ASSERT_C(secondRecord.GetCdcDataChange().HasReset(), "Second record should be a reset");
    }

    Y_UNIT_TEST(ResetVsUpsertColumnStateSerialization) {
        TPortManager portManager;
        TServer::TPtr server = new TServer(TServerSettings(portManager.GetPort(2137), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root")
            .SetEnableChangefeedInitialScan(true)
        );

        auto& runtime = *server->GetRuntime();
        const TActorId edgeActor = runtime.AllocateEdgeActor();

        InitRoot(server, edgeActor);
        CreateShardedTable(server, edgeActor, "/Root", "Table", ThreeColumnTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value, extra) VALUES (1, 10, 100);
        )");

        WaitTxNotification(server, edgeActor, AsyncCreateContinuousBackup(server, "/Root", "Table"));

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/Table` (key, value) VALUES (1, 15);
        )");

        ExecSQL(server, edgeActor, R"(
            REPLACE INTO `/Root/Table` (key, value) VALUES (1, 25);
        )");

        SimulateSleep(server, TDuration::Seconds(2));

        auto records = GetRecords(runtime, edgeActor, "/Root/Table/0_continuousBackupImpl", 0);
        UNIT_ASSERT_C(records.size() >= 2, "Should have at least 2 records");

        for (size_t i = 0; i < records.size(); ++i) {
            NKikimrChangeExchange::TChangeRecord parsedRecord;
            UNIT_ASSERT(parsedRecord.ParseFromString(records[i].second));
            const auto& dataChange = parsedRecord.GetCdcDataChange();

            UNIT_ASSERT_C(dataChange.HasUpsert() || dataChange.HasReset(),
                         "Record should be either upsert or reset operation");
        }
    }

    Y_UNIT_TEST(IncrementalBackupNonExistentTable) {
        TPortManager portManager;
        auto settings = TServerSettings(portManager.GetPort(2134), {}, DefaultPQConfig())
            .SetUseRealThreads(false)
            .SetDomainName("Root");

        settings.SetEnableBackupService(true);

        TServer::TPtr server = new TServer(settings);
        auto& runtime = *server->GetRuntime();
        const auto edgeActor = runtime.AllocateEdgeActor();

        InitRoot(server, edgeActor);

        ExecSQL(server, edgeActor, R"(
            CREATE BACKUP COLLECTION `MixedCollection`
              ( TABLE `/Root/NonExistentTable`
              )
            WITH (
                STORAGE = 'cluster',
                INCREMENTAL_BACKUP_ENABLED = 'true'
            );
        )", false, Ydb::StatusIds::SUCCESS);

        ExecSQL(server, edgeActor, R"(BACKUP `MixedCollection` INCREMENTAL;)", false, Ydb::StatusIds::SCHEME_ERROR);

        ExecSQL(server, edgeActor, "SELECT 1;");
    }

    Y_UNIT_TEST(QueryIncrementalBackupImplTableAfterRestore) {
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

        // Create backup collection structure manually
        ExecSQL(server, edgeActor, R"(
            CREATE BACKUP COLLECTION `TestCollection`
              ( TABLE `/Root/Table`
              )
            WITH
              ( STORAGE = 'cluster'
              , INCREMENTAL_BACKUP_ENABLED = 'true'
              );
            )", false);

        // Manually create full backup table with initial data
        CreateShardedTable(server, edgeActor, "/Root/.backups/collections/TestCollection/19700101000001Z_full", "Table", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/.backups/collections/TestCollection/19700101000001Z_full/Table` (key, value) VALUES
                (1, 10), (2, 20), (3, 30), (4, 40), (5, 50);
        )");

        // Manually create first incremental backup table
        auto incrOpts = TShardedTableOptions()
            .AllowSystemColumnNames(true)
            .Columns({
                {"key", "Uint32", true, false},
                {"value", "Uint32", false, false},
                {"__ydb_incrBackupImpl_changeMetadata", "String", false, false}
            });

        CreateShardedTable(server, edgeActor, "/Root/.backups/collections/TestCollection/19700101000002Z_incremental", "Table", incrOpts);

        auto normalMetadata = SerializeChangeMetadata(false); // Not deleted
        auto deletedMetadata = SerializeChangeMetadata(true);  // Deleted

        // First incremental backup: delete key=1, update key=2 to 200, insert key=6 with 600
        ExecSQL(server, edgeActor, TStringBuilder() << R"(
            UPSERT INTO `/Root/.backups/collections/TestCollection/19700101000002Z_incremental/Table` (key, value, __ydb_incrBackupImpl_changeMetadata) VALUES
              (1, NULL, ')" << deletedMetadata << R"(')
            , (2, 200, ')" << normalMetadata << R"(')
            , (6, 600, ')" << normalMetadata << R"(')
            ;
        )");

        // Manually create second incremental backup table
        CreateShardedTable(server, edgeActor, "/Root/.backups/collections/TestCollection/19700101000003Z_incremental", "Table", incrOpts);

        // Second incremental backup: delete key=4, update key=3 to 300
        ExecSQL(server, edgeActor, TStringBuilder() << R"(
            UPSERT INTO `/Root/.backups/collections/TestCollection/19700101000003Z_incremental/Table` (key, value, __ydb_incrBackupImpl_changeMetadata) VALUES
              (3, 300, ')" << normalMetadata << R"(')
            , (4, NULL, ')" << deletedMetadata << R"(')
            ;
        )");

        // Restore from backup collection
        ExecSQL(server, edgeActor, R"(RESTORE `TestCollection`;)", false);
        runtime.SimulateSleep(TDuration::Seconds(10));

        // Verify restored table has expected data (full backup + both incremental backups applied)
        auto restoredData = KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/Table`
            ORDER BY key
        )");

        UNIT_ASSERT_VALUES_EQUAL(restoredData,
            "{ items { uint32_value: 2 } items { uint32_value: 200 } }, "
            "{ items { uint32_value: 3 } items { uint32_value: 300 } }, "
            "{ items { uint32_value: 5 } items { uint32_value: 50 } }, "
            "{ items { uint32_value: 6 } items { uint32_value: 600 } }");

        // Now test querying incremental backup implementation tables
        // These should still be accessible after restore

        // Query the first incremental backup table
        auto incrBackup1Result = KqpSimpleExec(runtime, R"(
            SELECT key, value, LENGTH(__ydb_incrBackupImpl_changeMetadata) as metadata_len
            FROM `/Root/.backups/collections/TestCollection/19700101000002Z_incremental/Table`
            ORDER BY key
        )");

        // Should contain the changes from first incremental backup
        UNIT_ASSERT_C(incrBackup1Result.find("uint32_value: 1") != TString::npos,
            "First incremental backup should contain deleted key 1");
        UNIT_ASSERT_C(incrBackup1Result.find("uint32_value: 2") != TString::npos,
            "First incremental backup should contain updated key 2");
        UNIT_ASSERT_C(incrBackup1Result.find("uint32_value: 6") != TString::npos,
            "First incremental backup should contain new key 6");

        // Query the second incremental backup table
        auto incrBackup2Result = KqpSimpleExec(runtime, R"(
            SELECT key, value, LENGTH(__ydb_incrBackupImpl_changeMetadata) as metadata_len
            FROM `/Root/.backups/collections/TestCollection/19700101000003Z_incremental/Table`
            ORDER BY key
        )");

        // Should contain the changes from second incremental backup
        UNIT_ASSERT_C(incrBackup2Result.find("uint32_value: 3") != TString::npos,
            "Second incremental backup should contain updated key 3");
        UNIT_ASSERT_C(incrBackup2Result.find("uint32_value: 4") != TString::npos,
            "Second incremental backup should contain deleted key 4");

        // Verify we can also query with WHERE clause on incremental backup tables
        auto filteredResult = KqpSimpleExec(runtime, R"(
            SELECT key FROM `/Root/.backups/collections/TestCollection/19700101000002Z_incremental/Table`
            WHERE key > 1 AND key < 10
            ORDER BY key
        )");

        UNIT_ASSERT_C(filteredResult.find("uint32_value: 2") != TString::npos,
            "Filtered query should return key 2");
        UNIT_ASSERT_C(filteredResult.find("uint32_value: 6") != TString::npos,
            "Filtered query should return key 6");

        // Verify we can join incremental backup table with restored table
        auto joinResult = KqpSimpleExec(runtime, R"(
            SELECT t.key, t.value as current_value
            FROM `/Root/Table` as t
            JOIN `/Root/.backups/collections/TestCollection/19700101000002Z_incremental/Table` as b
            ON t.key = b.key
            ORDER BY t.key
        )");

        // Should return keys that exist in both restored table and incremental backup
        UNIT_ASSERT_C(joinResult.find("uint32_value: 2") != TString::npos,
            "Join should include key 2");
        UNIT_ASSERT_C(joinResult.find("uint32_value: 6") != TString::npos,
            "Join should include key 6");

        // Additional test: Verify full backup table is still queryable
        auto fullBackupResult = KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/.backups/collections/TestCollection/19700101000001Z_full/Table`
            ORDER BY key
        )");

        UNIT_ASSERT_C(fullBackupResult.find("uint32_value: 1") != TString::npos,
            "Full backup should contain key 1");
        UNIT_ASSERT_C(fullBackupResult.find("uint32_value: 10") != TString::npos,
            "Full backup should contain original value 10");
    }

    Y_UNIT_TEST_TWIN(BackupMetadataDirectoriesSkippedDuringRestore, WithIncremental) {
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

        runtime.GetAppData(0).FeatureFlags.SetEnableSystemNamesProtection(true);

        ExecSQL(server, edgeActor, R"(
            CREATE BACKUP COLLECTION `MetaTestCollection`
              ( TABLE `/Root/TestTable`
              )
            WITH
              ( STORAGE = 'cluster'
              , INCREMENTAL_BACKUP_ENABLED = ')" + TString(WithIncremental ? "true" : "false") +  R"('
              );
            )", false);

        CreateShardedTable(server, edgeActor,
            "/Root/.backups/collections/MetaTestCollection/19700101000001Z_full",
            "TestTable", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/.backups/collections/MetaTestCollection/19700101000001Z_full/TestTable`
            (key, value) VALUES (1, 10), (2, 20), (3, 30);
        )");

        CreateShardedTable(server, edgeActor,
            "/Root/.backups/collections/MetaTestCollection/19700101000001Z_full/__ydb_backup_meta",
            "MetaTable1", SimpleTable());

        ExecSQL(server, edgeActor, R"(
            UPSERT INTO `/Root/.backups/collections/MetaTestCollection/19700101000001Z_full/__ydb_backup_meta/MetaTable1`
            (key, value) VALUES (100, 1000);
        )");

        if (WithIncremental) {
            auto normalMetadata = SerializeChangeMetadata(false); // Not deleted
            auto deletedMetadata = SerializeChangeMetadata(true);  // Deleted

            CreateShardedTable(
                server, edgeActor,
                "/Root/.backups/collections/MetaTestCollection/19700101000002Z_incremental",
                "TestTable",
                SimpleTable()
                    .AllowSystemColumnNames(true)
                    .Columns({
                        {"key", "Uint32", true, false},
                        {"value", "Uint32", false, false},
                        {"__ydb_incrBackupImpl_changeMetadata", "String", false, false}}));

            ExecSQL(server, edgeActor, TStringBuilder() << R"(
                UPSERT INTO `/Root/.backups/collections/MetaTestCollection/19700101000002Z_incremental/TestTable`
                (key, value, __ydb_incrBackupImpl_changeMetadata) VALUES
                (1, NULL, ')" << deletedMetadata << R"('),
                (4, 40, ')" << normalMetadata << R"(');
            )");

            CreateShardedTable(server, edgeActor,
                "/Root/.backups/collections/MetaTestCollection/19700101000002Z_incremental/__ydb_backup_meta",
                "MetaTable2", SimpleTable());

            ExecSQL(server, edgeActor, R"(
                UPSERT INTO `/Root/.backups/collections/MetaTestCollection/19700101000002Z_incremental/__ydb_backup_meta/MetaTable2`
                (key, value) VALUES (200, 2000);
            )");
        }

        auto checkMeta1 = KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/.backups/collections/MetaTestCollection/19700101000001Z_full/__ydb_backup_meta/MetaTable1`
        )");
        UNIT_ASSERT_C(checkMeta1.find("uint32_value: 100") != TString::npos,
            "MetaTable1 should exist in full backup before restore");

        if (WithIncremental) {
            auto checkMeta2 = KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/.backups/collections/MetaTestCollection/19700101000002Z_incremental/__ydb_backup_meta/MetaTable2`
            )");
            UNIT_ASSERT_C(checkMeta2.find("uint32_value: 200") != TString::npos,
                "MetaTable2 should exist in incremental backup before restore");
        }

        ExecSQL(server, edgeActor, R"(RESTORE `MetaTestCollection`;)", false);
        runtime.SimulateSleep(TDuration::Seconds(10));

        auto restoredTable = KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/TestTable` ORDER BY key
        )");

        if (!WithIncremental) {
            UNIT_ASSERT_C(restoredTable.find("uint32_value: 1") != TString::npos,
                "Restored table should contain key 1");
            UNIT_ASSERT_C(restoredTable.find("uint32_value: 2") != TString::npos,
                "Restored table should contain key 2");
            UNIT_ASSERT_C(restoredTable.find("uint32_value: 3") != TString::npos,
                "Restored table should contain key 3");
        } else {
            UNIT_ASSERT_C(restoredTable.find("uint32_value: 2") != TString::npos,
                "Restored table should contain key 2");
            UNIT_ASSERT_C(restoredTable.find("uint32_value: 3") != TString::npos,
                "Restored table should contain key 3");
            UNIT_ASSERT_C(restoredTable.find("uint32_value: 4") != TString::npos,
                "Restored table should contain key 4");
            // Key 1 should NOT be present (deleted by incremental)
            UNIT_ASSERT_C(restoredTable.find("uint32_value: 1") == TString::npos,
                "Restored table should NOT contain deleted key 1");
        }

        auto tryQueryMetaDir = KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/__ydb_backup_meta/MetaTable1`
        )", Ydb::StatusIds::SCHEME_ERROR); // Should fail - path doesn't exist

        UNIT_ASSERT_C(tryQueryMetaDir.empty() || tryQueryMetaDir.find("SCHEME_ERROR") != TString::npos,
            "__ydb_backup_meta should NOT be restored to /Root");

        auto tryQueryMetaTable1 = KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/MetaTable1`
        )", Ydb::StatusIds::SCHEME_ERROR);

        UNIT_ASSERT_C(tryQueryMetaTable1.empty() || tryQueryMetaTable1.find("SCHEME_ERROR") != TString::npos,
            "MetaTable1 should NOT be restored to /Root");

        if (WithIncremental) {
            auto tryQueryMetaTable2 = KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/MetaTable2`
            )", Ydb::StatusIds::SCHEME_ERROR);

            UNIT_ASSERT_C(tryQueryMetaTable2.empty() || tryQueryMetaTable2.find("SCHEME_ERROR") != TString::npos,
                "MetaTable2 should NOT be restored to /Root");
        }

        auto verifyMeta1StillInBackup = KqpSimpleExec(runtime, R"(
            SELECT key, value FROM `/Root/.backups/collections/MetaTestCollection/19700101000001Z_full/__ydb_backup_meta/MetaTable1`
        )");
        UNIT_ASSERT_C(verifyMeta1StillInBackup.find("uint32_value: 100") != TString::npos,
            "MetaTable1 should still exist in backup location after restore");

        if (WithIncremental) {
            auto verifyMeta2StillInBackup = KqpSimpleExec(runtime, R"(
                SELECT key, value FROM `/Root/.backups/collections/MetaTestCollection/19700101000002Z_incremental/__ydb_backup_meta/MetaTable2`
            )");
            UNIT_ASSERT_C(verifyMeta2StillInBackup.find("uint32_value: 200") != TString::npos,
                "MetaTable2 should still exist in backup location after restore");
        }
    }

} // Y_UNIT_TEST_SUITE(IncrementalBackup)

} // NKikimr
