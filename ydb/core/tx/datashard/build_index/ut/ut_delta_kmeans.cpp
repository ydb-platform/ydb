#include "ut_helpers.h"

#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/index_builder.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

#include <yql/essentials/public/issue/yql_issue_message.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
using namespace Tests;
using Ydb::Table::VectorIndexSettings;
using namespace NTableIndex::NKMeans;

static std::atomic<ui64> sId = 1;
static const TString kDatabaseName = "/Root";
static const TString kMainTable = "/Root/table-main";
static const TString kLevelTable = "/Root/table-level";
static const TString kPostingTable = "/Root/table-posting";
static const TString kPrefixTable = "/Root/table-prefix";

namespace {

TString ClusterLiteral(ui64 id) {
    return TStringBuilder() << "Unwrap(CAST(\"" << id << "\" AS Uint64))";
}

TString CentroidA() {
    return "\\x10\\x00\\x02";
}

TString CentroidB() {
    return "\\x00\\x10\\x02";
}

TString LevelUpsertSql(const TVector<std::tuple<ui64, ui64, TString>>& rows) {
    TStringBuilder sql;
    sql << "UPSERT INTO `" << kLevelTable << "` (`" << ParentColumn << "`, `" << IdColumn << "`, `"
        << CentroidColumn << "`) VALUES ";
    for (size_t i = 0; i < rows.size(); ++i) {
        if (i) {
            sql << ", ";
        }
        sql << "(" << ClusterLiteral(std::get<0>(rows[i])) << ", "
            << ClusterLiteral(std::get<1>(rows[i])) << ", \""
            << std::get<2>(rows[i]) << "\")";
    }
    sql << ";";
    return sql;
}

VectorIndexSettings MakeVectorSettings() {
    VectorIndexSettings settings;
    settings.set_vector_dimension(2);
    settings.set_vector_type(VectorIndexSettings::VECTOR_TYPE_UINT8);
    settings.set_metric(VectorIndexSettings::DISTANCE_COSINE);
    return settings;
}

struct TDeltaTestEnv {
    TPortManager PortManager;
    TServer::TPtr Server;
    TTestActorRuntime& Runtime;
    TActorId Sender;

    TDeltaTestEnv()
        : Server(new TServer(MakeServerSettings()))
        , Runtime(*Server->GetRuntime())
    {
        Runtime.SetLogPriority(NKikimrServices::BUILD_INDEX, NLog::PRI_DEBUG);
        Runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);
        Sender = Runtime.AllocateEdgeActor();
        InitRoot(Server, Sender);
    }

    TServerSettings MakeServerSettings() {
        TServerSettings settings(PortManager.GetPort(2134));
        settings.SetDomainName("Root");
        return settings;
    }

    void CreateTables(const TString& userColumn = {}) {
        TShardedTableOptions options;
        options.EnableOutOfOrder(true);
        options.Shards(1);

        if (userColumn.empty()) {
            CreateMainTable(Server, Sender, options);
            CreatePostingTable(Server, Sender, options);
        } else {
            TShardedTableOptions mainOptions = options;
            mainOptions.Columns({
                {userColumn, "String", true, true},
                {"key", "Uint32", true, true},
                {"embedding", "String", false, false},
                {"data", "String", false, false},
            });
            CreateShardedTable(Server, Sender, "/Root", "table-main", mainOptions);

            TShardedTableOptions postingOptions = options;
            postingOptions.AllowSystemColumnNames(true);
            postingOptions.Columns({
                {ParentColumn, ClusterIdTypeName, true, true},
                {userColumn, "String", true, true},
                {"key", "Uint32", true, true},
                {"data", "String", false, false},
            });
            CreateShardedTable(Server, Sender, "/Root", "table-posting", postingOptions);

            CreatePrefixTable(Server, Sender, options);
            CreatePrefixSequence();
        }

        CreateLevelTable(Server, Sender, options);
    }

    void CreatePrefixSequence() {
        auto request = MakeHolder<TEvTxUserProxy::TEvProposeTransaction>();
        auto* tx = request->Record.MutableTransaction()->MutableModifyScheme();
        tx->SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateSequence);
        tx->SetWorkingDir(kPrefixTable);
        auto* sequence = tx->MutableSequence();
        sequence->SetName(IdColumnSequence);
        sequence->SetMinValue(-0x7FFFFFFFFFFFFFFFLL);
        sequence->SetMaxValue(-1);
        sequence->SetStartValue(static_cast<i64>(SetPostingParentFlag(1)));
        sequence->SetRestart(true);

        Runtime.Send(new IEventHandle(MakeTxProxyID(), Sender, request.Release()));
        auto response = Runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvProposeTransactionStatus>(Sender);
        const auto status = static_cast<TEvTxUserProxy::TResultStatus::EStatus>(response->Get()->Record.GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(status, TEvTxUserProxy::TResultStatus::ExecInProgress);
        WaitTxNotification(Server, Sender, response->Get()->Record.GetTxId());
    }

    void Upsert(const TString& sql) {
        ExecSQL(Server, Sender, sql);
    }

    NKikimrIndexBuilder::EBuildStatus DoDeltaKMeans(const TRowVersion& from, const TRowVersion& to,
        ui32 prefixColumns = 0, const TString& prefixTable = {})
    {
        auto id = sId.fetch_add(1, std::memory_order_relaxed);
        auto datashards = GetTableShards(Server, Sender, kMainTable);
        TTableId tableId = ResolveTableId(Server, Sender, kMainTable);
        UNIT_ASSERT_VALUES_EQUAL(datashards.size(), 1);

        auto ev = std::make_unique<TEvDataShard::TEvDeltaKMeansRequest>();
        auto& rec = ev->Record;
        rec.SetId(1);

        rec.SetSeqNoGeneration(id);
        rec.SetSeqNoRound(1);

        rec.SetTabletId(datashards[0]);
        tableId.PathId.ToProto(rec.MutablePathId());

        rec.SetFromSnapshotTxId(from.TxId);
        rec.SetFromSnapshotStep(from.Step);
        rec.SetToSnapshotTxId(to.TxId);
        rec.SetToSnapshotStep(to.Step);

        *rec.MutableSettings() = MakeVectorSettings();

        rec.SetLevelName(kLevelTable);
        rec.SetOutputName(kPostingTable);
        rec.SetPrefixColumns(prefixColumns);
        if (prefixColumns > 0) {
            rec.SetPrefixName(prefixTable);
        }

        rec.SetEmbeddingColumn("embedding");
        rec.AddDataColumns("data");

        rec.SetDatabaseName(kDatabaseName);

        Runtime.SendToPipe(datashards[0], Sender, ev.release(), 0, GetPipeConfigWithRetries());

        auto reply = Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvDeltaKMeansResponse>(Sender);
        while (reply->Get()->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS) {
            reply = Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvDeltaKMeansResponse>(Sender);
        }

        auto& record = reply->Get()->Record;
        NYql::TIssues issues;
        NYql::IssuesFromMessage(record.GetIssues(), issues);
        Cerr << "Delta KMeans status: " << record.GetStatus() << ", issues: " << issues.ToOneLineString() << Endl;

        return record.GetStatus();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TTxDataShardDeltaKMeansScan) {

    Y_UNIT_TEST(ApplyChanges) {
        TDeltaTestEnv env;
        env.CreateTables({});

        env.Upsert(LevelUpsertSql({
            {0, 1, CentroidA()},
            {0, 2, CentroidB()},
            {1, SetPostingParentFlag(11), CentroidA()},
            {1, SetPostingParentFlag(12), CentroidB()},
            {2, SetPostingParentFlag(21), CentroidA()},
            {2, SetPostingParentFlag(22), CentroidB()},
        }));

        env.Upsert(TStringBuilder()
            << "UPSERT INTO `" << kMainTable << "` (key, embedding, data) VALUES "
            << "(1, \"\\x18\\x01\\x02\", \"one\"), "
            << "(2, \"\\x01\\x18\\x02\", \"two\");");
        env.Upsert(TStringBuilder()
            << "UPSERT INTO `" << kPostingTable << "` (`" << ParentColumn << "`, key, data) VALUES "
            << "(" << ClusterLiteral(SetPostingParentFlag(11)) << ", 1, \"one\"), "
            << "(" << ClusterLiteral(SetPostingParentFlag(22)) << ", 2, \"two\");");

        auto from = CreateVolatileSnapshot(env.Server, {kMainTable});

        // key 1 moves to another cluster, key 2 is deleted, key 3 is inserted
        env.Upsert(TStringBuilder()
            << "UPSERT INTO `" << kMainTable << "` (key, embedding, data) VALUES "
            << "(1, \"\\x01\\x18\\x02\", \"one-updated\"), "
            << "(3, \"\\x18\\x01\\x02\", \"three\");");
        env.Upsert(TStringBuilder() << "DELETE FROM `" << kMainTable << "` WHERE key = 2;");

        auto to = CreateVolatileSnapshot(env.Server, {kMainTable});

        UNIT_ASSERT_VALUES_EQUAL(env.DoDeltaKMeans(from, to), NKikimrIndexBuilder::EBuildStatus::DONE);

        auto posting = ReadShardedTable(env.Server, kPostingTable);
        Cerr << "Posting:" << Endl << posting << Endl;
        UNIT_ASSERT_VALUES_EQUAL(posting,
            "__ydb_parent = 9223372036854775819, key = 3, data = three\n"
            "__ydb_parent = 9223372036854775830, key = 1, data = one-updated\n"
        );
    }

    Y_UNIT_TEST(NewPrefix) {
        TDeltaTestEnv env;
        env.CreateTables("user");

        env.Upsert(LevelUpsertSql({
            {10, SetPostingParentFlag(31), CentroidA()},
            {10, SetPostingParentFlag(32), CentroidB()},
        }));
        env.Upsert(TStringBuilder()
            << "UPSERT INTO `" << kPrefixTable << "` (`user`, `" << IdColumn << "`) VALUES "
            << "(\"a\", " << ClusterLiteral(10) << ");");

        env.Upsert(TStringBuilder()
            << "UPSERT INTO `" << kMainTable << "` (user, key, embedding, data) VALUES "
            << "(\"a\", 1, \"\\x18\\x01\\x02\", \"one\");");
        env.Upsert(TStringBuilder()
            << "UPSERT INTO `" << kPostingTable << "` (`" << ParentColumn << "`, user, key, data) VALUES "
            << "(" << ClusterLiteral(SetPostingParentFlag(31)) << ", \"a\", 1, \"one\");");

        auto from = CreateVolatileSnapshot(env.Server, {kMainTable});

        // user-2 is a prefix that the prefix table does not know yet
        env.Upsert(TStringBuilder()
            << "UPSERT INTO `" << kMainTable << "` (user, key, embedding, data) VALUES "
            << "(\"b\", 2, \"\\x18\\x01\\x02\", \"two\");");

        auto to = CreateVolatileSnapshot(env.Server, {kMainTable});

        UNIT_ASSERT_VALUES_EQUAL(env.DoDeltaKMeans(from, to, 1, kPrefixTable),
            NKikimrIndexBuilder::EBuildStatus::DONE);

        auto prefix = ReadShardedTable(env.Server, kPrefixTable);
        Cerr << "Prefix:" << Endl << prefix << Endl;
        UNIT_ASSERT_VALUES_EQUAL(prefix,
            "user = a, __ydb_id = 10\n"
            "user = b, __ydb_id = 9223372036854775809\n"
        );

        auto posting = ReadShardedTable(env.Server, kPostingTable);
        Cerr << "Posting:" << Endl << posting << Endl;
        UNIT_ASSERT_VALUES_EQUAL(posting,
            "__ydb_parent = 9223372036854775809, user = b, key = 2, data = two\n"
            "__ydb_parent = 9223372036854775839, user = a, key = 1, data = one\n"
        );
    }

    Y_UNIT_TEST(BadRequest) {
        TDeltaTestEnv env;
        env.CreateTables({});

        auto from = CreateVolatileSnapshot(env.Server, {kMainTable});
        auto to = CreateVolatileSnapshot(env.Server, {kMainTable});

        UNIT_ASSERT_VALUES_EQUAL(env.DoDeltaKMeans(to, from), NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);
    }
}

} // namespace NKikimr
