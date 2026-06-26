#include "table_creator.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

namespace NKikimr {

namespace {

class TTestTablesCreator : public NTableCreator::TMultiTableCreator {
    using TBase = NTableCreator::TMultiTableCreator;

public:
    explicit TTestTablesCreator(NThreading::TPromise<void> promise)
        : TBase({
            GetFirstCreator(),
            GetSecondCreator()
        })
        , Promise(promise)
    {}

private:
    static IActor* GetFirstCreator() {
        return CreateTableCreator(
            { "path", "to", "table" },
            {
                Col("key", "Uint32"),
                Col("value", "String"),
            },
            { "key" },
            NKikimrServices::STATISTICS
        );
    }

    static IActor* GetSecondCreator() {
        return CreateTableCreator(
            { "path", "to", "other", "table" },
            {
                Col("key", NScheme::NTypeIds::Uint32),
                Col("expire_at", NScheme::NTypeIds::Timestamp),
            },
            { "key" },
            NKikimrServices::STATISTICS,
            TtlCol("expire_at", TDuration::Zero(), TDuration::Minutes(60))
        );
    }

    void OnTablesCreated(bool success, NYql::TIssues issues) override {
        UNIT_ASSERT_C(success, issues.ToString());
        Promise.SetValue();
    }

private:
    NThreading::TPromise<void> Promise;
};

class TIndexedTableCreator : public NTableCreator::TMultiTableCreator {
    using TBase = NTableCreator::TMultiTableCreator;

public:
    struct TResult {
        bool Success = false;
        NYql::TIssues Issues;
    };

    explicit TIndexedTableCreator(NThreading::TPromise<TResult> promise)
        : TBase({ GetCreator() })
        , Promise(std::move(promise))
    {}

private:
    static IActor* GetCreator() {
        NKikimrSchemeOp::TSequenceDescription sequence;
        sequence.SetName("id_seq");

        NKikimrSchemeOp::TIndexDescription index;
        index.SetName("ext_id_uniq");
        index.AddKeyColumnNames("ext_id");
        index.SetType(NKikimrSchemeOp::EIndexTypeGlobalUnique);
        index.SetState(NKikimrSchemeOp::EIndexStateReady);

        auto idColumn = Col("id", NScheme::NTypeIds::Uint64);
        idColumn.SetNotNull(true);
        idColumn.SetDefaultFromSequence("id_seq");

        auto extIdColumn = Col("ext_id", NScheme::NTypeIds::Text);
        extIdColumn.SetNotNull(true);

        return CreateTableCreator(
            { "path", "to", "indexed", "table" },
            { idColumn, extIdColumn },
            { "id" },
            NKikimrServices::STATISTICS,
            Nothing(),
            {},
            false,
            Nothing(),
            Nothing(),
            { index },
            { sequence }
        );
    }

    void OnTablesCreated(bool success, NYql::TIssues issues) override {
        Promise.SetValue({success, std::move(issues)});
    }

private:
    NThreading::TPromise<TResult> Promise;
};

} // namespace

Y_UNIT_TEST_SUITE(TableCreator) {
    Y_UNIT_TEST(CreateTables) {
        TPortManager tp;
        ui16 mbusPort = tp.GetPort();
        ui16 grpcPort = tp.GetPort();
        auto settings = Tests::TServerSettings(mbusPort);
        settings.SetNodeCount(1);

        Tests::TServer server(settings);
        Tests::TClient client(settings);

        server.EnableGRpc(grpcPort);
        client.InitRootScheme();
        auto runtime = server.GetRuntime();

        auto promise = NThreading::NewPromise();
        TActorId edgeActor = server.GetRuntime()->AllocateEdgeActor(0);
        runtime->Register(new TTestTablesCreator(promise),
            0, 0, TMailboxType::Simple, 0, edgeActor);
        promise.GetFuture().GetValueSync();

        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << grpcPort).SetDatabase(Tests::TestDomainName);
        NYdb::TDriver driver(cfg);
        NYdb::NTable::TTableClient tableClient(driver);
        auto createSessionResult = tableClient.CreateSession().ExtractValueSync();
        UNIT_ASSERT_C(createSessionResult.IsSuccess(), createSessionResult.GetIssues().ToString());
        NYdb::NTable::TSession session(createSessionResult.GetSession());

        {  // First table
            auto path = TStringBuilder() << "/" << Tests::TestDomainName << "/path/to/table";
            auto result = session.DescribeTable(path).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const auto& createdColumns = result.GetTableDescription().GetColumns();
            UNIT_ASSERT_VALUES_EQUAL_C(createdColumns.size(), 2, "expected 2 columns");
            UNIT_ASSERT_VALUES_EQUAL_C(createdColumns[0].Name, "key", "expected key column");
            UNIT_ASSERT_VALUES_EQUAL_C(createdColumns[1].Name, "value", "expected value column");
            UNIT_ASSERT_VALUES_EQUAL_C(createdColumns[1].Type.ToString(), "String?", "expected type string");
        }

        {  // Second table
            auto path = TStringBuilder() << "/" << Tests::TestDomainName << "/path/to/other/table";
            auto result = session.DescribeTable(path).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const auto& createdColumns = result.GetTableDescription().GetColumns();
            UNIT_ASSERT_VALUES_EQUAL_C(createdColumns.size(), 2, "expected 2 columns");
            UNIT_ASSERT_VALUES_EQUAL_C(createdColumns[0].Name, "key", "expected key column");
            UNIT_ASSERT_VALUES_EQUAL_C(createdColumns[1].Name, "expire_at", "expected expire_at column");
            UNIT_ASSERT_VALUES_EQUAL_C(createdColumns[1].Type.ToString(), "Timestamp?", "expected type timestamp");
        }
    }

    Y_UNIT_TEST(CreateIndexedTableWithSequenceAndUniqueIndex) {
        TPortManager tp;
        ui16 mbusPort = tp.GetPort();
        ui16 grpcPort = tp.GetPort();
        auto settings = Tests::TServerSettings(mbusPort);
        settings.SetNodeCount(1);

        Tests::TServer server(settings);
        Tests::TClient client(settings);

        server.EnableGRpc(grpcPort);
        client.InitRootScheme();
        auto runtime = server.GetRuntime();

        auto promise = NThreading::NewPromise<TIndexedTableCreator::TResult>();
        TActorId edgeActor = runtime->AllocateEdgeActor(0);
        runtime->Register(new TIndexedTableCreator(promise), 0, 0, TMailboxType::Simple, 0, edgeActor);
        const auto result = promise.GetFuture().GetValueSync();
        UNIT_ASSERT_C(result.Success, result.Issues.ToString());

        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << grpcPort).SetDatabase(Tests::TestDomainName);
        NYdb::TDriver driver(cfg);
        NYdb::NTable::TTableClient tableClient(driver);
        auto createSessionResult = tableClient.CreateSession().ExtractValueSync();
        UNIT_ASSERT_C(createSessionResult.IsSuccess(), createSessionResult.GetIssues().ToString());
        NYdb::NTable::TSession session(createSessionResult.GetSession());

        const auto path = TStringBuilder() << "/" << Tests::TestDomainName << "/path/to/indexed/table";
        auto describeResult = session.DescribeTable(path).ExtractValueSync();
        UNIT_ASSERT_C(describeResult.IsSuccess(), describeResult.GetIssues().ToString());

        const auto& indexes = describeResult.GetTableDescription().GetIndexDescriptions();
        UNIT_ASSERT_VALUES_EQUAL(indexes.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(indexes[0].GetIndexName(), "ext_id_uniq");
        UNIT_ASSERT_VALUES_EQUAL(indexes[0].GetIndexType(), NYdb::NTable::EIndexType::GlobalUnique);
        UNIT_ASSERT_VALUES_EQUAL(indexes[0].GetIndexColumns().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(indexes[0].GetIndexColumns()[0], "ext_id");

        NYdb::TParamsBuilder insertParams;
        insertParams.AddParam("$ext").Utf8("ext-1").Build();
        const auto insertResult = session.ExecuteDataQuery(
            TStringBuilder() << "DECLARE $ext AS Text; INSERT INTO `" << path << "` (ext_id) VALUES ($ext);",
            NYdb::NTable::TTxControl::BeginTx().CommitTx(),
            insertParams.Build()).ExtractValueSync();
        UNIT_ASSERT_C(insertResult.IsSuccess(), insertResult.GetIssues().ToString());

        NYdb::TParamsBuilder selectParams;
        selectParams.AddParam("$ext").Utf8("ext-1").Build();
        const auto selectResult = session.ExecuteDataQuery(
            TStringBuilder() << "DECLARE $ext AS Text; SELECT id FROM `" << path << "` WHERE ext_id = $ext;",
            NYdb::NTable::TTxControl::BeginTx().CommitTx(),
            selectParams.Build()).ExtractValueSync();
        UNIT_ASSERT_C(selectResult.IsSuccess(), selectResult.GetIssues().ToString());
        UNIT_ASSERT(!selectResult.GetResultSets().empty());

        NYdb::TResultSetParser parser(selectResult.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_GT(parser.ColumnParser("id").GetUint64(), 0u);
        UNIT_ASSERT(!parser.TryNextRow());
    }

    Y_UNIT_TEST(RejectIndexedTableUpgradeOnExistingTable) {
        TPortManager tp;
        ui16 mbusPort = tp.GetPort();
        ui16 grpcPort = tp.GetPort();
        auto settings = Tests::TServerSettings(mbusPort);
        settings.SetNodeCount(1);

        Tests::TServer server(settings);
        Tests::TClient client(settings);

        server.EnableGRpc(grpcPort);
        client.InitRootScheme();
        auto runtime = server.GetRuntime();
        TActorId edgeActor = runtime->AllocateEdgeActor(0);

        {
            auto promise = NThreading::NewPromise<TIndexedTableCreator::TResult>();
            runtime->Register(new TIndexedTableCreator(promise), 0, 0, TMailboxType::Simple, 0, edgeActor);
            const auto result = promise.GetFuture().GetValueSync();
            UNIT_ASSERT_C(result.Success, result.Issues.ToString());
        }

        {
            auto promise = NThreading::NewPromise<TIndexedTableCreator::TResult>();
            runtime->Register(new TIndexedTableCreator(promise), 0, 0, TMailboxType::Simple, 0, edgeActor);
            const auto result = promise.GetFuture().GetValueSync();
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToString(),
                "Table already exists; index and sequence upgrade is not supported");
        }
    }
}

} // namespace NKikimr
