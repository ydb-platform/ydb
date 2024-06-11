#include "table_creator.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

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
}

} // namespace NKikimr
