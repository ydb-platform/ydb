#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/services/sql/analyze/events.h>
#include <ydb/services/sql/parse/parser.h>

using namespace NKikimr;
using namespace NKikimr::NSql;
using namespace NKikimr::Tests;

namespace {

TEvAnalyzeResult::TPtr Analyze(TServer& server, const TString& database, const TString& sql) {
    const TParseResult parsed = Parse(sql);
    UNIT_ASSERT_C(parsed.Select, parsed.Message);
    auto* runtime = server.GetRuntime();
    const TActorId edge = runtime->AllocateEdgeActor();
    runtime->Register(CreateSelectAnalyzer(edge, database, *parsed.Select));
    return runtime->GrabEdgeEventRethrow<TEvAnalyzeResult>(edge);
}

} // namespace

Y_UNIT_TEST_SUITE(SelectAnalyzer) {
    Y_UNIT_TEST(ChecksColumnsBeforeAnyShardRead) {
        TPortManager ports;
        TServerSettings settings(ports.GetTcpPort());
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();

        const TString database = "/" + settings.DomainName;
        NKikimrSchemeOp::TTableDescription table;
        table.SetName("t");
        auto* key = table.AddColumns();
        key->SetName("a");
        key->SetType("Uint32");
        key->SetNotNull(true);
        auto* value = table.AddColumns();
        value->SetName("b");
        value->SetType("Utf8");
        table.AddKeyColumnNames("a");
        auto* family = table.MutablePartitionConfig()->AddColumnFamilies();
        family->SetId(0);
        family->MutableStorageConfig()->MutableData()->SetPreferredPoolKind("test");
        UNIT_ASSERT_VALUES_EQUAL(client.CreateTable(database, table), NMsgBusProxy::MSTATUS_OK);

        const auto known = Analyze(server, database, "SELECT a, b FROM t");
        UNIT_ASSERT(known->Get()->Ok);
        UNIT_ASSERT_VALUES_EQUAL(known->Get()->Path, database + "/t");
        UNIT_ASSERT_VALUES_EQUAL(known->Get()->ColumnIds.size(), 2u);

        const auto missing = Analyze(server, database, "SELECT missing FROM t");
        UNIT_ASSERT(!missing->Get()->Ok);
        UNIT_ASSERT_VALUES_EQUAL(missing->Get()->SqlState, "42703");
        UNIT_ASSERT_VALUES_EQUAL(missing->Get()->Message, "missing");

        client.MkDir(database, "app");
        table.SetName("t");
        UNIT_ASSERT_VALUES_EQUAL(client.CreateTable(database + "/app", table), NMsgBusProxy::MSTATUS_OK);
        const auto qualified = Analyze(server, database, "SELECT a FROM app.t");
        UNIT_ASSERT_C(qualified->Get()->Ok, qualified->Get()->Message);
        UNIT_ASSERT_VALUES_EQUAL(qualified->Get()->Path, database + "/app/t");
    }
}
