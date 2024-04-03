#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>


namespace NKikimr {
namespace NKqp {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;
using namespace NYdb;
using namespace NYdb::NTable;


namespace {

using TRows = TVector<std::pair<TSerializedCellVec, TString>>;
using TRowTypes = TVector<std::pair<TString, Ydb::Type>>;

static void DoStartUploadTestRows(
        const Tests::TServer::TPtr& server,
        const TActorId& sender,
        const TString& tableName,
        Ydb::Type::PrimitiveTypeId typeId,
        bool uploadNull)
{
    auto& runtime = *server->GetRuntime();

    std::shared_ptr<TRows> rows(new TRows);
    auto types = std::make_shared<TRowTypes>();
    Ydb::Type type;
    type.set_type_id(typeId);
    types->emplace_back("key", type);
    types->emplace_back("value", type);
    auto makeCell = [&uploadNull](ui32 i) -> TCell {
        return uploadNull ? TCell(nullptr, 0) : TCell::Make(i);
    };
    for (ui32 i = 0; i < 32; i++) {
        auto key = TVector<TCell>{makeCell(i)};
        auto value = TVector<TCell>{makeCell(i)};
        TSerializedCellVec serializedKey(key);
        TString serializedValue = TSerializedCellVec::Serialize(value);
        rows->emplace_back(serializedKey, serializedValue);
    }

    auto actor = NTxProxy::CreateUploadRowsInternal(sender, tableName, types, rows);
    runtime.Register(actor);
}

static void DoWaitUploadTestRows(
        const Tests::TServer::TPtr& server,
        const TActorId& sender,
        Ydb::StatusIds::StatusCode expected)
{
    auto& runtime = *server->GetRuntime();

    auto ev = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvUploadRowsResponse>(sender);
    UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Status, expected);
}

static void DoUploadTestRows(Tests::TServer::TPtr server, const TActorId& sender,
                             const TString& tableName, Ydb::Type::PrimitiveTypeId typeId,
                             Ydb::StatusIds::StatusCode expected, bool uploadNull)
{
    DoStartUploadTestRows(server, sender, tableName, typeId, uploadNull);
    DoWaitUploadTestRows(server, sender, expected);
}

} // namespace

Y_UNIT_TEST_SUITE(KqpUserConstraint) {
    Y_UNIT_TEST_TWIN(KqpReadNull, UploadNull) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);

        TVector<NKikimr::TShardedTableOptions::TColumn> columns;
        columns.emplace_back("key", "Uint32", true, false);
        columns.emplace_back("value", "Uint32", false, true);
        auto opts = TShardedTableOptions()
            .Shards(4)
            .EnableOutOfOrder(false)
            .Columns(std::move(columns));

        CreateShardedTable(server, sender, "/Root", "table-1", std::move(opts));

        DoUploadTestRows(server, sender, "/Root/table-1", Ydb::Type::UINT32, Ydb::StatusIds::SUCCESS, UploadNull);
            
        auto request = MakeSQLRequest("SELECT * FROM `/Root/table-1`", true);
        runtime.Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime.GetNodeId()), sender, request.Release()));
        auto ev = runtime.GrabEdgeEventRethrow<NKqp::TEvKqp::TEvQueryResponse>(sender);

        if (UploadNull) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetRef().GetYdbStatus(), Ydb::StatusIds::INTERNAL_ERROR);
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->Record.GetRef().GetResponse().GetQueryIssues(), issues);
            UNIT_ASSERT(HasIssue(issues, NYql::TIssuesIds::KIKIMR_CONSTRAINT_VIOLATION, [](const NYql::TIssue& issue) {
                return issue.GetMessage().Contains("got NULL from NOT NULL column");
            }));
        } else {
            UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetRef().GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        }
    }

}
} // namespace NKqp
} // namespace NKikimr
