#include <ydb/core/grpc_services/base/base.h>

#include <ydb/core/tx/datashard/defs.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;

namespace {

using TEvReadRowsRequest = NGRpcService::TGrpcRequestNoOperationCall<Ydb::Table::ReadRowsRequest, Ydb::Table::ReadRowsResponse>;

using TRows = TVector<std::pair<TSerializedCellVec, TString>>;
using TRowTypes = TVector<std::pair<TString, Ydb::Type>>;


Ydb::Table::ReadRowsRequest MakeReadRowsRequest(const TString& tablePath, const TVector<ui32>& keys) {
    Ydb::Table::ReadRowsRequest request;
    request.set_path(tablePath);

    NYdb::TValueBuilder keysBuilder;
    keysBuilder.BeginList();
    for (ui32 key : keys) {
        keysBuilder.AddListItem().BeginStruct().AddMember("key").Uint32(key).EndStruct();
    }
    keysBuilder.EndList();

    auto keysValuesCpp = keysBuilder.Build();
    auto keysTypeCpp = keysValuesCpp.GetType();
    request.mutable_keys()->mutable_type()->CopyFrom(NYdb::TProtoAccessor::GetProto(keysTypeCpp));
    request.mutable_keys()->mutable_value()->CopyFrom(NYdb::TProtoAccessor::GetProto(keysValuesCpp));

    return request;
}

} // namespace

Y_UNIT_TEST_SUITE(ReadRows) {

    Y_UNIT_TEST(PgCompatibility) {
        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableTablePgTypes(true)
            .SetDisablePgCompatibility(false);

        Tests::TServer::TPtr server = new TServer(settings);
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "Mixed", TShardedTableOptions().Columns({
            {"key", "Uint32", true, false},
            {"native", "Uint32", false, false},
            {"legacy", "pgint4", false, false},
        }));
        CreateShardedTable(server, sender, "/Root", "PgKey", TShardedTableOptions().Columns({
            {"key", "pgint4", true, false},
            {"native", "Uint32", false, false},
        }));
        ExecSQL(server, sender, "UPSERT INTO `/Root/Mixed` (key, native, legacy) VALUES (1u, 7u, 42p);");
        runtime.GetAppData().FeatureFlags.SetDisablePgCompatibility(true);

        const auto read = [&](Ydb::Table::ReadRowsRequest input) {
            auto result = NRpcService::DoLocalRpc<TEvReadRowsRequest>(
                std::move(input), "/Root", "", runtime.GetActorSystem(0));
            return runtime.WaitFuture(result, TDuration::Seconds(10));
        };

        auto request = MakeReadRowsRequest("/Root/Mixed", {1});
        request.add_columns("native");
        const auto success = read(request);
        UNIT_ASSERT_VALUES_EQUAL_C(success.status(), Ydb::StatusIds::SUCCESS, success.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(success.result_set().rows_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(success.result_set().rows(0).items(0).uint32_value(), 7);

        auto pgProjection = request;
        pgProjection.add_columns("legacy");
        const auto projection = read(std::move(pgProjection));
        UNIT_ASSERT_VALUES_EQUAL_C(projection.status(), Ydb::StatusIds::SCHEME_ERROR, projection.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(projection.DebugString(), "PostgreSQL compatibility is disabled for column 'legacy'");

        auto pgKey = request;
        pgKey.set_path("/Root/PgKey");
        auto* keys = pgKey.mutable_keys();
        keys->mutable_type()->mutable_list_type()->mutable_item()->mutable_struct_type()
            ->mutable_members(0)->mutable_type()->mutable_pg_type()->set_type_name("pgint4");
        keys->mutable_value()->mutable_items(0)->mutable_items(0)->set_text_value("1");
        const auto key = read(std::move(pgKey));
        UNIT_ASSERT_VALUES_EQUAL_C(key.status(), Ydb::StatusIds::SCHEME_ERROR, key.DebugString());
        UNIT_ASSERT_STRING_CONTAINS(key.DebugString(), "PostgreSQL compatibility is disabled for column 'key'");
    }

    Y_UNIT_TEST(KillTabletDuringRead) {
        // Init cluster
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);

        // Create table
        CreateShardedTable(server, sender, "/Root", "table-1", 1, false);
        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 100), (3, 300), (5, 500);");

        // Check normal behavior
        {
            Ydb::Table::ReadRowsRequest request = MakeReadRowsRequest("/Root/table-1", {1, 5});
            auto readRowsFuture = NRpcService::DoLocalRpc<TEvReadRowsRequest>(
                std::move(request), "/Root", "", runtime.GetActorSystem(0));
            auto res = runtime.WaitFuture(readRowsFuture, TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(res.status(), ::Ydb::StatusIds::SUCCESS);
        }

        // Get tablet id of the only table shard
        auto tablets = GetTableShards(server, sender, "/Root/table-1");
        UNIT_ASSERT(tablets.size() == 1);
        ui64 tabletId = tablets.at(0);

        // Reboot tablet during read
        auto dsReadResultOberver = runtime.AddObserver<TEvDataShard::TEvReadResult>([&](TEvDataShard::TEvReadResult::TPtr& ev) {
            Cerr << "Stoping tablet id: " << tabletId;
            RebootTablet(runtime, tabletId, sender);
            ev.Reset();
        });

        // Check read with tablet reboot
        {
            Ydb::Table::ReadRowsRequest request = MakeReadRowsRequest("/Root/table-1", {1, 5});
            auto readRowsFuture = NRpcService::DoLocalRpc<TEvReadRowsRequest>(
                std::move(request), "/Root", "", runtime.GetActorSystem(0));
            auto res = runtime.WaitFuture(readRowsFuture, TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(res.status(), ::Ydb::StatusIds::UNAVAILABLE);
        }

        dsReadResultOberver.Remove();
        

        // Slow read
        auto dsReadResultObserver = runtime.AddObserver<TEvDataShard::TEvReadResult>([&](auto&) {
            SimulateSleep(runtime, TDuration::Seconds(100));
        });

        // Timeout
        {
            Ydb::Table::ReadRowsRequest request = MakeReadRowsRequest("/Root/table-1", {1, 5});
            auto readRowsFuture = NRpcService::DoLocalRpc<TEvReadRowsRequest>(
                std::move(request), "/Root", "", runtime.GetActorSystem(0));
            auto res = runtime.WaitFuture(readRowsFuture, TDuration::Seconds(10));
            UNIT_ASSERT_VALUES_EQUAL(res.status(), ::Ydb::StatusIds::TIMEOUT);
            UNIT_ASSERT_VALUES_EQUAL(
                res.issues().begin()->Getmessage(), 
                "ReadRows from table /Root/table-1 timed out, duration: 60 sec\n"
            );
        }
    }
}

} // namespace NKikimr
