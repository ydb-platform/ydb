#include <ydb/core/grpc_services/base/base.h>

#include <ydb/core/tx/datashard/defs.h>
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/tx/datashard/datashard_ut_common_kqp.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

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
