#include "flat_ut_client.h"

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/datashard_failpoints.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/protos/query_stats.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/text_format.h>

namespace NKikimr {
namespace NFlatTests {

using namespace Tests;
using NClient::TValue;

Y_UNIT_TEST_SUITE(TQueryStats) {

    TServer PrepareTest() {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        if (false) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_NOTICE);
        }

        TFlatMsgBusClient annoyingClient(port);

        const char* table1 = R"(
                Name: "T"
                Columns { Name: "key"    Type: "Uint32" }
                Columns { Name: "value"  Type: "Int32" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )";

        const char* table2 = R"(
                Name: "T2"
                Columns { Name: "key"    Type: "Uint32" }
                Columns { Name: "value"  Type: "Int32" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2
            )";

        annoyingClient.InitRoot();
        annoyingClient.CreateTable("/dc-1", table1);
        annoyingClient.CreateTable("/dc-1", table2);

        annoyingClient.FlatQuery(
                    "("
                    "   (return (AsList"
                    "       (UpdateRow '/dc-1/T '('('key (Uint32 '0)))  '('('value (Int32 '11111))) )"
                    "       (UpdateRow '/dc-1/T '('('key (Uint32 '3000000000)))  '('('value (Int32 '22222))) )"
                    "   ))"
                    ")"
                    );
        return cleverServer;
    }

    NKikimrClient::TResponse FlatQueryWithStats(TFlatMsgBusClient& annoyingClient, const TString& mkql) {
        TClient::TFlatQueryOptions opts;
        opts.CollectStats = true;

        NKikimrClient::TResponse response;
        annoyingClient.FlatQueryRaw(mkql, opts, response);

        return response;
    }

    Y_UNIT_TEST(OffByDefault) {
        TString query = R"(
                (
                    (let row1 '('('key (Uint32 '0)) ))
                    (let cols '('value))
                    (let select1 (SelectRow '/dc-1/T row1 cols 'head))
                    (let ret (AsList
                        (SetResult 'ret1 select1)
                    ))
                    (return ret)
                )
            )";

        TServer server = PrepareTest();
        TFlatMsgBusClient annoyingClient(server.GetSettings().Port);
        NKikimrClient::TResponse res;
        TClient::TFlatQueryOptions opts;
        annoyingClient.FlatQueryRaw(query, opts, res);
        // Cerr << res << Endl;
        UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT_VALUES_EQUAL(res.GetExecutionEngineResponseStatus(), ui32(NMiniKQL::IEngineFlat::EStatus::Complete));
        UNIT_ASSERT_VALUES_EQUAL(res.HasTxStats(), false);
    }

    Y_UNIT_TEST(ImmediateMkql) {
        TString query = R"(
                (
                    (let row1 '('('key (Uint32 '0)) ))
                    (let row2 '('('key (Uint32 '3000000000)) ))
                    (let cols '('value))
                    (let select1 (SelectRow '/dc-1/T row1 cols 'head))
                    (let select2 (SelectRow '/dc-1/T row2 cols 'head))
                    (let range (SelectRange '/dc-1/T '('IncFrom '('key (Uint32 '0) (Uint32 '1) ) ) cols '() 'head))
                    (let ret (AsList
                        (SetResult 'ret1 select1)
                        (SetResult 'ret2 select2)
                        (SetResult 'range range)
                    ))
                    (return ret)
                )
            )";

        TServer server = PrepareTest();
        TFlatMsgBusClient annoyingClient(server.GetSettings().Port);
        NKikimrClient::TResponse res = FlatQueryWithStats(annoyingClient, query);
        // Cerr << res << Endl;
        UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT_VALUES_EQUAL(res.GetExecutionEngineResponseStatus(), ui32(NMiniKQL::IEngineFlat::EStatus::Complete));

        UNIT_ASSERT_VALUES_EQUAL(res.HasTxStats(), true);
        auto stats = res.GetTxStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.HasDurationUs(), true);
        UNIT_ASSERT(stats.GetDurationUs() > 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.TableAccessStatsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetTableAccessStats(0).GetTableInfo().GetName(), "/dc-1/T");
        UNIT_ASSERT_VALUES_EQUAL(stats.GetTableAccessStats(0).GetSelectRow().GetCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetTableAccessStats(0).GetSelectRow().GetRows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.PerShardStatsSize(), 2);
        UNIT_ASSERT_VALUES_UNEQUAL(stats.GetPerShardStats(0).GetCpuTimeUsec(), 0);
        UNIT_ASSERT_VALUES_UNEQUAL(stats.GetPerShardStats(1).GetCpuTimeUsec(), 0);
        UNIT_ASSERT_VALUES_UNEQUAL(stats.GetComputeCpuTimeUsec(), 0);
    }

    Y_UNIT_TEST(CrossShardMkql) {
        TString query = R"(
                (
                    (let row1 '('('key (Uint32 '0)) ))
                    (let row2 '('('key (Uint32 '3000000000)) ))
                    (let cols '('value))
                    (let select1 (SelectRow '/dc-1/T row1 cols))
                    (let select2 (SelectRow '/dc-1/T row2 cols))
                    (let range (SelectRange '/dc-1/T '('IncFrom '('key (Uint32 '0) (Void) ) ) cols '() ))
                    (let ret (AsList
                        (SetResult 'ret1 select1)
                        (SetResult 'ret2 select2)
                        (SetResult 'range range)
                        (UpdateRow '/dc-1/T '('('key (Uint32 '10)))  '('('value (Int32 '10))) )
                        (EraseRow '/dc-1/T '('('key (Uint32 '0))) )
                    ))
                    (return ret)
                )
            )";

        TServer server = PrepareTest();
        TFlatMsgBusClient annoyingClient(server.GetSettings().Port);
        NKikimrClient::TResponse res = FlatQueryWithStats(annoyingClient, query);
        // Cerr << res << Endl;
        UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT_VALUES_EQUAL(res.GetExecutionEngineResponseStatus(), ui32(NMiniKQL::IEngineFlat::EStatus::Complete));

        UNIT_ASSERT_VALUES_EQUAL(res.HasTxStats(), true);
        auto stats = res.GetTxStats();
        UNIT_ASSERT_VALUES_EQUAL(stats.HasDurationUs(), true);
        UNIT_ASSERT(stats.GetDurationUs() > 0);
        UNIT_ASSERT_VALUES_EQUAL(stats.TableAccessStatsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetTableAccessStats(0).GetTableInfo().GetName(), "/dc-1/T");
        UNIT_ASSERT_VALUES_EQUAL(stats.GetTableAccessStats(0).GetSelectRow().GetCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.GetTableAccessStats(0).GetSelectRow().GetRows(), 2);
        UNIT_ASSERT_VALUES_EQUAL(stats.PerShardStatsSize(), 2);
        UNIT_ASSERT_VALUES_UNEQUAL(stats.GetPerShardStats(0).GetCpuTimeUsec(), 0);
        UNIT_ASSERT_VALUES_UNEQUAL(stats.GetPerShardStats(1).GetCpuTimeUsec(), 0);
        UNIT_ASSERT_VALUES_UNEQUAL(stats.GetComputeCpuTimeUsec(), 0);
    }
}

}}
