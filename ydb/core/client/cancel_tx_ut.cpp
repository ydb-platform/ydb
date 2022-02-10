#include "flat_ut_client.h"

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/datashard_failpoints.h>
#include <ydb/core/engine/mkql_engine_flat.h>

#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/text_format.h>

namespace NKikimr {
namespace NFlatTests {

using namespace Tests;
using NClient::TValue;

Y_UNIT_TEST_SUITE(TCancelTx) {

    TServer PrepareTest(bool allowCancelROwithReadsets = false) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        if (false) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_NOTICE);
        }

        TFlatMsgBusClient annoyingClient(port);

        const char * table = R"(Name: "T"
                Columns { Name: "key"    Type: "Uint32" }
                Columns { Name: "value"  Type: "Uint32" }
                KeyColumnNames: ["key"]
                UniformPartitionsCount: 2)";

                annoyingClient.InitRoot();
        annoyingClient.CreateTable("/dc-1", table);

        annoyingClient.FlatQuery(
                    "("
                    "   (return (AsList"
                    "       (UpdateRow '/dc-1/T '('('key (Uint32 '0)))  '('('value (Uint32 '11111))) )"
                    "       (UpdateRow '/dc-1/T '('('key (Uint32 '3000000000)))  '('('value (Uint32 '22222))) )"
                    "   ))"
                    ")"
                    );

        TAtomic prevVal;
        cleverServer.GetRuntime()->GetAppData().Icb->SetValue("DataShardControls.CanCancelROWithReadSets", allowCancelROwithReadsets ? 1 : 0 , prevVal);

        return cleverServer;
    }

    void TestMkqlTxCancellation(TString queryText, bool canBeCancelled, bool allowCancelROwithReadsets = false) {
        TServer server = PrepareTest(allowCancelROwithReadsets);
        TFlatMsgBusClient annoyingClient(server.GetSettings().Port);\

        for (ui64 datashard : {72075186224037888, 72075186224037889}) {
            int failAt = 0;
            for (; failAt < 100; ++failAt) {
                NDataShard::gCancelTxFailPoint.Enable(datashard, -1, failAt);

                TFlatMsgBusClient::TFlatQueryOptions opts;
                NKikimrClient::TResponse response;
                annoyingClient.FlatQueryRaw(queryText, opts, response, 2);

                if (false)
                    Cerr << response << Endl;

                bool requestFailed = (response.GetStatus() == NMsgBusProxy::MSTATUS_ERROR);
                UNIT_ASSERT_VALUES_EQUAL_C(NDataShard::gCancelTxFailPoint.Hit, requestFailed, "Request should fail iff there was a cancellation");
                if (!canBeCancelled) {
                    UNIT_ASSERT_VALUES_EQUAL_C(requestFailed, false, "Tx is not supposed to be cancelled");
                }

                NDataShard::gCancelTxFailPoint.Disable();

                if (!requestFailed) {
                    UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), NMsgBusProxy::MSTATUS_OK);
                    break;
                }
            }
            if (canBeCancelled) {
                UNIT_ASSERT_C(failAt > 0, "Failpoint never fired");
            }
        }
    }

    Y_UNIT_TEST(CrossShardReadOnly) {
        TString query = R"(
                (
                    (let row1 '('('key (Uint32 '0)) ))
                    (let row2 '('('key (Uint32 '3000000000)) ))
                    (let cols '('value))
                    (let select1 (SelectRow '/dc-1/T row1 cols))
                    (let select2 (SelectRow '/dc-1/T row2 cols))
                    (let ret (AsList
                        (SetResult 'ret1 select1)
                        (SetResult 'ret2 select2)
                    ))
                    (return ret)
                )
            )";
        TestMkqlTxCancellation(query, true);
    }

    Y_UNIT_TEST(CrossShardReadOnlyWithReadSets) {
        TString query = R"(
                (
                    (let row1 '('('key (Uint32 '0)) ))
                    (let row2 '('('key (Uint32 '3000000000)) ))
                    (let cols '('value))
                    (let select1 (SelectRow '/dc-1/T row1 cols))
                    (let val (IfPresent select1
                        (lambda '(r) (block '(
                            (let select2 (SelectRow '/dc-1/T row2 cols))
                            (let res (SetResult 'ret2 select2))
                            (return res)
                        )))
                        (Void)
                    ))
                    (let ret (AsList
                        (SetResult 'ret1 select1)
                        val
                    ))
                    (return ret)
                )
            )";

        // Normal scenario: cancellation is not allowed
        TestMkqlTxCancellation(query, false);

        // Hack scenario: cancellation is allowed by a flag
        TestMkqlTxCancellation(query, true, true);
    }

    Y_UNIT_TEST(ImmediateReadOnly) {
        TString query = R"(
                (
                    (let row1 '('('key (Uint32 '0)) ))
                    (let row2 '('('key (Uint32 '3000000000)) ))
                    (let cols '('value))
                    (let select1 (SelectRow '/dc-1/T row1 cols 'head))
                    (let select2 (SelectRow '/dc-1/T row2 cols 'head))
                    (let ret (AsList
                        (SetResult 'ret1 select1)
                        (SetResult 'ret2 select2)
                    ))
                    (return ret)
                )
            )";
        TestMkqlTxCancellation(query, true);
    }
}

}}
