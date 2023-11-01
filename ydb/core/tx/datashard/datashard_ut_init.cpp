#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>

#include <ydb/core/base/tablet.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/util/pb.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <util/string/printf.h>

namespace NKikimr {

using namespace NSchemeShard;
using namespace Tests;
using NClient::TValue;

namespace {

TString GetTablePath(TTestActorRuntime &runtime,
                     TActorId sender,
                     ui64 tableId,
                     ui64 shard)
{
    auto request = MakeHolder<TEvTablet::TEvLocalMKQL>();
    const char *miniKQL =   R"___((
        (let row '('('Tid (Uint64 '%lu))))
        (let select '('Schema))
        (let pgmReturn (AsList
            (SetResult 'myRes (SelectRow 'UserTables row select))
        ))
        (return pgmReturn)
    ))___";

    request->Record.MutableProgram()->MutableProgram()->SetText(Sprintf(miniKQL, tableId));
    runtime.SendToPipe(shard, sender, request.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvTablet::TEvLocalMKQLResponse>(handle);
    auto &res = reply->Record.GetExecutionEngineEvaluatedResponse();
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), 0);
    TValue value = TValue::Create(res.GetValue(), res.GetType());
    TString schema = value["myRes"]["Schema"];

    NKikimrSchemeOp::TTableDescription desc;
    Y_PROTOBUF_SUPPRESS_NODISCARD desc.ParseFromArray(schema.data(), schema.size());

    return desc.GetPath();
}

}

Y_UNIT_TEST_SUITE(TTxDataShardTestInit) {

    Y_UNIT_TEST(TestGetShardStateAfterInitialization) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        TActorId sender = runtime.AllocateEdgeActor();
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::DataShard), &CreateDataShard);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        runtime.DispatchEvents(options);

        Y_UNUSED(sender);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvDataShard::TEvGetShardState(sender));
        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvDataShard::TEvGetShardStateResult>(handle);
        UNIT_ASSERT(event);
        UNIT_ASSERT_EQUAL(event->GetOrigin(), TTestTxConfig::TxTablet0);
        UNIT_ASSERT_EQUAL(event->GetState(), NDataShard::TShardState::WaitScheme);
    }

    void TestTablePath(bool oldCreate, bool restart)
    {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        ui64 tableId = 0;
        bool sawResolve = false;
        bool dropResolve = restart;
        // Remove table path from propose.
        auto captureTableId = [&](TAutoPtr<IEventHandle> &event) -> auto {
            if (event->GetTypeRewrite() == TEvDataShard::EvProposeTransaction) {
                auto &rec = event->Get<TEvDataShard::TEvProposeTransaction>()->Record;
                if (rec.GetTxKind() == NKikimrTxDataShard::TX_KIND_SCHEME) {
                    TString body = rec.GetTxBody();
                    NKikimrTxDataShard::TFlatSchemeTransaction tx;
                    Y_PROTOBUF_SUPPRESS_NODISCARD tx.ParseFromArray(body.Data(), body.Size());
                    if (tx.HasCreateTable()) {
                        tableId = tx.GetCreateTable().GetId_Deprecated();
                        if (tx.GetCreateTable().HasPathId()) {
                            UNIT_ASSERT_EQUAL(ChangeStateStorage(Tests::SchemeRoot, serverSettings.Domain), tx.GetCreateTable().GetPathId().GetOwnerId());
                            tableId = tx.GetCreateTable().GetPathId().GetLocalId();
                        }
                        if (oldCreate) {
                            tx.MutableCreateTable()->ClearPath();
                            Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&body);
                            rec.SetTxBody(body);
                        }
                    }
                }
            } else if (event->GetTypeRewrite() == TEvSchemeShard::EvDescribeSchemeResult) {
                auto &rec = event->Get<TEvSchemeShard::TEvDescribeSchemeResult>()->GetRecord();
                const bool hasPartitioning = rec.GetPathDescription().TablePartitionsSize();
                // there are few in-flight TEvDescribeSchemeResult msgs, we need one with no partitioning
                if (!hasPartitioning && rec.GetPathDescription().GetSelf().GetPathId() == tableId) {
                    sawResolve = true;
                    if (dropResolve)
                        return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(captureTableId);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        UNIT_ASSERT(tableId);

        dropResolve = false;
        ui64 tabletId = GetTableShards(server, sender, "/Root/table-1")[0];
        if (restart) {
            sawResolve = false;
            runtime.Register(CreateTabletKiller(tabletId));

            if (oldCreate) {
                while (!sawResolve) {
                    TDispatchOptions options;
                    options.FinalEvents.emplace_back(TEvSchemeShard::EvDescribeSchemeResult);
                    runtime.DispatchEvents(options);
                }
            }
        }

        TString path = GetTablePath(runtime, sender, tableId, tabletId);
        UNIT_ASSERT_VALUES_EQUAL(path, "/Root/table-1");
    }

    Y_UNIT_TEST(TestTableHasPath) {
        TestTablePath(false, false);
    }

    Y_UNIT_TEST(TestResolvePathAfterRestart) {
        TestTablePath(true, true);
    }
}

}
