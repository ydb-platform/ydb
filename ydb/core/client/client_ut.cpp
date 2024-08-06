#include <ydb/core/base/appdata.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tablet/tablet_impl.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/util/console.h>
#include <ydb/core/client/minikql_compile/yql_expr_minikql.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <ydb/library/actors/interconnect/interconnect_impl.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/diff/diff.h>

#include <ydb/library/yql/ast/yql_ast.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <util/folder/path.h>
#include <util/generic/xrange.h>
#include <util/string/subst.h>
#include <util/thread/pool.h>

namespace NKikimr {

using NClient::TValue;

namespace Tests {
    using namespace NMiniKQL;
//    const ui32 TestDomain = 1;

static const TString TablePlacement = "/dc-1/Berkanavt/tables";

namespace {

void SetupLogging(TServer& server) {
    server.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
#if 0
    server.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
    server.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
    server.GetRuntime()->SetLogPriority(NKikimrServices::PIPE_CLIENT, NActors::NLog::PRI_DEBUG);
    server.GetRuntime()->SetLogPriority(NKikimrServices::PIPE_SERVER, NActors::NLog::PRI_DEBUG);
    server.GetRuntime()->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
#endif
}

// Creates test tables on ctor and deletes on dtor
struct TTestTables {
    enum EVariant {
        OneShard_NoOpts,
        OneShard_OutOfOrder,
        OneShard_SoftUpdates,
        OneShard_OutOfOrder_SoftUpdates,
        Sharded_NoOpts,
        Sharded_OutOfOrder,
        Sharded_SoftUpdates,
        Sharded_OutOfOrder_SoftUpdates,
    };

    struct TOpts {
        bool Sharded = false;
        bool OutOfOrder = false;
        bool SoftUpdates = false;
        ui32 FollowerCount = 0;

        TOpts(EVariant var = OneShard_NoOpts, ui32 numFollowers = 0)
            : Sharded(false)
            , OutOfOrder(false)
            , SoftUpdates(false)
            , FollowerCount(numFollowers)
        {
            switch (var) {
            case OneShard_NoOpts:
                break;
            case OneShard_OutOfOrder:
                OutOfOrder = true;
                break;
            case OneShard_SoftUpdates:
                SoftUpdates = true;
                break;
            case OneShard_OutOfOrder_SoftUpdates:
                OutOfOrder = true;
                SoftUpdates = true;
                break;
            default:
                break;
            }

            switch (var) {
            case Sharded_NoOpts:
            case Sharded_OutOfOrder:
            case Sharded_SoftUpdates:
            case Sharded_OutOfOrder_SoftUpdates:
                Sharded = true;
                break;
            default:
                break;
            }
        }
    };

    TTestTables(TClient& client, EVariant var, ui32 numFollowers = 0)
        : Client(client)
    {
        TOpts opts(var, numFollowers);
        NKikimrSchemeOp::TTableDescription tableSimple;
        {
            tableSimple.SetName("Simple");
            auto *c1 = tableSimple.AddColumns();
            c1->SetName("key");
            c1->SetType("Uint64");
            auto *c2 = tableSimple.AddColumns();
            c2->SetName("uint");
            c2->SetType("Uint64");
            auto *c3 = tableSimple.AddColumns();
            c3->SetName("bytes");
            c3->SetType("String");

            if (opts.Sharded)
                tableSimple.SetUniformPartitionsCount(10);

            if (opts.FollowerCount)
                tableSimple.MutablePartitionConfig()->SetFollowerCount(opts.FollowerCount);

            if (opts.OutOfOrder) {
                tableSimple.MutablePartitionConfig()->MutablePipelineConfig()->SetNumActiveTx(8);
                tableSimple.MutablePartitionConfig()->MutablePipelineConfig()->SetEnableOutOfOrder(true);
            }

            if (opts.SoftUpdates)
                tableSimple.MutablePartitionConfig()->MutablePipelineConfig()->SetEnableSoftUpdates(true);

            *tableSimple.AddKeyColumnNames() = "key";
        }

        NKikimrSchemeOp::TTableDescription tableByBytes;
        {
            tableByBytes.SetName("ByBytes");
            auto *c1 = tableByBytes.AddColumns();
            c1->SetName("key");
            c1->SetType("String");
            auto *c2 = tableByBytes.AddColumns();
            c2->SetName("uint");
            c2->SetType("Uint64");
            auto *c3 = tableByBytes.AddColumns();
            c3->SetName("bytes");
            c3->SetType("String");

            *tableByBytes.AddKeyColumnNames() = "key";
        }

        NKikimrSchemeOp::TTableDescription tableComp;
        {
            tableComp.SetName("Comp");
            auto *c1 = tableComp.AddColumns();
            c1->SetName("key");
            c1->SetType("Uint64");

            auto *c1a = tableComp.AddColumns();
            c1a->SetName("secondary");
            c1a->SetType("String");

            auto *c2 = tableComp.AddColumns();
            c2->SetName("uint");
            c2->SetType("Uint64");
            auto *c3 = tableComp.AddColumns();
            c3->SetName("bytes");
            c3->SetType("String");

            if (opts.Sharded)
                tableComp.SetUniformPartitionsCount(10);

            *tableComp.AddKeyColumnNames() = "key";
            *tableComp.AddKeyColumnNames() = "secondary";
        }

        client.MkDir("/dc-1", "Berkanavt");
        client.MkDir("/dc-1/Berkanavt", "tables");
        UNIT_ASSERT_EQUAL(client.CreateTable(TablePlacement, tableSimple), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT_EQUAL(client.CreateTable(TablePlacement, tableByBytes), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT_EQUAL(client.CreateTable(TablePlacement, tableComp), NMsgBusProxy::MSTATUS_OK);
    }

    ~TTestTables() {
        UNIT_ASSERT_EQUAL(Client.DeleteTable(TablePlacement, "Simple"), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT_EQUAL(Client.DeleteTable(TablePlacement, "ByBytes"), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT_EQUAL(Client.DeleteTable(TablePlacement, "Comp"), NMsgBusProxy::MSTATUS_OK);
    }

private:
    TClient& Client;
};

void CreateOldTypesTables(TClient &client) {
    NKikimrSchemeOp::TTableDescription tableOld;
    {
        tableOld.SetName("Old");
        auto * c1 = tableOld.AddColumns();
        c1->SetName("key");
        c1->SetType("Uint32");
        auto * c2 = tableOld.AddColumns();
        c2->SetName("strKey");
        c2->SetType("ByteString");
        auto * c3 = tableOld.AddColumns();
        c3->SetName("utf8Key");
        c3->SetType("Utf8String");
        auto * c4 = tableOld.AddColumns();
        c4->SetName("strData");
        c4->SetType("ByteString");
        auto * c5 = tableOld.AddColumns();
        c5->SetName("utf8Data");
        c5->SetType("Utf8String");

        *tableOld.AddKeyColumnNames() = "key";
        *tableOld.AddKeyColumnNames() = "strKey";
        *tableOld.AddKeyColumnNames() = "utf8Key";

        tableOld.SetUniformPartitionsCount(4);
    }

    NKikimrSchemeOp::TTableDescription tableNew;
    {
        tableNew.SetName("New");
        auto * c1 = tableNew.AddColumns();
        c1->SetName("key");
        c1->SetType("Uint32");
        auto * c2 = tableNew.AddColumns();
        c2->SetName("strKey");
        c2->SetType("String");
        auto * c3 = tableNew.AddColumns();
        c3->SetName("utf8Key");
        c3->SetType("Utf8");
        auto * c4 = tableNew.AddColumns();
        c4->SetName("strData");
        c4->SetType("String");
        auto * c5 = tableNew.AddColumns();
        c5->SetName("utf8Data");
        c5->SetType("Utf8");

        *tableNew.AddKeyColumnNames() = "key";
        *tableNew.AddKeyColumnNames() = "strKey";
        *tableNew.AddKeyColumnNames() = "utf8Key";

        tableNew.SetUniformPartitionsCount(4);
    }

    client.MkDir("/dc-1", "Berkanavt");
    client.MkDir("/dc-1/Berkanavt", "tables");
    UNIT_ASSERT(client.CreateTable(TablePlacement, tableOld));
    UNIT_ASSERT(client.CreateTable(TablePlacement, tableNew));
}

void AlterTestTables(TClient& client) {
    NKikimrSchemeOp::TTableDescription alterSimple;
    {
        alterSimple.SetName("Simple");
        auto* dc1 = alterSimple.AddDropColumns();
        dc1->SetName("uint");
    }

    client.AlterTable(TablePlacement, alterSimple);

    NKikimrSchemeOp::TTableDescription alterSimple1;
    {
        alterSimple1.SetName("Simple");
        auto *c2 = alterSimple1.AddColumns();
        c2->SetName("uint");
        c2->SetType("Uint64");
    }

    usleep(100*1000); // FIXME
    client.AlterTable(TablePlacement, alterSimple1);
}

struct TTxInfo {
    ui64 TabletId;
    TActorId ActorId;
    std::pair<ui32, ui64> GenStep;
    bool IsFollower;
    ui64 TxId;
    ui64 TxStep;
    ui32 Status;
    ui64 PrepareArriveTime;
    ui64 ProposeLatency;
    ui64 ExecLatency;

    void PrintTabletInfo() const {
        Cerr << "TabletId: " << TabletId
            << " Generation:Step: " << GenStep.first << ":" << GenStep.second
            << " ActorId: " << ActorId
            << " IsFollower: " << (ui32)IsFollower << Endl;
    }

    void PrintTxInfo() const {
        Cerr << "Step:TxId: " << TxStep << ":" << TxId
            << " Status: " << Status
            << " PrepareArriveTime: " << PrepareArriveTime
            << " ProposeLatency: " << ProposeLatency
            << " ExecLatency: " << ExecLatency << Endl;
    }
};

void ExtractResultInfo(const NKikimrMiniKQL::TResult& result, TVector<TTxInfo>& out) {
    TValue value = TValue::Create(result.GetValue(), result.GetType());

    TValue list = value["__tx_info"];
    UNIT_ASSERT(list.HaveValue());
    for (ui32 i = 0; i < list.Size(); ++i) {
        TValue row = list[i];
        UNIT_ASSERT_VALUES_EQUAL(row.Size(), 12);

        TTxInfo info;
        info.TabletId = row["TabletId"];
        info.ActorId = TActorId(row["ActorIdRawX1"], row["ActorIdRawX2"]);
        info.GenStep = std::make_pair(row["Generation"], row["GenStep"]);
        info.IsFollower = row["IsFollower"];
        info.TxId = row["TxId"];
        info.TxStep = row["TxStep"];
        info.Status = row["Status"];
        info.PrepareArriveTime = row["PrepareArriveTime"];
        info.ProposeLatency = row["ProposeLatency"];
        info.ExecLatency = row["ExecLatency"];

        out.push_back(info);
    }
}

} // namelesspace

Y_UNIT_TEST_SUITE(TClientTest) {
    Y_UNIT_TEST(TestInspectProxy) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        auto settings = TServerSettings(port);
        settings.SetSupportsRedirect(false);

        TServer server(settings);
        TClient client(settings);

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT(client.LocalQuery(TxAllocator, "("
            "(let row '('('dummyKey (Bool 'true))))"
            "(let select '('reservedIds))"
            "(return (AsList (SetResult 'reservedIds (SelectRow 'config row select))))"
        ")", res));

        {
            TValue value = TValue::Create(res.GetValue(), res.GetType());
            UNIT_ASSERT(value["reservedIds"].Size() <= 1);
        }
    }

    Y_UNIT_TEST(NoAffectedProgram) {
        using namespace NScheme;
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT(client.FlatQuery("("
            "(return (AsList (SetResult 'res1 (Int32 '42))))"
        ")", res));

        {
            TValue value = TValue::Create(res.GetValue(), res.GetType());
            TValue resOpt = value["res1"];
            UNIT_ASSERT(resOpt.HaveValue());
            UNIT_ASSERT_EQUAL(i32(resOpt), 42);
        }
    }

    Y_UNIT_TEST(ReadWriteMiniKQL) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();
        TTestTables tables(client, TTestTables::OneShard_NoOpts);

        NKikimrMiniKQL::TResult writeRes;
        const TString writeQuery = R"((
            (let row_ '('('key (Uint64 '42))))
            (let update_ '('('uint (Uint64 '0))))
            (let result_ (UpdateRow '/dc-1/Berkanavt/tables/Simple row_ update_))
            (return (AsList result_))
        ))";
        UNIT_ASSERT(client.FlatQuery(writeQuery, writeRes));

        NKikimrMiniKQL::TResult readRes;
        const TString readQuery = R"((
            (let row_ '('('key (Uint64 '42))))
            (let select_ '('uint))
            (let result_ (SelectRow '/dc-1/Berkanavt/tables/Simple row_ select_))
            (return (AsList (SetResult 'res result_)))
        ))";

        UNIT_ASSERT(client.FlatQuery(readQuery, readRes));

        {
            TValue value = TValue::Create(readRes.GetValue(), readRes.GetType());
            TValue resOpt = value["res"];
            UNIT_ASSERT(resOpt.HaveValue());
            ui64 xval = resOpt[0];
            UNIT_ASSERT_VALUES_EQUAL(xval, 0);
        }
    }

    void ReadWriteViaMiniKQLBody(TClient &client, bool useHead, bool useFollower) {
        NKikimrMiniKQL::TResult writeRes;
        const TString writeQuery = R"___(
            (
                (let row1 '('('key (Uint64 '2))))
                (let row2 '('('key (Uint64 '2305843009213693951))))
                (let update '( '('uint (Uint64 '10))))
                (let result1 (UpdateRow '/dc-1/Berkanavt/tables/Simple row1 update))
                (let result2 (UpdateRow '/dc-1/Berkanavt/tables/Simple row2 update))

                (return (AsList result1 result2))
            )
            )___";

        UNIT_ASSERT(client.FlatQuery(writeQuery, writeRes));

        NKikimrMiniKQL::TResult readRes;
        const TString readQueryTemplate = R"___(
            (
                (let row1 '('('key (Uint64 '2))))
                (let nonExistRow1 '('('key (Uint64 '3))))
                (let row2 '('('key (Uint64 '2305843009213693951))))
                (let nonExistRow2 '('('key (Uint64 '2305843009213693952))))
                (let select '('uint))
                (let result1 (SelectRow '/dc-1/Berkanavt/tables/Simple row1 select __HEAD__))
                (let result2 (SelectRow '/dc-1/Berkanavt/tables/Simple row2 select __HEAD__))
                (let entry1 (Coalesce (FlatMap result1 (lambda '(x) (Member x 'uint))) (Uint64 '0)))
                (let entry2 (Coalesce (FlatMap result2 (lambda '(x) (Member x 'uint))) (Uint64 '0)))
                (let non1 (SelectRow '/dc-1/Berkanavt/tables/Simple nonExistRow1 select __HEAD__))
                (let non2 (SelectRow '/dc-1/Berkanavt/tables/Simple nonExistRow2 select __HEAD__))
                (return (AsList (SetResult 'uint1 entry1) (SetResult 'uint2 entry2) (SetResult 'empty1 (Exists non1)) (SetResult 'empty2 (Exists non2)) ))
            )
        )___";

        TString readQuery = readQueryTemplate;
        SubstGlobal(readQuery, "__HEAD__", !useHead ? (useFollower ? "'follower" : "'online") : "'head");
        UNIT_ASSERT(client.FlatQuery(readQuery, readRes));

        {
            TValue value = TValue::Create(readRes.GetValue(), readRes.GetType());
            TValue resOpt1 = value["uint1"];
            TValue resOpt2 = value["uint2"];
            UNIT_ASSERT(resOpt1.HaveValue() && ui64(resOpt1) == 10);
            UNIT_ASSERT(resOpt2.HaveValue() && ui64(resOpt2) == 10);
            TValue resEmpty1 = value["empty1"];
            TValue resEmpty2 = value["empty2"];
            UNIT_ASSERT(resEmpty1.HaveValue() && bool(resEmpty1) == false);
            UNIT_ASSERT(resEmpty2.HaveValue() && bool(resEmpty2) == false);
        }

        const TString rangeQueryTemplate = R"___(
            (
                (let range '('('key (Uint64 '0) (Void))))
                (let select '('uint))
                (let options '('('ItemsLimit (Uint64 '30))) )
                (let result (SelectRange '/dc-1/Berkanavt/tables/Simple range select options __HEAD__))
                (return (AsList (SetResult 'result result) ))
            )
        )___";

        TString rangeQuery = rangeQueryTemplate;
        SubstGlobal(rangeQuery, "__HEAD__", !useHead ? (useFollower ? "'follower" : "'online") : "'head");
        NKikimrMiniKQL::TResult rangeRes;
        UNIT_ASSERT(client.FlatQuery(rangeQuery, rangeRes));

        {
            //Cerr << rangeRes << Endl;
            TValue value = TValue::Create(rangeRes.GetValue(), rangeRes.GetType());
            TValue rangeOpt = value["result"];
            UNIT_ASSERT(rangeOpt.HaveValue());
            TValue list = rangeOpt["List"];
            UNIT_ASSERT_VALUES_EQUAL(list.Size(), 2);
        }
    }

    Y_UNIT_TEST(ReadWriteViaMiniKQL) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();

        {
            TTestTables tables(client, TTestTables::OneShard_NoOpts);
            ReadWriteViaMiniKQLBody(client, false, false);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_OutOfOrder);
            ReadWriteViaMiniKQLBody(client, false, false);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_SoftUpdates);
            ReadWriteViaMiniKQLBody(client, false, false);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_OutOfOrder_SoftUpdates);
            ReadWriteViaMiniKQLBody(client, false, false);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts);
            ReadWriteViaMiniKQLBody(client, false, false);
        }
    }

    Y_UNIT_TEST(ReadWrite_MiniKQL_AfterAlter) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();

        {
            TTestTables tables(client, TTestTables::OneShard_NoOpts);
            AlterTestTables(client);
            ReadWriteViaMiniKQLBody(client, false, false);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_OutOfOrder);
            AlterTestTables(client);
            ReadWriteViaMiniKQLBody(client, false, false);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_SoftUpdates);
            AlterTestTables(client);
            ReadWriteViaMiniKQLBody(client, false, false);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_OutOfOrder_SoftUpdates);
            AlterTestTables(client);
            ReadWriteViaMiniKQLBody(client, false, false);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts);
            AlterTestTables(client);
            ReadWriteViaMiniKQLBody(client, false, false);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_OutOfOrder);
            AlterTestTables(client);
            ReadWriteViaMiniKQLBody(client, false, false);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_SoftUpdates);
            AlterTestTables(client);
            ReadWriteViaMiniKQLBody(client, false, false);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_OutOfOrder_SoftUpdates);
            AlterTestTables(client);
            ReadWriteViaMiniKQLBody(client, false, false);
        }
    }

    Y_UNIT_TEST(ReadWrite_MiniKQL_BeforeAndAfterAlter) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();

        {
            TTestTables tables(client, TTestTables::OneShard_NoOpts);
            ReadWriteViaMiniKQLBody(client, false, false);
            AlterTestTables(client);
            ReadWriteViaMiniKQLBody(client, false, false);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts);
            ReadWriteViaMiniKQLBody(client, false, false);
            AlterTestTables(client);
            ReadWriteViaMiniKQLBody(client, false, false);
        }
    }

    Y_UNIT_TEST(ReadWriteViaMiniKQLRecreateDifferentTable) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();

        {
            TTestTables tables(client, TTestTables::OneShard_NoOpts);
            ReadWriteViaMiniKQLBody(client, false, false);
        }

        {
            NKikimrSchemeOp::TTableDescription tableSimple;
            tableSimple.SetName("Simple");
            auto *c1 = tableSimple.AddColumns();
            c1->SetName("key");
            c1->SetType("Uint32");
            auto *c2 = tableSimple.AddColumns();
            c2->SetName("uint");
            c2->SetType("Uint64");
            auto *c3 = tableSimple.AddColumns();
            c3->SetName("bytes");
            c3->SetType("String");

            *tableSimple.AddKeyColumnNames() = "key";
            client.CreateTable(TablePlacement, tableSimple);
        }

        const TString writeQuery = R"___(
            (
                (let row1 '('('key (Uint32 '2))))
                (let row2 '('('key (Uint32 '3))))
                (let update '( '('uint (Uint64 '10))))
                (let result1 (UpdateRow '/dc-1/Berkanavt/tables/Simple row1 update))
                (let result2 (UpdateRow '/dc-1/Berkanavt/tables/Simple row2 update))

                (return (AsList result1 result2))
            )
        )___";

        NKikimrMiniKQL::TResult writeRes;
        UNIT_ASSERT(client.FlatQuery(writeQuery, writeRes));
    }

    Y_UNIT_TEST(ReadWriteViaMiniKQLRecreateSameTable) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();

        {
            TTestTables tables(client, TTestTables::OneShard_NoOpts);
            ReadWriteViaMiniKQLBody(client, false, false);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts);
            ReadWriteViaMiniKQLBody(client, false, false);
        }
    }

    Y_UNIT_TEST(ReadWriteViaMiniKQLShardedHead) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();
        TTestTables tables(client, TTestTables::Sharded_NoOpts);

        //server.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
        ReadWriteViaMiniKQLBody(client, true, false);
    }

    Y_UNIT_TEST(ReadWriteViaMiniKQLShardedFollower) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        // Currently followers cannot be started at the same node with leader
        // so we need 2 nodes
        auto settings = TServerSettings(port);
        settings.SetNodeCount(2);

        TServer server(settings);
        TClient client(settings);

        server.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        client.InitRootScheme();

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts, 1);
            ReadWriteViaMiniKQLBody(client, false, true);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts, 3);
            ReadWriteViaMiniKQLBody(client, false, true);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts, 2); //max followers is 3
            ReadWriteViaMiniKQLBody(client, false, true);
        }
    }

    void GetStepTxIdBody(TClient &client, bool useHead) {
        NKikimrMiniKQL::TResult writeRes;
        const TString writeQuery = R"___(
            (
                (let row1 '('('key (Uint64 '2))))
                (let row2 '('('key (Uint64 '2305843009213693951))))
                (let update '( '('uint (Uint64 '10))))
                (let result1 (UpdateRow '/dc-1/Berkanavt/tables/Simple row1 update))
                (let result2 (UpdateRow '/dc-1/Berkanavt/tables/Simple row2 update))

                            (return (AsList result1 result2))
            )
            )___";

        UNIT_ASSERT(client.FlatQuery(writeQuery, writeRes));

        NKikimrMiniKQL::TResult readRes;
        const TString readQueryTemplate = R"___(
            (
                (let row1 '('('key (Uint64 '2))))
                (let row2 '('('key (Uint64 '2305843009213693951))))
                (let select '('uint))
                (let result1 (SelectRow '/dc-1/Berkanavt/tables/Simple row1 select __HEAD__))
                (let result2 (SelectRow '/dc-1/Berkanavt/tables/Simple row2 select __HEAD__))
                (let entry1 (Coalesce (FlatMap result1 (lambda '(x) (Member x 'uint))) (Uint64 '0)))
                (let entry2 (Coalesce (FlatMap result2 (lambda '(x) (Member x 'uint))) (Uint64 '0)))
                (return (AsList (SetResult 'uint1 entry1) (SetResult 'uint2 entry2)
                    (SetResult 'step (Nth (StepTxId) '0)) (SetResult 'txid (Nth (StepTxId) '1)) ))
            )
        )___";

        TString readQuery = readQueryTemplate;
        SubstGlobal(readQuery, "__HEAD__", !useHead ? "'online" : "'head");
        UNIT_ASSERT(client.FlatQuery(readQuery, readRes));

        {
            TValue value = TValue::Create(readRes.GetValue(), readRes.GetType());
            TValue resOpt1 = value["uint1"];
            TValue resOpt2 = value["uint2"];
            UNIT_ASSERT(resOpt1.HaveValue() && ui64(resOpt1) == 10);
            UNIT_ASSERT(resOpt2.HaveValue() && ui64(resOpt2) == 10);

            TValue resStep = value["step"];
            UNIT_ASSERT(resStep.HaveValue());
            ui64 stepValue = resStep;
            TValue resTxId = value["txid"];
            UNIT_ASSERT(resTxId.HaveValue());
            ui64 txIdValue = resTxId;
            UNIT_ASSERT((useHead ? (stepValue == 0) : (stepValue > 0)) && (txIdValue > 0));
        }
    }

    Y_UNIT_TEST(GetStepTxId) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();
        TTestTables tables(client, TTestTables::Sharded_NoOpts);

        GetStepTxIdBody(client, false);
    }

    Y_UNIT_TEST(GetStepTxIdHead) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();
        TTestTables tables(client, TTestTables::Sharded_NoOpts);

        GetStepTxIdBody(client, true);
    }

    void CASViaMiniKQLBody(TClient &client) {
        NKikimrMiniKQL::TResult writeRes;
        const TString writeQuery = R"___(
            (
                (let table '/dc-1/Berkanavt/tables/Simple)
                (let row1 '('('key (Uint64 '2))))
                (let row2 '('('key (Uint64 '2305843009213693951))))
                (return (AsList
                    (UpdateRow table row1 '( '('uint (Uint64 '10))))
                    (UpdateRow table row2 '( '('uint (Uint64 '20)))))
                )
            )
            )___";

        UNIT_ASSERT(client.FlatQuery(writeQuery, writeRes));

        NKikimrMiniKQL::TResult updateRes;
        const TString updateQuery = R"___(
            (
                (let table '/dc-1/Berkanavt/tables/Simple)
                (let row1 '('('key (Uint64 '2))))
                (let row2 '('('key (Uint64 '2305843009213693951))))
                (let select '('uint))
                (let read1 (SelectRow table row1 select))
                (let read2 (SelectRow table row2 select))
                (let cmp1 (IfPresent read1 (lambda '(x) (Coalesce (Equal (Member x 'uint) (Uint64 '10)) (Bool 'false))) (Bool 'false)))
                (let cmp2 (IfPresent read2 (lambda '(x) (Coalesce (Equal (Member x 'uint) (Uint64 '10)) (Bool 'false))) (Bool 'false)))
                (return (Extend (Extend
                    (AsList (SetResult 'cmp1 cmp1) (SetResult 'cmp2 cmp2))
                    (ListIf cmp2 (UpdateRow table row1 '('('uint (Uint64 '50))))))
                    (ListIf cmp1 (UpdateRow table row2 '('('uint (Uint64 '50)))))
                ))
            )
        )___";

        UNIT_ASSERT(client.FlatQuery(updateQuery, updateRes));

        {
            TValue value = TValue::Create(updateRes.GetValue(), updateRes.GetType());
            TValue cmp1Opt = value["cmp1"];
            TValue cmp2Opt = value["cmp2"];

            UNIT_ASSERT(cmp1Opt.HaveValue() && bool(cmp1Opt) == true);
            UNIT_ASSERT(cmp2Opt.HaveValue() && bool(cmp2Opt) == false);
        }

        NKikimrMiniKQL::TResult readRes;
        const TString readQuery = R"___(
            (
                (let table '/dc-1/Berkanavt/tables/Simple)
                (let row1 '('('key (Uint64 '2))))
                (let row2 '('('key (Uint64 '2305843009213693951))))
                (let select '('uint))
                (let read1 (SelectRow table row1 select))
                (let read2 (SelectRow table row2 select))
                (let emptyList (List (ListType(VoidType))))
                (return (Extend
                    (IfPresent read1 (lambda '(x) (IfPresent (Member x 'uint) (lambda '(z) (AsList (SetResult 'row1 z))) emptyList )) emptyList)
                    (IfPresent read2 (lambda '(x) (IfPresent (Member x 'uint) (lambda '(z) (AsList (SetResult 'row2 z))) emptyList )) emptyList)
                ))
            )
        )___";

        UNIT_ASSERT(client.FlatQuery(readQuery, readRes));

        {
            TValue value = TValue::Create(readRes.GetValue(), readRes.GetType());
            TValue row1Opt = value["row1"];
            TValue row2Opt = value["row2"];

            UNIT_ASSERT(row1Opt.HaveValue() && ui64(row1Opt) == 10);
            UNIT_ASSERT(row2Opt.HaveValue() && ui64(row2Opt) == 50);
        }
    }

    Y_UNIT_TEST(CASViaMiniKQL) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();

        {
            TTestTables tables(client, TTestTables::OneShard_NoOpts);
            CASViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_OutOfOrder);
            CASViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_SoftUpdates);
            CASViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_OutOfOrder_SoftUpdates);
            CASViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts);
            CASViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_OutOfOrder);
            CASViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_SoftUpdates);
            CASViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_OutOfOrder_SoftUpdates);
            CASViaMiniKQLBody(client);
        }
    }

    void RowEraseViaMiniKQLBody(TClient &client) {
        NKikimrMiniKQL::TResult writeRes;
        const TString writeQuery = R"___(
            (
                (let table '/dc-1/Berkanavt/tables/Simple)
                (let row1 '('('key (Uint64 '2))))
                (let row2 '('('key (Uint64 '2305843009213693951))))
                (return (AsList
                    (UpdateRow table row1 '( '('uint (Uint64 '10))))
                    (UpdateRow table row2 '( '('uint (Uint64 '20)))))
                )
            )
            )___";

        UNIT_ASSERT(client.FlatQuery(writeQuery, writeRes));

        NKikimrMiniKQL::TResult updateRes;
        const TString updateQuery = R"___(
            (
                (let table '/dc-1/Berkanavt/tables/Simple)
                (let row1 '('('key (Uint64 '2))))
                (let row2 '('('key (Uint64 '2305843009213693951))))
                (return (AsList (EraseRow table row2)))
            )
        )___";

        UNIT_ASSERT(client.FlatQuery(updateQuery, updateRes));

        NKikimrMiniKQL::TResult readRes;
        const TString readQuery = R"___(
            (
                (let table '/dc-1/Berkanavt/tables/Simple)
                (let row1 '('('key (Uint64 '2))))
                (let row2 '('('key (Uint64 '2305843009213693951))))
                (let select '('uint))
                (let read1 (SelectRow table row1 select))
                (let read2 (SelectRow table row2 select))
                (let emptyList (List (ListType(VoidType))))
                (return (Extend
                    (IfPresent read1 (lambda '(x) (IfPresent (Member x 'uint) (lambda '(z) (AsList (SetResult 'row1 z))) emptyList )) emptyList)
                    (IfPresent read2 (lambda '(x) (IfPresent (Member x 'uint) (lambda '(z) (AsList (SetResult 'row2 z))) emptyList )) emptyList)
                ))
            )
        )___";

        UNIT_ASSERT(client.FlatQuery(readQuery, readRes));

        {
            TValue value = TValue::Create(readRes.GetValue(), readRes.GetType());
            TValue row1Opt = value["row1"];
            TValue row2Opt = value["row2"];

            UNIT_ASSERT(row1Opt.HaveValue() && ui64(row1Opt) == 10);
            UNIT_ASSERT(!row2Opt.HaveValue());
        }
    }

    Y_UNIT_TEST(RowEraseViaMiniKQL) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();

        {
            TTestTables tables(client, TTestTables::OneShard_NoOpts);
            RowEraseViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_OutOfOrder);
            RowEraseViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_SoftUpdates);
            RowEraseViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_OutOfOrder_SoftUpdates);
            RowEraseViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts);
            RowEraseViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_OutOfOrder);
            RowEraseViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_SoftUpdates);
            RowEraseViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_OutOfOrder_SoftUpdates);
            RowEraseViaMiniKQLBody(client);
        }
    }

    void ReadRangeViaMiniKQLBody(TClient &client) {
        NKikimrMiniKQL::TResult writeRes;
        const TString writeQuery = R"___(
            (
                (let table '/dc-1/Berkanavt/tables/Simple)
                (let row1 '('('key (Uint64 '2))))
                (let row2 '('('key (Uint64 '2305843009213693951))))
                (return (AsList
                    (UpdateRow table row1 '( '('uint (Uint64 '10))))
                    (UpdateRow table row2 '( '('uint (Uint64 '20)))))
                )
            )
            )___";

        UNIT_ASSERT(client.FlatQuery(writeQuery, writeRes));

        NKikimrMiniKQL::TResult readRes;
        const TString readQuery = R"___(
            (
                (let table '/dc-1/Berkanavt/tables/Simple)
                (let range '('IncFrom '('key (Uint64 '0) (Void))))
                (let select '('uint 'key))
                (let options '())
                (let res (SelectRange table range select options))
                (let reslist (Member res 'List))
                (return (AsList
                    (SetResult 'list reslist)
                ))
            )
        )___";

        UNIT_ASSERT(client.FlatQuery(readQuery, readRes));

        {
            TValue value = TValue::Create(readRes.GetValue(), readRes.GetType());
            TValue list = value["list"];

            UNIT_ASSERT_VALUES_EQUAL(list.Size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(ui64(list[0]["uint"]), 10);
            UNIT_ASSERT_VALUES_EQUAL(ui64(list[0]["key"]), 2);
            UNIT_ASSERT_VALUES_EQUAL(ui64(list[1]["uint"]), 20);
            UNIT_ASSERT_VALUES_EQUAL(ui64(list[1]["key"]), 2305843009213693951);
        }
    }

    Y_UNIT_TEST(ReadRangeViaMiniKQL) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();

        {
            TTestTables tables(client, TTestTables::OneShard_NoOpts);
            ReadRangeViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_OutOfOrder);
            ReadRangeViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_SoftUpdates);
            ReadRangeViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_OutOfOrder_SoftUpdates);
            ReadRangeViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts);
            ReadRangeViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_OutOfOrder);
            ReadRangeViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_SoftUpdates);
            ReadRangeViaMiniKQLBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_OutOfOrder_SoftUpdates);
            ReadRangeViaMiniKQLBody(client);
        }
    }

    void SelectRangeOptionsBody(TClient &client) {
        NKikimrMiniKQL::TResult writeRes;
        const TString writeQuery = R"___(
            (
                (let table '/dc-1/Berkanavt/tables/Simple)
                (let row1 '('('key (Uint64 '2))))
                (let row2 '('('key (Uint64 '3))))
                (let row3 '('('key (Uint64 '2305843009213693951))))
                (let row4 '('('key (Uint64 '2305843009213693952))))
                (return (AsList
                    (UpdateRow table row1 '( '('uint (Uint64 '10))))
                    (UpdateRow table row2 '( '('uint (Uint64 '20))))
                    (UpdateRow table row3 '( '('uint (Uint64 '30))))
                    (UpdateRow table row4 '( '('uint (Uint64 '40))))
                ))
            )
            )___";

        UNIT_ASSERT(client.FlatQuery(writeQuery, writeRes));

        {
            NKikimrMiniKQL::TResult readRes;
            const TString readQuery = R"___(
                (
                    (let from (Parameter 'FROM (DataType 'Uint64)))
                    (let to (Parameter 'TO (DataType 'Uint64)))
                    (let table '/dc-1/Berkanavt/tables/Simple)
                    (let range '('IncFrom 'IncTo '('key from to)))
                    (let select '('uint 'key))
                    (let options '())
                    (let res (SelectRange table range select options))
                    (let reslist (Member res 'List))
                    (return (AsList
                        (SetResult 'list reslist)
                    ))
                )
            )___";

            const TString readParams = R"___(
                (
                    (let params (Parameters))
                    (let params (AddParameter params 'FROM (Uint64 '2)))
                    (let params (AddParameter params 'TO (Uint64 '2305843009213693952)))
                    (return params)
                )
            )___";

            UNIT_ASSERT(client.FlatQueryParams(readQuery, readParams, false, readRes));

            {
                TValue value = TValue::Create(readRes.GetValue(), readRes.GetType());
                TValue list = value["list"];

                UNIT_ASSERT_VALUES_EQUAL(list.Size(), 4);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[0]["uint"]), 10);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[0]["key"]), 2);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[1]["uint"]), 20);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[1]["key"]), 3);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[2]["uint"]), 30);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[2]["key"]), 2305843009213693951);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[3]["uint"]), 40);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[3]["key"]), 2305843009213693952);
            }
        }

        {
            TString binQuery;
            NKikimrMiniKQL::TResult readRes;

            const TString readQuery = R"___(
                (
                    (let from (Parameter 'FROM (DataType 'Uint64)))
                    (let to (Parameter 'TO (DataType 'Uint64)))
                    (let table '/dc-1/Berkanavt/tables/Simple)
                    (let range '('ExcFrom 'IncTo '('key from to)))
                    (let select '('uint 'key))
                    (let options '())
                    (let res (SelectRange table range select options))
                    (let reslist (Member res 'List))
                    (return (AsList
                        (SetResult 'list reslist)
                    ))
                )
            )___";

            UNIT_ASSERT(client.Compile(readQuery, binQuery));

            const TString readParams = R"___(
                (
                    (let params (Parameters))
                    (let params (AddParameter params 'FROM (Uint64 '2)))
                    (let params (AddParameter params 'TO (Uint64 '2305843009213693952)))
                    (return params)
                )
            )___";

            UNIT_ASSERT(client.FlatQueryParams(binQuery, readParams, true, readRes));

            {
                TValue value = TValue::Create(readRes.GetValue(), readRes.GetType());
                TValue list = value["list"];

                UNIT_ASSERT_VALUES_EQUAL(list.Size(), 3);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[0]["uint"]), 20);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[0]["key"]), 3);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[1]["uint"]), 30);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[1]["key"]), 2305843009213693951);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[2]["uint"]), 40);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[2]["key"]), 2305843009213693952);
            }
        }

        {
            NKikimrMiniKQL::TResult readRes;
            const TString readQuery = R"___(
                (
                    (let table '/dc-1/Berkanavt/tables/Simple)
                    (let range '('IncFrom 'ExcTo '('key (Uint64 '4) (Uint64 '2305843009213693952))))
                    (let select '('uint 'key))
                    (let options '())
                    (let res (SelectRange table range select options))
                    (let reslist (Member res 'List))
                    (return (AsList
                        (SetResult 'list reslist)
                    ))
                )
            )___";

            UNIT_ASSERT(client.FlatQuery(readQuery, readRes));

            {
                TValue value = TValue::Create(readRes.GetValue(), readRes.GetType());
                TValue list = value["list"];

                UNIT_ASSERT_VALUES_EQUAL(list.Size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[0]["uint"]), 30);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[0]["key"]), 2305843009213693951);
            }
        }

        {
            NKikimrMiniKQL::TResult readRes;
            const TString readQuery = R"___(
                (
                    (let table '/dc-1/Berkanavt/tables/Simple)
                    (let range '('ExcFrom 'IncTo '('key (Uint64 '3) (Uint64 '2305843009213693950))))
                    (let select '('uint 'key))
                    (let options '())
                    (let res (SelectRange table range select options))
                    (let reslist (Member res 'List))
                    (return (AsList
                        (SetResult 'list reslist)
                    ))
                )
            )___";

            UNIT_ASSERT(client.FlatQuery(readQuery, readRes));

            {
                TValue value = TValue::Create(readRes.GetValue(), readRes.GetType());
                TValue list = value["list"];
                UNIT_ASSERT_VALUES_EQUAL(list.Size(), 0);
            }
        }
    }

    Y_UNIT_TEST(SelectRangeOptions) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);
        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();

        {
            TTestTables tables(client, TTestTables::OneShard_NoOpts);
            SelectRangeOptionsBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts);
            SelectRangeOptionsBody(client);
        }
    }

    void MultiSelectBody(TClient &client, bool useFlatMap = false) {
        NKikimrMiniKQL::TResult writeRes;
        const TString writeQuery = R"___(
            (
                (let table '/dc-1/Berkanavt/tables/Simple)
                (let row1 '('('key (Uint64 '2))))
                (let row2 '('('key (Uint64 '3))))
                (let row3 '('('key (Uint64 '2305843009213693951))))
                (let row4 '('('key (Uint64 '2305843009213693952))))
                (return (AsList
                    (UpdateRow table row1 '( '('uint (Uint64 '10))))
                    (UpdateRow table row2 '( '('uint (Uint64 '20))))
                    (UpdateRow table row3 '( '('uint (Uint64 '30))))
                    (UpdateRow table row4 '( '('uint (Uint64 '40))))
                ))
            )
            )___";

        UNIT_ASSERT(client.FlatQuery(writeQuery, writeRes));

        {
            NKikimrMiniKQL::TResult readRes;
            const TString readQuery = useFlatMap ?
                R"___(
                (
                    (let list (Parameter 'LIST (ListType (DataType 'Uint64))))
                    (let table '/dc-1/Berkanavt/tables/Simple)
                    (let select '('uint 'key))
                    (let results (FlatMapParameter list (lambda '(item) (block '(
                        (let row '('('key item)))
                        (let res (AsList (SelectRow table row select)))
                        (return res)
                    )))))
                    (return (AsList (SetResult 'list (FlatMap results (lambda '(item) item)))))
                )
            )___" :
            R"___(
                (
                    (let list (Parameter 'LIST (ListType (DataType 'Uint64))))
                    (let table '/dc-1/Berkanavt/tables/Simple)
                    (let select '('uint 'key))
                    (let results (MapParameter list (lambda '(item) (block '(
                        (let row '('('key item)))
                        (let res (SelectRow table row select))
                        (return res)
                    )))))
                    (return (AsList (SetResult 'list (FlatMap results (lambda '(item) item)))))
                )
            )___";

            const TString readParams = R"___(
                (
                    (let params (Parameters))
                    (let params (AddParameter params 'LIST (AsList (Uint64 '2) (Uint64 '4))))
                    (return params)
                )
            )___";

            UNIT_ASSERT(client.FlatQueryParams(readQuery, readParams, false, readRes));

            {
                TValue value = TValue::Create(readRes.GetValue(), readRes.GetType());
                TValue list = value["list"];

                UNIT_ASSERT_VALUES_EQUAL(list.Size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[0]["uint"]), 10);
                UNIT_ASSERT_VALUES_EQUAL(ui64(list[0]["key"]), 2);
            }
        }
    }

    Y_UNIT_TEST(TestMultiSelect) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();

        {
            TTestTables tables(client, TTestTables::OneShard_NoOpts);
            MultiSelectBody(client);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_OutOfOrder);
            MultiSelectBody(client);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_SoftUpdates);
            MultiSelectBody(client);
        }

        {
            TTestTables tables(client, TTestTables::OneShard_OutOfOrder_SoftUpdates);
            MultiSelectBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts);
            MultiSelectBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_OutOfOrder);
            MultiSelectBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_SoftUpdates);
            MultiSelectBody(client);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_OutOfOrder_SoftUpdates);
            MultiSelectBody(client);
        }
    }

    Y_UNIT_TEST(TestMultiSelectFlat) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();

        {
            TTestTables tables(client, TTestTables::OneShard_NoOpts);
            MultiSelectBody(client, true);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts);
            MultiSelectBody(client, true);
        }
    }

    void PrepareTestData(TClient& client, bool allowFollowerPromotion) {
        client.InitRootScheme();
        client.MkDir("/dc-1", "Berkanavt");
        client.MkDir("/dc-1/Berkanavt", "tables");
        client.CreateTable(TablePlacement, Sprintf(
                            R"___(
                                Name: "Simple"
                                Columns { Name: "key"        Type: "Uint64"}
                                Columns { Name: "uint"       Type: "Uint64"}
                                KeyColumnNames: ["key"]
                                PartitionConfig { FollowerCount: 1 AllowFollowerPromotion: %s }
                            )___", allowFollowerPromotion ? "true": "false"));

        NKikimrMiniKQL::TResult writeRes;
        const TString writeQuery = R"___(
            (
                (let table '/dc-1/Berkanavt/tables/Simple)
                (let row1 '('('key (Uint64 '2))))
                (return (AsList
                    (UpdateRow table row1 '( '('uint (Uint64 '10))))
                ))
            )
            )___";

        UNIT_ASSERT(client.FlatQuery(writeQuery, writeRes));
    }

    void CheckRead(TClient& client, bool readFromFollower) {
        const TString readQuery = Sprintf(
            R"___(
            (
                (let key '('('key (Uint64 '2))))
                (let columns '('uint 'key))
                (let row (SelectRow '/dc-1/Berkanavt/tables/Simple key columns %s))
                (return (AsList (SetResult 'row row)))
            )
            )___", readFromFollower ? "'follower" : "");

        NKikimrMiniKQL::TResult readRes;
        UNIT_ASSERT(client.FlatQuery(readQuery, readRes));

        {
            //Cerr << readRes << Endl;
            TValue value = TValue::Create(readRes.GetValue(), readRes.GetType());
            TValue row = value["row"];
            UNIT_ASSERT_VALUES_EQUAL(2, row.Size());
            UNIT_ASSERT_VALUES_EQUAL(10, ui64(row["uint"]));
            UNIT_ASSERT_VALUES_EQUAL(2, ui64(row["key"]));
        }
    }

    void WaitForLeaderStart(TClient& client, TTestActorRuntime* runtime, ui64 tabletId, const TDuration& timeout) {
        if (client.WaitForTabletAlive(runtime, tabletId, true, timeout))
            return;

        UNIT_ASSERT(!"Timeout expired while waiting for leader start");
    }

    void WaitForFollowerStart(TClient& client, TTestActorRuntime* runtime, ui64 tabletId, const TDuration& timeout) {
        if (client.WaitForTabletAlive(runtime, tabletId, false, timeout))
            return;

        UNIT_ASSERT(!"Timeout expired while waiting for follower start");
    }

    Y_UNIT_TEST(ReadFromFollower) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        auto settings = TServerSettings(port);
        settings.SetNodeCount(2);
        TServer server(settings);
        TClient client(settings);

        SetupLogging(server);

        PrepareTestData(client, false);
        CheckRead(client, true);
    }

    Y_UNIT_TEST(FollowerCacheRefresh) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        auto settings = TServerSettings(port);
        settings.SetNodeCount(2);
        TServer server(settings);
        TClient client(settings);

        server.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        client.MarkNodeInHive(server.GetRuntime(), 1, false);

        PrepareTestData(client, true);

        client.MarkNodeInHive(server.GetRuntime(), 1, true);

        const ui64 tabletId = 72075186224037888ull;
        WaitForLeaderStart(client, server.GetRuntime(), tabletId, TDuration::Seconds(5));
        WaitForFollowerStart(client, server.GetRuntime(), tabletId, TDuration::Seconds(5));

        // force some rounds of compaction
        for (ui32 i : xrange(50, 3000)) {
            NKikimrMiniKQL::TResult writeRes;
            const TString writeQuery = "((let table '/dc-1/Berkanavt/tables/Simple)"
                "(let row1 '('('key (Uint64 '" + ToString(i) + "))))"
                "(return (AsList"
                    "(UpdateRow table row1 '( '('uint (Uint64 '" + ToString(i) + "))))"
                ")))";

            UNIT_ASSERT(client.FlatQuery(writeQuery, writeRes));
        }

        Cout << "Read from leader" << Endl;
        CheckRead(client, false);
        Cout << "Read from leader again" << Endl;
        CheckRead(client, false);
        Cout << "Read from follower" << Endl;
        CheckRead(client, true);
    }

    Y_UNIT_TEST(FollowerDrop) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        auto settings = TServerSettings(port);
        settings.SetNodeCount(1);
        TServer server(settings);
        TClient client(settings);

        server.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);

        PrepareTestData(client, false);

        const ui64 tabletId = 72075186224037888ull;
        WaitForLeaderStart(client, server.GetRuntime(), tabletId, TDuration::Seconds(5));
        WaitForFollowerStart(client, server.GetRuntime(), tabletId, TDuration::Seconds(5));

        NTabletPipe::TClientConfig pipeClientConfig;
        pipeClientConfig.ForceFollower = true;
        server.GetRuntime()->SendToPipe(tabletId, TActorId(), new TEvents::TEvPoisonPill(), 0, pipeClientConfig);

        client.AlterTable(TablePlacement, Sprintf(
                            R"___(
                                Name: "Simple"
                                Columns { Name: "key"        Type: "Uint64"}
                                Columns { Name: "uint"       Type: "Uint64"}
                                KeyColumnNames: ["key"]
                                PartitionConfig { FollowerCount: 0 }
                            )___"));

        // force some rounds of compaction
        for (ui32 i : xrange(50, 3000)) {
            NKikimrMiniKQL::TResult writeRes;
            const TString writeQuery = "((let table '/dc-1/Berkanavt/tables/Simple)"
                "(let row1 '('('key (Uint64 '" + ToString(i) + "))))"
                "(return (AsList"
                "(UpdateRow table row1 '( '('uint (Uint64 '" + ToString(i) + "))))"
                ")))";

            UNIT_ASSERT(client.FlatQuery(writeQuery, writeRes));
        }
    }

    Y_UNIT_TEST(PromoteFollower) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        auto settings = TServerSettings(port);
        settings.SetNodeCount(2);

        TServer server(settings);
        TClient client(settings);

        PrepareTestData(client, true);

        SetupLogging(server);

        const ui64 tabletId = 72075186224037888ull;

        ui32 leaderNode = client.GetLeaderNode(server.GetRuntime(), tabletId);

        WaitForLeaderStart(client, server.GetRuntime(), tabletId, TDuration::Seconds(5));
        WaitForFollowerStart(client, server.GetRuntime(), tabletId, TDuration::Seconds(5));

        TVector<ui32> followerNodes = client.GetFollowerNodes(server.GetRuntime(), tabletId);
        while (followerNodes.empty()) {
            followerNodes = client.GetFollowerNodes(server.GetRuntime(), tabletId);
        }
        UNIT_ASSERT_VALUES_EQUAL(1, followerNodes.size());
        UNIT_ASSERT_VALUES_UNEQUAL(leaderNode, followerNodes[0]);

        Cout << "Read from follower" << Endl;
        CheckRead(client, true);
        Cout << "Read from leader" << Endl;
        CheckRead(client, false);

        Cout << "Disable node, leader should move back" << Endl;
        client.MarkNodeInHive(server.GetRuntime(), leaderNode, false);
        client.KickNodeInHive(server.GetRuntime(), leaderNode);
        WaitForLeaderStart(client, server.GetRuntime(), tabletId, TDuration::Seconds(5));

        Cout << "Read from new leader" << Endl;
        CheckRead(client, false);
        ui32 newLeaderNode = client.GetLeaderNode(server.GetRuntime(), tabletId);
        UNIT_ASSERT_VALUES_UNEQUAL_C(newLeaderNode, leaderNode, "Leader has moved");

        Cout << "Reenable node, follower should start there" << Endl;
        client.MarkNodeInHive(server.GetRuntime(), leaderNode, true);
        WaitForFollowerStart(client, server.GetRuntime(), tabletId, TDuration::Seconds(5));
        followerNodes = client.GetFollowerNodes(server.GetRuntime(), tabletId);

        CheckRead(client, true);
    }

    void DiagnosticsBody(TClient &client, bool testWrite, bool allowFollower = false) {
        TString query;
        if (testWrite) {
            query = R"((
                (let key_ '('('key (Uint64 '0))))
                (let value_ '('('uint (Uint64 '10))))
                (return (AsList
                    (UpdateRow '/dc-1/Berkanavt/tables/Simple key_ value_)
                    (Diagnostics)
                ))
            ))";
        } else {
            query = Sprintf(R"((
                (let key_ '('('key (Uint64 '0))))
                (let select_ '('uint))
                (let result_ (SelectRow '/dc-1/Berkanavt/tables/Simple key_ select_ %s))
                (return (AsList
                    (SetResult 'res result_)
                    (Diagnostics)
                ))
            ))", allowFollower ? "'follower" : "");
        }

        NKikimrMiniKQL::TResult result;
        UNIT_ASSERT(client.FlatQuery(query, result));

        {
            //Cerr << result.DebugString() << Endl;

            TVector<TTxInfo> infos;
            ExtractResultInfo(result, infos);

            for (const auto& txInfo : infos) {
                txInfo.PrintTabletInfo();
                txInfo.PrintTxInfo();
            }

            UNIT_ASSERT_EQUAL(infos.size(), 1);
            UNIT_ASSERT(allowFollower || (infos[0].IsFollower == false));
        }
    }

    Y_UNIT_TEST(Diagnostics) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts);
            DiagnosticsBody(client, true);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts, 1);
            DiagnosticsBody(client, true);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts);
            DiagnosticsBody(client, false);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts, 1);
            DiagnosticsBody(client, false);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts);
            DiagnosticsBody(client, false, true);
        }

        {
            TTestTables tables(client, TTestTables::Sharded_NoOpts, 1);
            DiagnosticsBody(client, false, true);
        }
    }

    // local

    TString DiffStrings(const TString& newStr, const TString& oldStr) {
        TVector<NDiff::TChunk<char>> chunks;
        NDiff::InlineDiff(chunks, newStr, oldStr, "\n");

        TString res;
        TStringOutput out(res);
        for (const auto& c : chunks) {
            TString left(c.Left.begin(), c.Left.end());
            TString right(c.Right.begin(), c.Right.end());
            if (!left.empty() || !right.empty()) {
                out  << ">>>>>" << Endl
                     << left << Endl
                     << "=====" << Endl
                     << right << Endl
                     << "<<<<<" << Endl;
            }
        }
        return res;
    }

    TString ToString(const NTabletFlatScheme::TSchemeChanges& scheme) {
        TString str;
        ::google::protobuf::TextFormat::PrintToString(scheme, &str);
        return str;
    }

    Y_UNIT_TEST(LocalSchemeTxRead) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        NTabletFlatScheme::TSchemeChanges scheme1;
        NTabletFlatScheme::TSchemeChanges scheme2;
        TString err;
        bool success = client.LocalSchemeTx(Tests::Hive, "", true, scheme1, err);
        UNIT_ASSERT(success);
        success = client.LocalSchemeTx(Tests::Hive, "", false, scheme2, err);
        UNIT_ASSERT(success);

        UNIT_ASSERT_VALUES_EQUAL(ToString(scheme1), ToString(scheme2));
    }

    Y_UNIT_TEST(LocalSchemeTxModify) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        NTabletFlatScheme::TSchemeChanges scheme;
        TString err;
        bool success = false;

        success = client.LocalSchemeTx(Tests::Hive, "", true, scheme, err);
        UNIT_ASSERT(success);
        TString oldScheme = ToString(scheme);

        TString change =  R"___(
                Delta {
                    DeltaType: AddColumn
                    TableId: 10
                    ColumnId: 1001
                    ColumnName: "NewColumn"
                    ColumnType: 2
                }
                Delta {
                    DeltaType: UpdateExecutorInfo
                    ExecutorCacheSize: 10000000
                }
                )___";

        // Dry run first
        success = client.LocalSchemeTx(Tests::Hive, change, true, scheme, err);
        UNIT_ASSERT(success);
        TString dryRunScheme = ToString(scheme);
        // Re-read
        success = client.LocalSchemeTx(Tests::Hive, "", true, scheme, err);
        TString newScheme = ToString(scheme);
        UNIT_ASSERT_VALUES_EQUAL_C(newScheme, oldScheme, "Schema changed by dry-run");

        // Update
        success = client.LocalSchemeTx(Tests::Hive, change, false, scheme, err);
        UNIT_ASSERT(success);
        newScheme = ToString(scheme);
        UNIT_ASSERT_VALUES_EQUAL_C(newScheme, dryRunScheme, "Dry-run result is not equal");

        TString schemaDiff = DiffStrings(oldScheme, newScheme);
        Cout << schemaDiff << Endl;
        UNIT_ASSERT_C(!schemaDiff.empty(), "Schema not changed after update");
    }

    Y_UNIT_TEST(LocalSchemeDropTable) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        server.StartDummyTablets();
        WaitForLeaderStart(client, server.GetRuntime(), ChangeStateStorage(Tests::DummyTablet1, TestDomain), TDuration::Seconds(1));

        NTabletFlatScheme::TSchemeChanges schemeInitial;
        TString err;
        bool success = false;

        success = client.LocalSchemeTx(Tests::DummyTablet1, "", true, schemeInitial, err);
        UNIT_ASSERT(success);
        const TString oldScheme = ToString(schemeInitial);

        TString change =  R"___(
                Delta {
                    DeltaType: DropTable
                    TableId: 32
                }
                )___";

        // Update
        NTabletFlatScheme::TSchemeChanges schemeChanged;
        success = client.LocalSchemeTx(Tests::DummyTablet1, change, false, schemeChanged, err);
        UNIT_ASSERT(success);
        const TString newScheme = ToString(schemeChanged);

        TString schemaDiff = DiffStrings(oldScheme, newScheme);
        UNIT_ASSERT_C(!schemaDiff.empty(), "Schema not changed after update");
    }

    Y_UNIT_TEST(TestOldTypes) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();
        CreateOldTypesTables(client);

        const char * writeOldTypes = R"((
            (let row_ '('('key (Uint32 '42)) '('strKey (ByteString 'old)) '('utf8Key (Utf8String 'old))))
            (let update_ '('('strData (ByteString 'old)) '('utf8Data (Utf8String 'old))))
            (let result_ (UpdateRow '%s/%s row_ update_))
            (return (AsList result_))
        ))";

        const char * writeNewTypes = R"((
            (let row_ '('('key (Uint32 '42)) '('strKey (String 'new)) '('utf8Key (Utf8 'new))))
            (let update_ '('('strData (String 'new)) '('utf8Data (Utf8 'new))))
            (let result_ (UpdateRow '%s/%s row_ update_))
            (return (AsList result_))
        ))";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT(client.FlatQuery(Sprintf(writeOldTypes, TablePlacement.data(), "Old"), res));
        UNIT_ASSERT(client.FlatQuery(Sprintf(writeOldTypes, TablePlacement.data(), "New"), res));
        UNIT_ASSERT(client.FlatQuery(Sprintf(writeNewTypes, TablePlacement.data(), "Old"), res));
        UNIT_ASSERT(client.FlatQuery(Sprintf(writeNewTypes, TablePlacement.data(), "New"), res));

        const char * readOldTypes = R"((
            (let row_ '('('key (Uint32 '42)) '('strKey (ByteString 'old)) '('utf8Key (Utf8String 'old))))
            (let select_ '('strData 'utf8Data))
            (let result_ (SelectRow '%s/%s row_ select_))
            (return (AsList (SetResult 'res result_)))
        ))";

        const char * readNewTypes = R"((
            (let row_ '('('key (Uint32 '42)) '('strKey (String 'old)) '('utf8Key (Utf8 'old))))
            (let select_ '('strData 'utf8Data))
            (let result_ (SelectRow '%s/%s row_ select_))
            (return (AsList (SetResult 'res result_)))
        ))";

        UNIT_ASSERT(client.FlatQuery(Sprintf(readOldTypes, TablePlacement.data(), "Old"), res));
        UNIT_ASSERT(client.FlatQuery(Sprintf(readOldTypes, TablePlacement.data(), "New"), res));
        UNIT_ASSERT(client.FlatQuery(Sprintf(readNewTypes, TablePlacement.data(), "Old"), res));
        UNIT_ASSERT(client.FlatQuery(Sprintf(readNewTypes, TablePlacement.data(), "New"), res));

        // TODO: check resluts

        const char * rangeOldTypes = R"((
            (let $1 '('key (Uint32 '42) (Uint32 '42)))
            (let $2 '('strKey (ByteString 'old) (ByteString 'old)))
            (let $3 '('utf8Key (Utf8String 'old) (Utf8String 'old)))
            (let range_ '('IncFrom 'IncTo $1 $2 $3))
            (let select_ '('strData 'utf8Data))
            (let result_ (SelectRange '%s/%s range_ select_ '()))
            (return (AsList (SetResult 'res result_)))
        ))";

        const char * rangeNewTypes = R"((
            (let $1 '('key (Uint32 '42) (Uint32 '42)))
            (let $2 '('strKey (String 'old) (String 'old)))
            (let $3 '('utf8Key (Utf8 'old) (Utf8 'old)))
            (let range_ '('IncFrom 'IncTo $1 $2 $3))
            (let select_ '('strData 'utf8Data))
            (let result_ (SelectRange '%s/%s range_ select_ '()))
            (return (AsList (SetResult 'res result_)))
        ))";

        UNIT_ASSERT(client.FlatQuery(Sprintf(rangeOldTypes, TablePlacement.data(), "Old"), res));
        UNIT_ASSERT(client.FlatQuery(Sprintf(rangeOldTypes, TablePlacement.data(), "New"), res));
        UNIT_ASSERT(client.FlatQuery(Sprintf(rangeNewTypes, TablePlacement.data(), "Old"), res));
        UNIT_ASSERT(client.FlatQuery(Sprintf(rangeNewTypes, TablePlacement.data(), "New"), res));

        // TODO: check resluts
    }

    Y_UNIT_TEST(TestOldTypeParams) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);

        client.InitRootScheme();
        CreateOldTypesTables(client);

        const char * paramsOld = R"((
            (let params (Parameters))
            (let params (AddParameter params 'STR_KEY (ByteString 'old)))
            (let params (AddParameter params 'UTF8_KEY (Utf8String 'old)))
            (return params)
        ))";

        const char * paramsNew = R"((
            (let params (Parameters))
            (let params (AddParameter params 'STR_KEY (String 'old)))
            (let params (AddParameter params 'UTF8_KEY (Utf8 'old)))
            (return params)
        ))";

        const char * writeOldTypes = R"((
            (let strKey_ (Parameter 'STR_KEY (DataType 'ByteString)))
            (let utf8Key_ (Parameter 'UTF8_KEY (DataType 'Utf8String)))
            (let row_ '('('key (Uint32 '42)) '('strKey strKey_) '('utf8Key utf8Key_)))
            (let update_ '('('strData (ByteString 'old)) '('utf8Data (Utf8String 'old))))
            (let result_ (UpdateRow '%s/%s row_ update_))
            (return (AsList result_))
        ))";

        const char * writeNewTypes = R"((
            (let strKey_ (Parameter 'STR_KEY (DataType 'String)))
            (let utf8Key_ (Parameter 'UTF8_KEY (DataType 'Utf8)))
            (let row_ '('('key (Uint32 '42)) '('strKey (String 'new)) '('utf8Key (Utf8 'new))))
            (let update_ '('('strData (String 'new)) '('utf8Data (Utf8 'new))))
            (let result_ (UpdateRow '%s/%s row_ update_))
            (return (AsList result_))
        ))";

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(writeOldTypes, TablePlacement.data(), "Old"), paramsOld, false, res));
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(writeOldTypes, TablePlacement.data(), "New"), paramsOld, false, res));
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(writeNewTypes, TablePlacement.data(), "Old"), paramsOld, false, res));
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(writeNewTypes, TablePlacement.data(), "New"), paramsOld, false, res));
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(writeOldTypes, TablePlacement.data(), "Old"), paramsNew, false, res));
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(writeOldTypes, TablePlacement.data(), "New"), paramsNew, false, res));
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(writeNewTypes, TablePlacement.data(), "Old"), paramsNew, false, res));
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(writeNewTypes, TablePlacement.data(), "New"), paramsNew, false, res));

        const char * readOldTypes = R"((
            (let strKey_ (Parameter 'STR_KEY (DataType 'ByteString)))
            (let utf8Key_ (Parameter 'UTF8_KEY (DataType 'Utf8String)))
            (let row_ '('('key (Uint32 '42)) '('strKey strKey_) '('utf8Key utf8Key_)))
            (let select_ '('strData 'utf8Data))
            (let result_ (SelectRow '%s/%s row_ select_))
            (return (AsList (SetResult 'res result_)))
        ))";

        const char * readNewTypes = R"((
            (let strKey_ (Parameter 'STR_KEY (DataType 'ByteString)))
            (let utf8Key_ (Parameter 'UTF8_KEY (DataType 'Utf8String)))
            (let row_ '('('key (Uint32 '42)) '('strKey strKey_) '('utf8Key utf8Key_)))
            (let select_ '('strData 'utf8Data))
            (let result_ (SelectRow '%s/%s row_ select_))
            (return (AsList (SetResult 'res result_)))
        ))";

        UNIT_ASSERT(client.FlatQueryParams(Sprintf(readOldTypes, TablePlacement.data(), "Old"), paramsOld, false, res));
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(readOldTypes, TablePlacement.data(), "New"), paramsOld, false, res));
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(readNewTypes, TablePlacement.data(), "Old"), paramsOld, false, res));
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(readNewTypes, TablePlacement.data(), "New"), paramsOld, false, res));
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(readOldTypes, TablePlacement.data(), "Old"), paramsNew, false, res));
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(readOldTypes, TablePlacement.data(), "New"), paramsNew, false, res));
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(readNewTypes, TablePlacement.data(), "Old"), paramsNew, false, res));
        UNIT_ASSERT(client.FlatQueryParams(Sprintf(readNewTypes, TablePlacement.data(), "New"), paramsNew, false, res));

        // TODO: check resluts
    }

    void OfflineFollowerContinueWork() {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port);
        TServer server(settings);
        TClient client(settings);
        SetupLogging(server);

        TTestActorRuntime &runtime = *server.GetRuntime();

        const ui64 tabletId = ChangeStateStorage(DummyTablet1, settings.Domain);
        TIntrusivePtr<TTabletStorageInfo> tabletInfo = CreateTestTabletInfo(tabletId, TTabletTypes::Dummy);
        TIntrusivePtr<TTabletSetupInfo> setupInfo = new TTabletSetupInfo(&CreateFlatDummyTablet, TMailboxType::Simple, 0, TMailboxType::Simple, 0);

        const TActorId edge = runtime.AllocateEdgeActor();

        const TActorId leaderTablet = runtime.Register(CreateTablet(edge, tabletInfo.Get(), setupInfo.Get(), 0, nullptr, nullptr));
        const TActorId leaderId = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(edge)->Get()->UserTabletActor;

        const TActorId followerTablet = runtime.Register(CreateTabletFollower(edge, tabletInfo.Get(), setupInfo.Get(), 1, nullptr, nullptr));
        const TActorId followerId = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(edge)->Get()->UserTabletActor;
        Y_UNUSED(followerTablet);

        const char *writeQuery = R"__((
                (let row_ '('('key (Uint64 '42))))
                (let update_ '('('v_ui64 (Uint64 '%lu))))
                (let result_ (UpdateRow 't_by_ui64 row_ update_))
                (return (AsList result_))
            ))__";

        const char *readQuery = R"__((
                (let row_ '('('key (Uint64 '42))))
                (let select_ '('v_ui64))
                (let pgmReturn (AsList
                    (SetResult 'res (SelectRow 't_by_ui64 row_ select_))
                ))
                (return pgmReturn)
            ))__";

        {
            THolder<TEvTablet::TEvLocalMKQL> reqWrite = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqWrite->Record.MutableProgram()->MutableProgram()->SetText(Sprintf(writeQuery, ui64(10)));
            runtime.Send(new IEventHandle(leaderId, edge, reqWrite.Release()));

            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
        }

        {
            runtime.Send(new IEventHandle(leaderTablet, edge, new TEvents::TEvPoisonPill()));
            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvTabletDead>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->TabletID, tabletId);
        }

        {
            THolder<TEvTablet::TEvLocalMKQL> reqRead = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqRead->Record.MutableProgram()->MutableProgram()->SetText(readQuery);
            runtime.Send(new IEventHandle(followerId, edge, reqRead.Release()));

            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
        }

        {
            NTabletPipe::TClientConfig pipeClientConfig;
            pipeClientConfig.AllowFollower = true;
            pipeClientConfig.ForceFollower = true;
            runtime.Register(NTabletPipe::CreateClient(edge, tabletId, pipeClientConfig));

            auto reply = runtime.GrabEdgeEvent<TEvTabletPipe::TEvClientConnected>(edge);

            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Status, NKikimrProto::OK);
        }
    }

    Y_UNIT_TEST(OfflineFollowerContinueWork) {
        OfflineFollowerContinueWork();
    }

    void FollowerOfflineBoot() {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port)
                .SetUseRealThreads(false);
        TServer server(settings);
        TClient client(settings);
        SetupLogging(server);

        TTestActorRuntime &runtime = *server.GetRuntime();

        const ui64 tabletId = ChangeStateStorage(DummyTablet1, settings.Domain);
        TIntrusivePtr<TTabletStorageInfo> tabletInfo = CreateTestTabletInfo(tabletId, TTabletTypes::Dummy);
        TIntrusivePtr<TTabletSetupInfo> setupInfo = new TTabletSetupInfo(&CreateFlatDummyTablet, TMailboxType::Simple, 0, TMailboxType::Simple, 0);

        const TActorId edge = runtime.AllocateEdgeActor();

        {
            ui64 confirmationsCount = 0;
            auto observeConfirmations = [&](TAutoPtr<IEventHandle>& ev) {
                switch (ev->GetTypeRewrite()) {
                    case TEvBlobStorage::TEvPut::EventType: {
                        const auto* msg = ev->Get<TEvBlobStorage::TEvPut>();
                        // step 1 is snapshot
                        // step 2 is schema alter
                        // step 3 is expected write below
                        if (msg->Id.TabletID() == tabletId &&
                            msg->Id.Channel() == 0 &&
                            msg->Id.Cookie() == 1 &&
                            msg->Id.Step() > 2)
                        {
                            ++confirmationsCount;
                        }
                        break;
                    }
                }
                return TTestActorRuntime::EEventAction::PROCESS;
            };
            runtime.SetObserverFunc(observeConfirmations);

            const TActorId leaderTablet = runtime.Register(CreateTablet(edge, tabletInfo.Get(), setupInfo.Get(), 0, nullptr, nullptr));
            const TActorId leaderId = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(edge)->Get()->UserTabletActor;

            // we use it to kill leader only when it has sent the write to the follower and it is confirmed
            const TActorId followerTablet = runtime.Register(CreateTabletFollower(edge, tabletInfo.Get(), setupInfo.Get(), 1, nullptr, nullptr));

            auto doLeaderWrite = [&](ui64 key, ui64 value) {
                const char *writeQuery = R"__((
                        (let row_ '('('key (Uint64 '%lu))))
                        (let update_ '('('v_ui64 (Uint64 '%lu))))
                        (let result_ (UpdateRow 't_by_ui64 row_ update_))
                        (return (AsList result_))
                    ))__";

                THolder<TEvTablet::TEvLocalMKQL> reqWrite = MakeHolder<TEvTablet::TEvLocalMKQL>();
                reqWrite->Record.MutableProgram()->MutableProgram()->SetText(Sprintf(writeQuery, key, value));
                runtime.Send(new IEventHandle(leaderId, edge, reqWrite.Release()));

                auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(edge);
                UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
            };

            doLeaderWrite(42, 51);

            auto waitFor = [&](const auto& condition, const TString& description) {
                if (!condition()) {
                    Cerr << "... waiting for " << description << Endl;
                    TDispatchOptions options;
                    options.CustomFinalCondition = [&]() {
                        return condition();
                    };
                    runtime.DispatchEvents(options);
                    UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
                }
            };

            waitFor([&](){ return confirmationsCount > 0; }, "Write confirmed");

            runtime.Send(new IEventHandle(leaderTablet, edge, new TEvents::TEvPoisonPill()));
            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvTabletDead>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->TabletID, tabletId);

            runtime.Send(new IEventHandle(followerTablet, edge, new TEvents::TEvPoisonPill()));
            reply = runtime.GrabEdgeEvent<TEvTablet::TEvTabletDead>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->TabletID, tabletId);
        }

        // now we start follower without its leader

        const TActorId followerEdge = runtime.AllocateEdgeActor();
        const TActorId followerTablet = runtime.Register(CreateTabletFollower(followerEdge, tabletInfo.Get(), setupInfo.Get(), 1, nullptr, nullptr));
        Y_UNUSED(followerTablet);
        const TActorId followerId = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(followerEdge)->Get()->UserTabletActor;

        {
            NTabletPipe::TClientConfig pipeClientConfig;
            pipeClientConfig.AllowFollower = true;
            pipeClientConfig.ForceFollower = true;
            pipeClientConfig.RetryPolicy = {.RetryLimitCount = 2};
            runtime.Register(NTabletPipe::CreateClient(followerEdge, tabletId, pipeClientConfig));

            auto reply = runtime.GrabEdgeEvent<TEvTabletPipe::TEvClientConnected>(followerEdge);

            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Status, NKikimrProto::OK);
        }

        auto doFollowerRead = [&](ui64 key) -> TMaybe<ui64> {
            const char *readQuery = R"__((
                    (let row_ '('('key (Uint64 '%lu))))
                    (let select_ '('v_ui64))
                    (let pgmReturn (AsList
                        (SetResult 'res (SelectRow 't_by_ui64 row_ select_))
                    ))
                    (return pgmReturn)
                ))__";

            THolder<TEvTablet::TEvLocalMKQL> reqRead = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqRead->Record.MutableProgram()->MutableProgram()->SetText(Sprintf(readQuery, key));
            runtime.Send(new IEventHandle(followerId, followerEdge, reqRead.Release()));

            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(followerEdge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
            const auto res = reply->Get()->Record
                    .GetExecutionEngineEvaluatedResponse()
                    .GetValue()
                    .GetStruct(0)
                    .GetOptional();
            if (!res.HasOptional()) {
                return Nothing();
            }

            return res
                    .GetOptional()
                    .GetStruct(0)
                    .GetOptional()
                    .GetUint64();
        };

        // Perform basic sanity checks
        UNIT_ASSERT_VALUES_EQUAL(doFollowerRead(41), Nothing());
        UNIT_ASSERT_VALUES_EQUAL(doFollowerRead(42), 51u);
    }

    Y_UNIT_TEST(FollowerOfflineBoot) {
        FollowerOfflineBoot();
    }

    Y_UNIT_TEST(OfflineFollowerLastConfirmedCommitRestored) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port)
                .SetUseRealThreads(false);
        TServer server(settings);
        TClient client(settings);
        SetupLogging(server);

        TTestActorRuntime &runtime = *server.GetRuntime();

        const ui64 tabletId = ChangeStateStorage(DummyTablet1, settings.Domain);
        TIntrusivePtr<TTabletStorageInfo> tabletInfo = CreateTestTabletInfo(tabletId, TTabletTypes::Dummy);
        TIntrusivePtr<TTabletSetupInfo> setupInfo = new TTabletSetupInfo(&CreateFlatDummyTablet, TMailboxType::Simple, 0, TMailboxType::Simple, 0);

        const TActorId edge = runtime.AllocateEdgeActor();

        const TActorId leaderTablet = runtime.Register(CreateTablet(edge, tabletInfo.Get(), setupInfo.Get(), 0, nullptr, nullptr));
        const TActorId leaderId = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(edge)->Get()->UserTabletActor;

        const TActorId followerTablet = runtime.Register(CreateTabletFollower(edge, tabletInfo.Get(), setupInfo.Get(), 1, nullptr, nullptr));
        const TActorId followerId = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(edge)->Get()->UserTabletActor;

        const char *writeQuery = R"__((
                (let row_ '('('key (Uint64 '42))))
                (let update_ '('('v_ui64 (Uint64 '%lu))))
                (let result_ (UpdateRow 't_by_ui64 row_ update_))
                (return (AsList result_))
            ))__";

        const char *readQuery = R"__((
                (let row_ '('('key (Uint64 '42))))
                (let select_ '('v_ui64))
                (let pgmReturn (AsList
                    (SetResult 'res (SelectRow 't_by_ui64 row_ select_))
                ))
                (return pgmReturn)
            ))__";

        {
            THolder<TEvTablet::TEvLocalMKQL> reqWrite = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqWrite->Record.MutableProgram()->MutableProgram()->SetText(Sprintf(writeQuery, ui64(10)));
            runtime.Send(new IEventHandle(leaderId, edge, reqWrite.Release()));

            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
        }

        {
            THolder<TEvTablet::TEvLocalMKQL> reqRead = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqRead->Record.MutableProgram()->MutableProgram()->SetText(readQuery);
            runtime.Send(new IEventHandle(followerId, edge, reqRead.Release()));

            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
            // Cerr << reply->Get()->Record.DebugString() << Endl;
            const auto res = reply->Get()->Record
                    .GetExecutionEngineEvaluatedResponse()
                    .GetValue()
                    .GetStruct(0)
                    .GetOptional()
                    .GetOptional()
                    .GetStruct(0)
                    .GetOptional()
                    .GetUint64();
            UNIT_ASSERT_VALUES_EQUAL(res, ui64(10));
        }

        {
            // Kill leader
            runtime.Send(new IEventHandle(leaderTablet, edge, new TEvents::TEvPoisonPill()));
            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvTabletDead>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->TabletID, tabletId);
        }

        {
            // Kill follower
            runtime.Send(new IEventHandle(followerTablet, edge, new TEvents::TEvPoisonPill()));
            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvTabletDead>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->TabletID, tabletId);
        }

        // Start a new follower (without leader)
        const TActorId followerTablet2 = runtime.Register(CreateTabletFollower(edge, tabletInfo.Get(), setupInfo.Get(), 1, nullptr, nullptr));
        const TActorId followerId2 = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(edge)->Get()->UserTabletActor;
        Y_UNUSED(followerTablet2);

        {
            // Connect pipe to follower (indicates it's up and usable)
            NTabletPipe::TClientConfig pipeClientConfig;
            pipeClientConfig.AllowFollower = true;
            pipeClientConfig.ForceFollower = true;
            pipeClientConfig.RetryPolicy = {.RetryLimitCount = 2};
            runtime.Register(NTabletPipe::CreateClient(edge, tabletId, pipeClientConfig));

            auto reply = runtime.GrabEdgeEvent<TEvTabletPipe::TEvClientConnected>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Status, NKikimrProto::OK);
        }

        {
            // Read row from offline follower
            THolder<TEvTablet::TEvLocalMKQL> reqRead = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqRead->Record.MutableProgram()->MutableProgram()->SetText(readQuery);
            runtime.Send(new IEventHandle(followerId2, edge, reqRead.Release()));

            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
            // Cerr << reply->Get()->Record.DebugString() << Endl;
            const auto res = reply->Get()->Record
                    .GetExecutionEngineEvaluatedResponse()
                    .GetValue()
                    .GetStruct(0)
                    .GetOptional()
                    .GetOptional()
                    .GetStruct(0)
                    .GetOptional()
                    .GetUint64();
            UNIT_ASSERT_VALUES_EQUAL(res, ui64(10));
        }

    }

    Y_UNIT_TEST(OfflineFollowerLastUnconfirmedCommitIgnored) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port)
                .SetUseRealThreads(false);
        TServer server(settings);
        TClient client(settings);
        SetupLogging(server);

        TTestActorRuntime &runtime = *server.GetRuntime();

        const ui64 tabletId = ChangeStateStorage(DummyTablet1, settings.Domain);
        TIntrusivePtr<TTabletStorageInfo> tabletInfo = CreateTestTabletInfo(tabletId, TTabletTypes::Dummy);
        TIntrusivePtr<TTabletSetupInfo> setupInfo = new TTabletSetupInfo(&CreateFlatDummyTablet, TMailboxType::Simple, 0, TMailboxType::Simple, 0);

        const TActorId edge = runtime.AllocateEdgeActor();

        const TActorId leaderTablet = runtime.Register(CreateTablet(edge, tabletInfo.Get(), setupInfo.Get(), 0, nullptr, nullptr));
        const TActorId leaderId = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(edge)->Get()->UserTabletActor;

        const TActorId followerTablet = runtime.Register(CreateTabletFollower(edge, tabletInfo.Get(), setupInfo.Get(), 1, nullptr, nullptr));
        const TActorId followerId = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(edge)->Get()->UserTabletActor;

        const char *writeQuery = R"__((
                (let row_ '('('key (Uint64 '42))))
                (let update_ '('('v_ui64 (Uint64 '%lu))))
                (let result_ (UpdateRow 't_by_ui64 row_ update_))
                (return (AsList result_))
            ))__";

        const char *readQuery = R"__((
                (let row_ '('('key (Uint64 '42))))
                (let select_ '('v_ui64))
                (let pgmReturn (AsList
                    (SetResult 'res (SelectRow 't_by_ui64 row_ select_))
                ))
                (return pgmReturn)
            ))__";

        TDeque<THolder<IEventHandle>> blockedConfirmations;
        auto blockConfirmations = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::TEvPut::EventType: {
                    const auto* msg = ev->Get<TEvBlobStorage::TEvPut>();
                    // step 1 is snapshot
                    // step 2 is schema alter
                    // step 3 is expected write below
                    if (msg->Id.TabletID() == tabletId &&
                        msg->Id.Channel() == 0 &&
                        msg->Id.Cookie() == 1 &&
                        msg->Id.Step() > 2)
                    {
                        Cerr << "--- blocked confirmation commit: " << msg->Id << Endl;
                        blockedConfirmations.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(blockConfirmations);

        {
            THolder<TEvTablet::TEvLocalMKQL> reqWrite = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqWrite->Record.MutableProgram()->MutableProgram()->SetText(Sprintf(writeQuery, ui64(10)));
            runtime.Send(new IEventHandle(leaderId, edge, reqWrite.Release()));

            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
        }

        {
            THolder<TEvTablet::TEvLocalMKQL> reqRead = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqRead->Record.MutableProgram()->MutableProgram()->SetText(readQuery);
            runtime.Send(new IEventHandle(followerId, edge, reqRead.Release()));

            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
            // Cerr << reply->Get()->Record.DebugString() << Endl;
            const auto res = reply->Get()->Record
                    .GetExecutionEngineEvaluatedResponse()
                    .GetValue()
                    .GetStruct(0)
                    .GetOptional();
            UNIT_ASSERT(!res.HasOptional());
        }

        {
            // Kill leader
            runtime.Send(new IEventHandle(leaderTablet, edge, new TEvents::TEvPoisonPill()));
            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvTabletDead>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->TabletID, tabletId);
        }

        {
            // Kill follower
            runtime.Send(new IEventHandle(followerTablet, edge, new TEvents::TEvPoisonPill()));
            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvTabletDead>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->TabletID, tabletId);
        }

        // Start a new follower (without leader)
        const TActorId followerTablet2 = runtime.Register(CreateTabletFollower(edge, tabletInfo.Get(), setupInfo.Get(), 1, nullptr, nullptr));
        const TActorId followerId2 = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(edge)->Get()->UserTabletActor;
        Y_UNUSED(followerTablet2);

        {
            // Connect pipe to follower (indicates it's up and usable)
            NTabletPipe::TClientConfig pipeClientConfig;
            pipeClientConfig.AllowFollower = true;
            pipeClientConfig.ForceFollower = true;
            pipeClientConfig.RetryPolicy = {.RetryLimitCount = 2};
            runtime.Register(NTabletPipe::CreateClient(edge, tabletId, pipeClientConfig));

            auto reply = runtime.GrabEdgeEvent<TEvTabletPipe::TEvClientConnected>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Status, NKikimrProto::OK);
        }

        {
            // Read row from offline follower (unconfirmed commit must be ignored)
            THolder<TEvTablet::TEvLocalMKQL> reqRead = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqRead->Record.MutableProgram()->MutableProgram()->SetText(readQuery);
            runtime.Send(new IEventHandle(followerId2, edge, reqRead.Release()));

            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
            // Cerr << reply->Get()->Record.DebugString() << Endl;
            const auto res = reply->Get()->Record
                    .GetExecutionEngineEvaluatedResponse()
                    .GetValue()
                    .GetStruct(0)
                    .GetOptional();
            UNIT_ASSERT_C(!res.HasOptional(), "Unexpected result: " << res.DebugString());
        }

        // Unblock previously blocked puts
        runtime.SetObserverFunc(prevObserverFunc);
        for (auto& ev : blockedConfirmations) {
            runtime.Send(ev.Release(), 0, /* viaActorSystem */ true);
        }

        {
            // Kill follower 2
            runtime.Send(new IEventHandle(followerTablet2, edge, new TEvents::TEvPoisonPill()));
            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvTabletDead>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->TabletID, tabletId);
        }

        // Start a new follower (without leader)
        const TActorId followerTablet3 = runtime.Register(CreateTabletFollower(edge, tabletInfo.Get(), setupInfo.Get(), 1, nullptr, nullptr));
        const TActorId followerId3 = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(edge)->Get()->UserTabletActor;
        Y_UNUSED(followerTablet3);

        {
            // Connect pipe to follower (indicates it's up and usable)
            NTabletPipe::TClientConfig pipeClientConfig;
            pipeClientConfig.AllowFollower = true;
            pipeClientConfig.ForceFollower = true;
            pipeClientConfig.RetryPolicy = {.RetryLimitCount = 2};
            runtime.Register(NTabletPipe::CreateClient(edge, tabletId, pipeClientConfig));

            auto reply = runtime.GrabEdgeEvent<TEvTabletPipe::TEvClientConnected>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Status, NKikimrProto::OK);
        }

        {
            // Read row from offline follower
            THolder<TEvTablet::TEvLocalMKQL> reqRead = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqRead->Record.MutableProgram()->MutableProgram()->SetText(readQuery);
            runtime.Send(new IEventHandle(followerId3, edge, reqRead.Release()));

            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
            // Cerr << reply->Get()->Record.DebugString() << Endl;
            const auto res = reply->Get()->Record
                    .GetExecutionEngineEvaluatedResponse()
                    .GetValue()
                    .GetStruct(0)
                    .GetOptional()
                    .GetOptional()
                    .GetStruct(0)
                    .GetOptional()
                    .GetUint64();
            UNIT_ASSERT_VALUES_EQUAL(res, ui64(10));
        }

    }

    Y_UNIT_TEST(DeadFollowerDoesNotBlockGc) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port)
                .SetNodeCount(2)
                .SetUseRealThreads(false);
        TServer server(settings);
        TClient client(settings);
        SetupLogging(server);

        TTestActorRuntime &runtime = *server.GetRuntime();

        const ui64 tabletId = ChangeStateStorage(DummyTablet1, settings.Domain);
        TIntrusivePtr<TTabletStorageInfo> tabletInfo = CreateTestTabletInfo(tabletId, TTabletTypes::Dummy);
        TIntrusivePtr<TTabletSetupInfo> setupInfo = new TTabletSetupInfo(&CreateFlatDummyTablet, TMailboxType::Simple, 0, TMailboxType::Simple, 0);

        // Start leader on node 1
        const TActorId leaderEdge = runtime.AllocateEdgeActor();
        const TActorId leaderTablet = runtime.Register(CreateTablet(leaderEdge, tabletInfo.Get(), setupInfo.Get(), 0, nullptr, nullptr));
        const TActorId leaderId = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(leaderEdge)->Get()->UserTabletActor;
        Y_UNUSED(leaderTablet);

        auto doLeaderWrite = [&](ui64 key, ui64 value) {
            const char *writeQuery = R"__((
                    (let row_ '('('key (Uint64 '%lu))))
                    (let update_ '('('v_ui64 (Uint64 '%lu))))
                    (let result_ (UpdateRow 't_by_ui64 row_ update_))
                    (return (AsList result_))
                ))__";

            THolder<TEvTablet::TEvLocalMKQL> reqWrite = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqWrite->Record.MutableProgram()->MutableProgram()->SetText(Sprintf(writeQuery, key, value));
            runtime.Send(new IEventHandle(leaderId, leaderEdge, reqWrite.Release()));

            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(leaderEdge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
        };

        doLeaderWrite(42, 51);

        // Start follower on node 2
        const TActorId followerEdge = runtime.AllocateEdgeActor(1);
        const TActorId followerTablet = runtime.Register(CreateTabletFollower(followerEdge, tabletInfo.Get(), setupInfo.Get(), 1, nullptr, nullptr), 1);
        const TActorId followerId = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(followerEdge)->Get()->UserTabletActor;

        auto doFollowerRead = [&](ui64 key) -> TMaybe<ui64> {
            const char *readQuery = R"__((
                    (let row_ '('('key (Uint64 '%lu))))
                    (let select_ '('v_ui64))
                    (let pgmReturn (AsList
                        (SetResult 'res (SelectRow 't_by_ui64 row_ select_))
                    ))
                    (return pgmReturn)
                ))__";

            THolder<TEvTablet::TEvLocalMKQL> reqRead = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqRead->Record.MutableProgram()->MutableProgram()->SetText(Sprintf(readQuery, key));
            runtime.Send(new IEventHandle(followerId, followerEdge, reqRead.Release()), 1);

            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(followerEdge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
            // Cerr << reply->Get()->Record.DebugString() << Endl;
            const auto res = reply->Get()->Record
                    .GetExecutionEngineEvaluatedResponse()
                    .GetValue()
                    .GetStruct(0)
                    .GetOptional();
            if (!res.HasOptional()) {
                return Nothing();
            }

            return res
                    .GetOptional()
                    .GetStruct(0)
                    .GetOptional()
                    .GetUint64();
        };

        // Perform basic sanity checks
        UNIT_ASSERT_VALUES_EQUAL(doFollowerRead(41), Nothing());
        UNIT_ASSERT_VALUES_EQUAL(doFollowerRead(42), 51u);

        ui64 detachCounter = 0;
        ui64 gcWaitCounter = 0;
        ui64 gcAppliedCounter = 0;
        auto nemesis = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvTablet::TEvFollowerDetach::EventType: {
                    // Drop TEvFollowerDetach and simulate interconnect disconnection
                    Cerr << "... dropping TEvFollowerDetach" << Endl;
                    ++detachCounter;
                    runtime.Send(
                        new IEventHandle(
                            runtime.GetInterconnectProxy(0, 1),
                            { },
                            new TEvInterconnect::TEvDisconnect()),
                        0, true);
                    return TTestActorRuntime::EEventAction::DROP;
                }
                case TEvTablet::TEvCommit::EventType: {
                    const auto* msg = ev->Get<TEvTablet::TEvCommit>();
                    if (msg->TabletID == tabletId && msg->WaitFollowerGcAck) {
                        Cerr << "... observing TEvCommit at "
                            << msg->Generation << ':' << msg->Step
                            << " with WaitFollowerGcAck" << Endl;
                        ++gcWaitCounter;
                    }
                    break;
                }
                case TEvTablet::TEvFollowerGcApplied::EventType: {
                    const auto* msg = ev->Get<TEvTablet::TEvFollowerGcApplied>();
                    if (msg->TabletID == tabletId) {
                        Cerr << "... observing TEvFollowerGcApplied at "
                            << msg->Generation << ':' << msg->Step
                            << Endl;
                        ++gcAppliedCounter;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(nemesis);

        {
            // Kill follower
            Cerr << "... killing follower " << followerTablet << Endl;
            runtime.Send(new IEventHandle(followerTablet, followerEdge, new TEvents::TEvPoisonPill()), 1);
            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvTabletDead>(followerEdge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->TabletID, tabletId);
        }

        auto waitFor = [&](const auto& condition, const TString& description) {
            if (!condition()) {
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                runtime.DispatchEvents(options);
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            }
        };

        // Wait until follower attempts to detach
        waitFor([&](){ return detachCounter > 0; }, "TEvFollowerDetach message");
        UNIT_ASSERT_VALUES_EQUAL(detachCounter, 1u);

        // Perform a large number of write transactions
        // This should trigger compaction and garbage collection
        gcWaitCounter = 0;
        gcAppliedCounter = 0;
        Cerr << "... performing multiple writes" << Endl;
        for (ui64 key = 1; key <= 1200; ++key) {
            doLeaderWrite(key, key);
        }
        waitFor([&](){ return gcWaitCounter > 0; }, "WaitForFollowerGcAck commits");
        waitFor([&](){ return gcAppliedCounter >= gcWaitCounter; }, "TEvFollowerGcApplied messages");
    }

    Y_UNIT_TEST(ErrorWaitForEarlierCommits) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port)
                .SetNodeCount(2)
                .SetUseRealThreads(false);
        TServer server(settings);
        TClient client(settings);
        SetupLogging(server);

        TTestActorRuntime &runtime = *server.GetRuntime();

        const ui64 tabletId = ChangeStateStorage(DummyTablet1, settings.Domain);
        TIntrusivePtr<TTabletStorageInfo> tabletInfo = CreateTestTabletInfo(tabletId, TTabletTypes::Dummy);
        TIntrusivePtr<TTabletSetupInfo> setupInfo = new TTabletSetupInfo(&CreateFlatDummyTablet, TMailboxType::Simple, 0, TMailboxType::Simple, 0);

        const TActorId leaderEdge = runtime.AllocateEdgeActor();
        const TActorId leaderTablet = runtime.Register(CreateTablet(leaderEdge, tabletInfo.Get(), setupInfo.Get(), 0, nullptr, nullptr));
        const TActorId leaderId = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(leaderEdge)->Get()->UserTabletActor;
        Y_UNUSED(leaderTablet);

        auto sendLeaderWrite = [&](ui64 key, ui64 value) -> TActorId {
            const char *writeQuery = R"__((
                    (let row_ '('('key (Uint64 '%lu))))
                    (let update_ '('('v_ui64 (Uint64 '%lu))))
                    (let result_ (UpdateRow 't_by_ui64 row_ update_))
                    (return (AsList result_))
                ))__";

            TActorId edge = runtime.AllocateEdgeActor();
            THolder<TEvTablet::TEvLocalMKQL> reqWrite = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqWrite->Record.MutableProgram()->MutableProgram()->SetText(Sprintf(writeQuery, key, value));
            runtime.Send(new IEventHandle(leaderId, edge, reqWrite.Release()));
            return edge;
        };

        auto waitLeaderReply = [&](const TActorId& edge) {
            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
        };

        waitLeaderReply(sendLeaderWrite(42, 42));

        bool logResultsCapture = true;
        TVector<THolder<IEventHandle>> logResultsEvents;
        auto observer = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvTabletBase::TEvWriteLogResult::EventType: {
                    if (logResultsCapture) {
                        logResultsEvents.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(observer);

        auto waitFor = [&](const auto& condition, const TString& description) {
            if (!condition()) {
                Cerr << "... waiting for " << description << Endl;
                TDispatchOptions options;
                options.CustomFinalCondition = [&]() {
                    return condition();
                };
                runtime.DispatchEvents(options);
                UNIT_ASSERT_C(condition(), "... failed to wait for " << description);
            }
        };

        auto write43 = sendLeaderWrite(43, 430);
        waitFor([&]{ return logResultsEvents.size() >= 1; }, "first log write result");
        UNIT_ASSERT_VALUES_EQUAL(logResultsEvents.size(), 1u);

        auto write44 = sendLeaderWrite(44, 440);
        waitFor([&]{ return logResultsEvents.size() >= 2; }, "second log write result");
        UNIT_ASSERT_VALUES_EQUAL(logResultsEvents.size(), 2u);
        Y_UNUSED(write44);

        logResultsCapture = false;

        // synchronously send the second reply, changing status to BLOCKED
        {
            auto* msg = logResultsEvents[1]->Get<TEvTabletBase::TEvWriteLogResult>();
            const_cast<NKikimrProto::EReplyStatus&>(msg->Status) = NKikimrProto::BLOCKED;
            runtime.Send(logResultsEvents[1].Release());
        }

        // asynchronously send the first reply
        runtime.Send(logResultsEvents[0].Release(), 0, true);

        // write43 should succeed!
        Cerr << "... waiting for first tx result" << Endl;
        waitLeaderReply(write43);
    }

    Y_UNIT_TEST(OfflineFollowerNoGcReorder) {
        TPortManager tp;
        ui16 port = tp.GetPort(2134);

        const auto settings = TServerSettings(port)
                .SetUseRealThreads(false);
        TServer server(settings);
        TClient client(settings);
        SetupLogging(server);
        server.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);

        TTestActorRuntime &runtime = *server.GetRuntime();

        const ui64 tabletId = ChangeStateStorage(DummyTablet1, settings.Domain);
        TIntrusivePtr<TTabletStorageInfo> tabletInfo = CreateTestTabletInfo(tabletId, TTabletTypes::Dummy);
        TIntrusivePtr<TTabletSetupInfo> setupInfo = new TTabletSetupInfo(&CreateFlatDummyTablet, TMailboxType::Simple, 0, TMailboxType::Simple, 0);

        const TActorId edge = runtime.AllocateEdgeActor();

        const TActorId leaderTablet = runtime.Register(CreateTablet(edge, tabletInfo.Get(), setupInfo.Get(), 0, nullptr, nullptr));
        const TActorId leaderId = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(edge)->Get()->UserTabletActor;
        Y_UNUSED(leaderTablet);

        const TActorId followerTablet = runtime.Register(CreateTabletFollower(edge, tabletInfo.Get(), setupInfo.Get(), 1, nullptr, nullptr));
        const TActorId followerId = runtime.GrabEdgeEvent<TEvTablet::TEvRestored>(edge)->Get()->UserTabletActor;
        Y_UNUSED(followerTablet);
        Y_UNUSED(followerId);

        const char *writeQuery = R"__((
                (let row_ '('('key (Uint64 '42))))
                (let update_ '('('v_ui64 (Uint64 '%lu))))
                (let result_ (UpdateRow 't_by_ui64 row_ update_))
                (return (AsList result_))
            ))__";

        const char *readQuery = R"__((
                (let row_ '('('key (Uint64 '42))))
                (let select_ '('v_ui64))
                (let pgmReturn (AsList
                    (SetResult 'res (SelectRow 't_by_ui64 row_ select_))
                ))
                (return pgmReturn)
            ))__";
        Y_UNUSED(readQuery);

        TDeque<THolder<IEventHandle>> blockedConfirmations;
        bool blockConfirmations = true;
        std::pair<ui32, ui32> maxGc{ 0, 0 };
        auto observerFunc = [&](TAutoPtr<IEventHandle>& ev) {
            switch (ev->GetTypeRewrite()) {
                case TEvBlobStorage::TEvPut::EventType: {
                    const auto* msg = ev->Get<TEvBlobStorage::TEvPut>();
                    // step 1 is snapshot
                    // step 2 is schema alter
                    // step 3 is expected write below
                    if (blockConfirmations &&
                        msg->Id.TabletID() == tabletId &&
                        msg->Id.Channel() == 0 &&
                        msg->Id.Cookie() == 1 &&
                        msg->Id.Step() > 2)
                    {
                        Cerr << "--- blocked confirmation commit: " << msg->Id << Endl;
                        blockedConfirmations.emplace_back(ev.Release());
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    break;
                }
                case TEvBlobStorage::TEvCollectGarbage::EventType: {
                    const auto* msg = ev->Get<TEvBlobStorage::TEvCollectGarbage>();
                    if (msg->TabletId == tabletId &&
                        msg->Channel == 0)
                    {
                        std::pair<ui32, ui32> gc{ msg->CollectGeneration, msg->CollectStep };
                        Cerr << "--- observed syslog gc up to " << gc.first << ":" << gc.second << Endl;
                        maxGc = Max(maxGc, gc);
                    }
                    break;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        auto prevObserverFunc = runtime.SetObserverFunc(observerFunc);

        for (int i = 0; i < 350; ++i) {
            THolder<TEvTablet::TEvLocalMKQL> reqWrite = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqWrite->Record.MutableProgram()->MutableProgram()->SetText(Sprintf(writeQuery, ui64(i + 10)));
            runtime.Send(new IEventHandle(leaderId, edge, reqWrite.Release()));

            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
        }
        UNIT_ASSERT(blockedConfirmations.size() > 0);
        auto maxGc1 = maxGc;

        blockConfirmations = false;
        for (auto& ev : blockedConfirmations) {
            runtime.Send(ev.Release(), 0, true);
        }

        for (int i = 0; i < 350; ++i) {
            THolder<TEvTablet::TEvLocalMKQL> reqWrite = MakeHolder<TEvTablet::TEvLocalMKQL>();
            reqWrite->Record.MutableProgram()->MutableProgram()->SetText(Sprintf(writeQuery, ui64(i + 10)));
            runtime.Send(new IEventHandle(leaderId, edge, reqWrite.Release()));

            auto reply = runtime.GrabEdgeEvent<TEvTablet::TEvLocalMKQLResponse>(edge);
            UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetStatus(), 0);
        }
        auto maxGc2 = maxGc;

        UNIT_ASSERT(maxGc1 < maxGc2);
    }

}

} // namespace Tests
} // namespace NKikimr

