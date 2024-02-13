#include "flat_ut_client.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tablet_flat/test/libs/rows/misc.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <ydb/core/engine/mkql_engine_flat.h>

#include <library/cpp/http/io/stream.h>
#include <library/cpp/http/server/http_ex.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/text_format.h>
#include <util/generic/xrange.h>
#include <util/random/mersenne.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NFlatTests {

using namespace Tests;
using NClient::TValue;

namespace {
    class TFailingMtpQueue: public TSimpleThreadPool {
    private:
        bool FailOnAdd_ = false;
    public:
        void SetFailOnAdd(bool fail = true) {
            FailOnAdd_ = fail;
        }
        [[nodiscard]] bool Add(IObjectInQueue* pObj) override {
            if (FailOnAdd_) {
                return false;
            }

            return TSimpleThreadPool::Add(pObj);
        }
        TFailingMtpQueue() = default;
        TFailingMtpQueue(IThreadFactory* pool)
            : TSimpleThreadPool(pool)
        {
        }
    };

    using TFailingServerMtpQueue =
        TThreadPoolBinder<TFailingMtpQueue, THttpServer::ICallBack>;

    class THTTP200OkServer: public THttpServer::ICallBack {
        class TRequest: public THttpClientRequestEx {
        public:
            inline TRequest(THTTP200OkServer* parent)
                : Parent_(parent)
            {
            }

            bool Reply(void* /*tsr*/) override {
                if (!ProcessHeaders()) {
                    return true;
                }

                if (strncmp(RequestString.data(), "GET /hosts HTTP/1.", 18) == 0) {
                    TString list = Sprintf("[\"localhost\"]");
                    Output() << "HTTP/1.1 200 Ok\r\n";
                    Output() << "Connection: close\r\n";
                    Output() << "X-Server: unit test server\r\n";
                    Output() << "Content-Length: " << list.size() << "\r\n";
                    Output() << "\r\n";
                    Output() << list;
                    return true;
                }

                Output() << "HTTP/1.1 200 Ok\r\n";
                if (Buf.Size()) {
                    Output() << "X-Server: unit test server\r\n";
                    Output() << "Content-Length: " << Buf.Size() << "\r\n\r\n";
                    Output().Write(Buf.AsCharPtr(), Buf.Size());
                } else {
                    Output() << "X-Server: unit test server\r\n";
                    Output() << "Content-Length: " << (Parent_->Res_).size()
                             << "\r\n\r\n";
                    Output() << Parent_->Res_;
                }
                Output().Finish();

                return true;
            }

        private:
            THTTP200OkServer* Parent_ = nullptr;
        };

    public:
        inline THTTP200OkServer(TString res)
            : Res_(std::move(res))
        {
        }

        TClientRequest* CreateClient() override {
            return new TRequest(this);
        }

    private:
        TString Res_;
    };
}


Y_UNIT_TEST_SUITE(TFlatTest) {

    Y_UNIT_TEST(Init) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        if (true) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
        }

        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        auto status = annoyingClient.MkDir("/dc-1", "Berkanavt");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
        status = annoyingClient.MkDir("/dc-1/Berkanavt", "tables");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
        status = annoyingClient.MkDir("/dc-1/Berkanavt", "tables");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);

        annoyingClient.CreateTable("/dc-1/Berkanavt/tables",
                                   "Name: \"Table1\""
                                       "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                       "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                       "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                       "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                       "KeyColumnNames: [\"RowId\", \"key1\", \"key2\"]"
                                   );

        annoyingClient.CreateTable("/dc-1/Berkanavt/tables",
                                   "Name: \"Students\""
                                        "Columns { Name: \"Id\"          Type: \"Uint32\"}"
                                        "Columns { Name: \"Name\"        Type: \"Utf8\"}"
                                        "Columns { Name: \"LastName\"    Type: \"Utf8\"}"
                                        "Columns { Name: \"Age\"         Type: \"Uint32\"}"
                                        "KeyColumnNames: [\"Id\"]"
                                        "UniformPartitionsCount: 10"
                                   );
        annoyingClient.CreateTable("/dc-1/Berkanavt/tables",
                                   "Name: \"Classes\""
                                        "Columns { Name: \"Id\"         Type: \"Uint32\"}"
                                        "Columns { Name: \"Name\"       Type: \"Utf8\"}"
                                        "Columns { Name: \"ProfessorName\" Type: \"Utf8\"}"
                                        "Columns { Name: \"Level\"      Type: \"Uint32\"}"
                                        "KeyColumnNames: [\"Id\"]"
                                   );

        annoyingClient.MkDir("/dc-1/Berkanavt/tables", "Table1");
        status = annoyingClient.MkDir("/dc-1/Berkanavt/tables/Table1", "col42");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_ERROR);

        annoyingClient.Ls("/");
        annoyingClient.Ls("/dc-100");
        annoyingClient.Ls("/dc-1/Argonaut");
        annoyingClient.Ls("/dc-1/Berkanavt/tabls");
        annoyingClient.Ls("/dc-1/Berkanavt/tables");
        annoyingClient.Ls("/dc-1/Berkanavt/tables/Table1");
        annoyingClient.Ls("/dc-1/Berkanavt/tables/Students");
        annoyingClient.Ls("/dc-1/Berkanavt/tables/Classes");
        annoyingClient.Ls("/dc-1/Berkanavt/tables/Table1/key1");

        annoyingClient.FlatQuery(
                    "("
                    "   (return (AsList (SetResult 'res1 (Int32 '2016))))"
                    ")");

        // Update
        annoyingClient.FlatQuery(
                    "("
                    "(let row '('('Id (Uint32 '42))))"
                    "(let myUpd '("
                    "    '('Name (Utf8 'Robert))"
                    "    '('LastName (Utf8 '\"\\\"); DROP TABLE Students; --\"))"
                    "    '('Age (Uint32 '21))))"
                    "(let pgmReturn (AsList"
                    "    (UpdateRow '/dc-1/Berkanavt/tables/Students row myUpd)"
                    "))"
                    "(return pgmReturn)"
                    ")");

        // SelectRow
        annoyingClient.FlatQuery(
                    "("
                    "(let row '('('Id (Uint32 '42))))"
                    "(let select '('Name))"
                    "(let pgmReturn (AsList"
                    "    (SetResult 'myRes (SelectRow '/dc-1/Berkanavt/tables/Students row select))"
                    "))"
                    "(return pgmReturn)"
                    ")");

        // Cross-shard
        annoyingClient.FlatQuery(
                    "("
                    "(let row '('('Id (Uint32 '2))))"
                    "(let select '('Name))"
                    "(let selectRes (SelectRow '/dc-1/Berkanavt/tables/Classes row select))"
                    "(let name (FlatMap selectRes (lambda '(x) (Member x 'Name))))"
                    "(let row '('('Id (Uint32 '3))))"
                    "(let myUpd '("
                    "    '('Name name)"
                    "    '('LastName (Utf8 'Tables))"
                    "    '('Age (Uint32 '21))))"
                    "(let pgmReturn (AsList"
                    "    (UpdateRow '/dc-1/Berkanavt/tables/Students row myUpd)"
                    "))"
                    "    (return pgmReturn)"
                    ")"
                    );

        // SelectRange
        annoyingClient.FlatQuery(
                    "("
                    "(let range '('ExcFrom '('Id (Uint32 '2) (Void))))"
                    "(let select '('Id 'Name 'LastName))"
                    "(let options '())"
                    "(let pgmReturn (AsList"
                    "    (SetResult 'myRes (SelectRange '/dc-1/Berkanavt/tables/Students range select options))"
                    "))"
                    "(return pgmReturn)"
                    ")"
                    );

        // Erase
        annoyingClient.FlatQuery(
                    "("
                    "(let row '('('Id (Uint32 '42))))"
                    "(let pgmReturn (AsList"
                    "    (EraseRow '/dc-1/Berkanavt/tables/Students row)"
                    "))"
                    "(return pgmReturn)"
                    ")"
                    );

        Cout << "Drop tables" << Endl;
        annoyingClient.DeleteTable("/dc-1/Berkanavt/tables", "Table1");
        annoyingClient.DeleteTable("/dc-1/Berkanavt/tables", "Students");
        annoyingClient.DeleteTable("/dc-1/Berkanavt/tables", "Classes");
    }

    Y_UNIT_TEST(SelectBigRangePerf) {
        // Scenario from KIKIMR-2715
        // Increase N_ROWS and N_REQS for profiling
        const int N_ROWS = 100; // 10000
        const int N_REQS = 10;  // 100500

        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        if (!true) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::PIPE_CLIENT, NActors::NLog::PRI_DEBUG);
        }

        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");
        annoyingClient.MkDir("/dc-1/test", "perf");

        annoyingClient.CreateTable("/dc-1/test/perf",
                                   R"(Name: "FlatDaoPerfTestClient"
                                       Columns { Name: "ls"             Type: "Utf8"}
                                       Columns { Name: "kg"             Type: "Uint32"}
                                       Columns { Name: "localId"        Type: "Uint64"}
                                       Columns { Name: "createdSeconds" Type: "Uint64"}
                                       Columns { Name: "mode1"          Type: "Uint32"}
                                       KeyColumnNames: ["ls"]
                                   )");

        // insert rows
        for (int i = 0; i < N_ROWS; ++i) {
            annoyingClient.FlatQuery(Sprintf(
                    R"(
                    (
                    (let key '('('ls (Utf8 '%d)) ))
                    (let myUpd '(
                        '('kg (Uint32 '101))
                        '('localId (Uint64 '102))
                        '('createdSeconds (Uint64 '103))
                        '('mode1 (Uint32 '104))
                    ))
                    (let pgmReturn (AsList
                        (UpdateRow '"/dc-1/test/perf/FlatDaoPerfTestClient" key myUpd)
                    ))
                    (return pgmReturn)
                    )
                    )", i));
        }

        Cerr << "insert finished" << Endl;

        // SelectRange
        for (int i = 0; i < N_REQS; ++i) {
            TInstant start = TInstant::Now();
            annoyingClient.FlatQuery(
                    R"(
                        ((return (AsList
                            (SetResult 'x
                                (SelectRange '"/dc-1/test/perf/FlatDaoPerfTestClient"
                                    '('ExcFrom 'IncTo '('ls (Utf8 '"") (Void)))
                                    '('ls 'kg 'localId 'createdSeconds 'mode1)
                                    '('('BytesLimit (Uint64 '3000000)))
                                )
                            )
                        )))
                    )"
                    );
            Cerr << (TInstant::Now()-start).MicroSeconds() << " usec" << Endl;
        }
    }

    Y_UNIT_TEST(SelectRowWithTargetParameter) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));

        TFlatMsgBusClient annoyingClient(port);
        annoyingClient.InitRoot();

        annoyingClient.CreateTable("/dc-1", R"(
            Name: "TestTable"
            Columns { Name: "key" Type: "Uint32"}
            Columns { Name: "value" Type: "String"}
            KeyColumnNames: ["key"]
        )");

        annoyingClient.FlatQuery(R"(
            (
                (let row '('('key (Uint32 '1))))
                (let upd '('('value (String 'test1))))
                (let ret (AsList
                    (UpdateRow '/dc-1/TestTable row upd)
                ))
                (return ret)
            )
        )");

        TString query = R"(
            (
                (let row '('('key (Uint32 '1))))
                (let cols '('value))
                (let select (SelectRow '/dc-1/TestTable row cols (Parameter 'rt (DataType 'Uint32))))
                (let ret (AsList (SetResult 'result (Member select 'value))))
                (return ret)
            )
        )";

        TString params = R"(
            (
                (let params (Parameters))
                (let params (AddParameter params 'rt (Uint32 '0)))
                (return params)
            )
        )";

        NKikimrMiniKQL::TResult result;
        annoyingClient.FlatQueryParams(query, params, false, result);
        UNIT_ASSERT_NO_DIFF(result.GetValue().GetStruct(0).GetOptional().GetOptional().GetBytes(), "test1");
    }

    Y_UNIT_TEST(ModifyMultipleRowsCrossShardAllToAll) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        if (!true) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::PIPE_CLIENT, NActors::NLog::PRI_DEBUG);
        }

        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");
        annoyingClient.MkDir("/dc-1/test", "perf");

        annoyingClient.CreateTable("/dc-1/test/perf",
                                   R"(Name: "FlatDaoPerfTestClient"
                                       Columns { Name: "hash"           Type: "Uint32"}
                                       Columns { Name: "ls"             Type: "Utf8"}
                                       Columns { Name: "kg"             Type: "Uint32"}
                                       Columns { Name: "localId"        Type: "Uint64"}
                                       Columns { Name: "createdSeconds" Type: "Uint64"}
                                       Columns { Name: "mode1"          Type: "Uint32"}
                                       KeyColumnNames: ["hash", "ls"]
                                       UniformPartitionsCount: 4
                                   )");

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        TString query =  R"(
                (
                    (let list_merges_updates
                        (MapParameter
                            (Parameter 'p
                                (ListType
                                    (StructType
                                        '('hash (DataType 'Uint32))
                                        '('ls (DataType 'Utf8))
                                        '('kg (DataType 'Uint32))
                                        '('localId (DataType 'Uint64))
                                        '('createdSeconds (DataType 'Uint64))
                                        '('mode1 (DataType 'Uint32))
                                    )
                                )
                            )
                            (lambda '(item) (block '(
                                (let merged_hash (Just (Member item 'hash)))
                                (let merged_ls (Just (Member item 'ls)))
                                (let row (SelectRow '"/dc-1/test/perf/FlatDaoPerfTestClient" '('('hash (Member item 'hash)) '('ls (Member item 'ls))) '('kg 'localId 'createdSeconds 'mode1)))
                                (let merged_kg (IfPresent row (lambda '(x) (block '((return (Member x 'kg))))) (Just (Member item 'kg))))
                                (let merged_localId (IfPresent row (lambda '(x) (block '((return (Member x 'localId))))) (Just (Member item 'localId))))
                                (let merged_createdSeconds (IfPresent row (lambda '(x) (block '((return (Member x 'createdSeconds))))) (Just (Member item 'createdSeconds))))
                                (let merged_mode1 (Just (Member item 'mode1)))
                                (return '(
                                    (AsStruct
                                        '('hash merged_hash)
                                        '('ls merged_ls)
                                        '('kg merged_kg)
                                        '('localId merged_localId)
                                        '('createdSeconds merged_createdSeconds)
                                        '('mode1 merged_mode1)
                                    )
                                    (UpdateRow
                                        '"/dc-1/test/perf/FlatDaoPerfTestClient"
                                        '(
                                            '('hash (Member item 'hash))
                                            '('ls (Member item 'ls))
                                        )
                                        '(
                                            '('kg merged_kg)
                                            '('localId merged_localId)
                                            '('createdSeconds merged_createdSeconds)
                                            '('mode1 merged_mode1)
                                        )
                                    )
                                ))
                            )))
                        )
                    )
                    (return
                        (Append
                            (Map list_merges_updates
                                (lambda '(item) (block '(
                                    (return (Nth item '1))
                                )))
                            )
                            (SetResult 'Result (AsStruct
                                '('List (Map
                                    list_merges_updates
                                    (lambda '(item) (block '(
                                        (return (Nth item '0))
                                    )))
                                ))
                                '('Truncated (Bool 'False)))
                            )
                        )
                    )
                )
            )";

        const TString params = R"(
                (
                    (let params (Parameters))
                    (let params (AddParameter params 'p (AsList
                        (AsStruct
                            '('hash             (Uint32 '0))
                            '('ls               (Utf8 'A))
                            '('kg               (Uint32 '10))
                            '('localId          (Uint64 '20))
                            '('createdSeconds   (Uint64 '30))
                            '('mode1            (Uint32 '40))
                        )
                        (AsStruct
                            '('hash             (Uint32 '1500000000))
                            '('ls               (Utf8 'B))
                            '('kg               (Uint32 '10))
                            '('localId          (Uint64 '20))
                            '('createdSeconds   (Uint64 '30))
                            '('mode1            (Uint32 '40))
                        )
                        (AsStruct
                            '('hash             (Uint32 '3000000000))
                            '('ls               (Utf8 'C))
                            '('kg               (Uint32 '10))
                            '('localId          (Uint64 '20))
                            '('createdSeconds   (Uint64 '30))
                            '('mode1            (Uint32 '40))
                        )
                    )))
                    (return params)
                )
            )";

        NKikimrMiniKQL::TResult result;
        annoyingClient.FlatQueryParams(query, params, false, result);
    }

    Y_UNIT_TEST(CrossRW) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        if (!true) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
            //cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::PIPE_CLIENT, NActors::NLog::PRI_DEBUG);
        }

        TFlatMsgBusClient annoyingClient(port);

        const char * table = R"(Name: "A"
            Columns { Name: "key"    Type: "Uint32" }
            Columns { Name: "value"  Type: "Uint32" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2)";

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "Dir");
        annoyingClient.CreateTable("/dc-1/Dir", table);

        // UPDATE a SET value = 42 WHERE key IN(0, 1, Max, Max-1)
        annoyingClient.FlatQuery("("
            "(let row0_ '('('key (Uint32 '0))))"
            "(let row1_ '('('key (Uint32 '1))))"
            "(let row2_ '('('key (Uint32 '4294967294))))"
            "(let row3_ '('('key (Uint32 '4294967295))))"
            "(let update_ '('('value (Uint32 '42))))"
            "(let ret_ (AsList"
            "    (UpdateRow '/dc-1/Dir/A row0_ update_)"
            "    (UpdateRow '/dc-1/Dir/A row1_ update_)"
            "    (UpdateRow '/dc-1/Dir/A row2_ update_)"
            "    (UpdateRow '/dc-1/Dir/A row3_ update_)"
            "))"
            "(return ret_)"
        ")");

        // UPDATE a SET value = 0 WHERE key IN(0, 1)
        annoyingClient.FlatQuery("("
            "(let row2_ '('('key (Uint32 '0))))"
            "(let row3_ '('('key (Uint32 '1))))"
            "(let update_ '('('value (Uint32 '0))))"
            "(let ret_ (AsList"
            "    (UpdateRow '/dc-1/Dir/A row2_ update_)"
            "    (UpdateRow '/dc-1/Dir/A row3_ update_)"
            "))"
            "(return ret_)"
        ")");

        // Cross read-write. Swaps values in different DataShards (A[0] <-> A[Max])
        annoyingClient.FlatQuery("("
            "(let row0_ '('('key (Uint32 '0))))"
            "(let row3_ '('('key (Uint32 '4294967295))))"
            "(let cols_ '('value))"
            "(let read0_ (SelectRow '/dc-1/Dir/A row0_ cols_))"
            "(let read3_ (SelectRow '/dc-1/Dir/A row3_ cols_))"
            "(let val0_ (Member read0_ 'value))"
            "(let val3_ (Member read3_ 'value))"
            "(let update0_ '('('value val3_)))"
            "(let update3_ '('('value val0_)))"
            "(let ret_ (AsList"
            "    (UpdateRow '/dc-1/Dir/A row0_ update0_)"
            "    (UpdateRow '/dc-1/Dir/A row3_ update3_)"
            "))"
            "(return ret_)"
        ")");

        // SELECT value FROM a WHERE key IN(0, 1, Max, Max-1)
        NKikimrMiniKQL::TResult res;
        annoyingClient.FlatQuery("("
            "(let row0_ '('('key (Uint32 '0))))"
            "(let row1_ '('('key (Uint32 '1))))"
            "(let row2_ '('('key (Uint32 '4294967294))))"
            "(let row3_ '('('key (Uint32 '4294967295))))"
            "(let cols_ '('value))"
            "(let select0_ (SelectRow '/dc-1/Dir/A row0_ cols_))"
            "(let select1_ (SelectRow '/dc-1/Dir/A row1_ cols_))"
            "(let select2_ (SelectRow '/dc-1/Dir/A row2_ cols_))"
            "(let select3_ (SelectRow '/dc-1/Dir/A row3_ cols_))"
            "(let ret_ (AsList"
            "    (SetResult 'res0_ select0_)"
            "    (SetResult 'res1_ select1_)"
            "    (SetResult 'res2_ select2_)"
            "    (SetResult 'res3_ select3_)"
            "))"
            "(return ret_)"
        ")", res);

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue res0 = value["res0_"];
        TValue res1 = value["res1_"];
        TValue res2 = value["res2_"];
        TValue res3 = value["res3_"];
        ui32 s0 = res0[0];
        ui32 s1 = res1[0];
        ui32 s2 = res2[0];
        ui32 s3 = res3[0];
        UNIT_ASSERT_VALUES_EQUAL(s0, 42);
        UNIT_ASSERT_VALUES_EQUAL(s1, 0);
        UNIT_ASSERT_VALUES_EQUAL(s2, 42);
        UNIT_ASSERT_VALUES_EQUAL(s3, 0);
    }

    Y_UNIT_TEST(ShardFreezeRejectBadProtobuf) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        if (!true) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
        }

        TFlatMsgBusClient annoyingClient(port);

        const char * table = R"(Name: "Table"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "value" Type: "Uint32" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2)";

        annoyingClient.InitRoot();
        annoyingClient.CreateTable("/dc-1", table);

        {
            // Alter and freeze
            auto res = annoyingClient.AlterTable("/dc-1", R"(
                Name: "Table"
                PartitionConfig { FreezeState: Freeze FollowerCount: 1 }
            )");
            UNIT_ASSERT_VALUES_EQUAL(res, NMsgBusProxy::MSTATUS_ERROR);
        }

        {
            auto res = annoyingClient.AlterTable("/dc-1", R"(
                Name: "Table"
                PartitionConfig { FreezeState: Unspecified}
            )");
            UNIT_ASSERT_VALUES_EQUAL(res, NMsgBusProxy::MSTATUS_ERROR);
        }

        {
            auto res = annoyingClient.AlterTable("/dc-1", R"(
                Name: "Table"
                DropColumns { Name: "value" }
                PartitionConfig { FreezeState: Unspecified }
            )");
            UNIT_ASSERT_VALUES_EQUAL(res, NMsgBusProxy::MSTATUS_ERROR);
        }

        {
            auto res = annoyingClient.AlterTable("/dc-1", R"(
                Name: "Table"
                DropColumns { Name: "value" }
                PartitionConfig { FreezeState: Unfreeze }
            )");
            UNIT_ASSERT_VALUES_EQUAL(res, NMsgBusProxy::MSTATUS_ERROR);
        }
    }

    Y_UNIT_TEST(ShardUnfreezeNonFrozen) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        if (!true) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
        }

        TFlatMsgBusClient annoyingClient(port);

        const char * table = R"(Name: "Table"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "value" Type: "Uint32" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2)";

        annoyingClient.InitRoot();
        annoyingClient.CreateTable("/dc-1", table);

        {
            auto res = annoyingClient.AlterTable("/dc-1", R"(
                Name: "Table"
                PartitionConfig { FreezeState: Unfreeze}
            )");
            UNIT_ASSERT_VALUES_EQUAL(res, NMsgBusProxy::MSTATUS_OK);
        }
    }

    Y_UNIT_TEST(ShardFreezeUnfreezeAlreadySet) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        if (!true) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
        }

        TFlatMsgBusClient annoyingClient(port);

        const char * table = R"(Name: "Table"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "value" Type: "Uint32" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2)";

        annoyingClient.InitRoot();
        annoyingClient.CreateTable("/dc-1", table);

        {
            auto res = annoyingClient.AlterTable("/dc-1", R"(
                Name: "Table"
                PartitionConfig { FreezeState: Freeze}
            )");
            UNIT_ASSERT_VALUES_EQUAL(res, NMsgBusProxy::MSTATUS_OK);
        }

        {
            auto res = annoyingClient.AlterTable("/dc-1", R"(
                Name: "Table"
                PartitionConfig { FreezeState: Freeze}
            )");
            UNIT_ASSERT_VALUES_EQUAL(res, NMsgBusProxy::MSTATUS_OK);
        }

        {
            auto res = annoyingClient.AlterTable("/dc-1", R"(
                Name: "Table"
                PartitionConfig { FreezeState: Unfreeze}
            )");
            UNIT_ASSERT_VALUES_EQUAL(res, NMsgBusProxy::MSTATUS_OK);
        }

        {
            auto res = annoyingClient.AlterTable("/dc-1", R"(
                Name: "Table"
                PartitionConfig { FreezeState: Unfreeze}
            )");
            UNIT_ASSERT_VALUES_EQUAL(res, NMsgBusProxy::MSTATUS_OK);
        }
    }

    Y_UNIT_TEST(ShardFreezeUnfreezeRejectScheme) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        if (!true) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
        }

        TFlatMsgBusClient annoyingClient(port);

        const char * table = R"(Name: "Table"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "value" Type: "Uint32" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2)";

        annoyingClient.InitRoot();
        annoyingClient.CreateTable("/dc-1", table);

        // Alter, freeze datashard
        annoyingClient.AlterTable("/dc-1", R"(
            Name: "Table"
            PartitionConfig { FreezeState: Freeze }
        )");
        {
            auto res = annoyingClient.AlterTable("/dc-1", R"(
                Name: "Table"
                DropColumns { Name: "value" }
            )");
            UNIT_ASSERT_VALUES_EQUAL(res, NMsgBusProxy::MSTATUS_ERROR);
        }

        annoyingClient.AlterTable("/dc-1", R"(
            Name: "Table"
            PartitionConfig { FreezeState: Unfreeze }
        )");
        {
            auto res = annoyingClient.AlterTable("/dc-1", R"(
                Name: "Table"
                DropColumns { Name: "value" }
            )");
            UNIT_ASSERT_VALUES_EQUAL(res, NMsgBusProxy::MSTATUS_OK);
        }
    }

    Y_UNIT_TEST(ShardFreezeUnfreeze) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        if (!true) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
        }

        TFlatMsgBusClient annoyingClient(port);

        const char * table = R"(Name: "Table"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "value" Type: "Uint32" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2)";

        annoyingClient.InitRoot();
        annoyingClient.CreateTable("/dc-1", table);

        // Alter, freeze datashard
        annoyingClient.AlterTable("/dc-1", R"(
            Name: "Table"
            PartitionConfig { FreezeState: Freeze }
        )");
        {
            // write should be rejected
            TString insertRowQuery = "("
                "(let key '('('key (Uint32 '%u))))"
                "(let value '('('value (Uint32 '%u))))"
                "(let ret_ (AsList"
                "    (UpdateRow '/dc-1/Table key value)"
                "))"
                "(return ret_)"
                ")";

            TClient::TFlatQueryOptions opts;
            NKikimrClient::TResponse response;

            ui32 status = annoyingClient.FlatQueryRaw(Sprintf(insertRowQuery.data(), 1, 6000000).data(), opts, response);
            UNIT_ASSERT(status == NMsgBusProxy::MSTATUS_REJECTED);

            Cout << "SELECT value FROM Table WHERE key = 0" << Endl;
            annoyingClient.FlatQuery(R"((
                (let row_ '('('key (Uint32 '0))))
                (let cols_ '('value))
                (let select_ (SelectRow '/dc-1/Table row_ cols_))
                (let ret_ (AsList (SetResult 'ret0 select_)))
                (return ret_)
            ))", NMsgBusProxy::MSTATUS_OK);
        }

        annoyingClient.AlterTable("/dc-1", R"(
            Name: "Table"
            PartitionConfig { FreezeState: Unfreeze }
        )");

        {

            TString insertRowQuery = "("
                "(let key '('('key (Uint32 '%u))))"
                "(let value '('('value (Uint32 '%u))))"
                "(let ret_ (AsList"
                "    (UpdateRow '/dc-1/Table key value)"
                "))"
                "(return ret_)"
                ")";

            TClient::TFlatQueryOptions opts;
            NKikimrClient::TResponse response;

            ui32 status = annoyingClient.FlatQueryRaw(Sprintf(insertRowQuery.data(), 1, 6000000).data(), opts, response);

            UNIT_ASSERT(status == NMsgBusProxy::MSTATUS_OK);
        }
    }

    Y_UNIT_TEST(Mix_DML_DDL) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        if (!true) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
        }

        TFlatMsgBusClient annoyingClient(port);

        const char * table = R"(Name: "Table"
            Columns { Name: "key"   Type: "Uint32" }
            Columns { Name: "value" Type: "Uint32" }
            KeyColumnNames: ["key"]
            UniformPartitionsCount: 2)";

        annoyingClient.InitRoot();
        annoyingClient.CreateTable("/dc-1", table);

        Cout << "SELECT value From Table WHERE key = 0" << Endl;
        annoyingClient.FlatQuery(R"((
            (let row_ '('('key (Uint32 '0))))
            (let cols_ '('value))
            (let select_ (SelectRow '/dc-1/Table row_ cols_))
            (let ret_ (AsList (SetResult 'ret0 select_)))
            (return ret_)
        ))");

        Cout << "ALTER TABLE Table DROP COLUMN value" << Endl;
        annoyingClient.AlterTable("/dc-1", R"(
            Name: "Table"
            DropColumns { Name: "value" }
        )");

        Cout << "SELECT value FROM Table WHERE key = 0 -- fail (rejected)" << Endl;
        annoyingClient.FlatQuery(R"((
            (let row_ '('('key (Uint32 '0))))
            (let cols_ '('value))
            (let select_ (SelectRow '/dc-1/Table row_ cols_))
            (let ret_ (AsList (SetResult 'ret0 select_)))
            (return ret_)
        ))", NMsgBusProxy::MSTATUS_ERROR);

        Cout << "SELECT key FROM Table WHERE key = 0" << Endl;
        annoyingClient.FlatQuery(R"((
            (let row_ '('('key (Uint32 '0))))
            (let cols_ '('key))
            (let select_ (SelectRow '/dc-1/Table row_ cols_))
            (let ret_ (AsList (SetResult 'ret0 select_)))
            (return ret_)
        ))");

        Cout << "ALTER TABLE Table ADD COLUMN more" << Endl;
        annoyingClient.AlterTable("/dc-1", R"(
            Name: "Table"
            Columns { Name: "more" Type: "Uint64" }
        )");

        Cout << "SELECT more FROM Table WHERE key = 0" << Endl;
        annoyingClient.FlatQuery(R"((
            (let row_ '('('key (Uint32 '0))))
            (let cols_ '('more))
            (let select_ (SelectRow '/dc-1/Table row_ cols_))
            (let ret_ (AsList (SetResult 'ret0 select_)))
            (return ret_)
        ))");

        Cout << "ALTER TABLE Table ADD COLUMN value" << Endl;
        annoyingClient.AlterTable("/dc-1", R"(
            Name: "Table"
            Columns { Name: "value" Type: "Uint64" }
        )");

        Cout << "SELECT value FROM Table WHERE key = 0" << Endl;
        annoyingClient.FlatQuery(R"((
            (let row_ '('('key (Uint32 '0))))
            (let cols_ '('value))
            (let select_ (SelectRow '/dc-1/Table row_ cols_))
            (let ret_ (AsList (SetResult 'ret0 select_)))
            (return ret_)
        ))");

        Cout << "ALTER TABLE Table DROP PRIMARY KEY, ADD COLUMN key2 ADD PRIMARY KEY(key, key2)" << Endl;
        annoyingClient.AlterTable("/dc-1", R"(
            Name: "Table"
            Columns { Name: "key2" Type: "Uint64" }
            KeyColumnNames: ["key", "key2"]
        )");

        Cout << "SELECT value FROM Table WHERE key = 0 AND key2 = 0" << Endl;
        annoyingClient.FlatQuery(R"((
            (let row_ '('('key (Uint32 '0)) '('key2 (Uint64 '0))))
            (let cols_ '('value))
            (let select_ (SelectRow '/dc-1/Table row_ cols_))
            (let ret_ (AsList (SetResult 'ret0 select_)))
            (return ret_)
        ))");

        Cout << "SELECT value FROM Table WHERE key = 0 AND key2 IS NULL" << Endl;
        annoyingClient.FlatQuery(R"((
            (let row_ '('('key (Uint32 '0)) '('key2 (Null))))
            (let cols_ '('value))
            (let select_ (SelectRow '/dc-1/Table row_ cols_))
            (let ret_ (AsList (SetResult 'ret0 select_)))
            (return ret_)
        ))");

        Cout << "SELECT value FROM Table WHERE key = 0" << Endl;
        annoyingClient.FlatQuery(R"((
            (let row_ '('('key (Uint32 '0))))
            (let cols_ '('value))
            (let select_ (SelectRow '/dc-1/Table row_ cols_))
            (let ret_ (AsList (SetResult 'ret0 select_)))
            (return ret_)
        ))", NMsgBusProxy::MSTATUS_ERROR);
    }

    Y_UNIT_TEST(MiniKQLRanges) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        if (!true) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::PIPE_CLIENT, NActors::NLog::PRI_DEBUG);
        }

        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "spuchin");

        annoyingClient.CreateTable("/dc-1/spuchin",
                                   R"___(
                                       Name: "TestTable2"
                                       Columns { Name: "Group" Type: "Uint32" }
                                       Columns { Name: "Name" Type: "String" }
                                       Columns { Name: "Amount" Type: "Uint64" }
                                       Columns { Name: "Comment" Type: "String"}
                                       KeyColumnNames: "Group"
                                       KeyColumnNames: "Name"
                                   )___"
                                   );

        // SelectRange, where end < begin
        annoyingClient.FlatQuery(
                R"___(
                    (
                    (let $1 (VoidType))
                    (let $2 (ListType $1))
                    (let $3 (List $2))
                    (let $4 (Uint32 '"1002"))
                    (let $5 '('"Group" $4 $4))
                    (let $6 (String '"Name2"))
                    (let $7 (String '"Name1"))
                    (let $8 '('"Name" $6 $7))
                    (let $9 '('"ExcFrom" '"IncTo" $5 $8))
                    (let $10 '('"Group" '"Name" '"Amount"))
                    (let $11 '())
                    (let $12 (SelectRange '"/dc-1/spuchin/TestTable2" $9 $10 $11))
                    (let $13 (Member $12 '"List"))
                    (let $14 (lambda '($19) (block '(
                      (let $21 (Member $19 '"Amount"))
                      (let $22 '('"Amount" $21))
                      (let $23 (AsStruct $22))
                      (return $23)
                    ))))
                    (let $15 (Map $13 $14))
                    (let $16 '($15))
                    (let $17 (SetResult '"Result" $16))
                    (let $18 (Append $3 $17))
                    (return $18)
                    )
                )___"
                );
    }

    void TestLsSuccess(TFlatMsgBusClient& annoyingClient, const TString& name, const TVector<TString>& children) {
        TString selfName = name;
        if (selfName != "/") {
            selfName= name.substr(name.find_last_of('/')+1);
        }
        TAutoPtr<NMsgBusProxy::TBusResponse> res = annoyingClient.Ls(name);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Record.GetPathDescription().GetSelf().GetName(), selfName, "Self name doesn't match");

        // Compare expected and actual children count
        UNIT_ASSERT_VALUES_EQUAL_C(res->Record.GetPathDescription().ChildrenSize(), children.size(),
                                   "Unexpected number of children for " + name);

        THashSet<TString> actualChildren;
        TString prevName;
        for (size_t i = 0; i < res->Record.GetPathDescription().ChildrenSize(); ++i) {
            TString name = res->Record.GetPathDescription().GetChildren(i).GetName();
            bool res = actualChildren.insert(name).second;
            UNIT_ASSERT_C(res, "Repeating child: " + name);
            UNIT_ASSERT_C(prevName < name, "Children are not sorted: " + prevName + ", " + name);
            prevName = name;
        }
        // compare expected and actual children lists
        for (const auto& cname : children) {
            bool res = actualChildren.count(cname);
            UNIT_ASSERT_C(res, "Child not found: " + cname);
        }
    }

    void TestLsUknownPath(TFlatMsgBusClient& annoyingClient, const TString& name) {
        TAutoPtr<NMsgBusProxy::TBusResponse> res = annoyingClient.Ls(name);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Record.HasPathDescription(), false,
                                   "Unxepected description for " + name);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Record.GetStatus(), NMsgBusProxy::MSTATUS_ERROR,
                                   "Unexpected status for " + name);
    }

    Y_UNIT_TEST(Ls) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port).SetEnableSystemViews(false));

        TFlatMsgBusClient annoyingClient(port);
        annoyingClient.InitRoot();

        // Listing all domains always works
        TestLsSuccess(annoyingClient, "/", {"dc-1"});
        TestLsUknownPath(annoyingClient, "");
        TestLsUknownPath(annoyingClient, "//");

        TestLsSuccess(annoyingClient, "/", {"dc-1"});
        TestLsSuccess(annoyingClient, "/dc-1", {});
        TestLsUknownPath(annoyingClient, "/dc-11");
        TestLsUknownPath(annoyingClient, "/dc-2");

        annoyingClient.MkDir("/dc-1", "Berkanavt");
        TestLsSuccess(annoyingClient, "/", {"dc-1"});
        TestLsSuccess(annoyingClient, "/dc-1", {"Berkanavt"});
        TestLsSuccess(annoyingClient, "/dc-1/Berkanavt", {});
        annoyingClient.MkDir("/dc-1", "Berkanavt");
        TestLsSuccess(annoyingClient, "/dc-1", {"Berkanavt"});

        TestLsUknownPath(annoyingClient, "/dc-1/arcadia");
        annoyingClient.MkDir("/dc-1", "arcadia");
        TestLsSuccess(annoyingClient, "/dc-1", {"Berkanavt", "arcadia"});
        TestLsSuccess(annoyingClient, "/dc-1/arcadia", {});
    }

    void SetSchemeshardReadOnly(TServer& cleverServer, TFlatMsgBusClient& annoyingClient, bool isReadOnly) {
        ui64 schemeShardTabletId = Tests::ChangeStateStorage(Tests::SchemeRoot, Tests::TestDomain);

        NKikimrMiniKQL::TResult result;
        bool ok = annoyingClient.LocalQuery(schemeShardTabletId, Sprintf(R"(
                                   (
                                        (let key '('('Id (Uint64 '3)))) # SysParam_IsReadOnlyMode
                                        (let value '('('Value (Utf8 '"%s"))))
                                        (let ret (AsList (UpdateRow 'SysParams key value)))
                                        (return ret)
                                   ))", (isReadOnly ? "1" : "0")), result);
        // Cerr << result << "\n";
        UNIT_ASSERT(ok);
        annoyingClient.KillTablet(cleverServer, schemeShardTabletId);

        // Wait for schemeshard to restart
        TInstant waitStart = TInstant::Now();
        while (true) {
            Cerr << "Waiting for schemeshard to restart...\n";
            TAutoPtr<NMsgBusProxy::TBusResponse> res = annoyingClient.Ls("/dc-1");
            if (res->Record.GetStatus() == NMsgBusProxy::MSTATUS_OK)
                break;
            Sleep(TDuration::MilliSeconds(20));
            UNIT_ASSERT_C((TInstant::Now()-waitStart).MilliSeconds() < 5000, "Schemeshard cannot start for too long");
        }
    }

    Y_UNIT_TEST(ReadOnlyMode) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port).SetEnableMockOnSingleNode(false));

        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        NMsgBusProxy::EResponseStatus status;

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);

        status = annoyingClient.MkDir("/dc-1", "Dir1");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);

        //
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::PIPE_SERVER, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::PIPE_CLIENT, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);

        SetSchemeshardReadOnly(cleverServer, annoyingClient, true);

        status = annoyingClient.MkDir("/dc-1", "Dir222");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_ERROR);

        SetSchemeshardReadOnly(cleverServer, annoyingClient, false);

        status = annoyingClient.MkDir("/dc-1", "Dir222");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
        status = annoyingClient.MkDir("/dc-1", "Dir3333");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
    }

    Y_UNIT_TEST(PathSorting) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port).SetEnableSystemViews(false));

        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "Dir1");
        annoyingClient.MkDir("/dc-1", "Dir3");
        annoyingClient.MkDir("/dc-1", "B");
        annoyingClient.MkDir("/dc-1", "Dir4");
        annoyingClient.MkDir("/dc-1", "Dir2");
        annoyingClient.MkDir("/dc-1", "A");
        TestLsSuccess(annoyingClient, "/dc-1", {"A", "B", "Dir1", "Dir2", "Dir3", "Dir4"});
    }

    void TestLsPathIdSuccess(TFlatMsgBusClient& annoyingClient, ui64 schemeshardId, ui64 pathId, const TString& selfName, const TVector<TString>& children) {
        TAutoPtr<NMsgBusProxy::TBusResponse> res = annoyingClient.LsPathId(schemeshardId, pathId);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Record.GetPathDescription().GetSelf().GetName(), selfName, "Self name doesn't match");

        // Compare expected and actual children count
        UNIT_ASSERT_VALUES_EQUAL_C(res->Record.GetPathDescription().ChildrenSize(), children.size(),
                                   "Unexpected number of children for " + selfName);

        THashSet<TString> actualChildren;
        for (size_t i = 0; i < res->Record.GetPathDescription().ChildrenSize(); ++i) {
            TString n = res->Record.GetPathDescription().GetChildren(i).GetName();
            bool res = actualChildren.insert(n).second;
            UNIT_ASSERT_C(res, "Repeating child: " + n);
        }
        // compare expected and actual children lists
        for (const auto& cname : children) {
            bool res = actualChildren.count(cname);
            UNIT_ASSERT_C(res, "Child not found: " + cname);
        }
    }

    void TestLsUknownPathId(TFlatMsgBusClient& annoyingClient,  ui64 schemeshardId, ui64 pathId) {
        TAutoPtr<NMsgBusProxy::TBusResponse> res = annoyingClient.LsPathId(schemeshardId, pathId);
        UNIT_ASSERT_VALUES_EQUAL_C(res->Record.HasPathDescription(), false,
                                   "Unxepected description for pathId " + ToString(pathId));
        UNIT_ASSERT_VALUES_EQUAL_C(res->Record.GetStatus(), NMsgBusProxy::MSTATUS_ERROR,
                                   "Unexpected status for pathId " + ToString(pathId));
    }

    Y_UNIT_TEST(LsPathId) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port).SetEnableSystemViews(false));

        TFlatMsgBusClient annoyingClient(port);

        TAutoPtr<NMsgBusProxy::TBusResponse> res = annoyingClient.Ls("/");
        ui64 schemeshardId = res->Record.GetPathDescription().GetChildren(0).GetSchemeshardId();

        annoyingClient.InitRoot();
        TestLsPathIdSuccess(annoyingClient, schemeshardId, 1, "dc-1", {});
        TestLsUknownPathId(annoyingClient, schemeshardId, 2);
        annoyingClient.MkDir("/dc-1", "Berkanavt");
        TestLsPathIdSuccess(annoyingClient, schemeshardId, 1, "dc-1", {"Berkanavt"});
        TestLsPathIdSuccess(annoyingClient, schemeshardId, 2, "Berkanavt", {});
        TestLsUknownPathId(annoyingClient, schemeshardId, 3);
        annoyingClient.MkDir("/dc-1", "arcadia");
        TestLsPathIdSuccess(annoyingClient, schemeshardId, 1, "dc-1", {"Berkanavt", "arcadia"});
        TestLsPathIdSuccess(annoyingClient, schemeshardId, 3, "arcadia", {});
    }

    ui32 TestInitRoot(TFlatMsgBusClient& annoyingClient, const TString& name) {
        TAutoPtr<NBus::TBusMessage> reply = annoyingClient.InitRootSchemeWithReply(name);
        TAutoPtr<NMsgBusProxy::TBusResponse> res = dynamic_cast<NMsgBusProxy::TBusResponse*>(reply.Release());
        UNIT_ASSERT(res);
        return res->Record.GetStatus();
    }

    Y_UNIT_TEST(CheckACL) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBuiltinDomain(true);
        TServer cleverServer = TServer(TServerSettings(port, authConfig));
        if (!true) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::HIVE, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_MAIN, NActors::NLog::PRI_DEBUG);
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::PIPE_CLIENT, NActors::NLog::PRI_DEBUG);
        }

        TFlatMsgBusClient annoyingClient(port);
        annoyingClient.InitRoot();
        annoyingClient.ModifyOwner("/", "dc-1", "berkanavt@" BUILTIN_ACL_DOMAIN);
        annoyingClient.SetSecurityToken("berkanavt@" BUILTIN_ACL_DOMAIN); // there is should be something like "234ba4f44ef7c"

        NMsgBusProxy::EResponseStatus status;

        status = annoyingClient.MkDir("/dc-1", "Berkanavt");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
        status = annoyingClient.MkDir("/dc-1/Berkanavt", "tables");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);

        status = annoyingClient.CreateTable("/dc-1/Berkanavt",
                                   "Name: \"Unused\""
                                       "Columns { Name: \"key1\"       Type: \"Uint32\"}"
                                       "Columns { Name: \"key2\"       Type: \"Utf8\"}"
                                       "Columns { Name: \"RowId\"      Type: \"Uint64\"}"
                                       "Columns { Name: \"Value\"      Type: \"Utf8\"}"
                                       "KeyColumnNames: [\"RowId\", \"key1\", \"key2\"]"
                                   );
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);

        status = annoyingClient.CreateTable("/dc-1/Berkanavt/tables",
                                   "Name: \"Students\""
                                        "Columns { Name: \"Id\"          Type: \"Uint32\"}"
                                        "Columns { Name: \"Name\"        Type: \"Utf8\"}"
                                        "Columns { Name: \"LastName\"    Type: \"Utf8\"}"
                                        "Columns { Name: \"Age\"         Type: \"Uint32\"}"
                                        "KeyColumnNames: [\"Id\"]"
                                        "UniformPartitionsCount: 10"
                                   );
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);

        TAutoPtr<NMsgBusProxy::TBusResponse> response;

        response = annoyingClient.Ls("/");
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        response = annoyingClient.Ls("/dc-100");
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_ERROR);
        response = annoyingClient.Ls("/dc-1/Argonaut");
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_ERROR);
        response = annoyingClient.Ls("/dc-1/Berkanavt/tabls");
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_ERROR);
        response = annoyingClient.Ls("/dc-1/Berkanavt/tables");
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        response = annoyingClient.Ls("/dc-1/Berkanavt/tables/Students");
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);

        ui64 studentsTableId;
        {
            TAutoPtr<NMsgBusProxy::TBusResponse> response = annoyingClient.Ls("/dc-1/Berkanavt/tables/Students");
            studentsTableId = response.Get()->Record.GetPathDescription().GetSelf().GetPathId();
        }
        TTableId tabletId(ChangeStateStorage(Tests::SchemeRoot, TestDomain), studentsTableId);

        annoyingClient.SetSecurityToken("argonaut@" BUILTIN_ACL_DOMAIN); // there is should be something like "234ba4f44ef7c"
        annoyingClient.FlatQuery("((return (AsList (SetResult 'res1 (Int32 '2016)))))");

        const char * updateProgram = R"((
            (let update_ '('('Name (Utf8 'Robert)) '('Age (Uint32 '21))))
            (return (AsList (UpdateRow '/dc-1/Berkanavt/tables/Students '('('Id (Uint32 '42))) update_)))
        ))";

        // Update
        annoyingClient.FlatQuery(updateProgram,
                    NMsgBusProxy::MSTATUS_ERROR,
                    TEvTxUserProxy::TResultStatus::AccessDenied); // as argonaut@

        annoyingClient.SetSecurityToken("berkanavt@" BUILTIN_ACL_DOMAIN); // there is should be something like "234ba4f44ef7c"
        annoyingClient.FlatQuery(updateProgram); // as berkanavt@

        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericWrite, "argonaut@" BUILTIN_ACL_DOMAIN);

        annoyingClient.SetSecurityToken("argonaut@" BUILTIN_ACL_DOMAIN);
        annoyingClient.ModifyACL("/", "dc-1", acl.SerializeAsString()); // as argonaut@
        annoyingClient.ResetSchemeCache(cleverServer, tabletId);
        annoyingClient.FlatQuery(updateProgram,
            NMsgBusProxy::MSTATUS_ERROR,
            TEvTxUserProxy::TResultStatus::AccessDenied); // as argonaut@

        annoyingClient.SetSecurityToken("berkanavt@" BUILTIN_ACL_DOMAIN);
        annoyingClient.ModifyACL("/", "dc-1", acl.SerializeAsString()); // as berkanavt@
        annoyingClient.ResetSchemeCache(cleverServer, tabletId);
        annoyingClient.SetSecurityToken("argonaut@" BUILTIN_ACL_DOMAIN);
        annoyingClient.FlatQuery(updateProgram); // as argonaut@

        // the same but without first '/'
        annoyingClient.ModifyACL("", "dc-1", acl.SerializeAsString()); // as berkanavt@
        annoyingClient.ResetSchemeCache(cleverServer, tabletId);
        annoyingClient.SetSecurityToken("argonaut@" BUILTIN_ACL_DOMAIN);
        annoyingClient.FlatQuery(updateProgram); // as argonaut@

        acl.ClearAccess();
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericRead, "argonaut@" BUILTIN_ACL_DOMAIN);

        annoyingClient.ModifyACL("/dc-1", "Berkanavt", acl.SerializeAsString()); // as argonaut@
        annoyingClient.ResetSchemeCache(cleverServer, tabletId);
        annoyingClient.SetSecurityToken("argonaut@" BUILTIN_ACL_DOMAIN);
        annoyingClient.FlatQuery(updateProgram); // as argonaut@
#if 0
        annoyingClient.SetSecurityToken("berkanavt@" BUILTIN_ACL_DOMAIN);
        annoyingClient.ModifyACL("/dc-1", "Berkanavt", acl.SerializeAsString()); // as berkanavt@
        annoyingClient.ResetSchemeCache(cleverServer, tabletId);
        annoyingClient.SetSecurityToken("argonaut@" BUILTIN_ACL_DOMAIN);
        annoyingClient.FlatQuery(updateProgram,
            NMsgBusProxy::MSTATUS_ERROR,
            TEvTxUserProxy::TResultStatus::AccessDenied); // as argonaut@

        annoyingClient.SetSecurityToken("berkanavt@" BUILTIN_ACL_DOMAIN); // as berkanavt@
        NACLib::TDiffACL newAcl;
        newAcl.ClearAccess();
        newAcl.AddAccess(NACLib::EAccessType::Allow, NACLib::GenericWrite, "argonaut@" BUILTIN_ACL_DOMAIN);
        annoyingClient.ModifyACL("/dc-1", "Berkanavt", newAcl.SerializeAsString()); // as berkanavt@
        annoyingClient.ResetSchemeCache(cleverServer, tabletId);
        annoyingClient.SetSecurityToken("argonaut@" BUILTIN_ACL_DOMAIN);
        annoyingClient.FlatQuery(updateProgram); // as argonaut@
#endif
    }

    Y_UNIT_TEST(OutOfDiskSpace) {
        return;  // TODO KIKIMR-2279
        TPortManager pm;
        ui16 port = pm.GetPort(2134);

        NFake::TStorage diskParams;
        diskParams.DiskSize = 500ull*1024*1024;
        diskParams.SectorSize = 512;
        diskParams.ChunkSize = 50ull*1024*1024;

        NKikimrConfig::TImmediateControlsConfig controls;
        controls.MutableTxLimitControls()->SetPerRequestDataSizeLimit(1000000000);
        controls.MutableTxLimitControls()->SetPerShardIncomingReadSetSizeLimit(1000000000);
        controls.MutableTxLimitControls()->SetPerShardReadSizeLimit(1000000000);

        TServer cleverServer = TServer(TServerSettings(port)
                                       .SetControls(controls)
                                       .SetCustomDiskParams(diskParams)
                                       .SetEnableMockOnSingleNode(false));

        TFlatMsgBusClient annoyingClient(port);

        const char * table = "Name: \"Table\""
                            "Columns { Name: \"Key\"    Type: \"Uint32\"}"
                            "Columns { Name: \"Value\"  Type: \"Utf8\"}"
                            "KeyColumnNames: [\"Key\"]"
                            "UniformPartitionsCount: 2";

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "Dir");
        annoyingClient.CreateTable("/dc-1/Dir", table);

        TString insertRowQuery = "("
                "(let key '('('Key (Uint32 '%u))))"
                "(let value '('('Value (Utf8 '%s))))"
                "(let ret_ (AsList"
                "    (UpdateRow '/dc-1/Dir/Table key value)"
                "))"
                "(return ret_)"
                ")";

        TClient::TFlatQueryOptions opts;
        NKikimrClient::TResponse response;

        int errorCount = 0;
        for (ui32 i = 0; i < 20; ++i) {
            Cout << "row " << i << Endl;
            ui32 status = annoyingClient.FlatQueryRaw(Sprintf(insertRowQuery.data(), i, TString(6000000, 'A').data()), opts, response);
            UNIT_ASSERT(status == NMsgBusProxy::MSTATUS_OK || status == NMsgBusProxy::MSTATUS_REJECTED);
            if (status == NMsgBusProxy::MSTATUS_REJECTED) {
                ++errorCount;
            }
        }
        UNIT_ASSERT_C(errorCount > 0, "Out of disk space error must have happened");

        TString readQuery =
                "("
                "(let range '('ExcFrom '('Key (Uint32 '0) (%s))))"
                "(let select '('Key))"
                "(let options '())"
                "(let pgmReturn (AsList"
                "    (SetResult 'myRes (SelectRange '/dc-1/Dir/Table range select options %s))"
                "))"
                "(return pgmReturn)"
                ")";

        ui32 status = 0;
        status = annoyingClient.FlatQueryRaw(Sprintf(readQuery.data(), "Uint32 '10", "'head"), opts, response);
        UNIT_ASSERT_VALUES_EQUAL_C(status, NMsgBusProxy::MSTATUS_OK, "Single-shard read query should not fail");

        status = annoyingClient.FlatQueryRaw(Sprintf(readQuery.data(), "Uint32 '10", ""), opts, response);
        UNIT_ASSERT_VALUES_EQUAL_C(status, NMsgBusProxy::MSTATUS_OK, "Single-shard read query should not fail");

        status = annoyingClient.FlatQueryRaw(Sprintf(readQuery.data(), "Uint32 '3000000000", ""), opts, response);
        UNIT_ASSERT_VALUES_EQUAL_C(status, NMsgBusProxy::MSTATUS_REJECTED, "Multi-shard read query should fail");
    }

    void TestRejectByPerShardReadSize(const NKikimrConfig::TImmediateControlsConfig& controls,
                                      TString tableConfig) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port)
                                       .SetControls(controls));

        TFlatMsgBusClient annoyingClient(port);

        TString table =
                " Name: \"Table\""
                " Columns { Name: \"Key\"    Type: \"Uint32\"}"
                " Columns { Name: \"Value\"  Type: \"Utf8\"}"
                " KeyColumnNames: [\"Key\"]"
                " UniformPartitionsCount: 2 "
                + tableConfig;

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "Dir");
        annoyingClient.CreateTable("/dc-1/Dir", table);

        TString insertRowQuery = "("
                "(let key '('('Key (Uint32 '%u))))"
                "(let value '('('Value (Utf8 '%s))))"
                "(let ret_ (AsList"
                "    (UpdateRow '/dc-1/Dir/Table key value)"
                "))"
                "(return ret_)"
                ")";

        for (ui32 i = 0; i < 100; ++i) {
            annoyingClient.FlatQuery(Sprintf(insertRowQuery.data(), i, TString(1000000, 'A').data()));
        }

        ui32 status = 0;
        TClient::TFlatQueryOptions opts;
        NKikimrClient::TResponse response;
        status = annoyingClient.FlatQueryRaw(Sprintf(R"(
            (
            (let range '('ExcFrom 'IncTo '('Key (Null) (Void))))
            (let data (Member (SelectRange '/dc-1/Dir/Table range '('Key 'Value) '()) 'List))
            (let result (Filter data (lambda '(row)
                (Coalesce (NotEqual (Member row 'Key) (Uint32 '1)) (Bool 'false))
            )))
            (return (AsList (SetResult 'Result result)))
            )
            )"
        ), opts, response);

        UNIT_ASSERT_VALUES_EQUAL_C((NMsgBusProxy::EResponseStatus)status, NMsgBusProxy::MSTATUS_ERROR, "Big read should fail");
    }

    Y_UNIT_TEST(RejectByPerShardReadSize) {
        NKikimrConfig::TImmediateControlsConfig controls;
        controls.MutableTxLimitControls()->SetPerRequestDataSizeLimit(1000000000);
        controls.MutableTxLimitControls()->SetPerShardIncomingReadSetSizeLimit(1000000000);
        controls.MutableTxLimitControls()->SetPerShardReadSizeLimit(1000000000);

        // Test per-table limit
        TestRejectByPerShardReadSize(controls, " PartitionConfig { TxReadSizeLimit: 10000 } ");

        // Test per-domain limit
        controls.MutableTxLimitControls()->SetPerShardReadSizeLimit(10000);
        TestRejectByPerShardReadSize(controls, "");
    }

    Y_UNIT_TEST(RejectByPerRequestSize) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);

        NKikimrConfig::TImmediateControlsConfig controls;
        controls.MutableTxLimitControls()->SetPerRequestDataSizeLimit(10000);
        controls.MutableTxLimitControls()->SetPerShardIncomingReadSetSizeLimit(1000000000);
        controls.MutableTxLimitControls()->SetPerShardReadSizeLimit(1000000000);

        TServer cleverServer = TServer(TServerSettings(port)
                                       .SetControls(controls));

        TFlatMsgBusClient annoyingClient(port);

        const char * table =
                " Name: \"Table\""
                " Columns { Name: \"Key\"    Type: \"Uint32\"}"
                " Columns { Name: \"Value\"  Type: \"Utf8\"}"
                " KeyColumnNames: [\"Key\"]"
                " SplitBoundary { KeyPrefix { Tuple { Optional { Uint32 : 10000 } } }}"
                " PartitionConfig { TxReadSizeLimit: 100000000 } ";

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "Dir");
        annoyingClient.CreateTable("/dc-1/Dir", table);

        TString insertRowQuery = "("
                "(let key '('('Key (Uint32 '%u))))"
                "(let value '('('Value (Utf8 '%s))))"
                "(let ret_ (AsList"
                "    (UpdateRow '/dc-1/Dir/Table key value)"
                "))"
                "(return ret_)"
                ")";

        for (ui32 i = 5000; i < 5020; ++i) {
            annoyingClient.FlatQuery(Sprintf(insertRowQuery.data(), i, TString(1000000, 'A').data()));
            annoyingClient.FlatQuery(Sprintf(insertRowQuery.data(), (i/2)+10000, TString(1000000, 'A').data()));
        }

        TString readQuery = R"(
                (
                (let range1 '('ExcFrom '('Key (Uint32 '0) (Void))))
                (let range2 '('ExcFrom '('Key (Uint32 '10) (Void))))
                (let select '('Key 'Value))
                (let options '())

                (let filterFunc (lambda '(row)
                    (Coalesce (NotEqual (Member row 'Key) (Uint32 '1)) (Bool 'false))
                ))

                (let list1 (Member (SelectRange '/dc-1/Dir/Table range1 select options) 'List))
                (let list2 (Member (SelectRange '/dc-1/Dir/Table range2 select options) 'List))

                (let pgmReturn (AsList
                    (SetResult 'myRes1 (Filter list1 filterFunc) )
                    (SetResult 'myRes2 (Filter list2 filterFunc) )
                ))
                (return pgmReturn)
                )

                )";

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        ui32 status = 0;
        TClient::TFlatQueryOptions opts;
        NKikimrClient::TResponse response;
        status = annoyingClient.FlatQueryRaw(readQuery, opts, response);
        UNIT_ASSERT_VALUES_EQUAL_C((NMsgBusProxy::EResponseStatus)status, NMsgBusProxy::MSTATUS_ERROR, "Big read should fail");
    }

    Y_UNIT_TEST(RejectByIncomingReadSetSize) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        NKikimrConfig::TImmediateControlsConfig controls;
        controls.MutableTxLimitControls()->SetPerRequestDataSizeLimit(100000000);
        controls.MutableTxLimitControls()->SetPerShardIncomingReadSetSizeLimit(1000);
        controls.MutableTxLimitControls()->SetPerShardReadSizeLimit(100000000);

        TServer cleverServer = TServer(TServerSettings(port)
                                       .SetControls(controls));

        TFlatMsgBusClient annoyingClient(port);

        const char * table =
                " Name: \"Table\""
                " Columns { Name: \"Key\"    Type: \"Uint32\"}"
                " Columns { Name: \"Value\"  Type: \"Utf8\"}"
                " KeyColumnNames: [\"Key\"]"
                " UniformPartitionsCount: 2"
                " PartitionConfig { TxReadSizeLimit: 100000000 } ";

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "Dir");
        annoyingClient.CreateTable("/dc-1/Dir", table);

        TString insertRowQuery = "("
                "(let key '('('Key (Uint32 '%u))))"
                "(let value '('('Value (Utf8 '%s))))"
                "(let ret_ (AsList"
                "    (UpdateRow '/dc-1/Dir/Table key value)"
                "))"
                "(return ret_)"
                ")";

        for (ui32 i = 4241; i < 4281; ++i) {
            annoyingClient.FlatQuery(Sprintf(insertRowQuery.data(), i, TString(1000000, 'A').data()));
        }

        TString readQuery =
                "("
                "(let key1 '('('Key (Uint32 '4242))))"
                "(let row (SelectRow '/dc-1/Dir/Table key1 '('Value)))"
                "(let val (IfPresent row (lambda '(r) ( Coalesce (Member r 'Value) (Utf8 'AAA))) (Utf8 'BBB)))"
                "(let key2 '('('Key (Uint32 '3333333333))))"
                "(let upd (UpdateRow '/dc-1/Dir/Table key2 '('('Value val))))"
                "(let range1 '('ExcFrom '('Key (Uint32 '0) (Void))))"
                "(let range2 '('ExcFrom '('Key (Uint32 '10) (Void))))"
                "(let select '('Key 'Value))"
                "(let options '())"
                "(let pgmReturn (AsList upd))"
                "(return pgmReturn)"
                ")";

        ui32 status = 0;
        TClient::TFlatQueryOptions opts;
        NKikimrClient::TResponse response;
        status = annoyingClient.FlatQueryRaw(readQuery, opts, response);
        UNIT_ASSERT_VALUES_EQUAL_C((NMsgBusProxy::EResponseStatus)status, NMsgBusProxy::MSTATUS_ERROR, "Big read should fail");
    }

    void RunWriteQueryRetryOverloads(TFlatMsgBusClient& annoyingClient, TString query) {
        i32 retries = 10;
        TFlatMsgBusClient::TFlatQueryOptions opts;
        NKikimrClient::TResponse response;
        while (retries) {
            annoyingClient.FlatQueryRaw(query, opts, response);

            if (response.GetStatus() !=  NMsgBusProxy::MSTATUS_REJECTED ||
                response.GetProxyErrorCode() != TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardOverloaded) {
                break;
            }

            --retries;
            Sleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_C(retries > 0, "Failed to write row");
        UNIT_ASSERT_VALUES_EQUAL_C(response.GetStatus(), NMsgBusProxy::MSTATUS_OK, "Failed to write row");
        UNIT_ASSERT_VALUES_EQUAL_C(response.GetExecutionEngineResponseStatus(), (ui32)NMiniKQL::IEngineFlat::EStatus::Complete, "Failed to write row");
    }

    void WriteRow(TFlatMsgBusClient& annoyingClient, TString table, ui32 key, TString value, TArrayRef<const char> large) {
        TString insertRowQuery = "("
                "(let key '('('Key (Uint32 '%u))))"
                "(let value '('Value (Utf8 '%s)))"
                "(let large '('Large (String '%s)))"
                "(let ret_ (AsList"
                "    (UpdateRow '/dc-1/Dir/%s key '(value large))"
                "))"
                "(return ret_)"
                ")";

        RunWriteQueryRetryOverloads(annoyingClient, Sprintf(insertRowQuery.data(), key, value.data(), large.data(), table.data()));
    }

    void WriteRow(TFlatMsgBusClient& annoyingClient, TString table, ui32 key, TString value) {
        TString insertRowQuery = "("
                "(let key '('('Key (Uint32 '%u))))"
                "(let value '('('Value (Utf8 '%s))))"
                "(let ret_ (AsList"
                "    (UpdateRow '/dc-1/Dir/%s key value)"
                "))"
                "(return ret_)"
                ")";

        RunWriteQueryRetryOverloads(annoyingClient, Sprintf(insertRowQuery.data(), key, value.data(), table.data()));
    }

    void WriteRandomRows(TFlatMsgBusClient &client, TString table, ui64 seed, ui32 rows) {
        TMersenne<ui64> rnd(seed);
        NTable::NTest::TRandomString<decltype(rnd)> blobs(rnd);

        for (auto seq : xrange(rows)) {
            WriteRow(client, table, seq, blobs.Do(rnd.Uniform(4, 1600)));
        }
    }

    TString ReadRow(TFlatMsgBusClient& annoyingClient, TString table, ui32 key) {
        TString query =
                    R"(
                    (
                    (let row '('('Key (Uint32 '%u))))
                    (let select '('Value))
                    (let pgmReturn (AsList
                        (SetResult 'row (SelectRow '/dc-1/Dir/%s row select))
                    ))
                    (return pgmReturn)
                    )
                    )";

        NKikimrMiniKQL::TResult readRes;
        bool res = annoyingClient.FlatQuery(Sprintf(query.data(), key, table.data()), readRes);
        UNIT_ASSERT(res);

        //Cerr << readRes << Endl;
        TValue value = TValue::Create(readRes.GetValue(), readRes.GetType());
        TValue row = value["row"];
        TString strRes(row["Value"]);
        return strRes;
    }

    void PrepareSourceTable(TFlatMsgBusClient& annoyingClient, bool withFollowers = false) {
        const char * table = R"___(
                Name: "TableOld"
                Columns { Name: "Key"    Type: "Uint32"}
                Columns { Name: "Value"  Type: "Utf8"}
                Columns { Name: "Large"  Type: "String" Family: 0 }
                Columns { Name: "unused001"  Type: "Bool"}
                Columns { Name: "unused002"  Type: "Uint32"}
                Columns { Name: "unused003"  Type: "Int64"}
                Columns { Name: "unused004"  Type: "Float"}
                KeyColumnNames: ["Key"]
                UniformPartitionsCount: 2

                PartitionConfig {
                    FollowerCount: %d
                    PartitioningPolicy {
                        MinPartitionsCount: 0
                    }
                    CompactionPolicy {
                      InMemSizeToSnapshot: 100000
                      InMemStepsToSnapshot: 2
                      InMemForceStepsToSnapshot: 3
                      InMemForceSizeToSnapshot: 1000000
                      InMemCompactionBrokerQueue: 0
                      ReadAheadHiThreshold: 200000
                      ReadAheadLoThreshold: 100000
                      MinDataPageSize: 7168
                      SnapBrokerQueue: 0
                        Generation {
                            GenerationId: 0
                            SizeToCompact: 10000
                            CountToCompact: 2
                            ForceCountToCompact: 2
                            ForceSizeToCompact: 20000
                            CompactionBrokerQueue: 1
                            KeepInCache: true
                        }
                    }
                    ColumnFamilies {
                        Id: 0
                        Storage: ColumnStorageTest_1_2_1k
                        ColumnCache: ColumnCacheNone
                    }
                    EnableFilterByKey: true
                }
            )___";

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "Dir");
        annoyingClient.CreateTable("/dc-1/Dir", Sprintf(table, withFollowers ? 2 : 0));

        TMersenne<ui64> rnd;
        NTable::NTest::TRandomString<decltype(rnd)> blobs(rnd);

        for (ui32 i = 0; i < 8; ++i) {
            WriteRow(annoyingClient, "TableOld", i, "AAA", blobs.Do(rnd.Uniform(512, 1536)));
            WriteRow(annoyingClient, "TableOld", 0x80000000 + i, "BBB", blobs.Do(rnd.Uniform(512, 1536)));
        }
    }

    TString ReadFromTable(TFlatMsgBusClient& annoyingClient, TString table, ui32 fromKey = 0, bool follower = false) {
        const char* readQuery =
                "("
                "(let range1 '('IncFrom '('Key (Uint32 '%d) (Void) )))"
                "(let select '('Key 'Value 'Large))"
                "(let options '())"
                "(let pgmReturn (AsList"
                "    (SetResult 'range1 (SelectRange '%s range1 select options (Uint32 '%d)))"
                "))"
                "(return pgmReturn)"
                ")";

        TFlatMsgBusClient::TFlatQueryOptions opts;
        NKikimrClient::TResponse response;
        annoyingClient.FlatQueryRaw(Sprintf(readQuery, fromKey, table.data(), follower ? TReadTarget::Follower().GetMode()
            : TReadTarget::Head().GetMode()), opts, response);

        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT(response.GetExecutionEngineResponseStatus() == ui32(NMiniKQL::IEngineFlat::EStatus::Complete));
        NKikimrMiniKQL::TResult result;
        result.Swap(response.MutableExecutionEngineEvaluatedResponse());

        TString strResult;
        ::google::protobuf::TextFormat::PrintToString(result.GetValue(), &strResult);
        return strResult;
    }

    TString ReadFromTwoKeysTable(TFlatMsgBusClient& annoyingClient, TString table, ui32 fromKey = 0, ui32 fromKey2 = 0, bool follower = false) {
        const char* readQuery =
                "("
                "(let range1 '('IncFrom '('Key (Uint32 '%d) (Void)) '('Key2 (Uint32 '%d) (Void)) ))"
                "(let select '('Key 'Key2 'Value 'Large))"
                "(let options '())"
                "(let pgmReturn (AsList"
                "    (SetResult 'range1 (SelectRange '%s range1 select options (Uint32 '%d)))"
                "))"
                "(return pgmReturn)"
                ")";

        TFlatMsgBusClient::TFlatQueryOptions opts;
        NKikimrClient::TResponse response;
        annoyingClient.FlatQueryRaw(Sprintf(readQuery, fromKey, fromKey2, table.data(), follower ? TReadTarget::Follower().GetMode()
            : TReadTarget::Head().GetMode()), opts, response);

        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT(response.GetExecutionEngineResponseStatus() == ui32(NMiniKQL::IEngineFlat::EStatus::Complete));
        NKikimrMiniKQL::TResult result;
        result.Swap(response.MutableExecutionEngineEvaluatedResponse());

        TString strResult;
        ::google::protobuf::TextFormat::PrintToString(result.GetValue(), &strResult);
        return strResult;
    }

    template <class TSetType>
    void WaitForTabletsToBeDeletedInHive(TFlatMsgBusClient& annoyingClient, TTestActorRuntime* runtime,
                                         const TSetType& tabletIds, const TDuration& timeout = TDuration::Seconds(20)) {
        TInstant waitStart = TInstant::Now();
        for (ui64 tabletId : tabletIds) {
            Cerr << "Check that tablet " << tabletId << " was deleted\n";
            while (annoyingClient.TabletExistsInHive(runtime, tabletId)) {
                UNIT_ASSERT_C((TInstant::Now()-waitStart) < timeout, "Tablet " << tabletId << " was not deleted");
                Sleep(TDuration::MilliSeconds(300));
            }
        }
    }


    Y_UNIT_TEST(CopyTableAndRead) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);

        TServer cleverServer = TServer(TServerSettings(port));

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);

        PrepareSourceTable(annoyingClient);

        // Copy the table
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
        Cerr << "Copy TableOld to Table" << Endl;

        annoyingClient.CreateTable("/dc-1/Dir", " Name: \"Table\" CopyFromTable: \"/dc-1/Dir/TableOld\"");

        TString strResultOld = ReadFromTable(annoyingClient, "/dc-1/Dir/TableOld");
        TString strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/Table");

        Cout << strResultOld << Endl;
        UNIT_ASSERT_NO_DIFF(strResult, strResultOld);

        // Make second copy of the old table
        Cerr << "Copy TableOld to Table2" << Endl;
        annoyingClient.CreateTable("/dc-1/Dir", " Name: \"Table2\" CopyFromTable: \"/dc-1/Dir/TableOld\"");
        strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/Table2");
        UNIT_ASSERT_NO_DIFF(strResult, strResultOld);
    }

    Y_UNIT_TEST(CopyTableAndCompareColumnsSchema) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);

        TServer cleverServer = TServer(TServerSettings(port));

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::BS_CONTROLLER, NActors::NLog::PRI_ERROR);

        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "Dir");

        for (int colCount = 1; colCount < 100; ++colCount) {
            NKikimrSchemeOp::TTableDescription schema;
            TString name = "Table_" + ToString(colCount);
            schema.SetName(name);
            Cout << name << Endl;
            for (int ci : xrange(colCount)) {
                auto col = schema.AddColumns();
                col->SetName("col_" + ToString(ci));
                col->SetType("Int32");
            }
            schema.AddKeyColumnNames("col_0");

            while (annoyingClient.CreateTable("/dc-1/Dir", schema) != NMsgBusProxy::MSTATUS_OK) {}
            while (annoyingClient.CreateTable("/dc-1/Dir", " Name: '" + name + "_Copy' CopyFromTable: '/dc-1/Dir/" + name + "'") != NMsgBusProxy::MSTATUS_OK) {}

            auto fnGetColumns = [](const NMsgBusProxy::TBusResponse* lsRes) {
                UNIT_ASSERT(lsRes);
                //Cout << lsRes->Record << Endl;
                UNIT_ASSERT_VALUES_EQUAL(lsRes->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
                THashMap<ui32, TString> columns;
                for (const auto& ci : lsRes->Record.GetPathDescription().GetTable().GetColumns()) {
                    columns[ci.GetId()] = ci.GetName();
                }
                UNIT_ASSERT(!columns.empty());
                return columns;
            };

            auto orig = annoyingClient.Ls("/dc-1/Dir/" + name);
            auto copy = annoyingClient.Ls("/dc-1/Dir/" + name + "_Copy");

            auto origColumns = fnGetColumns(orig.Get());
            auto copyColumns = fnGetColumns(copy.Get());

            UNIT_ASSERT_VALUES_EQUAL(origColumns.size(), copyColumns.size());

            for (const auto& oc : origColumns) {
                UNIT_ASSERT_VALUES_EQUAL(oc.second, copyColumns[oc.first]);
            }
        }
    }


    Y_UNIT_TEST(CopyCopiedTableAndRead) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);

        PrepareSourceTable(annoyingClient);

        // Copy the table
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);
        Cerr << "Copy TableOld to Table" << Endl;
        annoyingClient.CreateTable("/dc-1/Dir", " Name: \"Table\" CopyFromTable: \"/dc-1/Dir/TableOld\"");
        TString strResultOld = ReadFromTable(annoyingClient, "/dc-1/Dir/TableOld");
        TString strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/Table");

        Cout << strResult << Endl;
        UNIT_ASSERT_NO_DIFF(strResult, strResultOld);

        // Make second copy of the old table
        Cerr << "Copy Table to Table2" << Endl;
        annoyingClient.CreateTable("/dc-1/Dir", " Name: \"Table3\" CopyFromTable: \"/dc-1/Dir/Table\"");
        strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/Table3");
        UNIT_ASSERT_NO_DIFF(strResult, strResultOld);
    }

    Y_UNIT_TEST(CopyTableAndAddFollowers) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);

        PrepareSourceTable(annoyingClient);

        // Copy the table
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        Cerr << "Copy TableOld to Table" << Endl;
        annoyingClient.CreateTable("/dc-1/Dir", " Name: \"Table\" CopyFromTable: \"/dc-1/Dir/TableOld\""
                        " PartitionConfig { FollowerCount: 1 }");

        auto fnGetFollowerCount = [&annoyingClient] (const TString& path) {
            auto res = annoyingClient.Ls(path);
            UNIT_ASSERT(res);
            UNIT_ASSERT(res->Record.HasPathDescription());
            UNIT_ASSERT(res->Record.GetPathDescription().HasTable());
            return res->Record.GetPathDescription().GetTable().GetPartitionConfig().GetFollowerCount();
        };

        UNIT_ASSERT_VALUES_EQUAL(fnGetFollowerCount("/dc-1/Dir/Table"), 1);

        TString strResultOld = ReadFromTable(annoyingClient, "/dc-1/Dir/TableOld");
        TString strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/Table");

        Cout << strResult << Endl;
        UNIT_ASSERT_NO_DIFF(strResult, strResultOld);

        // Make copy of copy and disable followers
        Cerr << "Copy TableOld to Table2" << Endl;
        annoyingClient.CreateTable("/dc-1/Dir", " Name: \"Table2\" CopyFromTable: \"/dc-1/Dir/Table\""
                        " PartitionConfig { FollowerCount: 0 }");

        UNIT_ASSERT_VALUES_EQUAL(fnGetFollowerCount("/dc-1/Dir/Table2"), 0);

        strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/Table2");
        UNIT_ASSERT_NO_DIFF(strResult, strResultOld);
    }

    Y_UNIT_TEST(CopyTableAndDropCopy) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);

        PrepareSourceTable(annoyingClient);
        TString strResultOld = ReadFromTable(annoyingClient, "/dc-1/Dir/TableOld");
        Cerr << strResultOld << Endl;

        THashSet<ui64> datashards;
        TVector<ui64> partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/TableOld");
        datashards.insert(partitions.begin(), partitions.end());

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        // Make a copy and delete it
        annoyingClient.CreateTable("/dc-1/Dir", " Name: \"Table\" CopyFromTable: \"/dc-1/Dir/TableOld\"");
        partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/Table");
        datashards.insert(partitions.begin(), partitions.end());
        annoyingClient.DeleteTable("/dc-1/Dir", "Table");

        TString strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/TableOld");
        UNIT_ASSERT_NO_DIFF(strResult, strResultOld);

        // Make a new copy
        annoyingClient.CreateTable("/dc-1/Dir", " Name: \"Table\" CopyFromTable: \"/dc-1/Dir/TableOld\"");
        partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/Table");
        datashards.insert(partitions.begin(), partitions.end());

        strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/Table");
        UNIT_ASSERT_NO_DIFF(strResult, strResultOld);

        // Delete original table
        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");
        // Delete the copy
        annoyingClient.DeleteTable("/dc-1/Dir", "Table");

        // Recreate table with the same name
        PrepareSourceTable(annoyingClient);

        // Wait for all datashards to be deleted
        WaitForTabletsToBeDeletedInHive(annoyingClient, cleverServer.GetRuntime(), datashards);
    }

    Y_UNIT_TEST(CopyTableAndDropOriginal) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);

        PrepareSourceTable(annoyingClient);
        TString strResultOld = ReadFromTable(annoyingClient, "/dc-1/Dir/TableOld");
        Cerr << strResultOld << Endl;

        THashSet<ui64> datashards;
        auto oldShards = annoyingClient.GetTablePartitions("/dc-1/Dir/TableOld");
        datashards.insert(oldShards.begin(), oldShards.end());

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        // Make a copy
        annoyingClient.CreateTable("/dc-1/Dir", " Name: \"Table\" CopyFromTable: \"/dc-1/Dir/TableOld\"");
        auto newShards = annoyingClient.GetTablePartitions("/dc-1/Dir/Table");
        datashards.insert(newShards.begin(), newShards.end());

        // Drop original
        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");

        TString strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/Table");
        UNIT_ASSERT_NO_DIFF(strResult, strResultOld);

        // Delete the copy
        annoyingClient.DeleteTable("/dc-1/Dir", "Table");

        // Wait for all datashards to be deleted
        WaitForTabletsToBeDeletedInHive(annoyingClient, cleverServer.GetRuntime(), datashards);
    }

    Y_UNIT_TEST(CopyTableAndReturnPartAfterCompaction) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);

        PrepareSourceTable(annoyingClient);

        THashSet<ui64> datashards;
        TVector<ui64> partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/TableOld");
        datashards.insert(partitions.begin(), partitions.end());

        // Copy the table
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        Cerr << "Copy TableOld to Table" << Endl;
        annoyingClient.CreateTable("/dc-1/Dir", R"___(
                Name: "Table"
                CopyFromTable: "/dc-1/Dir/TableOld"

                PartitionConfig {
                    CompactionPolicy {
                      InMemSizeToSnapshot: 100000
                      InMemStepsToSnapshot: 2
                      InMemForceStepsToSnapshot: 3
                      InMemForceSizeToSnapshot: 1000000
                      InMemCompactionBrokerQueue: 0
                      ReadAheadHiThreshold: 200000
                      ReadAheadLoThreshold: 100000
                      MinDataPageSize: 7168
                      SnapBrokerQueue: 0
                        Generation {
                            GenerationId: 0
                            SizeToCompact: 10000
                            CountToCompact: 2
                            ForceCountToCompact: 2
                            ForceSizeToCompact: 20000
                            CompactionBrokerQueue: 1
                            KeepInCache: true
                        }
                    }
                    ColumnFamilies {
                        Id: 0
                        Storage: ColumnStorageTest_1_2_1k
                        ColumnCache: ColumnCacheNone
                    }
                }
            )___");

        partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/Table");
        datashards.insert(partitions.begin(), partitions.end());

        // Write new rows to the copy in order to trigger compaction
        WriteRandomRows(annoyingClient, "Table", 666, 100);
        TString strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/Table");

        // Delete original table
        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");

        TString strResultAfter = ReadFromTable(annoyingClient, "/dc-1/Dir/Table");
        UNIT_ASSERT_NO_DIFF(strResultAfter, strResult);

        // Delete the copy
        annoyingClient.DeleteTable("/dc-1/Dir", "Table");

        WaitForTabletsToBeDeletedInHive(annoyingClient, cleverServer.GetRuntime(), datashards);
    }

    Y_UNIT_TEST(CopyTableDropOriginalAndReturnPartAfterCompaction) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);

        PrepareSourceTable(annoyingClient);

        THashSet<ui64> datashards;
        TVector<ui64> partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/TableOld");
        datashards.insert(partitions.begin(), partitions.end());

        // Copy the table
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        Cerr << "Copy TableOld to Table" << Endl;
        annoyingClient.CreateTable("/dc-1/Dir", R"___(
                Name: "Table"
                CopyFromTable: "/dc-1/Dir/TableOld"

                PartitionConfig {
                    CompactionPolicy {
                      InMemSizeToSnapshot: 100000
                      InMemStepsToSnapshot: 2
                      InMemForceStepsToSnapshot: 3
                      InMemForceSizeToSnapshot: 1000000
                      InMemCompactionBrokerQueue: 0
                      ReadAheadHiThreshold: 200000
                      ReadAheadLoThreshold: 100000
                      MinDataPageSize: 7168
                      SnapBrokerQueue: 0
                        Generation {
                            GenerationId: 0
                            SizeToCompact: 10000
                            CountToCompact: 2
                            ForceCountToCompact: 2
                            ForceSizeToCompact: 20000
                            CompactionBrokerQueue: 1
                            KeepInCache: true
                            ExtraCompactionPercent: 0
                            ExtraCompactionMinSize: 0
                            ExtraCompactionExpPercent: 0
                            ExtraCompactionExpMaxSize: 0
                            UpliftPartSize: 0
                        }
                    }
                    ColumnFamilies {
                        Id: 0
                        Storage: ColumnStorageTest_1_2_1k
                        ColumnCache: ColumnCacheNone
                    }
                }
            )___");

        // Delete original table
        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");

        // Write new rows to the copy in order to trigger compaction
        WriteRandomRows(annoyingClient, "Table", 666, 100);

        // Check that first partition of the original table is deleted after part is returned
        WaitForTabletsToBeDeletedInHive(annoyingClient, cleverServer.GetRuntime(), THashSet<ui64>({partitions[0]}));

        partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/Table");
        datashards.insert(partitions.begin(), partitions.end());

        TString strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/Table");
        for (int i = 0; i < 5; ++i) {
            TString strResultAfter = ReadFromTable(annoyingClient, "/dc-1/Dir/Table");
            UNIT_ASSERT_NO_DIFF(strResultAfter, strResult);
        }

        // Delete the copy
        annoyingClient.DeleteTable("/dc-1/Dir", "Table");

        WaitForTabletsToBeDeletedInHive(annoyingClient, cleverServer.GetRuntime(), datashards);
    }

    Y_UNIT_TEST(CopyCopiedTableAndDropFirstCopy) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);

        PrepareSourceTable(annoyingClient);
        TString strResultOld = ReadFromTable(annoyingClient, "/dc-1/Dir/TableOld");
        Cerr << strResultOld << Endl;

        THashSet<ui64> datashards;
        auto oldShards = annoyingClient.GetTablePartitions("/dc-1/Dir/TableOld");
        datashards.insert(oldShards.begin(), oldShards.end());

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        // Make a copy
        annoyingClient.CreateTable("/dc-1/Dir", " Name: \"Table\" CopyFromTable: \"/dc-1/Dir/TableOld\"");
        auto shards = annoyingClient.GetTablePartitions("/dc-1/Dir/Table");
        datashards.insert(shards.begin(), shards.end());

        // Make a copy-of-copy
        annoyingClient.CreateTable("/dc-1/Dir", " Name: \"TableNew\" CopyFromTable: \"/dc-1/Dir/Table\"");
        auto newShards = annoyingClient.GetTablePartitions("/dc-1/Dir/TableNew");
        datashards.insert(newShards.begin(), newShards.end());

        // Drop first copy
        annoyingClient.DeleteTable("/dc-1/Dir", "Table");

        TString strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/TableNew");
        UNIT_ASSERT_NO_DIFF(strResult, strResultOld);

        // Drop original
        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");

        strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/TableNew");
        UNIT_ASSERT_NO_DIFF(strResult, strResultOld);

        // Check that shards of Table are still alive
        for (ui64 tabletId : shards) {
            UNIT_ASSERT_C(annoyingClient.TabletExistsInHive(cleverServer.GetRuntime(), tabletId),
                          "Partitions of dropped Table must not be deleted until TableNew returns all borrowed parts");
        }
        // Check that oldShards of TableOld are also alive
        for (ui64 tabletId : oldShards) {
            UNIT_ASSERT_C(annoyingClient.TabletExistsInHive(cleverServer.GetRuntime(), tabletId),
                          "Partitions of dropped TableOld must not be deleted until TableNew returns all borrowed parts");
        }

        // Drop copy-of-copy
        annoyingClient.DeleteTable("/dc-1/Dir", "TableNew");

        // Wait for all datashards to be deleted
        WaitForTabletsToBeDeletedInHive(annoyingClient, cleverServer.GetRuntime(), datashards);
    }

    void DoSplitMergeTable(TFlatMsgBusClient& annoyingClient, TString table, const TVector<ui64>& srcPartitions, const TVector<ui32>& splitPoints, bool twoKeys = false) {
        TVector<ui64> partitionsBefore;
        partitionsBefore = annoyingClient.GetTablePartitions(table);
        UNIT_ASSERT(partitionsBefore.size() > 0);

        TString strResultBefore = twoKeys ? ReadFromTwoKeysTable(annoyingClient, table) : ReadFromTable(annoyingClient, table);

        TStringStream splitDescr;
        for (ui32 src : srcPartitions) {
            splitDescr << " SourceTabletId: " << partitionsBefore[src] << Endl;
        }
        for (ui32 p : splitPoints) {
            splitDescr <<  " SplitBoundary { KeyPrefix {Tuple { Optional { Uint32: " << p << " } } } }" << Endl;
        }
        annoyingClient.SplitTablePartition(table, splitDescr.Str());

        TVector<ui64> partitionsAfter;
        partitionsAfter = annoyingClient.GetTablePartitions(table);
        UNIT_ASSERT_VALUES_EQUAL(partitionsAfter.size(),
                                 partitionsBefore.size() - srcPartitions.size() + splitPoints.size() + 1);
        // TODO: check paritions that were not supposed to change
        //UNIT_ASSERT_VALUES_EQUAL(partitionsAfter.back(), partitionsBefore.back());

        TString strResultAfter = twoKeys ? ReadFromTwoKeysTable(annoyingClient, table) : ReadFromTable(annoyingClient, table);
        UNIT_ASSERT_NO_DIFF(strResultBefore, strResultAfter);
    }

    void SplitTwoKeysTable(TFlatMsgBusClient& annoyingClient, TString table, ui64 partitionIdx, const TVector<ui32>& splitPoints) {
        DoSplitMergeTable(annoyingClient, table, {partitionIdx}, splitPoints, /* twoKeys */ true);
    }

    void SplitTable(TFlatMsgBusClient& annoyingClient, TString table, ui64 partitionIdx, const TVector<ui32>& splitPoints) {
        DoSplitMergeTable(annoyingClient, table, {partitionIdx}, splitPoints);
    }

    void MergeTable(TFlatMsgBusClient& annoyingClient, TString table, const TVector<ui64>& partitionIdxs) {
        DoSplitMergeTable(annoyingClient, table, partitionIdxs, {});
    }

    void DisableSplitMergePartCountLimit(TServer& cleverServer) {
        SetSplitMergePartCountLimit(cleverServer.GetRuntime(), -1);
    }

    Y_UNIT_TEST(SplitInvalidPath) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        DisableSplitMergePartCountLimit(cleverServer);

        TFlatMsgBusClient annoyingClient(port);

        // cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "Dir1");

        NKikimrClient::TResponse response;
        annoyingClient.TrySplitTablePartition("/dc-1/Dir1", "SourceTabletId: 100500 SplitBoundary { KeyPrefix {Tuple { Optional { Uint32: 42 } } } }", response);
        // Cerr << response;
        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), NMsgBusProxy::MSTATUS_ERROR);
        UNIT_ASSERT_VALUES_EQUAL(response.GetSchemeStatus(), NKikimrScheme::StatusNameConflict);
    }

    Y_UNIT_TEST(SplitEmptyAndWrite) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);
        PrepareSourceTable(annoyingClient);
//        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
//        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        SplitTable(annoyingClient, "/dc-1/Dir/TableOld", 0, {100, 200});

        // Write new rows to the copy in order to trigger compaction
        TMap<ui32, TString> rows = {
            {1, "AAA"},
            {101, "BBB"},
            {201, "CCC"},
            {1001, "DDD"}
        };
        for (const auto& r : rows) {
            WriteRow(annoyingClient, "TableOld", r.first, r.second);
        }

        for (const auto& r : rows) {
            TString val = ReadRow(annoyingClient, "TableOld", r.first);
            UNIT_ASSERT_VALUES_EQUAL(val, r.second);
        }

        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");
        annoyingClient.Ls("/dc-1/Dir/TableOld");
    }

    Y_UNIT_TEST(SplitEmptyToMany) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);
        PrepareSourceTable(annoyingClient);

        const ui32 shardsBefore = annoyingClient.GetTablePartitions("/dc-1/Dir/TableOld").size();
        const ui32 SHARD_COUNT = 10000;
        TVector<ui32> points;
        points.reserve(SHARD_COUNT);
        for (ui32 p = 1; p <= SHARD_COUNT - shardsBefore; ++p) {
            points.push_back(p);
        }
        SplitTable(annoyingClient, "/dc-1/Dir/TableOld", 0, points);

        UNIT_ASSERT_VALUES_EQUAL(SHARD_COUNT, annoyingClient.GetTablePartitions("/dc-1/Dir/TableOld").size());

        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");
        annoyingClient.Ls("/dc-1/Dir/TableOld");
    }

    Y_UNIT_TEST(SplitEmptyTwice) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);
        PrepareSourceTable(annoyingClient);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
//        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        SplitTable(annoyingClient, "/dc-1/Dir/TableOld", 0, {100, 200});
        SplitTable(annoyingClient, "/dc-1/Dir/TableOld", 2, {300, 400});

        // Write new rows to the copy in order to trigger compaction
        TMap<ui32, TString> rows = {
            {1, "AAA"},
            {101, "BBB"},
            {201, "CCC"},
            {1001, "DDD"}
        };
        for (const auto& r : rows) {
            WriteRow(annoyingClient, "TableOld", r.first, r.second);
        }

        for (const auto& r : rows) {
            TString val = ReadRow(annoyingClient, "TableOld", r.first);
            UNIT_ASSERT_VALUES_EQUAL(val, r.second);
        }

        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");
        annoyingClient.Ls("/dc-1/Dir/TableOld");
    }

    Y_UNIT_TEST(MergeEmptyAndWrite) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);
        PrepareSourceTable(annoyingClient);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        MergeTable(annoyingClient, "/dc-1/Dir/TableOld", {0, 1});

        // Write new rows to the copy in order to trigger compaction
        TMap<ui32, TString> rows = {
            {1, "AAA"},
            {101, "BBB"},
            {201, "CCC"},
            {1001, "DDD"}
        };
        for (const auto& r : rows) {
            WriteRow(annoyingClient, "TableOld", r.first, r.second);
        }

        for (const auto& r : rows) {
            TString val = ReadRow(annoyingClient, "TableOld", r.first);
            UNIT_ASSERT_VALUES_EQUAL(val, r.second);
        }

        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");
        annoyingClient.Ls("/dc-1/Dir/TableOld");
    }

    Y_UNIT_TEST(WriteMergeAndRead) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);
        PrepareSourceTable(annoyingClient);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        // Write new rows to the copy in order to trigger compaction
        TMap<ui32, TString> rows = {
            {1, "AAA"},
            {101, "BBB"},
            {3000000201, "CCC"},
            {3000001001, "DDD"}
        };
        for (const auto& r : rows) {
            WriteRow(annoyingClient, "TableOld", r.first, r.second);
        }

        MergeTable(annoyingClient, "/dc-1/Dir/TableOld", {0, 1});

        for (const auto& r : rows) {
            TString val = ReadRow(annoyingClient, "TableOld", r.first);
            UNIT_ASSERT_VALUES_EQUAL(val, r.second);
        }

        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");
        annoyingClient.Ls("/dc-1/Dir/TableOld");
    }

    Y_UNIT_TEST(WriteSplitByPartialKeyAndRead) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);

        const char * table = R"___(
                Name: "TableOld"
                Columns { Name: "Key"    Type: "Uint32"}
                Columns { Name: "Key2"   Type: "Uint32"}
                Columns { Name: "Value"  Type: "Utf8"}
                Columns { Name: "Large"  Type: "Utf8"}
                KeyColumnNames: ["Key", "Key2"]
            )___";

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "Dir");
        annoyingClient.CreateTable("/dc-1/Dir", table);

        auto fnWriteRow = [&annoyingClient](TMaybe<ui32> key1, TMaybe<ui32> key2, TString value) {
            TString insertRowQuery = "("
                "(let key '('('Key (%s)) '('Key2 (%s))))"
                "(let value '('Value (Utf8 '%s)))"
                "(let ret_ (AsList"
                "    (UpdateRow '/dc-1/Dir/TableOld key '(value))"
                "))"
                "(return ret_)"
                ")";

            annoyingClient.FlatQuery(Sprintf(insertRowQuery.data(),
                 key1 ? Sprintf("Uint32 '%u", *key1).data() : "Nothing (OptionalType (DataType 'Uint32))",
                 key2 ? Sprintf("Uint32 '%u", *key2).data() : "Nothing (OptionalType (DataType 'Uint32))",
                 value.data()));
        };

        const ui32 splitKey = 100;

        fnWriteRow(0, 10000,        "AAA");

        fnWriteRow(splitKey, {},    "BBB");
        fnWriteRow(splitKey, 0,     "CCC");
        fnWriteRow(splitKey, -1,    "DDD");

        fnWriteRow(splitKey+1, {},  "EEE");
        fnWriteRow(splitKey+1, 0,   "FFF");
        fnWriteRow(splitKey+1, -1,  "GGG");

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        SplitTwoKeysTable(annoyingClient, "/dc-1/Dir/TableOld", 0, {splitKey});
    }

    Y_UNIT_TEST(SplitThenMerge) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);
        PrepareSourceTable(annoyingClient);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        SplitTable(annoyingClient, "/dc-1/Dir/TableOld", 0, {100, 200});
        MergeTable(annoyingClient, "/dc-1/Dir/TableOld", {1, 2});
        MergeTable(annoyingClient, "/dc-1/Dir/TableOld", {0, 1});

        // Write new rows to the copy in order to trigger compaction
        TMap<ui32, TString> rows = {
            {1, "AAA"},
            {101, "BBB"},
            {201, "CCC"},
            {1001, "DDD"}
        };
        for (const auto& r : rows) {
            WriteRow(annoyingClient, "TableOld", r.first, r.second);
        }

        for (const auto& r : rows) {
            TString val = ReadRow(annoyingClient, "TableOld", r.first);
            UNIT_ASSERT_VALUES_EQUAL(val, r.second);
        }

        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");
        annoyingClient.Ls("/dc-1/Dir/TableOld");
    }

    Y_UNIT_TEST(WriteSplitAndRead) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);
        PrepareSourceTable(annoyingClient);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        // Write new rows to the copy in order to trigger compaction
        TMap<ui32, TString> rows = {
            {1, "AAA"},
            {101, "BBB"},
            {201, "CCC"}
        };
        for (const auto& r : rows) {
            WriteRow(annoyingClient, "TableOld", r.first, r.second);
        }

        TString strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/TableOld");

        SplitTable(annoyingClient, "/dc-1/Dir/TableOld", 0, {100, 200});

        for (const auto& r : rows) {
            TString val = ReadRow(annoyingClient, "TableOld", r.first);
            UNIT_ASSERT_VALUES_EQUAL(val, r.second);
        }

        TString strResultAfter = ReadFromTable(annoyingClient, "/dc-1/Dir/TableOld");
        UNIT_ASSERT_NO_DIFF(strResultAfter, strResult);

        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");
        annoyingClient.Ls("/dc-1/Dir/TableOld");
    }

    Y_UNIT_TEST(WriteSplitAndReadFromFollower) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port).SetNodeCount(2));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);
        PrepareSourceTable(annoyingClient, true);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        // Write new rows to the copy in order to trigger compaction
        TMap<ui32, TString> rows ={
            {1, "AAA"},
            {201, "BBB"},
            {301, "CCC"}
        };

        UNIT_ASSERT(annoyingClient.WaitForTabletAlive(cleverServer.GetRuntime(), 72075186224037888, false, TDuration::Minutes(1)));

        for (const auto& r : rows) {
            WriteRow(annoyingClient, "TableOld", r.first, r.second);
        }

        TString strResult = ReadFromTable(annoyingClient, "/dc-1/Dir/TableOld", 201, false);

        SplitTable(annoyingClient, "/dc-1/Dir/TableOld", 0, {100, 200});

        for (const auto& r : rows) {
            TString val = ReadRow(annoyingClient, "TableOld", r.first);
            UNIT_ASSERT_VALUES_EQUAL(val, r.second);
        }

        for (auto tabletId : xrange(72075186224037888, 72075186224037893)) {
            cleverServer.GetRuntime()->Send(
                new IEventHandle(MakeTabletResolverID(), TActorId(),
                    new TEvTabletResolver::TEvTabletProblem(tabletId, TActorId())
                ));
        }

        for (auto tabletId : xrange(72075186224037890, 72075186224037893)) {
            UNIT_ASSERT(annoyingClient.WaitForTabletAlive(cleverServer.GetRuntime(), tabletId, false, TDuration::Minutes(1)));
        }

        for (ui32 i = 0; i < 20; ++i) { // multiple rounds to move some reads to followers
            TString strResultAfter = ReadFromTable(annoyingClient, "/dc-1/Dir/TableOld", 201, true);
            UNIT_ASSERT_NO_DIFF(strResultAfter, strResult);
        }

        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");
        annoyingClient.Ls("/dc-1/Dir/TableOld");
    }

    Y_UNIT_TEST(WriteSplitKillRead) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);
        PrepareSourceTable(annoyingClient);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);

        // Write new rows to the copy in order to trigger compaction
        TMap<ui32, TString> rows1 = {
            {1, "AAA"},
            {101, "BBB"},
            {201, "CCC"}
        };
        for (const auto& r : rows1) {
            WriteRow(annoyingClient, "TableOld", r.first, r.second);
        }

        SplitTable(annoyingClient, "/dc-1/Dir/TableOld", 0, {100, 200});

        TMap<ui32, TString> rows2 = {
            {2, "2222AAA"},
            {102, "2222BBB"},
            {202, "2222CCC"}
        };
        for (const auto& r : rows2) {
            WriteRow(annoyingClient, "TableOld", r.first, r.second);
        }

        for (auto tableId: annoyingClient.GetTablePartitions("/dc-1/Dir/TableOld"))
            annoyingClient.KillTablet(cleverServer, tableId);

        TMap<ui32, TString> rows = rows1;
        rows.insert(rows2.begin(), rows2.end());
        for (const auto& r : rows) {
            TString val = ReadRow(annoyingClient, "TableOld", r.first);
            UNIT_ASSERT_VALUES_EQUAL(val, r.second);
        }

        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");
        annoyingClient.Ls("/dc-1/Dir/TableOld");
    }

    Y_UNIT_TEST(SplitBoundaryRead) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);
        PrepareSourceTable(annoyingClient);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
//        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        // Write 1 row and split on its key
        WriteRow(annoyingClient, "TableOld", 11111, "Boundary");
        SplitTable(annoyingClient, "/dc-1/Dir/TableOld", 0, {11111});
    }

    Y_UNIT_TEST(WriteSplitWriteSplit) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);
        PrepareSourceTable(annoyingClient);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
//        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        // Write new rows to the copy in order to trigger compaction
        TMap<ui32, TString> rows1 = {
            {1, "AAA"},
            {101, "BBB"},
            {201, "CCC"}
        };
        for (const auto& r : rows1) {
            WriteRow(annoyingClient, "TableOld", r.first, r.second);
        }

        SplitTable(annoyingClient, "/dc-1/Dir/TableOld", 0, {100, 200});

        TMap<ui32, TString> rows2 = {
            {2, "2222AAA"},
            {102, "2222BBB"},
            {202, "2222CCC"}
        };
        for (const auto& r : rows2) {
            WriteRow(annoyingClient, "TableOld", r.first, r.second);
        }

        SplitTable(annoyingClient, "/dc-1/Dir/TableOld", 1, {101});

        TMap<ui32, TString> rows = rows1;
        rows.insert(rows2.begin(), rows2.end());
        for (const auto& r : rows) {
            TString val = ReadRow(annoyingClient, "TableOld", r.first);
            UNIT_ASSERT_VALUES_EQUAL(val, r.second);
        }

        annoyingClient.DeleteTable("/dc-1/Dir", "TableOld");
        annoyingClient.Ls("/dc-1/Dir/TableOld");
    }

    Y_UNIT_TEST(AutoSplitBySize) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_INFO);

        TFlatMsgBusClient annoyingClient(port);

        const char * tableDescr = R"___(
                Name: "T1"
                Columns { Name: "Key"    Type: "String"}
                Columns { Name: "Value"  Type: "Utf8"}
                KeyColumnNames: ["Key"]

                PartitionConfig {
                    PartitioningPolicy {
                        SizeToSplit: 45000000
                    }
                    CompactionPolicy {
                      InMemSizeToSnapshot: 100000
                      InMemStepsToSnapshot: 1
                      InMemForceStepsToSnapshot: 2
                      InMemForceSizeToSnapshot: 200000
                      InMemCompactionBrokerQueue: 0
                      ReadAheadHiThreshold: 200000
                      ReadAheadLoThreshold: 100000
                      MinDataPageSize: 7168
                      SnapBrokerQueue: 0
                        Generation {
                            GenerationId: 0
                            SizeToCompact: 10000
                            CountToCompact: 2
                            ForceCountToCompact: 2
                            ForceSizeToCompact: 20000
                            CompactionBrokerQueue: 1
                            KeepInCache: true
                        }
                    }
                }
            )___";

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "Dir");
        annoyingClient.CreateTable("/dc-1/Dir", tableDescr);

        TVector<ui64> partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/T1");
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);

        // Force stats reporting without delays
        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
//        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
//        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        // Write rows to trigger split
        auto fnWriteRow = [&](TString key, TString value) {
            Cerr << key << Endl;
            TString insertRowQuery = R"___(
                    (
                    (let key '('('Key (String '%s))))
                    (let value '('('Value (Utf8 '%s))))
                    (let ret_ (AsList
                        (UpdateRow '/dc-1/Dir/%s key value)
                    ))
                    (return ret_)
                    )
                    )___";

            int retryCnt = 20;
            while (retryCnt--) {
                TFlatMsgBusClient::TFlatQueryOptions opts;
                NKikimrClient::TResponse response;
                annoyingClient.FlatQueryRaw(Sprintf(insertRowQuery.data(), key.data(), value.data(), "T1"), opts, response);
                ui32 responseStatus = response.GetStatus();
                if (responseStatus == NMsgBusProxy::MSTATUS_REJECTED) {
                    Sleep(TDuration::Seconds(1));
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(responseStatus, NMsgBusProxy::MSTATUS_OK);
                    break;
                }
            }
        };

        TString bigValue(6*1024*1024, 'a');

        for (int i = 0; i < 4; ++i) {
            fnWriteRow(Sprintf("A-%d", i), bigValue);
            fnWriteRow(Sprintf("B-%d", i), bigValue);
        }

        // Check that split actually happened
        for (int retry = 0; retry < 30 && partitions.size() == 1; ++retry) {
            partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/T1");
            Sleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 2);

        // Write some more rows to trigger another split
        for (int i = 0; i < 4; ++i) {
            fnWriteRow(Sprintf("C-%d", i), bigValue);
        }

        // Check that split actually happened
        for (int retry = 0; retry < 30 && partitions.size() == 2; ++retry) {
            partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/T1");
            Sleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 3);
    }

    void WriteKVRow(TFlatMsgBusClient& annoyingClient, ui32 key, TString value) {
            Cerr << "WriteKVRow: " << key << Endl;
            TString insertRowQuery = R"___(
                    (
                    (let key '('('Key (Uint32 '%u))))
                    (let value '('('Value (Utf8 '%s))))
                    (let ret_ (AsList
                        (UpdateRow '/dc-1/Dir/%s key value)
                    ))
                    (return ret_)
                    )
                    )___";

            int retryCnt = 20;
            TDuration delay = TDuration::MilliSeconds(5);
            while (retryCnt--) {
                TFlatMsgBusClient::TFlatQueryOptions opts;
                NKikimrClient::TResponse response;
                annoyingClient.FlatQueryRaw(Sprintf(insertRowQuery.data(), key, value.data(), "T1"), opts, response);
                ui32 responseStatus = response.GetStatus();
                if (responseStatus == NMsgBusProxy::MSTATUS_REJECTED) {
                    Sleep(delay);
                    delay = Min(delay * 2, TDuration::Seconds(1));
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(responseStatus, NMsgBusProxy::MSTATUS_OK);
                    break;
                }
            }
    }

    void EraseKVRow(TFlatMsgBusClient& annoyingClient, ui32 key) {
            Cerr << "EraseKVRow: " << key << Endl;
            TString query = R"___(
                    (
                    (let key '('('Key (Uint32 '%u))))
                    (let ret_ (AsList
                        (EraseRow '/dc-1/Dir/%s key)
                    ))
                    (return ret_)
                    )
                    )___";

            int retryCnt = 20;
            TDuration delay = TDuration::MilliSeconds(5);
            while (retryCnt--) {
                TFlatMsgBusClient::TFlatQueryOptions opts;
                NKikimrClient::TResponse response;
                annoyingClient.FlatQueryRaw(Sprintf(query.data(), key, "T1"), opts, response);
                ui32 responseStatus = response.GetStatus();
                if (responseStatus == NMsgBusProxy::MSTATUS_REJECTED) {
                    Sleep(delay);
                    delay = Min(delay * 2, TDuration::Seconds(1));
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(responseStatus, NMsgBusProxy::MSTATUS_OK);
                    break;
                }
            }
    }

    Y_UNIT_TEST(AutoMergeBySize) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_ERROR);

        TFlatMsgBusClient annoyingClient(port);

        const char * tableDescr = R"___(
                Name: "T1"
                Columns { Name: "Key"    Type: "Uint32"}
                Columns { Name: "Value"  Type: "Utf8"}
                KeyColumnNames: ["Key"]
                UniformPartitionsCount: 4
                PartitionConfig {
                    PartitioningPolicy {
                        SizeToSplit: 50000000
                        MaxPartitionsCount: 6
                    }
                    CompactionPolicy {
                      InMemSizeToSnapshot: 100000
                      InMemStepsToSnapshot: 1
                      InMemForceStepsToSnapshot: 2
                      InMemForceSizeToSnapshot: 200000
                      InMemCompactionBrokerQueue: 0
                      ReadAheadHiThreshold: 200000
                      ReadAheadLoThreshold: 100000
                      MinDataPageSize: 7168
                      SnapBrokerQueue: 0
                        Generation {
                            GenerationId: 0
                            SizeToCompact: 10000
                            CountToCompact: 2
                            ForceCountToCompact: 2
                            ForceSizeToCompact: 20000
                            CompactionBrokerQueue: 1
                            KeepInCache: true
                        }
                    }
                }
            )___";

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "Dir");
        annoyingClient.CreateTable("/dc-1/Dir", tableDescr);

        TVector<ui64> partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/T1");
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 4);

        // Force stats reporting without delays
        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
//        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TABLET_EXECUTOR, NActors::NLog::PRI_DEBUG);
//        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        // Write rows to trigger split
        auto fnWriteRow = [&annoyingClient] (ui32 key, TString value)  {
            return WriteKVRow(annoyingClient, key, value);
        };

        TString smallValue(10*1024, '0');
        TString bigValue(7*1024*1024, '0');

        // Allow the table to shrink to 2 partitions
        annoyingClient.AlterTable("/dc-1/Dir", R"(
            Name: "T1"
            PartitionConfig {
                PartitioningPolicy {
                    MinPartitionsCount: 2
                }
            }
        )");

        // Write some values to trigger stats update and merge
        for (int i = 0; i < 5; ++i) {
            fnWriteRow(0x42, smallValue);
            fnWriteRow(0x40000042, smallValue);
            fnWriteRow(0x80000042, smallValue);
            fnWriteRow(0xc0000042, smallValue);
        }

        // Check that merge actually happened
        for (int retry = 0; retry < 15 && partitions.size() > 2; ++retry) {
            partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/T1");
            Sleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 2);

        // Write some more rows to trigger split
        for (int i = 0; i < 50; ++i) {
            fnWriteRow(0x20000000 + i, bigValue);
        }

        auto lsRes = annoyingClient.Ls("/dc-1/Dir/T1");
        Cout << lsRes->Record << Endl;

        // Check that split actually happened
        for (int retry = 0; retry < 15 && partitions.size() < 6; ++retry) {
            partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/T1");
            Sleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 6);
    }

    Y_UNIT_TEST(AutoSplitMergeQueue) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port).SetKeepSnapshotTimeout(TDuration::Seconds(1)));
        DisableSplitMergePartCountLimit(cleverServer);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_ERROR);
        //cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);

        TFlatMsgBusClient annoyingClient(port);

        const char * tableDescr = R"___(
                Name: "T1"
                Columns { Name: "Key"    Type: "Uint32"}
                Columns { Name: "Value"  Type: "Utf8"}
                KeyColumnNames: ["Key"]
                PartitionConfig {
                    PartitioningPolicy {
                        SizeToSplit: 300000
                        MaxPartitionsCount: 6
                        MinPartitionsCount: 1
                    }
                    CompactionPolicy {
                      InMemSizeToSnapshot: 10000
                      InMemStepsToSnapshot: 1
                      InMemForceStepsToSnapshot: 1
                      InMemForceSizeToSnapshot: 20000
                      InMemCompactionBrokerQueue: 0
                      ReadAheadHiThreshold: 200000
                      ReadAheadLoThreshold: 100000
                      MinDataPageSize: 7168
                      SnapBrokerQueue: 0
                      Generation {
                        GenerationId: 0
                        SizeToCompact: 10000
                        CountToCompact: 2
                        ForceCountToCompact: 2
                        ForceSizeToCompact: 20000
                        KeepInCache: true
                      }
                    }
                }
            )___";

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "Dir");
        annoyingClient.CreateTable("/dc-1/Dir", tableDescr);

        TVector<ui64> initialPartitions = annoyingClient.GetTablePartitions("/dc-1/Dir/T1");

        // Force stats reporting without delays
        NDataShard::gDbStatsReportInterval = TDuration::Seconds(0);
        NDataShard::gDbStatsDataSizeResolution = 80000;

        TString bigValue(100*1024, '0');
        int key = 0;

        // Write some values to the tail and delete from the head
        for (; key < 300; ++key) {
            WriteKVRow(annoyingClient, key, bigValue);
            EraseKVRow(annoyingClient, key-30);
            if (key % 50 == 0) {
                TVector<ui64> partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/T1");
                UNIT_ASSERT_C(partitions.size() <= 6, "Table grew beyond MaxPartitionsCount");
            }
        }
        TVector<ui64> partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/T1");
        UNIT_ASSERT_VALUES_UNEQUAL_C(partitions.size(), 1, "Split did't happen");

        // Delete rest of the rows
        for (key -= 30; key < 300; ++key) {
            EraseKVRow(annoyingClient, key);
        }

        // Wait for merge to happen
        for (int retry = 0; retry < 45 && annoyingClient.GetTablePartitions("/dc-1/Dir/T1").size() != 1; ++retry) {
            Sleep(TDuration::Seconds(1));
        }

        TVector<ui64> finalPartitions = annoyingClient.GetTablePartitions("/dc-1/Dir/T1");
        UNIT_ASSERT_VALUES_EQUAL_C(finalPartitions.size(), 1, "Empty table didn't merge into 1 shard");
        UNIT_ASSERT_VALUES_UNEQUAL_C(finalPartitions[0], initialPartitions[0], "Partitions din't change");
    }

    Y_UNIT_TEST(GetTabletCounters) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        TFlatMsgBusClient annoyingClient(port);

        PrepareSourceTable(annoyingClient);
        TVector<ui64> partitions = annoyingClient.GetTablePartitions("/dc-1/Dir/TableOld");

        TAutoPtr<NKikimr::NMsgBusProxy::TBusTabletCountersRequest> request(new NKikimr::NMsgBusProxy::TBusTabletCountersRequest());
        request->Record.SetTabletID(partitions[0]);
        request->Record.SetConnectToFollower(false);

        TAutoPtr<NBus::TBusMessage> reply;
        NBus::EMessageStatus status = annoyingClient.SyncCall(request, reply);

        UNIT_ASSERT_VALUES_EQUAL(status, NBus::MESSAGE_OK);
        const NKikimr::NMsgBusProxy::TBusResponse* res = dynamic_cast<NKikimr::NMsgBusProxy::TBusResponse*>(reply.Get());
        UNIT_ASSERT(res);
//        Cerr << res->Record << Endl;
        UNIT_ASSERT_VALUES_EQUAL(res->Record.GetStatus(), NKikimr::NMsgBusProxy::MSTATUS_OK);
        UNIT_ASSERT_VALUES_EQUAL(res->Record.GetTabletId(), partitions[0]);
        UNIT_ASSERT(res->Record.HasTabletCounters());
        bool found = false;
        for (const auto& sc : res->Record.GetTabletCounters().GetExecutorCounters().GetSimpleCounters()) {
            if (sc.GetName() == "DbDataBytes") {
                found = true;
                break;
            }
        }
        UNIT_ASSERT_C(found, "DbDataBytes counter not found");
    }

    void LargeDatashardReplyRO(TFlatMsgBusClient& client) {
        const ui32 TABLE_ROWS = 700;
        const ui32 BLOB_SIZE = 100 * 1024; // 100 Kb

        for (ui32 i = 0; i < TABLE_ROWS; ++i) {
            client.FlatQuery(Sprintf(R"(
                (
                (let key '('('Key (Uint64 '%d)) ))
                (let payload '('('Value (String '%s))))
                (return (AsList (UpdateRow '"/dc-1/test/BlobTable" key payload)))
                )
                )", i, TString(BLOB_SIZE, '0' + i % 10).c_str()
            ));
        }

        client.FlatQuery(Sprintf(R"(
            (
            (let range '('ExcFrom 'IncTo '('Key (Null) (Void))))
            (let data (Member (SelectRange '"/dc-1/test/BlobTable" range '('Key 'Value) '()) 'List))
            (let result (Filter data (lambda '(row)
                (Coalesce (NotEqual (Member row 'Key) (Uint64 '1)) (Bool 'false))
            )))
            (return (AsList (SetResult 'Result result)))
            )
            )"
        ), NMsgBusProxy::MSTATUS_ERROR, TEvTxUserProxy::TResultStatus::ExecResultUnavailable);
    }

    Y_UNIT_TEST(LargeDatashardReply) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");

        annoyingClient.CreateTable("/dc-1/test",
            R"(Name: "BlobTable"
                Columns { Name: "Key"   Type: "Uint64"}
                Columns { Name: "Value" Type: "String"}
                KeyColumnNames: ["Key"]
            )");

        LargeDatashardReplyRO(annoyingClient);
    }

    Y_UNIT_TEST(LargeDatashardReplyDistributed) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");

        annoyingClient.CreateTable("/dc-1/test",
            R"(Name: "BlobTable"
                Columns { Name: "Key"   Type: "Uint64"}
                Columns { Name: "Value" Type: "String"}
                KeyColumnNames: ["Key"]
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
            )");

        LargeDatashardReplyRO(annoyingClient);
    }

    Y_UNIT_TEST(LargeDatashardReplyRW) {
        const ui32 TABLE_ROWS = 700;
        const ui32 BLOB_SIZE = 100 * 1024; // 100 Kb
        const ui32 KEY_TO_ERASE = 1;

        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");

        annoyingClient.CreateTable("/dc-1/test",
            R"(Name: "BlobTable"
                Columns { Name: "Key"   Type: "Uint64"}
                Columns { Name: "Value" Type: "String"}
                KeyColumnNames: ["Key"]
            )");

        for (ui32 i = 0; i < TABLE_ROWS; ++i) {
            annoyingClient.FlatQuery(Sprintf(R"(
                (
                (let key '('('Key (Uint64 '%d)) ))
                (let payload '('('Value (String '%s))))
                (return (AsList (UpdateRow '"/dc-1/test/BlobTable" key payload)))
                )
                )", i, TString(BLOB_SIZE, '0' + i % 10).c_str()
            ));
        }

        auto selectQuery = Sprintf(R"(
            (
            (let read (SelectRow '"/dc-1/test/BlobTable" '('('Key (Uint64 '%d))) '('Key)))
            (return (AsList (SetResult 'Result read)))
            )
        )", KEY_TO_ERASE);

        // Make sure row exists
        auto res = annoyingClient.FlatQuery(selectQuery);
        UNIT_ASSERT(res.GetValue().GetStruct(0).GetOptional().HasOptional());

        annoyingClient.FlatQuery(Sprintf(R"(
            (
            (let range '('ExcFrom 'IncTo '('Key (Null) (Void))))
            (let data (Member (SelectRange '"/dc-1/test/BlobTable" range '('Key 'Value) '()) 'List))
            (let result (Take (Skip (Filter data (lambda '(row)
                (Coalesce (NotEqual (Member row 'Key) (Uint64 '1)) (Bool 'false))
            )) (Uint64 '"15")) (Uint64 '"10")) )
            (return (AsList (SetResult 'Result result) (EraseRow '"/dc-1/test/BlobTable" '('('Key (Uint64 '%d))))))
            )
            )", KEY_TO_ERASE
        ), NMsgBusProxy::MSTATUS_ERROR, TEvTxUserProxy::TResultStatus::ExecResultUnavailable);

        // Make sure row erased
        res = annoyingClient.FlatQuery(selectQuery);
        UNIT_ASSERT(!res.GetValue().GetStruct(0).GetOptional().HasOptional());
    }

    Y_UNIT_TEST(LargeProxyReply) {
        const ui32 TABLE_ROWS = 350;
        const ui32 BLOB_SIZE = 100 * 1024; // 100 Kb

        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");

        annoyingClient.CreateTable("/dc-1/test",
            R"(Name: "BlobTable"
                Columns { Name: "Key"   Type: "Uint64"}
                Columns { Name: "Value" Type: "String"}
                KeyColumnNames: ["Key"]
            )");

        for (ui32 i = 0; i < TABLE_ROWS; ++i) {
            annoyingClient.FlatQuery(Sprintf(R"(
                (
                (let key '('('Key (Uint64 '%d)) ))
                (let payload '('('Value (String '%s))))
                (return (AsList (UpdateRow '"/dc-1/test/BlobTable" key payload)))
                )
                )", i, TString(BLOB_SIZE, '0' + i % 10).c_str()
            ));
        }

        annoyingClient.FlatQuery(Sprintf(R"(
            (
            (let range '('ExcFrom 'IncTo '('Key (Null) (Void))))
            (let data (Member (SelectRange '"/dc-1/test/BlobTable" range '('Key 'Value) '()) 'List))
            (let result (OrderedMap data (lambda '(row)
                (AddMember row 'Value2 (Member row 'Value))
            )))
            (return (AsList (SetResult 'Result result)))
            )
            )"
        ), NMsgBusProxy::MSTATUS_ERROR, TEvTxUserProxy::TResultStatus::ExecResultUnavailable);
    }

    Y_UNIT_TEST(LargeProxyReplyRW) {
        const ui32 TABLE_ROWS = 350;
        const ui32 BLOB_SIZE = 100 * 1024; // 100 Kb
        const ui32 KEY_TO_ERASE = 1;

        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");

        annoyingClient.CreateTable("/dc-1/test",
            R"(Name: "BlobTable"
                Columns { Name: "Key"   Type: "Uint64"}
                Columns { Name: "Value" Type: "String"}
                KeyColumnNames: ["Key"]
            )");

        for (ui32 i = 0; i < TABLE_ROWS; ++i) {
            annoyingClient.FlatQuery(Sprintf(R"(
                (
                (let key '('('Key (Uint64 '%d)) ))
                (let payload '('('Value (String '%s))))
                (return (AsList (UpdateRow '"/dc-1/test/BlobTable" key payload)))
                )
                )", i, TString(BLOB_SIZE, '0' + i % 10).c_str()
            ));
        }

        auto selectQuery = Sprintf(R"(
            (
            (let read (SelectRow '"/dc-1/test/BlobTable" '('('Key (Uint64 '%d))) '('Key)))
            (return (AsList (SetResult 'Result read)))
            )
        )", KEY_TO_ERASE);

        // Make sure row exists
        auto res = annoyingClient.FlatQuery(selectQuery);
        UNIT_ASSERT(res.GetValue().GetStruct(0).GetOptional().HasOptional());

        annoyingClient.FlatQuery(Sprintf(R"(
            (
            (let range '('ExcFrom 'IncTo '('Key (Null) (Void))))
            (let data (Member (SelectRange '"/dc-1/test/BlobTable" range '('Key 'Value) '()) 'List))
            (let result (OrderedMap data (lambda '(row)
                (AddMember row 'Value2 (Member row 'Value))
            )))
            (return (AsList (SetResult 'Result result) (EraseRow '"/dc-1/test/BlobTable" '('('Key (Uint64 '%d))))))
            )
            )", KEY_TO_ERASE
        ), NMsgBusProxy::MSTATUS_ERROR, TEvTxUserProxy::TResultStatus::ExecResultUnavailable);

        // Make sure row erased
        res = annoyingClient.FlatQuery(selectQuery);
        UNIT_ASSERT(!res.GetValue().GetStruct(0).GetOptional().HasOptional());
    }

    Y_UNIT_TEST(PartBloomFilter) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        if (!true) {
            cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::OPS_COMPACT, NActors::NLog::PRI_DEBUG);
        }

        TFlatMsgBusClient annoyingClient(port);

        const char * table = R"(Name: "TableWithFilter"
            Columns { Name: "key1"   Type: "Uint32" }
            Columns { Name: "value" Type: "Uint32" }
            KeyColumnNames: ["key1"]
            PartitionConfig {
                CompactionPolicy {
                      InMemSizeToSnapshot: 100000
                      InMemStepsToSnapshot: 1
                      InMemForceStepsToSnapshot: 2
                      InMemForceSizeToSnapshot: 200000
                      InMemCompactionBrokerQueue: 0
                      ReadAheadHiThreshold: 200000
                      ReadAheadLoThreshold: 100000
                      MinDataPageSize: 7168
                      Generation {
                            GenerationId: 0
                            SizeToCompact: 10000
                            CountToCompact: 200
                            ForceCountToCompact: 200
                            ForceSizeToCompact: 20000
                            CompactionBrokerQueue: 1
                            KeepInCache: false
                      }
                }
                EnableFilterByKey: true
            }
            )";

        annoyingClient.InitRoot();
        auto res = annoyingClient.CreateTable("/dc-1", table);
        UNIT_ASSERT_VALUES_EQUAL(res, NMsgBusProxy::MSTATUS_OK);

        const ui32 ROW_COUNT = 30;

        // Write many rows to produce some flat parts
        Cout << "INSERT key = i" << Endl;
        for (ui32 i = 0; i < ROW_COUNT; ++i) {
            annoyingClient.FlatQuery(Sprintf(R"((
                (let row_ '('('key1 (Uint32 '%u))))
                (let cols_ '('('value (Uint32 '%u))))
                (let insert (UpdateRow '/dc-1/TableWithFilter row_ cols_))
                (let ret_ (AsList insert))
                (return ret_)
            ))", i, i));
        }

        Cout << "SELECT value FROM TableWithFilter WHERE key = i" << Endl;
        for (ui32 i = 0; i < ROW_COUNT; ++i) {
            auto res = annoyingClient.FlatQuery(Sprintf(R"((
                (let row_ '('('key1 (Uint32 '%u))))
                (let cols_ '('value))
                (let select_ (SelectRow '/dc-1/TableWithFilter row_ cols_))
                (let ret_ (AsList (SetResult 'ret0 select_)))
                (return ret_)
            ))", i));

            // Cout << res << Endl;
            TValue value = TValue::Create(res.GetValue(), res.GetType());
            TValue ret0 = value["ret0"];
            ui32 val = ret0[0];
            UNIT_ASSERT_VALUES_EQUAL(val, i);
        }

        // Extend the key
        Cout << "ALTER TABLE Table DROP PRIMARY KEY, ADD COLUMN key2 ADD PRIMARY KEY(key1, key2)" << Endl;
        res = annoyingClient.AlterTable("/dc-1", R"(
            Name: "TableWithFilter"
            Columns { Name: "key2" Type: "Uint64" }
            KeyColumnNames: ["key1", "key2"]
        )");
        UNIT_ASSERT_VALUES_EQUAL(res, NMsgBusProxy::MSTATUS_OK);

        // Read old rows
        Cout << "SELECT value FROM Table WHERE key1 = i AND key2 is NULL" << Endl;
        for (ui32 i = 0; i < ROW_COUNT; ++i) {
            auto res = annoyingClient.FlatQuery(Sprintf(R"((
                (let row_ '('('key1 (Uint32 '%u)) '('key2 (Null))))
                (let cols_ '('value))
                (let select_ (SelectRow '/dc-1/TableWithFilter row_ cols_))
                (let ret_ (AsList (SetResult 'ret0 select_)))
                (return ret_)
            ))", i));

            // Cout << res << Endl;
            TValue value = TValue::Create(res.GetValue(), res.GetType());
            TValue ret0 = value["ret0"];
            ui32 val = ret0[0];
            UNIT_ASSERT_VALUES_EQUAL(val, i);
        }
    }

    Y_UNIT_TEST(SelectRangeBytesLimit) {
        const ui32 TABLE_ROWS = 10;

        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");

        annoyingClient.CreateTable("/dc-1/test",
            R"(Name: "TestTable"
                Columns { Name: "Key"   Type: "Uint64"}
                Columns { Name: "Value" Type: "Uint64"}
                KeyColumnNames: ["Key"]
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
            )");

        for (ui32 shard = 0; shard < 3; ++shard) {
            for (ui32 i = 0; i < TABLE_ROWS; ++i) {
                auto key = shard * 100 + i;

                annoyingClient.FlatQuery(Sprintf(R"(
                    (
                    (let key '('('Key (Uint64 '%d)) ))
                    (let payload '('('Value (Uint64 '%d))))
                    (return (AsList (UpdateRow '"/dc-1/test/TestTable" key payload)))
                    )
                    )", key, i
                ));
            }
        }

        auto res = annoyingClient.FlatQuery(Sprintf(R"(
            (
            (let range '('ExcFrom 'IncTo '('Key (Null) (Void))))
            (let data (SelectRange
                '"/dc-1/test/TestTable"
                range
                '('Key 'Value)
                '(
                    '('"BytesLimit" (Uint64 '%d))
                 )
            ))
            (return (AsList (SetResult 'Result data)))
            )
            )", TABLE_ROWS * 8
        ));

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue result = value["Result"];
        UNIT_ASSERT_VALUES_EQUAL((bool)result["Truncated"], true);
        auto list = result["List"];
        for (ui32 i = 0; i < list.Size(); ++i) {
            auto key = (ui64) list[i]["Key"];
            UNIT_ASSERT(key < 100);
        }
    }

     Y_UNIT_TEST(SelectRangeItemsLimit) {
        const ui32 TABLE_ROWS = 10;

        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");

        annoyingClient.CreateTable("/dc-1/test",
            R"(Name: "TestTable"
                Columns { Name: "Key"   Type: "Uint64"}
                Columns { Name: "Value" Type: "Uint64"}
                KeyColumnNames: ["Key"]
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
            )");

        for (ui32 shard = 0; shard < 3; ++shard) {
            for (ui32 i = 0; i < TABLE_ROWS; ++i) {
                auto key = shard * 100 + i;

                annoyingClient.FlatQuery(Sprintf(R"(
                    (
                    (let key '('('Key (Uint64 '%d)) ))
                    (let payload '('('Value (Uint64 '%d))))
                    (return (AsList (UpdateRow '"/dc-1/test/TestTable" key payload)))
                    )
                    )", key, i
                ));
            }
        }

        auto res = annoyingClient.FlatQuery(Sprintf(R"(
            (
            (let range '('ExcFrom 'IncTo '('Key (Null) (Void))))
            (let data (SelectRange
                '"/dc-1/test/TestTable"
                range
                '('Key 'Value)
                '(
                    '('"ItemsLimit" (Uint64 '%d))
                 )
            ))
            (return (AsList (SetResult 'Result data)))
            )
            )", 5
        ));

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue result = value["Result"];
        UNIT_ASSERT_VALUES_EQUAL((bool)result["Truncated"], true);
        auto list = result["List"];
        for (ui32 i = 0; i < list.Size(); ++i) {
            auto key = (ui64) list[i]["Key"];
            UNIT_ASSERT(key < 100);
        }
    }

    Y_UNIT_TEST(SelectRangeBothLimit) {
        const ui32 TABLE_ROWS = 50;

        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");

        annoyingClient.CreateTable("/dc-1/test",
            R"(Name: "TestTable"
                Columns { Name: "Key"   Type: "Uint64"}
                Columns { Name: "Value" Type: "Uint64"}
                KeyColumnNames: ["Key"]
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
            )");

        for (ui32 shard = 0; shard < 3; ++shard) {
            for (ui32 i = 0; i < TABLE_ROWS; ++i) {
                auto key = shard * 100 + i;

                annoyingClient.FlatQuery(Sprintf(R"(
                    (
                    (let key '('('Key (Uint64 '%d)) ))
                    (let payload '('('Value (Uint64 '%d))))
                    (return (AsList (UpdateRow '"/dc-1/test/TestTable" key payload)))
                    )
                    )", key, i
                ));
            }
        }

        auto res = annoyingClient.FlatQuery(Sprintf(R"(
            (
            (let range '('ExcFrom 'IncTo '('Key (Null) (Void))))
            (let data (SelectRange
                '"/dc-1/test/TestTable"
                range
                '('Key 'Value)
                '(
                    '('"ItemsLimit" (Uint64 '%d))
                    '('"BytesLimit" (Uint64 '%d))
                 )
            ))
            (return (AsList (SetResult 'Result data)))
            )
            )", 30, TABLE_ROWS * 8
        ));

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue result = value["Result"];
        UNIT_ASSERT_VALUES_EQUAL((bool)result["Truncated"], true);
        auto list = result["List"];
        for (ui32 i = 0; i < list.Size(); ++i) {
            auto key = (ui64) list[i]["Key"];
            UNIT_ASSERT(key < 100);
        }
    }

    static NKikimrMiniKQL::TResult CreateTableAndExecuteMkql(const TString& mkql) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");

        annoyingClient.CreateTable("/dc-1/test",
            R"(Name: "TestTable"
                Columns { Name: "Key1"   Type: "Uint64"}
                Columns { Name: "Key2"   Type: "String"}
                Columns { Name: "Value"  Type: "String"}
                KeyColumnNames: ["Key1", "Key2"]
            )");

        auto fillValues = [&annoyingClient]
            (const TMaybe<ui64>& key1, const TMaybe<TString>& key2, const TString& value) {
                TString key1Str = key1
                    ? "(Uint64 '" + ToString(*key1) + ")"
                    : "(Null)";

                TString key2Str = key2
                    ? "(String '" + ToString(*key2) + ")"
                    : "(Null)";

                annoyingClient.FlatQuery(Sprintf(R"(
                    (
                    (let key '('('Key1 %s) '('Key2 %s) ))
                    (let payload '('('Value (String '%s))))
                    (return (AsList (UpdateRow '"/dc-1/test/TestTable" key payload)))
                    )
                    )", key1Str.c_str(), key2Str.c_str(), value.c_str()
                ));
            };

        fillValues({}, {},            "One");
        fillValues(1,  {},            "Two");
        fillValues(1,  TString("k1"), "Three");
        fillValues(2,  {},            "Four");
        fillValues(2,  TString("k1"), "Five");

        return annoyingClient.FlatQuery(mkql);
    }

    Y_UNIT_TEST(SelectRangeSkipNullKeys) {
        auto res = CreateTableAndExecuteMkql(Sprintf(R"(
            (
            (let range '('ExcFrom 'IncTo '('Key1 (Null) (Void)) '('Key2 (Null) (Void))))
            (let data (SelectRange
                '"/dc-1/test/TestTable"
                range
                '('Value)
                '(
                    '('"SkipNullKeys" '('Key2))
                 )
            ))
            (return (AsList (SetResult 'Result data)))
            )
            )"
        ));

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue result = value["Result"];
        auto list = result["List"];
        UNIT_ASSERT_VALUES_EQUAL(list.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL((TString)list[0]["Value"], "Three");
        UNIT_ASSERT_VALUES_EQUAL((TString)list[1]["Value"], "Five");
    }

    Y_UNIT_TEST(SelectRangeForbidNullArgs1) {
        auto res = CreateTableAndExecuteMkql(Sprintf(R"(
            (
            (let range '('IncFrom 'IncTo '('Key1 (Null) (Void)) '('Key2 (Null) (Void))))
            (let data (SelectRange
                '"/dc-1/test/TestTable"
                range
                '('Value)
                '(
                    '('"ForbidNullArgsFrom" '('Key2))
                 )
            ))
            (return (AsList (SetResult 'Result data)))
            )
            )"
        ));

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue result = value["Result"];
        auto list = result["List"];
        UNIT_ASSERT_VALUES_EQUAL(list.Size(), 0);
    }

    Y_UNIT_TEST(SelectRangeForbidNullArgs2) {
        auto res = CreateTableAndExecuteMkql(Sprintf(R"(
            (
            (let range '('IncFrom 'IncTo '('Key1 (Null) (Void)) '('Key2 (Null) (Void))))
            (let data (SelectRange
                '"/dc-1/test/TestTable"
                range
                '('Value)
                '(
                    '('"ForbidNullArgsFrom" '('Key1))
                 )
            ))
            (return (AsList (SetResult 'Result data)))
            )
            )"
        ));

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue result = value["Result"];
        auto list = result["List"];
        UNIT_ASSERT_VALUES_EQUAL(list.Size(), 0);
    }

    Y_UNIT_TEST(SelectRangeNullArgs3) {
        auto res = CreateTableAndExecuteMkql(Sprintf(R"(
            (
            (let range '('IncFrom 'IncTo '('Key1 (Uint64 '1) (Uint64 '1)) '('Key2 (Null) (Void))))
            (let data (SelectRange
                '"/dc-1/test/TestTable"
                range
                '('Value)
                '(
                 )
            ))
            (return (AsList (SetResult 'Result data)))
            )
            )"
        ));

        TValue value = TValue::Create(res.GetValue(), res.GetType());

        TValue result = value["Result"];
        auto list = result["List"];
        UNIT_ASSERT_VALUES_EQUAL(list.Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL((TString)list[0]["Value"], "Two");
        UNIT_ASSERT_VALUES_EQUAL((TString)list[1]["Value"], "Three");
    }

    Y_UNIT_TEST(SelectRangeForbidNullArgs3) {
        auto res = CreateTableAndExecuteMkql(Sprintf(R"(
            (
            (let range '('IncFrom 'IncTo '('Key1 (Uint64 '1) (Uint64 '1)) '('Key2 (Null) (Void))))
            (let data (SelectRange
                '"/dc-1/test/TestTable"
                range
                '('Value)
                '(
                    '('"ForbidNullArgsFrom" '('Key2))
                 )
            ))
            (return (AsList (SetResult 'Result data)))
            )
            )"
        ));

        TValue value = TValue::Create(res.GetValue(), res.GetType());

        TValue result = value["Result"];
        auto list = result["List"];
        UNIT_ASSERT_VALUES_EQUAL(list.Size(), 0);
    }

    Y_UNIT_TEST(SelectRangeNullArgs4) {
        auto res = CreateTableAndExecuteMkql(Sprintf(R"(
            (
            (let range '('IncFrom 'IncTo '('Key1 (Uint64 '1) (Uint64 '1)) '('Key2 (Null) (Null) )))
            (let data (SelectRange
                '"/dc-1/test/TestTable"
                range
                '('Value)
                '(
                 )
            ))
            (return (AsList (SetResult 'Result data)))
            )
            )"
        ));

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue result = value["Result"];
        auto list = result["List"];
        UNIT_ASSERT_VALUES_EQUAL(list.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL((TString)list[0]["Value"], "Two");
    }

    Y_UNIT_TEST(SelectRangeForbidNullArgs4) {
        auto res = CreateTableAndExecuteMkql(Sprintf(R"(
            (
            (let range '('IncFrom 'IncTo '('Key1 (Uint64 '1) (Uint64 '1)) '('Key2 (Null) (Null) )))
            (let data (SelectRange
                '"/dc-1/test/TestTable"
                range
                '('Value)
                '(
                    '('"ForbidNullArgsTo" '('Key2))
                 )
            ))
            (return (AsList (SetResult 'Result data)))
            )
            )"
        ));

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue result = value["Result"];
        auto list = result["List"];
        UNIT_ASSERT_VALUES_EQUAL(list.Size(), 0);
    }

    Y_UNIT_TEST(SelectRangeReverse) {
        const ui32 TABLE_ROWS = 10;

        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");

        annoyingClient.CreateTable("/dc-1/test",
            R"(Name: "TestTable"
                Columns { Name: "Key"   Type: "Uint64"}
                Columns { Name: "Value" Type: "Uint64"}
                KeyColumnNames: ["Key"]
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
            )");

        for (ui32 shard = 0; shard < 3; ++shard) {
            for (ui32 i = 0; i < TABLE_ROWS; ++i) {
                auto key = shard * 100 + i;

                annoyingClient.FlatQuery(Sprintf(R"(
                    (
                    (let key '('('Key (Uint64 '%d)) ))
                    (let payload '('('Value (Uint64 '%d))))
                    (return (AsList (UpdateRow '"/dc-1/test/TestTable" key payload)))
                    )
                    )", key, i
                ));
            }
        }

        auto res = annoyingClient.FlatQuery(R"(
            (
            (let range '('ExcFrom 'IncTo '('Key (Null) (Void))))
            (let data (SelectRange
                '"/dc-1/test/TestTable"
                range
                '('Key 'Value)
                '(
                    '('"Reverse" (Bool 'true))
                 )
            ))
            (return (AsList (SetResult 'Result data)))
            )
            )"
        );

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue result = value["Result"];
        UNIT_ASSERT_VALUES_EQUAL((bool)result["Truncated"], false);
        auto list = result["List"];
        ui64 expected = 200 + TABLE_ROWS - 1;
        for (ui32 i = 0; i < list.Size(); ++i) {
            auto key = (ui64) list[i]["Key"];
            UNIT_ASSERT_VALUES_EQUAL(key, expected);
            if ((expected % 100) == 0) {
                expected -= 100;
                expected += TABLE_ROWS - 1;
            } else {
                --expected;
            }
        }
    }

    Y_UNIT_TEST(SelectRangeReverseItemsLimit) {
        const ui32 TABLE_ROWS = 10;

        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");

        annoyingClient.CreateTable("/dc-1/test",
            R"(Name: "TestTable"
                Columns { Name: "Key"   Type: "Uint64"}
                Columns { Name: "Value" Type: "Uint64"}
                KeyColumnNames: ["Key"]
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
            )");

        for (ui32 shard = 0; shard < 3; ++shard) {
            for (ui32 i = 0; i < TABLE_ROWS; ++i) {
                auto key = shard * 100 + i;

                annoyingClient.FlatQuery(Sprintf(R"(
                    (
                    (let key '('('Key (Uint64 '%d)) ))
                    (let payload '('('Value (Uint64 '%d))))
                    (return (AsList (UpdateRow '"/dc-1/test/TestTable" key payload)))
                    )
                    )", key, i
                ));
            }
        }

        auto res = annoyingClient.FlatQuery(Sprintf(R"(
            (
            (let range '('ExcFrom 'IncTo '('Key (Null) (Void))))
            (let data (SelectRange
                '"/dc-1/test/TestTable"
                range
                '('Key 'Value)
                '(
                    '('"ItemsLimit" (Uint64 '%d))
                    '('"Reverse" (Bool 'true))
                 )
            ))
            (return (AsList (SetResult 'Result data)))
            )
            )", 5
        ));

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue result = value["Result"];
        UNIT_ASSERT_VALUES_EQUAL((bool)result["Truncated"], true);
        auto list = result["List"];
        ui64 expected = 200 + TABLE_ROWS - 1;
        for (ui32 i = 0; i < list.Size(); ++i) {
            auto key = (ui64) list[i]["Key"];
            UNIT_ASSERT_VALUES_EQUAL(key, expected);
            --expected;
        }
    }

    Y_UNIT_TEST(SelectRangeReverseIncludeKeys) {
        const ui32 TABLE_ROWS = 10;

        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");

        annoyingClient.CreateTable("/dc-1/test",
            R"(Name: "TestTable"
                Columns { Name: "Key"   Type: "Uint64"}
                Columns { Name: "Value" Type: "Uint64"}
                KeyColumnNames: ["Key"]
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
            )");

        for (ui32 shard = 0; shard < 3; ++shard) {
            for (ui32 i = 0; i < TABLE_ROWS; ++i) {
                auto key = shard * 100 + i;

                annoyingClient.FlatQuery(Sprintf(R"(
                    (
                    (let key '('('Key (Uint64 '%d)) ))
                    (let payload '('('Value (Uint64 '%d))))
                    (return (AsList (UpdateRow '"/dc-1/test/TestTable" key payload)))
                    )
                    )", key, i
                ));
            }
        }

        auto res = annoyingClient.FlatQuery(R"(
            (
            (let range '('IncFrom 'IncTo '('Key (Uint64 '5) (Uint64 '205))))
            (let data (SelectRange
                '"/dc-1/test/TestTable"
                range
                '('Key 'Value)
                '(
                    '('"Reverse" (Bool 'true))
                 )
            ))
            (return (AsList (SetResult 'Result data)))
            )
            )"
        );

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue result = value["Result"];
        UNIT_ASSERT_VALUES_EQUAL((bool)result["Truncated"], false);
        auto list = result["List"];
        ui64 expected = 205; // 205 should be included
        for (ui32 i = 0; i < list.Size(); ++i) {
            auto key = (ui64) list[i]["Key"];
            UNIT_ASSERT_VALUES_EQUAL(key, expected);
            if ((expected % 100) == 0) {
                expected -= 100;
                expected += TABLE_ROWS - 1;
            } else {
                --expected;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(expected, 4); // 5 should have been included
    }

    Y_UNIT_TEST(SelectRangeReverseExcludeKeys) {
        const ui32 TABLE_ROWS = 10;

        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        TFlatMsgBusClient annoyingClient(port);

        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "test");

        annoyingClient.CreateTable("/dc-1/test",
            R"(Name: "TestTable"
                Columns { Name: "Key"   Type: "Uint64"}
                Columns { Name: "Value" Type: "Uint64"}
                KeyColumnNames: ["Key"]
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 100 } } } }
                SplitBoundary { KeyPrefix { Tuple { Optional { Uint64: 200 } } } }
            )");

        for (ui32 shard = 0; shard < 3; ++shard) {
            for (ui32 i = 0; i < TABLE_ROWS; ++i) {
                auto key = shard * 100 + i;

                annoyingClient.FlatQuery(Sprintf(R"(
                    (
                    (let key '('('Key (Uint64 '%d)) ))
                    (let payload '('('Value (Uint64 '%d))))
                    (return (AsList (UpdateRow '"/dc-1/test/TestTable" key payload)))
                    )
                    )", key, i
                ));
            }
        }

        auto res = annoyingClient.FlatQuery(R"(
            (
            (let range '('ExcFrom 'ExcTo '('Key (Uint64 '5) (Uint64 '205))))
            (let data (SelectRange
                '"/dc-1/test/TestTable"
                range
                '('Key 'Value)
                '(
                    '('"Reverse" (Bool 'true))
                 )
            ))
            (return (AsList (SetResult 'Result data)))
            )
            )"
        );

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue result = value["Result"];
        UNIT_ASSERT_VALUES_EQUAL((bool)result["Truncated"], false);
        auto list = result["List"];
        ui64 expected = 204; // 205 should not be included
        for (ui32 i = 0; i < list.Size(); ++i) {
            auto key = (ui64) list[i]["Key"];
            UNIT_ASSERT_VALUES_EQUAL(key, expected);
            if ((expected % 100) == 0) {
                expected -= 100;
                expected += TABLE_ROWS - 1;
            } else {
                --expected;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(expected, 5); // 5 should not have been included
    }

}

}}
