#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/datashard/datashard_failpoints.h>
#include <ydb/library/aclib/aclib.h>

#include <util/string/subst.h>
#include <util/system/valgrind.h>

#include "kicli.h"

using namespace NKikimr;

namespace {

Tests::TServer StartupKikimr(NMsgBusProxy::TMsgBusClientConfig& clientConfig,
    const TAutoPtr<TLogBackend> logBackend = {})
{
    TPortManager pm;
    const ui32 port = pm.GetPort(12001);
    auto settings = Tests::TServerSettings(port);
    settings.SetLogBackend(logBackend);
    settings.SetEnableSystemViews(false);
    Tests::TServer Server(settings);
    Tests::TClient Client(settings);
    Client.InitRootScheme();
    clientConfig = Client.GetClientConfig();
    return Server;
}

Tests::TServer StartupKikimr(NGRpcProxy::TGRpcClientConfig& clientConfig,
                             const TAutoPtr<TLogBackend> logBackend = {},
                             const NKikimrConfig::TAppConfig &config = {})
{
    TPortManager pm;
    const ui32 msgbusPort = pm.GetPort(12001);
    const ui32 grpcPort = pm.GetPort(12002);

    auto settings = Tests::TServerSettings(msgbusPort);
    settings.SetLogBackend(logBackend);
    settings.AppConfig->CopyFrom(config);
    settings.SetEnableSystemViews(false);

    Tests::TServer Server(settings);
    Server.EnableGRpc(grpcPort);

    Tests::TClient Client(settings);
    Client.InitRootScheme();

    clientConfig.Locator = "[::1]:" + ToString(grpcPort);

    return Server;
}

struct TTestEnvironment {
    NMsgBusProxy::TMsgBusClientConfig ClientConfig;
    Tests::TServer Server;
    NClient::TKikimr Kikimr;
    NKikimr::NClient::TSchemaObject DC;
    NKikimr::NClient::TSchemaObject Zoo;
    NKikimr::NClient::TSchemaObject Animals;

    TTestEnvironment(const std::initializer_list<NKikimr::NClient::TColumn>& columns = {
                NClient::TKeyColumn("Id", NClient::TType::Uint64),
                NClient::TColumn("Species", NClient::TType::Utf8),
                NClient::TColumn("Name", NClient::TType::Utf8),
                NClient::TColumn("Weight", NClient::TType::Int64)
            })
        : Server(StartupKikimr(ClientConfig))
        , Kikimr(ClientConfig)
        , DC(Kikimr.GetSchemaRoot("dc-1"))
        , Zoo(DC.MakeDirectory("Zoo"))
        , Animals(Zoo.CreateTable("Animals", columns))
    {}
};

bool HasChild(const NKikimr::NClient::TSchemaObject& obj, const char * path) {
    auto children = obj.GetChildren();
    return std::find_if(children.begin(), children.end(),
        [&](const NClient::TSchemaObject& s) -> bool {
            return s.GetPath() == path;
        }) != children.end();
}

} // namespace

Y_UNIT_TEST_SUITE(ClientLibSchema) {
    static void RootExists(NClient::TKikimr& kikimr) {
        auto root = kikimr.GetSchemaRoot();
        auto children = root.GetChildren();
        // all root elements exists regardless of whether they have been initialized or not
        //UNIT_ASSERT(children.empty());
    }

    Y_UNIT_TEST(RootExists) {
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        RootExists(kikimr);
    }

    Y_UNIT_TEST(RootExists_GRpc) {
        NGRpcProxy::TGRpcClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        RootExists(kikimr);
    }

    static void RootDcExists(NClient::TKikimr& kikimr) {
        auto root = kikimr.GetSchemaRoot();
        auto children = root.GetChildren();
        UNIT_ASSERT(std::find_if(children.begin(), children.end(), [](const NClient::TSchemaObject& s) -> bool { return s.GetPath() == "/dc-1"; }) != children.end());
   }

    Y_UNIT_TEST(RootDcExists) {
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        RootDcExists(kikimr);
   }

    Y_UNIT_TEST(RootDcExists_GRpc) {
        NGRpcProxy::TGRpcClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        RootDcExists(kikimr);
   }

    static void RootDcStartsEmpty(NClient::TKikimr& kikimr) {
        auto root = kikimr.GetSchemaRoot();
        auto dc = kikimr.GetSchemaRoot("dc-1");
        UNIT_ASSERT(dc.GetPath() == "/dc-1");
        auto children = root.GetChildren();
        UNIT_ASSERT(std::find_if(children.begin(), children.end(), [](const NClient::TSchemaObject& s) -> bool { return s.GetPath() == "/dc-1"; }) != children.end());
        UNIT_ASSERT(dc.GetChildren().empty());
    }

    Y_UNIT_TEST(RootDcStartsEmpty) {
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        RootDcStartsEmpty(kikimr);
    }

    Y_UNIT_TEST(RootDcStartsEmpty_GRpc) {
        NGRpcProxy::TGRpcClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        RootDcStartsEmpty(kikimr);
    }

    static void CreateTable(NClient::TKikimr& kikimr) {
        auto dc = kikimr.GetSchemaRoot("dc-1");
        auto animals = dc.CreateTable("Animals", {
                NClient::TKeyColumn("Id", NClient::TType::Uint64),
                NClient::TColumn("Species", NClient::TType::Utf8),
                NClient::TColumn("Name", NClient::TType::Utf8),
                NClient::TColumn("Weight", NClient::TType::Int64),
                NClient::TColumn("MiscY", NClient::TType::Yson),
                NClient::TColumn("MiscJ", NClient::TType::Json)
            });
        auto children = dc.GetChildren();
        UNIT_ASSERT(HasChild(dc, "/dc-1/Animals"));
        UNIT_ASSERT(animals.IsTable());

        auto object = kikimr.GetSchemaObject("/dc-1/Animals");
        auto columns = object.GetColumns();
        UNIT_ASSERT(std::find_if(columns.begin(), columns.end(), [](const auto& c) -> bool { return c.Name == "Id" && c.Key == true; }));
        UNIT_ASSERT(std::find_if(columns.begin(), columns.end(), [](const auto& c) -> bool { return c.Name == "Species"; }));
        UNIT_ASSERT(std::find_if(columns.begin(), columns.end(), [](const auto& c) -> bool { return c.Name == "Name"; }));
        UNIT_ASSERT(std::find_if(columns.begin(), columns.end(), [](const auto& c) -> bool { return c.Name == "Weight"; }));
        UNIT_ASSERT(std::find_if(columns.begin(), columns.end(), [](const auto& c) -> bool { return c.Name == "MiscY"; }));
        UNIT_ASSERT(std::find_if(columns.begin(), columns.end(), [](const auto& c) -> bool { return c.Name == "MiscJ"; }));

        try {
            dc.CreateTable("FailedCreateTable", {
                NClient::TKeyColumn("Id", NClient::TType::Json),
                NClient::TColumn("Value", NClient::TType::Uint64)
            });
            UNIT_FAIL("Unexpected CreateTable success");
        } catch (const yexception& ) {
        }

        try {
            dc.CreateTable("FailedCreateTable", {
                NClient::TKeyColumn("Id", NClient::TType::Yson),
                NClient::TColumn("Value", NClient::TType::Uint64)
            });
            UNIT_FAIL("Unexpected CreateTable success");
        } catch (const yexception& ) {
        }

#if 1 // TODO: it should be an error
        auto sameTable = dc.CreateTable("Animals", {
                NClient::TKeyColumn("Id", NClient::TType::Uint64),
                NClient::TColumn("Species", NClient::TType::Utf8)
            });
#endif
    }

    Y_UNIT_TEST(CreateTable) {
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        CreateTable(kikimr);
    }

    Y_UNIT_TEST(CreateTable_GRpc) {
        NGRpcProxy::TGRpcClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        CreateTable(kikimr);
    }

    static void MkDir(NClient::TKikimr& kikimr) {
        auto dc = kikimr.GetSchemaRoot("dc-1");
        auto zoo = dc.MakeDirectory("Zoo");
        auto animals = zoo.CreateTable("Animals", {
            NClient::TKeyColumn("Id", NClient::TType::Uint64),
            NClient::TColumn("Name", NClient::TType::Utf8)
        });
        UNIT_ASSERT(HasChild(dc, "/dc-1/Zoo"));
        UNIT_ASSERT(HasChild(zoo, "/dc-1/Zoo/Animals"));
        UNIT_ASSERT(zoo.IsDirectory());
        UNIT_ASSERT(animals.IsTable());

        zoo.MakeDirectory("Garden");
        UNIT_ASSERT(HasChild(zoo, "/dc-1/Zoo/Garden"));

        try {
            animals.MakeDirectory("FailedMkDir");
            UNIT_FAIL("Unexpected MakeDirectory success");
        } catch (const yexception& ) {
        }

        try {
            animals.CreateTable("FailedCreateTable", {
                NClient::TKeyColumn("Id", NClient::TType::Uint64),
                NClient::TColumn("Value", NClient::TType::Uint64)
            });
            UNIT_FAIL("Unexpected CreateTable success");
        } catch (const yexception& ) {
        }
    }

    Y_UNIT_TEST(MkDir) {
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        MkDir(kikimr);
    }

    Y_UNIT_TEST(MkDir_GRpc) {
        NGRpcProxy::TGRpcClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        MkDir(kikimr);
    }

    static void CreatePartitionedTable(NClient::TKikimr& kikimr) {
        auto dc = kikimr.GetSchemaRoot("dc-1");
        auto zoo = dc.MakeDirectory("Zoo");
        auto animals = zoo.CreateTable("Animals", {
                            NClient::TKeyPartitioningColumn("Id", NClient::TType::Uint64, 4),
                            NClient::TColumn("Species", NClient::TType::Utf8),
                            NClient::TColumn("Name", NClient::TType::Utf8),
                            NClient::TColumn("Weight", NClient::TType::Int64)
                        });
        UNIT_ASSERT(zoo.IsDirectory());
        UNIT_ASSERT(animals.IsTable());

        UNIT_ASSERT_VALUES_EQUAL(zoo.GetChild("Animals").GetStats().PartitionsCount, 4);
    }

    Y_UNIT_TEST(CreatePartitionedTable) {
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        CreatePartitionedTable(kikimr);
    }

    Y_UNIT_TEST(CreatePartitionedTable_GRpc) {
        NGRpcProxy::TGRpcClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        CreatePartitionedTable(kikimr);
    }

    static void CreateVeryPartitionedTable(NClient::TKikimr& kikimr) {
        const ui64 numParts = NValgrind::PlainOrUnderValgrind(3000, 10);

        auto dc = kikimr.GetSchemaRoot("dc-1");
        auto zoo = dc.MakeDirectory("Zoo");
        auto animals = zoo.CreateTable("Animals", {
                            NClient::TKeyPartitioningColumn("Id", NClient::TType::Uint64, numParts),
                            NClient::TColumn("Species", NClient::TType::Utf8),
                            NClient::TColumn("Name", NClient::TType::Utf8),
                            NClient::TColumn("Weight", NClient::TType::Int64)
                        });

        UNIT_ASSERT_VALUES_EQUAL(zoo.GetChild("Animals").GetStats().PartitionsCount, numParts);
    }

    Y_UNIT_TEST(CreateVeryPartitionedTable) {
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        CreateVeryPartitionedTable(kikimr);
    }

    Y_UNIT_TEST(CreateVeryPartitionedTable_GRpc) {
        NGRpcProxy::TGRpcClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        CreateVeryPartitionedTable(kikimr);
    }

    static void Drop(NClient::TKikimr& kikimr) {
        using NKikimr::NClient::TSchemaObject;

        TSchemaObject dc = kikimr.GetSchemaRoot("dc-1");
        try {
            dc.Drop();
            UNIT_FAIL("Unexpected Drop success");
        } catch (const yexception& ) {
        }

        TSchemaObject dir = dc.MakeDirectory("happiness");
        UNIT_ASSERT_EQUAL(dc.GetChildren().size(), 1);

        TSchemaObject table1 = dir.CreateTable("joy", {
            NClient::TKeyColumn("id", NClient::TType::Uint64),
            NClient::TColumn("note", NClient::TType::Utf8)
        });
        dir.CreateTable("fun", {
            NClient::TKeyColumn("id", NClient::TType::Uint64),
            NClient::TColumn("note", NClient::TType::Utf8)
        });

        try {
            dir.Drop();
            UNIT_FAIL("Unexpected Drop success");
        } catch (const yexception& ) {
        }

        UNIT_ASSERT_EQUAL(dir.GetChildren().size(), 2);

        table1.Drop();

        auto children = dir.GetChildren();
        UNIT_ASSERT_EQUAL(children.size(), 1);

        TSchemaObject table2 = children[0];
        UNIT_ASSERT_EQUAL(table2.GetName(), "fun");
        table2.Drop();

        UNIT_ASSERT_EQUAL(dir.GetChildren().size(), 0);
        dir.Drop();

        UNIT_ASSERT_EQUAL(dc.GetChildren().size(), 0);
    }

    Y_UNIT_TEST(Drop) {
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        Drop(kikimr);
    }

    Y_UNIT_TEST(Drop_GRpc) {
        NGRpcProxy::TGRpcClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        Drop(kikimr);
    }
#if 0 // TODO: not implemented
    static void MaxPath(NClient::TKikimr& kikimr) {
        try {
            const ui32 TestedMaxPath = 32;
            TVector<NKikimr::NClient::TSchemaObject> paths;
            paths.reserve(TestedMaxPath+1);
            paths.emplace_back(kikimr.GetSchemaRoot("dc-1"));

            for (ui32 i = 0; i < TestedMaxPath; ++i) {
                paths.emplace_back(paths.back().MakeDirectory("Dir"));
            }
            UNIT_FAIL("Unexpected MaxPath exceeded");
        } catch (const yexception& ) {
        }
    }

    Y_UNIT_TEST(MaxPath) {
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        MaxPath(kikimr);
    }

    Y_UNIT_TEST(MaxPath_GRpc) {
        NGRpcProxy::TGRpcClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        MaxPath(kikimr);
    }
#endif
}

Y_UNIT_TEST_SUITE(ClientLib) {
    Y_UNIT_TEST(Test6) {
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        auto query = kikimr.Query(
                    "("
                    "(let row '('('Id (Uint64 '1))))"
                    "(let myUpd '("
                    "  '('Species (Utf8 '\"Rat\"))"
                    "  '('Name (Utf8 '\"Dobby\"))"
                    "  '('Weight (Int64 '350))))"
                    "(let pgmReturn (AsList"
                    "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                    "))"
                    "(return pgmReturn)"
                    ")");
        auto result = query.SyncPrepare().GetQuery().SyncExecute();
        auto value = result.GetValue();
        UNIT_ASSERT(value.IsNull());
    }

    Y_UNIT_TEST(Test7) {
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        {
            auto query = kikimr.Query(
                        "("
                        "(let row '('('Id (Uint64 '1))))"
                        "(let myUpd '("
                        "  '('Species (Utf8 '\"Rat\"))"
                        "  '('Name (Utf8 '\"Dobby\"))"
                        "  '('Weight (Int64 '350))))"
                        "(let pgmReturn (AsList"
                        "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                        "))"
                        "(return pgmReturn)"
                        ")");
            auto result = query.SyncPrepare().GetQuery().SyncExecute();
        }

        {
            auto query = kikimr.Query(
                        "("
                        "(let row '('('Id (Uint64 '1))))"
                        "(let select '('Species 'Name 'Weight))"
                        "(let pgmReturn (AsList"
                        "    (SetResult 'myRes (SelectRow '/dc-1/Zoo/Animals row select))"
                        "))"
                        "(return pgmReturn)"
                        ")");
            auto result = query.SyncExecute();
            auto value = result.GetValue();
            UNIT_ASSERT(!value["myRes"].IsNull());
            UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"]["Species"], "Rat");
            UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"]["Name"], "Dobby");
        }
    }

    Y_UNIT_TEST(Test8) {
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        {
            auto query = kikimr.Query(
                        "("
                        "(let row '('('Id (Uint64 '1))))"
                        "(let myUpd '("
                        "  '('Species (Utf8 '\"Rat\"))"
                        "  '('Name (Utf8 '\"Dobby\"))"
                        "  '('Weight (Int64 '350))))"
                        "(let pgmReturn (AsList"
                        "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                        "))"
                        "(return pgmReturn)"
                        ")");
            auto result = query.AsyncPrepare().GetValue(TDuration::Max()).GetQuery().AsyncExecute().GetValue(TDuration::Max());
        }

        {
            auto query = kikimr.Query(
                        "("
                        "(let row '('('Id (Uint64 '1))))"
                        "(let select '('Species 'Name 'Weight))"
                        "(let pgmReturn (AsList"
                        "    (SetResult 'myRes (SelectRow '/dc-1/Zoo/Animals row select))"
                        "))"
                        "(return pgmReturn)"
                        ")");
            auto result = query.AsyncPrepare().GetValue(TDuration::Max()).GetQuery().AsyncExecute().GetValue(TDuration::Max());
            auto value = result.GetValue();
            UNIT_ASSERT(!value["myRes"].IsNull());
            UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"]["Species"], "Rat");
            UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"]["Name"], "Dobby");
        }
    }

    Y_UNIT_TEST(Test9) {
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        {
            auto result = kikimr.Query(
                        "("
                        "(let row '('('Id (Uint64 '1))))"
                        "(let myUpd '("
                        "  '('Species (Utf8 '\"Rat\"))"
                        "  '('Name (Utf8 '\"Dobby\"))"
                        "  '('Weight (Int64 '350))))"
                        "(let pgmReturn (AsList"
                        "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                        "))"
                        "(return pgmReturn)"
                        ")").SyncPrepare().GetQuery().SyncExecute();
        }

        {
            auto result = kikimr.Query(
                        "("
                        "(let row '('('Id (Uint64 '2))))"
                        "(let myUpd '("
                        "  '('Species (Utf8 '\"Rat\"))"
                        "  '('Name (Utf8 '\"Korzhik\"))"
                        "  '('Weight (Int64 '500))))"
                        "(let pgmReturn (AsList"
                        "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                        "))"
                        "(return pgmReturn)"
                        ")").SyncPrepare().GetQuery().SyncExecute();
        }

        auto query = kikimr.Query(
                    "("
                    "(let range '('IncFrom '('Id (Uint64 '1) (Void))))"
                    "(let select '('Species 'Name 'Weight))"
                    "(let options '())"
                    "(let pgmReturn (AsList"
                    "    (SetResult 'myRes (Member (SelectRange '/dc-1/Zoo/Animals range select options) 'List))"
                    "))"
                    "(return pgmReturn)"
                    ")");
        auto result = query.SyncPrepare().GetQuery().SyncExecute();
        auto readResult = result.GetValue();

        UNIT_ASSERT_VALUES_EQUAL((TString)readResult["myRes"][0]["Species"], "Rat");
        UNIT_ASSERT_VALUES_EQUAL((TString)readResult["myRes"][0]["Name"], "Dobby");
        UNIT_ASSERT_VALUES_EQUAL((i64)readResult["myRes"][0]["Weight"], 350);
        UNIT_ASSERT_VALUES_EQUAL((TString)readResult["myRes"][1]["Species"], "Rat");
        UNIT_ASSERT_VALUES_EQUAL((TString)readResult["myRes"][1]["Name"], "Korzhik");
        UNIT_ASSERT_VALUES_EQUAL((i64)readResult["myRes"][1]["Weight"], 500);
    }

    Y_UNIT_TEST(Test10) {
        using namespace NClient;
        TTestEnvironment env({
            NClient::TKeyColumn("Id", NClient::TType::Uint64),
            NClient::TColumn("Species", NClient::TType::Utf8),
            NClient::TColumn("Name", NClient::TType::Utf8),
            NClient::TColumn("Description", NClient::TType::Utf8),
            NClient::TColumn("Weight", NClient::TType::Int64)
        });
        NClient::TKikimr& kikimr = env.Kikimr;

        auto updateQuery = kikimr.Query(
                        "("
                        "(let id (Parameter 'ID (DataType 'Uint64)))"
                        "(let row '('('Id id)))"
                        "(let sp (Parameter 'SPECIES (DataType 'Utf8)))"
                        "(let nm (Parameter 'NAME (DataType 'Utf8)))"
                        "(let ds (Parameter 'DESCRIPTION (DataType 'Utf8)))"
                        "(let wt (Parameter 'WEIGHT (DataType 'Int64)))"
                        "(let myUpd '("
                        "  '('Species sp)"
                        "  '('Name nm)"
                        "  '('Description ds)"
                        "  '('Weight wt)))"
                        "(let pgmReturn (AsList"
                        "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                        "))"
                        "(return pgmReturn)"
                        ")").SyncPrepare().GetQuery();
        auto selectQuery = kikimr.Query(
                    "("
                    "(let il (Parameter 'ITEMSLIMIT (DataType 'Uint64)))"
                    "(let bl (Parameter 'BYTESLIMIT (DataType 'Uint64)))"
                    "(let range '('IncFrom '('Id (Uint64 '1) (Void))))"
                    "(let select '('Species 'Name 'Weight 'Description))"
                    "(let options '('('ItemsLimit il) '('BytesLimit bl)))"
                    "(let pgmReturn (AsList"
                    "    (SetResult 'myRes (Member (SelectRange '/dc-1/Zoo/Animals range select options) 'List))"
                    "))"
                    "(return pgmReturn)"
                    ")").SyncPrepare().GetQuery();

        updateQuery.SyncExecute(
                                TParameter("ID", (ui64)1),
                                TParameter("SPECIES", "Rat"),
                                TParameter("NAME", "Dobby"),
                                TParameter("DESCRIPTION", "A test for \"double quotes\""),
                                TParameter("WEIGHT", (i64)350)
                            );
        updateQuery.SyncExecute(
                                TParameter("ID", (ui64)2),
                                TParameter("SPECIES", "Rat"),
                                TParameter("NAME", "Korzhik"),
                                TParameter("DESCRIPTION", "A test for 'single quotes'"),
                                TParameter("WEIGHT", (i64)500)
                            );

        auto result = selectQuery.SyncExecute(
                                TParameter("ITEMSLIMIT", (ui64)10),
                                TParameter("BYTESLIMIT", (ui64)10000)
                            );
        auto value = result.GetValue();

        UNIT_ASSERT_VALUES_EQUAL(value["myRes"].Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"][0]["Species"], "Rat");
        UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"][0]["Name"], "Dobby");
        UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"][0]["Description"], "A test for \"double quotes\"");
        UNIT_ASSERT_VALUES_EQUAL((i64)value["myRes"][0]["Weight"], 350);
        UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"][1]["Species"], "Rat");
        UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"][1]["Name"], "Korzhik");
        UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"][1]["Description"], "A test for 'single quotes'");
        UNIT_ASSERT_VALUES_EQUAL((i64)value["myRes"][1]["Weight"], 500);
    }

    Y_UNIT_TEST(Test11) {
        using namespace NClient;
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        auto updateQuery = kikimr.Query(
                        "("
                        "(let id (Parameter 'ID (DataType 'Uint64)))"
                        "(let row '('('Id id)))"
                        "(let sp (Parameter 'SPECIES (DataType 'Utf8)))"
                        "(let nm (Parameter 'NAME (DataType 'Utf8)))"
                        "(let wt (Parameter 'WEIGHT (DataType 'Int64)))"
                        "(let myUpd '("
                        "  '('Species sp)"
                        "  '('Name nm)"
                        "  '('Weight wt)))"
                        "(let pgmReturn (AsList"
                        "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                        "))"
                        "(return pgmReturn)"
                        ")").SyncPrepare().GetQuery();
        auto selectQuery = kikimr.Query(
                    "("
                    "(let range '('IncFrom '('Id (Uint64 '1) (Void))))"
                    "(let select '('Species 'Name 'Weight))"
                    "(let options '())"
                    "(let pgmReturn (AsList"
                    "    (SetResult 'myRes (Member (SelectRange '/dc-1/Zoo/Animals range select options) 'List))"
                    "))"
                    "(return pgmReturn)"
                    ")").SyncPrepare().GetQuery();

        updateQuery.SyncExecute(
                                TParameter("ID", (ui64)1),
                                TParameter("SPECIES", "Rat"),
                                TParameter("NAME", "Dobby"),
                                TParameter("WEIGHT", (i64)350)
                            );
        updateQuery.SyncExecute(
                                TParameter("ID", (ui64)2),
                                TParameter("SPECIES", "Rat"),
                                TParameter("NAME", "Korzhik"),
                                TParameter("WEIGHT", (i64)500)
                            );

        auto result = selectQuery.SyncExecute();
        auto value = result.GetValue();

        UNIT_ASSERT_VALUES_EQUAL(value.GetTypeText<TFormatCxx>(),
                          "struct{optional<list<struct{optional<Utf8> Name;optional<Utf8> Species;optional<Int64> Weight;}>> myRes;}");
        UNIT_ASSERT_VALUES_EQUAL(value.GetValueText<TFormatJSON>(),
                          "{\"myRes\": [{\"Name\": \"Dobby\", \"Species\": \"Rat\", \"Weight\": 350}, {\"Name\": \"Korzhik\", \"Species\": \"Rat\", \"Weight\": 500}]}");
        UNIT_ASSERT_VALUES_EQUAL(value.GetValueText<TFormatRowset>(),
                          "myRes\n"
                          "Name\tSpecies\tWeight\n"
                          "Dobby\tRat\t350\n"
                          "Korzhik\tRat\t500\n\n");
    }

    Y_UNIT_TEST(Test12) {
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        auto query = kikimr.Query(
                    "("
                    "(let row '('('Id (Uint64 '1))))"
                    "(leg myUpd '("
                    "  '('Species (Utf8 '\"Rat\"))"
                    "  '('Name (Utf8 '\"Dobby\"))"
                    "  '('Weight (Int64 '350))))"
                    "(let pgmReturn (AsList"
                    "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                    "))"
                    "(return pgmReturn)"
                    ")");
        auto result = query.SyncExecute();
        auto status = result.GetStatus();
        UNIT_ASSERT(status == NMsgBusProxy::MSTATUS_ERROR);
        auto error = result.GetError();
        UNIT_ASSERT(error.Permanent());
        UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), "MP-0128");
        UNIT_ASSERT_STRING_CONTAINS(error.GetMessage(), "<main>:1:34: Error: expected either let, return or import");
    }

    Y_UNIT_TEST(Test13) {
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        auto query = kikimr.Query(
                    "("
                    "(let row '('('Id (Uint64 '1))))"
                    "(let myUpd '("
                    "  '('Species (Utf8 '\"Rat\"))"
                    "  '('Name (Utf8 '\"Dobby\"))"
                    "  '('Weight (Utf8 '350))))"
                    "(let pgmReturn (AsList"
                    "  (UpdateRow '('/dc-1/Zoo/Animals '0 '0:0) row myUpd)"
                    "))"
                    "(return pgmReturn)"
                    ")");
        auto result = query.SyncExecute();
        auto status = result.GetStatus();
        UNIT_ASSERT(status == NMsgBusProxy::MSTATUS_ERROR);
        auto error = result.GetError();
        UNIT_ASSERT(error.Permanent());
        UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), "MP-0128");
        UNIT_ASSERT_STRING_CONTAINS(error.GetMessage(), "Mismatch of column type expectedType = 3 actual type = 4608");
    }

    Y_UNIT_TEST(Test14) {
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        auto query = kikimr.Query(
                    "("
                    "(let row '('('Id (Uint64 '1))))"
                    "(let myUpd '("
                    "  '('Species (Utf8 '\"Rat\"))"
                    "  '('Name (Int64 '\"Dobby\"))"
                    "  '('Weight (Utf8 '350))))"
                    "(let pgmReturn (AsList"
                    "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                    "))"
                    "(return pgmReturn)"
                    ")");
        auto result = query.SyncExecute();
        auto status = result.GetStatus();
        UNIT_ASSERT(status == NMsgBusProxy::MSTATUS_ERROR);
        auto error = result.GetError();
        UNIT_ASSERT(error.Permanent());
        UNIT_ASSERT_VALUES_EQUAL(error.GetCode(), "MP-0128");
        UNIT_ASSERT_VALUES_EQUAL(error.GetMessage(), "<main>:1:142: Error: At function: AsList\n"
        "    <main>:1:77: Error: At function: UpdateRow\n"
        "        <main>:1:84: Error: At function: Int64\n"
        "            <main>:1:91: Error: Bad atom format for type: Int64, value: \"Dobby\"\n");
    }

    Y_UNIT_TEST(Test15) {
        TTestEnvironment env({
            NClient::TKeyPartitioningColumn("Id", NClient::TType::Uint64, 4),
            NClient::TColumn("Species", NClient::TType::Utf8),
            NClient::TColumn("Name", NClient::TType::Utf8),
            NClient::TColumn("Weight", NClient::TType::Int64)
        });
        NClient::TKikimr& kikimr = env.Kikimr;

        {
            auto result = kikimr.Query(
                        "("
                        "(let row '('('Id (Uint64 '1))))"
                        "(let myUpd '("
                        "  '('Species (Utf8 '\"Rat\"))"
                        "  '('Name (Utf8 '\"Dobby\"))"
                        "  '('Weight (Int64 '350))))"
                        "(let pgmReturn (AsList"
                        "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                        "))"
                        "(return pgmReturn)"
                        ")").SyncPrepare().GetQuery().SyncExecute();
        }

        {
            auto result = kikimr.Query(
                        "("
                        "(let row '('('Id (Uint64 '2))))"
                        "(let myUpd '("
                        "  '('Species (Utf8 '\"Rat\"))"
                        "  '('Name (Utf8 '\"Korzhik\"))"
                        "  '('Weight (Int64 '500))))"
                        "(let pgmReturn (AsList"
                        "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                        "))"
                        "(return pgmReturn)"
                        ")").SyncPrepare().GetQuery().SyncExecute();
        }

        auto query = kikimr.Query(
                    "("
                    "(let range '('IncFrom '('Id (Uint64 '1) (Void))))"
                    "(let select '('Species 'Name 'Weight))"
                    "(let options '())"
                    "(let pgmReturn (AsList"
                    "    (SetResult 'myRes (Member (SelectRange '/dc-1/Zoo/Animals range select options) 'List))"
                    "))"
                    "(return pgmReturn)"
                    ")");
        auto result = query.SyncExecute();


        UNIT_ASSERT_EQUAL(result.GetStatus(), NMsgBusProxy::MSTATUS_OK);

        auto value = result.GetValue();

        UNIT_ASSERT_VALUES_EQUAL(value["myRes"].Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"][0]["Species"], "Rat");
        UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"][0]["Name"], "Dobby");
        UNIT_ASSERT_VALUES_EQUAL((i64)value["myRes"][0]["Weight"], 350);
        UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"][1]["Species"], "Rat");
        UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"][1]["Name"], "Korzhik");
        UNIT_ASSERT_VALUES_EQUAL((i64)value["myRes"][1]["Weight"], 500);
    }

    Y_UNIT_TEST(Test16) {
        using namespace NClient;
        TTestEnvironment env({
            TKeyPartitioningColumn("Id", NClient::TType::Uint64, 4),
            TColumn("Species", NClient::TType::String),
            TColumn("Name", NClient::TType::String),
            TColumn("Weight", NClient::TType::Int64)
        });
        NClient::TKikimr& kikimr = env.Kikimr;

        {
            auto result = kikimr.Query(
                        "("
                        "(let row '('('Id (Uint64 '1))))"
                        "(let s (Parameter 'SPECIES (DataType 'String)))"
                        "(let n (Parameter 'NAME (DataType 'String)))"
                        "(let w (Parameter 'WEIGHT (DataType 'Int64)))"
                        "(let myUpd '("
                        "  '('Species s)"
                        "  '('Name n)"
                        "  '('Weight w)))"
                        "(let pgmReturn (AsList"
                        "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                        "))"
                        "(return pgmReturn)"
                        ")").SyncPrepare().GetQuery().SyncExecute(
                            TParameterValue<TString, NScheme::NTypeIds::String>("SPECIES", "Rat"),
                            TParameterValue<TString, NScheme::NTypeIds::String>("NAME", "Dobby"),
                            TParameter("WEIGHT", (i64)350)
                        );
            UNIT_ASSERT_EQUAL(result.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        }

        {
            auto result = kikimr.Query(
                        "("
                        "(let row '('('Id (Uint64 '2))))"
                        "(let s (Parameter 'SPECIES (DataType 'String)))"
                        "(let n (Parameter 'NAME (DataType 'String)))"
                        "(let w (Parameter 'WEIGHT (DataType 'Int64)))"
                        "(let myUpd '("
                        "  '('Species s)"
                        "  '('Name n)"
                        "  '('Weight w)))"
                        "(let pgmReturn (AsList"
                        "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                        "))"
                        "(return pgmReturn)"
                        ")").SyncPrepare().GetQuery().SyncExecute(
                            TParameterValue<TString, NScheme::NTypeIds::String>("SPECIES", "Rat"),
                            TParameterValue<TString, NScheme::NTypeIds::String>("NAME", "Korzhik"),
                            TParameter("WEIGHT", (i64)500)
                        );
            UNIT_ASSERT_EQUAL(result.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        }

        auto query = kikimr.Query(
                    "("
                    "(let range '('IncFrom '('Id (Uint64 '1) (Void))))"
                    "(let select '('Species 'Name 'Weight))"
                    "(let options '())"
                    "(let pgmReturn (AsList"
                    "    (SetResult 'myRes (Member (SelectRange '/dc-1/Zoo/Animals range select options) 'List))"
                    "))"
                    "(return pgmReturn)"
                    ")");
        auto result = query.SyncExecute();


        UNIT_ASSERT_EQUAL(result.GetStatus(), NMsgBusProxy::MSTATUS_OK);

        auto value = result.GetValue();

        UNIT_ASSERT_VALUES_EQUAL(value["myRes"].Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"][0]["Species"], "Rat");
        UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"][0]["Name"], "Dobby");
        UNIT_ASSERT_VALUES_EQUAL((i64)value["myRes"][0]["Weight"], 350);
        UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"][1]["Species"], "Rat");
        UNIT_ASSERT_VALUES_EQUAL((TString)value["myRes"][1]["Name"], "Korzhik");
        UNIT_ASSERT_VALUES_EQUAL((i64)value["myRes"][1]["Weight"], 500);
    }

    struct TGenericParameterType {
        TGenericParameterType(const TString& name, const NKikimrMiniKQL::TParams& parameter)
            : Name(name)
            , Parameter(parameter)
        {
        }
        void StoreType(NKikimrMiniKQL::TStructType& type) const {
            auto& member = *type.AddMember();
            member.SetName(Name);
            member.MutableType()->CopyFrom(Parameter.GetType());
        }
        void StoreValue(NKikimrMiniKQL::TValue& value) const {
            value.CopyFrom(Parameter.GetValue());
        }
        private:
        TString Name;
        NKikimrMiniKQL::TParams Parameter;
    };

    NKikimrMiniKQL::TParams CreateShardRangeStruct() {
        NKikimrMiniKQL::TParams shardRange;
        {
            //set type
            shardRange.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Struct);
            auto begin = shardRange.MutableType()->MutableStruct()->AddMember();
            begin->SetName("Begin");
            begin->MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
            begin->MutableType()->MutableData()->SetScheme(NScheme::NTypeIds::Uint32);
            auto end = shardRange.MutableType()->MutableStruct()->AddMember();
            end->SetName("End");
            end->MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
            end->MutableType()->MutableData()->SetScheme(NScheme::NTypeIds::Uint32);
        }
        {
            //set value
            shardRange.MutableValue()->AddStruct()->SetUint32(1);
            shardRange.MutableValue()->AddStruct()->SetUint32(2);
        }
        return shardRange;
    }

    Y_UNIT_TEST(Test17) {
        using namespace NClient;
        TTestEnvironment env({
            TKeyPartitioningColumn("Id", NClient::TType::Uint64, 4),
            TColumn("Species", NClient::TType::String),
            TColumn("Name", NClient::TType::String),
            TColumn("Weight", NClient::TType::Int64)
        });
        NClient::TKikimr& kikimr = env.Kikimr;

        {
            auto result = kikimr.Query(
                        "("
                        "(let row '('('Id (Uint64 '1))))"
                        "(let s (Parameter 'SPECIES (DataType 'String)))"
                        "(let n (Parameter 'NAME (DataType 'String)))"
                        "(let w (Parameter 'WEIGHT (DataType 'Int64)))"
                        "(let myUpd '("
                        "  '('Species s)"
                        "  '('Name n)"
                        "  '('Weight w)))"
                        "(let pgmReturn (AsList"
                        "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                        "))"
                        "(return pgmReturn)"
                        ")").SyncPrepare().GetQuery().SyncExecute(
                            TGenericParameterType("Shard", CreateShardRangeStruct())
                        );
            UNIT_ASSERT_EQUAL(result.GetStatus(), NMsgBusProxy::MSTATUS_ERROR);
        }
    }

    Y_UNIT_TEST(Test18) {
        using namespace NClient;
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        auto updateQuery = kikimr.Query(
                    "(\n"
                    "(let id (Parameter 'ID (DataType 'Uint64)))\n"
                    "(let row '('('Id id)))\n"
                    "(let an_type (StructType '('SPECIES (DataType 'Utf8)) '('NAME (DataType 'Utf8)) '('WEIGHT (DataType 'Int64))))\n"
                    "(let an (Parameter 'ANIMAL an_type))\n"
                    "(let sp (Member an 'SPECIES))\n"
                    "(let nm (Member an 'NAME))\n"
                    "(let wt (Member an 'WEIGHT))\n"
                    "(let myUpd '(\n"
                    "  '('Species sp)\n"
                    "  '('Name nm)\n"
                    "  '('Weight wt)))\n"
                    "(let pgmReturn (AsList\n"
                    "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)\n"
                    "))\n"
                    "(return pgmReturn)\n"
                    ")\n").SyncPrepare().GetQuery();


        updateQuery.SyncExecute(
                                TParameter("ID", (ui64)1),
                                TParameter("ANIMAL", TStruct(
                                    TParameter("SPECIES", "Rat"),
                                    TParameter("NAME", "Dobby"),
                                    TParameter("WEIGHT", (i64)350)
                                    ))
                            );

        updateQuery.SyncExecute(
                                TParameter("ID", (ui64)2),
                                TParameter("ANIMAL", TStruct(
                                    TParameter("SPECIES", "Rat"),
                                    TParameter("NAME", "Korzhik"),
                                    TParameter("WEIGHT", (i64)500)
                                    ))
                            );

        auto selectQuery = kikimr.Query(
                    "("
                    "(let range '('IncFrom '('Id (Uint64 '1) (Void))))"
                    "(let select '('Species 'Name 'Weight))"
                    "(let options '())"
                    "(let pgmReturn (AsList"
                    "    (SetResult 'myRes (Member (SelectRange '/dc-1/Zoo/Animals range select options) 'List))"
                    "))"
                    "(return pgmReturn)"
                    ")").SyncPrepare().GetQuery();

        auto result = selectQuery.SyncExecute();
        auto value = result.GetValue();

        UNIT_ASSERT_VALUES_EQUAL(value.GetValueText<TFormatJSON>(),
                          "{\"myRes\": [{\"Name\": \"Dobby\", \"Species\": \"Rat\", \"Weight\": 350}, {\"Name\": \"Korzhik\", \"Species\": \"Rat\", \"Weight\": 500}]}");
    }

    Y_UNIT_TEST(Test19) {
        using namespace NClient;
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        auto updateQuery = kikimr.Query(
                    "(\n"
                    "(let id (Parameter 'ID (DataType 'Uint64)))\n"
                    "(let row '('('Id id)))\n"
                    "(let an_type (StructType '('SPECIES (DataType 'Utf8)) '('NAME (DataType 'Utf8)) '('WEIGHT (DataType 'Int64))))\n"
                    "(let an (Parameter 'ANIMAL an_type))\n"
                    "(let sp (Member an 'SPECIES))\n"
                    "(let nm (Member an 'NAME))\n"
                    "(let wt (Member an 'WEIGHT))\n"
                    "(let myUpd '(\n"
                    "  '('Species sp)\n"
                    "  '('Name nm)\n"
                    "  '('Weight wt)))\n"
                    "(let pgmReturn (AsList\n"
                    "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)\n"
                    "))\n"
                    "(return pgmReturn)\n"
                    ")\n").SyncPrepare().GetQuery();

        TParameters parameters;

        parameters["ID"] = (ui64)1;
        parameters["ANIMAL"]["SPECIES"] = "Rat";
        parameters["ANIMAL"]["NAME"] = "Dobby";
        parameters["ANIMAL"]["WEIGHT"] = (i64)350;

        updateQuery.SyncExecute(parameters);

        parameters["ID"] = (ui64)2;
        parameters["ANIMAL"]["NAME"] = "Korzhik";
        parameters["ANIMAL"]["WEIGHT"] = (i64)500;

        updateQuery.SyncExecute(parameters);

        auto selectQuery = kikimr.Query(
                    "("
                    "(let range '('IncFrom '('Id (Uint64 '1) (Void))))"
                    "(let select '('Species 'Name 'Weight))"
                    "(let options '())"
                    "(let pgmReturn (AsList"
                    "    (SetResult 'myRes (Member (SelectRange '/dc-1/Zoo/Animals range select options) 'List))"
                    "))"
                    "(return pgmReturn)"
                    ")").SyncPrepare().GetQuery();

        auto result = selectQuery.SyncExecute();
        auto value = result.GetValue();

        UNIT_ASSERT_VALUES_EQUAL(value.GetValueText<TFormatJSON>(),
                          "{\"myRes\": [{\"Name\": \"Dobby\", \"Species\": \"Rat\", \"Weight\": 350}, {\"Name\": \"Korzhik\", \"Species\": \"Rat\", \"Weight\": 500}]}");
    }

    Y_UNIT_TEST(Test20) {
        using namespace NClient;
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        auto updateQuery = kikimr.Query(R"___(
                                        (
                                        (let id (Parameter 'ID (DataType 'Uint64)))
                                        (let row '('('Id id)))
                                        (let an_type (StructType '('SPECIES (DataType 'Utf8)) '('NAME (DataType 'Utf8)) '('WEIGHT (DataType 'Int64))))
                                        (let an_opt_type (OptionalType an_type))
                                        (let an_opt (Parameter 'ANIMAL an_opt_type))
                                        (let ret
                                            (AsList
                                                (Coalesce
                                                    (Map an_opt
                                                        (lambda '(an) (
                                                            block '(
                                                                (let sp (Member an 'SPECIES))
                                                                (let nm (Member an 'NAME))
                                                                (let wt (Member an 'WEIGHT))
                                                                (let myUpd '(
                                                                  '('Species sp)
                                                                  '('Name nm)
                                                                  '('Weight wt)))
                                                                (return (UpdateRow '/dc-1/Zoo/Animals row myUpd))
                                                            )
                                                        ))
                                                    )
                                                    (Void)
                                                )
                                            )
                                        )
                                        (return ret)
                                        )
                                        )___").SyncPrepare().GetQuery();

        updateQuery.SyncExecute(
                                TParameter("ID", (ui64)1),
                                TParameter("ANIMAL", TOptional(
                                    TStruct(
                                        TParameter("SPECIES", "Rat"),
                                        TParameter("NAME", "Dobby"),
                                        TParameter("WEIGHT", (i64)350)
                                        )))
                            );

        updateQuery.SyncExecute(
                                TParameter("ID", (ui64)1),
                                TParameter("ANIMAL", TEmptyOptional(
                                    TStruct(
                                       TParameter("SPECIES", TString()),
                                       TParameter("NAME", TString()),
                                       TParameter("WEIGHT", i64())
                                       )))
                            );

        updateQuery.SyncExecute(
                                TParameter("ID", (ui64)2),
                                TParameter("ANIMAL", TOptional(
                                    TStruct(
                                        TParameter("SPECIES", "Rat"),
                                        TParameter("NAME", "Korzhik"),
                                        TParameter("WEIGHT", (i64)500)
                                        )))
                            );

        auto selectQuery = kikimr.Query(
                    "("
                    "(let range '('IncFrom '('Id (Uint64 '1) (Void))))"
                    "(let select '('Species 'Name 'Weight))"
                    "(let options '())"
                    "(let pgmReturn (AsList"
                    "    (SetResult 'myRes (Member (SelectRange '/dc-1/Zoo/Animals range select options) 'List))"
                    "))"
                    "(return pgmReturn)"
                    ")").SyncPrepare().GetQuery();

        auto result = selectQuery.SyncExecute();
        auto value = result.GetValue();
        UNIT_ASSERT_VALUES_EQUAL(value.GetValueText<TFormatJSON>(),
                          "{\"myRes\": [{\"Name\": \"Dobby\", \"Species\": \"Rat\", \"Weight\": 350}, {\"Name\": \"Korzhik\", \"Species\": \"Rat\", \"Weight\": 500}]}");
    }

    Y_UNIT_TEST(Test21) {
        using namespace NClient;
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        auto updateQuery = kikimr.Query(R"___(
                                        (
                                        (let id (Parameter 'ID (DataType 'Uint64)))
                                        (let row '('('Id id)))
                                        (let an_type (StructType '('SPECIES (DataType 'Utf8)) '('NAME (DataType 'Utf8)) '('WEIGHT (DataType 'Int64))))
                                        (let an_opt_type (OptionalType an_type))
                                        (let an_opt (Parameter 'ANIMAL an_opt_type))
                                        (let ret
                                            (AsList
                                                (Coalesce
                                                    (Map an_opt
                                                        (lambda '(an) (
                                                            block '(
                                                                (let sp (Member an 'SPECIES))
                                                                (let nm (Member an 'NAME))
                                                                (let wt (Member an 'WEIGHT))
                                                                (let myUpd '(
                                                                  '('Species sp)
                                                                  '('Name nm)
                                                                  '('Weight wt)))
                                                                (return (UpdateRow '/dc-1/Zoo/Animals row myUpd))
                                                            )
                                                        ))
                                                    )
                                                    (Void)
                                                )
                                            )
                                        )
                                        (return ret)
                                        )
                                        )___").SyncPrepare().GetQuery();

        TParameters parameters;

        parameters["ID"] = (ui64)1;
        parameters["ANIMAL"].Optional()["SPECIES"] = "Rat";
        parameters["ANIMAL"].Optional()["NAME"] = "Dobby";
        parameters["ANIMAL"].Optional()["WEIGHT"] = (i64)350;

        updateQuery.SyncExecute(parameters);

        parameters["ID"] = (ui64)2;
        parameters["ANIMAL"].Optional()["NAME"] = "Korzhik";
        parameters["ANIMAL"].Optional()["WEIGHT"] = (i64)500;

        updateQuery.SyncExecute(parameters);

        auto selectQuery = kikimr.Query(
                    "("
                    "(let range '('IncFrom '('Id (Uint64 '1) (Void))))"
                    "(let select '('Species 'Name 'Weight))"
                    "(let options '())"
                    "(let pgmReturn (AsList"
                    "    (SetResult 'myRes (Member (SelectRange '/dc-1/Zoo/Animals range select options) 'List))"
                    "))"
                    "(return pgmReturn)"
                    ")").SyncPrepare().GetQuery();

        auto result = selectQuery.SyncExecute();
        auto value = result.GetValue();

        UNIT_ASSERT_VALUES_EQUAL(value.GetValueText<TFormatJSON>(),
                          "{\"myRes\": [{\"Name\": \"Dobby\", \"Species\": \"Rat\", \"Weight\": 350}, {\"Name\": \"Korzhik\", \"Species\": \"Rat\", \"Weight\": 500}]}");
    }

    Y_UNIT_TEST(Test22) {
        using namespace NClient;
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        auto updateQuery = kikimr.Query(R"___(
                                        (
                                        (let an_type (StructType '('ID (DataType 'Uint64)) '('SPECIES (DataType 'Utf8)) '('NAME (DataType 'Utf8)) '('WEIGHT (DataType 'Int64))))
                                        (let an_lst_type (ListType an_type))
                                        (let an_lst (Parameter 'ANIMALS an_lst_type))
                                        (let ret
                                            (Map an_lst
                                                (lambda '(an) (
                                                    block '(
                                                        (let id (Member an 'ID))
                                                        (let row '('('Id id)))
                                                        (let sp (Member an 'SPECIES))
                                                        (let nm (Member an 'NAME))
                                                        (let wt (Member an 'WEIGHT))
                                                        (let myUpd '(
                                                          '('Species sp)
                                                          '('Name nm)
                                                          '('Weight wt)))
                                                        (return (UpdateRow '/dc-1/Zoo/Animals row myUpd))
                                                    )
                                                ))
                                            )
                                        )
                                        (return ret)
                                        )
                                        )___").SyncPrepare().GetQuery();

        TParameters parameters;

        {
            auto animal = parameters["ANIMALS"].AddListItem();
            animal["ID"] = (ui64)1;
            animal["SPECIES"] = "Rat";
            animal["NAME"] = "Dobby";
            animal["WEIGHT"] = (i64)350;
        }



        {
            auto animal = parameters["ANIMALS"].AddListItem();
            animal["ID"] = (ui64)2;
            animal["SPECIES"] = "Rat";
            animal["NAME"] = "Korzhik";
            animal["WEIGHT"] = (i64)500;
        }

        updateQuery.SyncExecute(parameters);

        auto selectQuery = kikimr.Query(
                    "("
                    "(let range '('IncFrom '('Id (Uint64 '1) (Void))))"
                    "(let select '('Species 'Name 'Weight))"
                    "(let options '())"
                    "(let pgmReturn (AsList"
                    "    (SetResult 'myRes (Member (SelectRange '/dc-1/Zoo/Animals range select options) 'List))"
                    "))"
                    "(return pgmReturn)"
                    ")").SyncPrepare().GetQuery();

        auto result = selectQuery.SyncExecute();
        auto value = result.GetValue();

        UNIT_ASSERT_VALUES_EQUAL(value.GetValueText<TFormatJSON>(),
                          "{\"myRes\": [{\"Name\": \"Dobby\", \"Species\": \"Rat\", \"Weight\": 350}, {\"Name\": \"Korzhik\", \"Species\": \"Rat\", \"Weight\": 500}]}");
    }

    Y_UNIT_TEST(Test24) {
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        auto query = kikimr.Query(
                    "("
                    "(let row '('('Id (Uint64 '1))))"
                    "(let myUpd '("
                    "  '('Species (Utf8 '\"Rat\"))"
                    "  '('Name (Utf8 '\"Dobby\"))"
                    "  '('Weight (Int64 '350))))"
                    "(let pgmReturn (AsList"
                    "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                    "))"
                    "(return pgmReturn)"
                    ")");
        auto result = query.SyncPrepare().GetQuery().SyncExecute();
        auto value = result.GetValue();
        UNIT_ASSERT(value.IsNull());
    }

    Y_UNIT_TEST(Test25) {
        TTestEnvironment env;
        NClient::TKikimr& kikimr = env.Kikimr;

        auto query = kikimr.Query(
                    "("
                    "(let row '('('Id (Uint64 '1))))"
                    "(let myUpd '("
                    "  '('Species (Utf8 '\"Rat\"))"
                    "  '('Name (Utf8 '\"Dobby\"))"
                    "  '('Weight (Int64 '350))))"
                    "(let pgmReturn (AsList"
                    "  (UpdateRow '/dc-1/Zoo/Animals row myUpd)"
                    "))"
                    "(return pgmReturn)"
                    ")");
        auto unbindedQuery = query.SyncPrepare().GetQuery().Unbind();
        auto result = kikimr.Query(unbindedQuery).SyncExecute();
        auto value = result.GetValue();
        UNIT_ASSERT(value.IsNull());
    }

    Y_UNIT_TEST(SameTableDifferentColumns) {
        using namespace NClient;
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        auto dc = kikimr.GetSchemaRoot("dc-1");
        auto example = dc.MakeDirectory("deduper");
        example.CreateTable("Groups", {
                                TKeyColumn("ShardId", NClient::TType::Uint32),
                                TKeyColumn("HostTitleHash", NClient::TType::Uint64),
                                TKeyColumn("GroupSimHash", NClient::TType::Uint64),
                                TKeyColumn("Rank", NClient::TType::Uint32),
                                TKeyColumn("UrlHash", NClient::TType::Uint32)
                            });

        auto updateQuery = kikimr.Query(R"___(
                                        (
                                        (let shardId (Parameter 'SHARDID (DataType 'Uint32)))
                                        (let hostTitleHash (Parameter 'HOSTTITLEHASH (DataType 'Uint64)))
                                        (let groupSimHash (Parameter 'GROUPSIMHASH (DataType 'Uint64)))
                                        (let rank (Parameter 'RANK (DataType 'Uint32)))
                                        (let urlHash (Parameter 'URLHASH (DataType 'Uint32)))
                                        (let key '(
                                            '('ShardId shardId)
                                            '('HostTitleHash hostTitleHash)
                                            '('GroupSimHash groupSimHash)
                                            '('Rank rank)
                                            '('UrlHash urlHash)
                                        ))
                                        (let value '())
                                        (let pgmReturn (AsList
                                            (UpdateRow '/dc-1/deduper/Groups key value)
                                        ))
                                        (return pgmReturn)
                                        )
                                        )___").SyncPrepare().GetQuery();

        {
            auto result = updateQuery.SyncExecute(
                            TParameter("SHARDID", (ui32)0),
                            TParameter("HOSTTITLEHASH", (ui64)1111),
                            TParameter("GROUPSIMHASH", (ui64)2222),
                            TParameter("RANK", (ui32)1),
                            TParameter("URLHASH", (ui32)424242)
                            );
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NMsgBusProxy::MSTATUS_OK,  result.GetError().GetMessage().c_str());
        }

        {
            auto result = updateQuery.SyncExecute(
                            TParameter("SHARDID", (ui32)0),
                            TParameter("HOSTTITLEHASH", (ui64)1111),
                            TParameter("GROUPSIMHASH", (ui64)2223),
                            TParameter("RANK", (ui32)0),
                            TParameter("URLHASH", (ui32)333333)
                            );
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NMsgBusProxy::MSTATUS_OK,  result.GetError().GetMessage().c_str());
        }

        {
            // The query does SelecRow and SelectRange from the same table but with different sets
            // of key columns and different selected columns
            auto rawq = kikimr.Query(R"___(
                                  (
                                  (let shardId (Parameter 'SHARDID (DataType 'Uint32)))
                                  (let hostTitleHash (Parameter 'HOSTTITLEHASH (DataType 'Uint64)))
                                  (let groupSimHash (Parameter 'GROUPSIMHASH (DataType 'Uint64)))
                                  (let rank (Parameter 'RANK (DataType 'Uint32)))
                                  (let urlHash (Parameter 'URLHASH (DataType 'Uint32)))
                                  (let key1 '('('ShardId shardId)
                                              '('HostTitleHash hostTitleHash)
                                              '('GroupSimHash groupSimHash)
                                              '('Rank rank)
                                              '('UrlHash urlHash)))
                                  (let column1 '('UrlHash))
                                  (let range '('ExcFrom 'IncTo '('ShardId shardId (Void))
                                                               '('HostTitleHash hostTitleHash (Void))
                                                               '('GroupSimHash groupSimHash (Void))
                                  ))
                                  (let column2 '('UrlHash))
                                  (let options '('('ItemsLimit (Uint64 '1))))
                                  # SelectRow by the full key (5 columns) with 1 key column in the result
                                  (let res1 (SelectRow 'dc-1/deduper/Groups key1 column1))
                                  # SelectRange by 3 key columns and 1 key column in the result
                                  (let res2 (SelectRange 'dc-1/deduper/Groups range column2 options))
                                  (let result (SetResult 'result (AsStruct '('rowResult res1) '('rangeResult res2))))
                                  (let pgmReturn (AsList result))
                                  (return pgmReturn)
                                  )
                                  )___");
            auto q = rawq.SyncPrepare();
            UNIT_ASSERT_VALUES_EQUAL_C(q.GetStatus(), NMsgBusProxy::MSTATUS_OK,  q.GetError().GetMessage().c_str());

            auto query = q.GetQuery();
            auto result = query.SyncExecute(
                    TParameter("SHARDID", (ui32)0),
                    TParameter("HOSTTITLEHASH", (ui64)1111),
                    TParameter("GROUPSIMHASH", (ui64)2222),
                    TParameter("RANK", (ui32)1),
                    TParameter("URLHASH", (ui32)424242)
                    );
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NMsgBusProxy::MSTATUS_OK,  result.GetError().GetMessage().c_str());
            // result.GetValue().DumpValue();
        }
    }

    void CheckSelectRange(NClient::TKikimr& kikimr,
                          ui32 key1Start, ui32 key1End,
                          bool includeFrom, bool includeTo,
                          const TVector<std::pair<ui32, ui32>>& expectedKeys)
    {
        using namespace NClient;

        Y_UNUSED(includeFrom);
        Y_UNUSED(includeTo);

        auto rawq = kikimr.Query(
                    Sprintf(R"___(
                              (
                              (let key1Start (Parameter 'KEY1START (DataType 'Uint32)))
                              (let key1End (Parameter 'KEY1END (DataType 'Uint32)))
                              (let range '('%s '%s '('Key1 key1Start key1End)))
                              (let column '('Key1 'Key2))
                              (let options '())
                              (let readRes (SelectRange 'dc-1/test/Table range column options))
                              (let result (SetResult 'readRes readRes))
                              (let pgmReturn (AsList result))
                              (return pgmReturn)
                              )
                              )___",
                              "ExcFrom" /* incomplete From is always non-inclusive */,
                              "IncTo" /* incomplete To is always inclusive */
                    ));
        auto q = rawq.SyncPrepare();
        UNIT_ASSERT_VALUES_EQUAL_C(q.GetStatus(), NMsgBusProxy::MSTATUS_OK,  q.GetError().GetMessage().c_str());

        auto query = q.GetQuery();
        auto result = query.SyncExecute(
                    TParameter("KEY1START", key1Start),
                    TParameter("KEY1END", key1End)
                );
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NMsgBusProxy::MSTATUS_OK,  result.GetError().GetMessage().c_str());
        // result.GetValue().DumpValue();
        auto valueResult = result.GetValue();
        ui32 sz = valueResult["readRes"]["List"].Size();
        UNIT_ASSERT_VALUES_EQUAL(sz, expectedKeys.size());
        for (ui32 i = 0; i < sz ; ++i) {
            ui32 actualKey1 = (ui32)valueResult["readRes"]["List"][i]["Key1"];
            ui64 actualKey2 = (ui64)valueResult["readRes"]["List"][i]["Key2"];

            UNIT_ASSERT_VALUES_EQUAL(actualKey1, expectedKeys[i].first);
            UNIT_ASSERT_VALUES_EQUAL(actualKey2, expectedKeys[i].second);
        }
    }

    Y_UNIT_TEST(SelectRangeWithInf) {
        using namespace NClient;
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        auto dc = kikimr.GetSchemaRoot("dc-1");
        auto example = dc.MakeDirectory("test");
        example.CreateTable("Table", {
                                TKeyColumn("Key1", NClient::TType::Uint32),
                                TKeyColumn("Key2", NClient::TType::Uint64),
                                TColumn("Value", NClient::TType::Utf8)
                            });

        auto rawQuery = kikimr.Query(R"___(
                                        (
                                        (let key1 (Parameter 'KEY1 (DataType 'Uint32)))
                                        (let key2 (Parameter 'KEY2 (DataType 'Uint64)))
                                        (let val (Parameter 'VALUE (DataType 'Utf8)))
                                        (let key '(
                                            '('Key1 key1)
                                            '('Key2 key2)
                                        ))
                                        (let value '('('Value val)))
                                        (let pgmReturn (AsList
                                            (UpdateRow '/dc-1/test/Table key value)
                                        ))
                                        (return pgmReturn)
                                        )
                                        )___");
        auto updateQuery = rawQuery.SyncPrepare();
        UNIT_ASSERT_VALUES_EQUAL_C(updateQuery.GetStatus(), NMsgBusProxy::MSTATUS_OK,  updateQuery.GetError().GetMessage().c_str());

        // Write many rows in order to trigger compaction and create a flat part
        for (ui32 k1 = 0; k1 < 300; ++k1) {
            for (ui64 k2 = 0; k2 < 3; ++k2) {
                auto result = updateQuery.GetQuery().SyncExecute(
                            TParameter("KEY1", k1),
                            TParameter("KEY2", k2),
                            TParameter("VALUE", TString(2048, 'A'))
                            );
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NMsgBusProxy::MSTATUS_OK,  result.GetError().GetMessage().c_str());
            }
        }

        for (ui32 k1 = 0; k1 < 299; ++k1) {
            // from (k1, inf) to (k1+1, inf)
            TVector<std::pair<ui32, ui32>> expected = {{k1+1,0},{k1+1,1},{k1+1,2}};
            CheckSelectRange(kikimr, k1, k1+1, false, false, expected);
            CheckSelectRange(kikimr, k1, k1+1, false, true, expected);
            CheckSelectRange(kikimr, k1, k1+1, true, false, expected);
            CheckSelectRange(kikimr, k1, k1+1, true, true, expected);

            // from (k1, inf) to (k1, inf)
            CheckSelectRange(kikimr, k1, k1, false, false, {});
            CheckSelectRange(kikimr, k1, k1, false, true, {});
            CheckSelectRange(kikimr, k1, k1, true, false, {});
            CheckSelectRange(kikimr, k1, k1, true, true, {});

            // from (k1+1, inf) to (k1, inf)
            CheckSelectRange(kikimr, k1+1, k1, false, false, {});
            CheckSelectRange(kikimr, k1+1, k1, false, true, {});
            CheckSelectRange(kikimr, k1+1, k1, true, false, {});
            CheckSelectRange(kikimr, k1+1, k1, true, true, {});
        }
    }


    Y_UNIT_TEST(TypicalCase1) {
        using namespace NClient;
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        auto dc = kikimr.GetSchemaRoot("dc-1");
        auto example = dc.MakeDirectory("Example");
        example.CreateTable("Table1", {TKeyColumn("Hash", NClient::TType::Uint64), TColumn("Data", NClient::TType::Utf8)});
        example.CreateTable("Table2", {TKeyColumn("Hash", NClient::TType::Uint64), TColumn("Data", NClient::TType::Utf8)});
        example.CreateTable("Table3", {TKeyColumn("Hash", NClient::TType::Uint64), TColumn("Data", NClient::TType::Utf8)});
        auto query = kikimr.Query(R"___(
                                  # Check 3 tables and write if none of 3 rows already exists
                                  (
                                  (let h1 (Parameter 'H1_PARAM (DataType 'Uint64)))
                                  (let h2 (Parameter 'H2_PARAM (DataType 'Uint64)))
                                  (let h3 (Parameter 'H3_PARAM (DataType 'Uint64)))
                                  (let row1 '('('Hash h1)))
                                  (let row2 '('('Hash h2)))
                                  (let row3 '('('Hash h3)))
                                  (let select '('Hash))
                                  (let res1 (SelectRow '/dc-1/Example/Table1 row1 select))
                                  (let res2 (SelectRow '/dc-1/Example/Table2 row2 select))
                                  (let res3 (SelectRow '/dc-1/Example/Table3 row3 select))
                                  (let keyH1Exists (Exists res1))
                                  (let keyH2Exists (Exists res2))
                                  (let keyH3Exists (Exists res3))
                                  (let anyExists (Or keyH1Exists (Or keyH2Exists keyH3Exists)))
                                  (let pgmReturn (If anyExists
                                      (block '(
                                          (let list (List (ListType (VoidType))))
                                          (let list (Append list (SetResult 'anyExists anyExists)))
                                          (return list)
                                      ))
                                      (block '(
                                          (let list (List (ListType (VoidType))))
                                          (let list (Append list (UpdateRow '/dc-1/Example/Table1 row1 '())))
                                          (let list (Append list (UpdateRow '/dc-1/Example/Table2 row2 '())))
                                          (let list (Append list (UpdateRow '/dc-1/Example/Table3 row3 '())))
                                          (let list (Append list (SetResult 'anyExists anyExists)))
                                          (return list)
                                      ))
                                  ))
                                  (return pgmReturn)
                                  )
                                  )___").SyncPrepare().GetQuery();
        THashSet<ui64> values_h1;
        THashSet<ui64> values_h2;
        THashSet<ui64> values_h3;
        for (ui64 cnt = 0; cnt < 10; ++cnt) {
            ui64 H1 = 100 + cnt;
            ui64 H2 = 200 - cnt * 2;
            ui64 H3 = 300 - cnt * 3;
            bool expectedResult = (values_h1.count(H1) != 0) || (values_h2.count(H2) != 0) || (values_h3.count(H3) != 0);
            auto result = query.SyncExecute(
                              TParameter("H1_PARAM", H1),
                              TParameter("H2_PARAM", H2),
                              TParameter("H3_PARAM", H3)
                          );

            UNIT_ASSERT_EQUAL(result.GetStatus(), NMsgBusProxy::MSTATUS_OK);

            auto valueResult = result.GetValue();
            bool gotResult = valueResult["anyExists"];

            UNIT_ASSERT_VALUES_EQUAL(gotResult, expectedResult);

            if (!gotResult) {
                values_h1.insert(H1);
                values_h2.insert(H2);
                values_h3.insert(H3);
            }
        }
    }

    Y_UNIT_TEST(TypicalCase2) {
        using namespace NClient;
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        auto dc = kikimr.GetSchemaRoot("dc-1");
        auto example = dc.MakeDirectory("Example");

        example.CreateTable("Table1", {TKeyColumn("Hash", NClient::TType::Uint64), TColumn("Version", NClient::TType::Uint64), TColumn("Data", NClient::TType::Utf8)});
        example.CreateTable("Table2", {TKeyColumn("Hash", NClient::TType::Uint64), TColumn("Version", NClient::TType::Uint64), TColumn("Data", NClient::TType::Utf8)});
        example.CreateTable("Table3", {TKeyColumn("Hash", NClient::TType::Uint64), TColumn("Version", NClient::TType::Uint64), TColumn("Data", NClient::TType::Utf8)});

        auto readQuery = kikimr.Query(R"___(
                                      (
                                      (let h1 (Parameter 'H1_PARAM (DataType 'Uint64)))
                                      (let h2 (Parameter 'H2_PARAM (DataType 'Uint64)))
                                      (let h3 (Parameter 'H3_PARAM (DataType 'Uint64)))

                                      # Read data and versions from the DB
                                      (let row1 '('('Hash h1)))
                                      (let row2 '('('Hash h2)))
                                      (let row3 '('('Hash h3)))
                                      (let select '('Hash 'Version 'Data))
                                      (let res1 (SelectRow '/dc-1/Example/Table1 row1 select))
                                      (let res2 (SelectRow '/dc-1/Example/Table2 row2 select))
                                      (let res3 (SelectRow '/dc-1/Example/Table3 row3 select))

                                      (let pgmReturn (List (ListType (VoidType))))
                                      (let pgmReturn (Append pgmReturn (SetResult 'H1 res1)))
                                      (let pgmReturn (Append pgmReturn (SetResult 'H2 res2)))
                                      (let pgmReturn (Append pgmReturn (SetResult 'H3 res3)))

                                      (return pgmReturn)
                                      )
                                      )___").SyncPrepare().GetQuery();

        auto updateQuery = kikimr.Query(R"___(
                                        # Check 3 tables and write if none of 3 row versions have changed since they were read
                                        (
                                        # Helper function to extract column value and substitute non-existing
                                        # row or NULL value with the provided default value
                                        (let ExtractVal (lambda '(res name defaultVal) (block '(
                                                (let e1 (IfPresent res
                                                    (lambda '(r) (block '(
                                                       (return (Member r name))
                                                    )))
                                                    (block '(
                                                       (return (Just defaultVal))
                                                    ))
                                                ))
                                                (let e2 (Coalesce e1 defaultVal))
                                                (return e2)
                                            ))
                                        ))

                                        (let h1 (Parameter 'H1_PARAM (DataType 'Uint64)))
                                        (let h2 (Parameter 'H2_PARAM (DataType 'Uint64)))
                                        (let h3 (Parameter 'H3_PARAM (DataType 'Uint64)))

                                        (let d1 (Parameter 'D1_PARAM (DataType 'Utf8)))
                                        (let d2 (Parameter 'D2_PARAM (DataType 'Utf8)))
                                        (let d3 (Parameter 'D3_PARAM (DataType 'Utf8)))

                                        # Read versions from the DB
                                        (let row1 '('('Hash h1)))
                                        (let row2 '('('Hash h2)))
                                        (let row3 '('('Hash h3)))
                                        (let select '('Version))
                                        (let res1 (SelectRow '/dc-1/Example/Table1 row1 select))
                                        (let res2 (SelectRow '/dc-1/Example/Table2 row2 select))
                                        (let res3 (SelectRow '/dc-1/Example/Table3 row3 select))
                                        (let rv1 (Apply ExtractVal res1 'Version (Uint64 '0)))
                                        (let rv2 (Apply ExtractVal res2 'Version (Uint64 '0)))
                                        (let rv3 (Apply ExtractVal res3 'Version (Uint64 '0)))

                                        # Get old versions from the parameters
                                        (let v1 (Parameter 'V1_PARAM (DataType 'Uint64)))
                                        (let v2 (Parameter 'V2_PARAM (DataType 'Uint64)))
                                        (let v3 (Parameter 'V3_PARAM (DataType 'Uint64)))
                                        (let v1 (Coalesce v1 (Uint64 '0)))
                                        (let v2 (Coalesce v2 (Uint64 '0)))
                                        (let v3 (Coalesce v3 (Uint64 '0)))

                                        ### Check versions ###
                                        (let predicate (Bool 'True))
                                        (let predicate (And predicate (Equal rv1 v1)))
                                        (let predicate (And predicate (Equal rv2 v2)))
                                        (let predicate (And predicate (Equal rv2 v3)))


                                        ### If versions are not changed -- do writes ###
                                        (let pgmReturn (If predicate
                                            (block '(
                                            (let list (List (ListType (VoidType))))
                                                (let list (Append list (UpdateRow '/dc-1/Example/Table1 row1 '('('Version (Increment v1))'('Data d1)))))
                                                (let list (Append list (UpdateRow '/dc-1/Example/Table2 row2 '('('Version (Increment v2))'('Data d2)))))
                                                (let list (Append list (UpdateRow '/dc-1/Example/Table3 row3 '('('Version (Increment v3))'('Data d3)))))
                                                (let list (Append list (SetResult 'myRes (Utf8 '"Updated"))))
                                                (return list)
                                            ))
                                            (block '(
                                                (let emptyList (List (ListType (VoidType))))
                                                (let emptyList (Append emptyList (SetResult 'myRes (Utf8 '"Version mismatch"))))
                                                (return emptyList)
                                            ))
                                        ))
                                        ######

                                        ## Output some debug values
                                        (let pgmReturn (Append pgmReturn (SetResult 'rv1 rv1)))
                                        (let pgmReturn (Append pgmReturn (SetResult 'v1 v1)))

                                        (return pgmReturn)
                                        )
                                        )___").SyncPrepare().GetQuery();

        for (ui64 cnt = 0; cnt < 10; ++cnt) {
            ui64 H1 = 100 + cnt;
            ui64 H2 = 200 - cnt * 2;
            ui64 H3 = 300 - cnt * 3;
            auto result = readQuery.SyncExecute(
                              TParameter("H1_PARAM", H1),
                              TParameter("H2_PARAM", H2),
                              TParameter("H3_PARAM", H3)
                          );
            auto valueResult = result.GetValue();
            auto V1 = valueResult["H1"]["Version"];
            auto V2 = valueResult["H2"]["Version"];
            auto V3 = valueResult["H3"]["Version"];
            result = updateQuery.SyncExecute(
                                             TParameter("H1_PARAM", H1),
                                             TParameter("H2_PARAM", H2),
                                             TParameter("H3_PARAM", H3),
                                             TParameter("V1_PARAM", V1.IsNull() ? (ui64)0 : V1),
                                             TParameter("V2_PARAM", V2.IsNull() ? (ui64)0 : V2),
                                             TParameter("V3_PARAM", V3.IsNull() ? (ui64)0 : V3),
                                             TParameter("D1_PARAM", "data" + ToString(H1)),
                                             TParameter("D2_PARAM", "data" + ToString(H2)),
                                             TParameter("D3_PARAM", "data" + ToString(H3))
                                         );
            UNIT_ASSERT_EQUAL(result.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        }
    }

    Y_UNIT_TEST(TypicalCase2A) {
        using namespace NClient;
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        auto dc = kikimr.GetSchemaRoot("dc-1");
        auto example = dc.MakeDirectory("Example");

        example.CreateTable("Table1", {TKeyColumn("Hash", NClient::TType::Uint64), TColumn("Version", NClient::TType::Uint64), TColumn("Data", NClient::TType::String)});
        example.CreateTable("Table2", {TKeyColumn("Hash", NClient::TType::Uint64), TColumn("Version", NClient::TType::Uint64), TColumn("Data", NClient::TType::String)});
        example.CreateTable("Table3", {TKeyColumn("Hash", NClient::TType::Uint64), TColumn("Version", NClient::TType::Uint64), TColumn("Data", NClient::TType::String)});

        auto readQuery = kikimr.Query(R"___(
                                      (
                                      (let h1 (Parameter 'H1_PARAM (DataType 'Uint64)))
                                      (let h2 (Parameter 'H2_PARAM (DataType 'Uint64)))
                                      (let h3 (Parameter 'H3_PARAM (DataType 'Uint64)))

                                      # Read data and versions from the DB
                                      (let row1 '('('Hash h1)))
                                      (let row2 '('('Hash h2)))
                                      (let row3 '('('Hash h3)))
                                      (let select '('Hash 'Version 'Data))
                                      (let res1 (SelectRow '/dc-1/Example/Table1 row1 select))
                                      (let res2 (SelectRow '/dc-1/Example/Table2 row2 select))
                                      (let res3 (SelectRow '/dc-1/Example/Table3 row3 select))

                                      (let pgmReturn (List (ListType (VoidType))))
                                      (let pgmReturn (Append pgmReturn (SetResult 'H1 res1)))
                                      (let pgmReturn (Append pgmReturn (SetResult 'H2 res2)))
                                      (let pgmReturn (Append pgmReturn (SetResult 'H3 res3)))

                                      (return pgmReturn)
                                      )
                                      )___").SyncPrepare().GetQuery();

        auto updateQuery = kikimr.Query(R"___(
                                        # Check 3 tables and write iff none of 3 row versions have changed since they were read
                                        (
                                        # Helper function to extract column value and substitute non-existing
                                        # row or NULL value with the provided default value
                                        (let ExtractVal (lambda '(res name defaultVal) (block '(
                                                (let e1 (IfPresent res
                                                    (lambda '(r) (block '(
                                                       (return (Member r name))
                                                    )))
                                                    (block '(
                                                       (return (Just defaultVal))
                                                    ))
                                                ))
                                                (let e2 (Coalesce e1 defaultVal))
                                                (return e2)
                                            ))
                                        ))

                                        (let h1 (Parameter 'H1_PARAM (DataType 'Uint64)))
                                        (let h2 (Parameter 'H2_PARAM (DataType 'Uint64)))
                                        (let h3 (Parameter 'H3_PARAM (DataType 'Uint64)))

                                        (let d1 (Parameter 'D1_PARAM (DataType 'String)))
                                        (let d2 (Parameter 'D2_PARAM (DataType 'String)))
                                        (let d3 (Parameter 'D3_PARAM (DataType 'String)))

                                        # Read versions from the DB
                                        (let row1 '('('Hash h1)))
                                        (let row2 '('('Hash h2)))
                                        (let row3 '('('Hash h3)))
                                        (let select '('Version))
                                        (let res1 (SelectRow '/dc-1/Example/Table1 row1 select))
                                        (let res2 (SelectRow '/dc-1/Example/Table2 row2 select))
                                        (let res3 (SelectRow '/dc-1/Example/Table3 row3 select))
                                        (let rv1 (Apply ExtractVal res1 'Version (Uint64 '0)))
                                        (let rv2 (Apply ExtractVal res2 'Version (Uint64 '0)))
                                        (let rv3 (Apply ExtractVal res3 'Version (Uint64 '0)))

                                        # Get old versions from the parameters
                                        (let v1 (Parameter 'V1_PARAM (DataType 'Uint64)))
                                        (let v2 (Parameter 'V2_PARAM (DataType 'Uint64)))
                                        (let v3 (Parameter 'V3_PARAM (DataType 'Uint64)))
                                        (let v1 (Coalesce v1 (Uint64 '0)))
                                        (let v2 (Coalesce v2 (Uint64 '0)))
                                        (let v3 (Coalesce v3 (Uint64 '0)))

                                        ### Check versions ###
                                        (let predicate (Bool 'True))
                                        (let predicate (And predicate (Equal rv1 v1)))
                                        (let predicate (And predicate (Equal rv2 v2)))
                                        (let predicate (And predicate (Equal rv2 v3)))


                                        ### If versions are not changed -- do writes ###
                                        (let pgmReturn (If predicate
                                            (block '(
                                            (let list (List (ListType (VoidType))))
                                                (let list (Append list (UpdateRow '/dc-1/Example/Table1 row1 '('('Version (Increment v1))'('Data d1)))))
                                                (let list (Append list (UpdateRow '/dc-1/Example/Table2 row2 '('('Version (Increment v2))'('Data d2)))))
                                                (let list (Append list (UpdateRow '/dc-1/Example/Table3 row3 '('('Version (Increment v3))'('Data d3)))))
                                                (let list (Append list (SetResult 'myRes (Utf8 '"Updated"))))
                                                (return list)
                                            ))
                                            (block '(
                                                (let emptyList (List (ListType (VoidType))))
                                                (let emptyList (Append emptyList (SetResult 'myRes (Utf8 '"Version mismatch"))))
                                                (return emptyList)
                                            ))
                                        ))
                                        ######

                                        ## Output some debug values
                                        (let pgmReturn (Append pgmReturn (SetResult 'rv1 rv1)))
                                        (let pgmReturn (Append pgmReturn (SetResult 'v1 v1)))

                                        (return pgmReturn)
                                        )
                                        )___").SyncPrepare().GetQuery();

        for (ui64 cnt = 0; cnt < 10; ++cnt) {
            ui64 H1 = 100 + cnt;
            ui64 H2 = 200 - cnt * 2;
            ui64 H3 = 300 - cnt * 3;
            auto result = readQuery.SyncExecute(
                              TParameter("H1_PARAM", H1),
                              TParameter("H2_PARAM", H2),
                              TParameter("H3_PARAM", H3)
                          );
            UNIT_ASSERT_EQUAL(result.GetStatus(), NMsgBusProxy::MSTATUS_OK);
            auto valueResult = result.GetValue();
            auto V1 = valueResult["H1"]["Version"];
            auto V2 = valueResult["H2"]["Version"];
            auto V3 = valueResult["H3"]["Version"];
            result = updateQuery.SyncExecute(
                                             TParameter("H1_PARAM", H1),
                                             TParameter("H2_PARAM", H2),
                                             TParameter("H3_PARAM", H3),
                                             TParameter("V1_PARAM", V1.IsNull() ? (ui64)0 : V1),
                                             TParameter("V2_PARAM", V2.IsNull() ? (ui64)0 : V2),
                                             TParameter("V3_PARAM", V3.IsNull() ? (ui64)0 : V3),
                                             TParameterValue<TString, NScheme::NTypeIds::String>("D1_PARAM", "data" + ToString(H1)),
                                             TParameterValue<TString, NScheme::NTypeIds::String>("D2_PARAM", "data" + ToString(H2)),
                                             TParameterValue<TString, NScheme::NTypeIds::String>("D3_PARAM", "data" + ToString(H3))
                                         );
            UNIT_ASSERT_EQUAL(result.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        }
    }

    Y_UNIT_TEST(TestParameterTypes) {
        using namespace NClient;
        NMsgBusProxy::TMsgBusClientConfig clientConfig;
        Tests::TServer server = StartupKikimr(clientConfig);
        NClient::TKikimr kikimr(clientConfig);

        auto query = kikimr.Query(R"___(
            (
            (let pb (Parameter 'PB (DataType 'Bool)))
            (let pui8 (Parameter 'PUI8 (DataType 'Uint8)))
            (let pi32 (Parameter 'PI32 (DataType 'Int32)))
            (let pui32 (Parameter 'PUI32 (DataType 'Uint32)))
            (let pi64 (Parameter 'PI64 (DataType 'Int64)))
            (let pui64 (Parameter 'PUI64 (DataType 'Uint64)))
            (let pf (Parameter 'PF (DataType 'Float)))
            (let pd (Parameter 'PD (DataType 'Double)))
            (let ps (Parameter 'PS (DataType 'String)))
            (let pu (Parameter 'PU (DataType 'Utf8)))
            (return (AsList
                (SetResult 'rb pb)
                (SetResult 'rui8 pui8)
                (SetResult 'ri32 pi32)
                (SetResult 'rui32 pui32)
                (SetResult 'ri64 pi64)
                (SetResult 'rui64 pui64)
                (SetResult 'rf pf)
                (SetResult 'rd pd)
                (SetResult 'rs ps)
                (SetResult 'ru pu)
            ))
            )
        )___").SyncPrepare().GetQuery();

        TParameters parameters;

        parameters["PB"] = true;
        parameters["PUI8"] = (ui8)1;
        parameters["PI32"] = (i32)-2;
        parameters["PUI32"] = (ui32)3;
        parameters["PI64"] = (i64)-4;
        parameters["PUI64"] = (ui64)5;
        parameters["PF"] = (float)6.5;
        parameters["PD"] = (double)7.5;
        parameters["PS"].Bytes("Str1");
        parameters["PU"] = "Str2";

        auto result = query.SyncExecute(parameters);
        UNIT_ASSERT_EQUAL(result.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        auto value = result.GetValue();
        UNIT_ASSERT_EQUAL((bool)value["rb"], true);
        UNIT_ASSERT_EQUAL((ui8)value["rui8"], 1);
        UNIT_ASSERT_EQUAL((i32)value["ri32"], -2);
        UNIT_ASSERT_EQUAL((ui32)value["rui32"], 3);
        UNIT_ASSERT_EQUAL((i64)value["ri64"], -4);
        UNIT_ASSERT_EQUAL((ui64)value["rui64"], 5);
        UNIT_ASSERT_EQUAL((float)value["rf"], 6.5);
        UNIT_ASSERT_EQUAL((double)value["rd"], 7.5);
        UNIT_ASSERT_EQUAL((TString)value["rs"], "Str1");
        UNIT_ASSERT_EQUAL((TString)value["ru"], "Str2");
    }

//    Y_UNIT_TEST(Wrongdoing1) {
//        using namespace NClient;
//        TString type = R"___(
//                    Kind: 6
//                    Struct {
//                      Member {
//                        Name: "r1"
//                        Type {
//                          Kind: 3
//                          Optional {
//                            Item {
//                              Kind: 4
//                              List {
//                                Item {
//                                  Kind: 3
//                                  Optional {
//                                    Item {
//                                      Kind: 6
//                                      Struct {
//                                        Member {
//                                          Name: "GroupSimHash"
//                                          Type {
//                                            Kind: 3
//                                            Optional {
//                                              Item {
//                                                Kind: 2
//                                                Data {
//                                                  Scheme: 4
//                                                }
//                                              }
//                                            }
//                                          }
//                                        }
//                                      }
//                                    }
//                                  }
//                                }
//                              }
//                            }
//                          }
//                        }
//                      }
//                      Member {
//                        Name: "r2"
//                        Type {
//                          Kind: 3
//                          Optional {
//                            Item {
//                              Kind: 4
//                              List {
//                                Item {
//                                  Kind: 4
//                                  List {
//                                    Item {
//                                      Kind: 6
//                                      Struct {
//                                        Member {
//                                          Name: "GroupSimHash"
//                                          Type {
//                                            Kind: 3
//                                            Optional {
//                                              Item {
//                                                Kind: 2
//                                                Data {
//                                                  Scheme: 4
//                                                }
//                                              }
//                                            }
//                                          }
//                                        }
//                                      }
//                                    }
//                                  }
//                                }
//                              }
//                            }
//                          }
//                        }
//                      }
//                    }
//                )___";
//        NKikimrMiniKQL::TValue minikqlValue;
//        NKikimrMiniKQL::TType minikqlType;
//        bool parseOk = ::google::protobuf::TextFormat::ParseFromString(type, &minikqlType);
//        UNIT_ASSERT(parseOk);
//        TValue value = TValue::Create(minikqlValue, minikqlType);

//        //int i = 0;
//        auto result = (ui64)value["r2"]["GroupSimHash"];
//        Y_UNUSED(result);
//    }
}

NKikimrTxUserProxy::TKeyRange MakeRange(const TVector<TString> from, const TVector<TString> to,
                                        bool fromInclusive, bool toInclusive)
{
    NKikimrTxUserProxy::TKeyRange range;
    if (!from.empty()) {
        range.MutableFrom()->MutableType()->SetKind(NKikimrMiniKQL::Tuple);
        auto &tuple = *range.MutableFrom()->MutableType()->MutableTuple();
        for (auto &s : from) {
            if (s)
                range.MutableFrom()->MutableValue()->AddTuple()->MutableOptional()->SetText(s);
            auto &elem = *tuple.AddElement();
            elem.SetKind(NKikimrMiniKQL::Optional);
            auto &item = *elem.MutableOptional()->MutableItem();
            item.SetKind(NKikimrMiniKQL::Data);
            item.MutableData()->SetScheme(NUdf::TDataType<NUdf::TUtf8>::Id);
        }
    }
    if (!to.empty()) {
        range.MutableTo()->MutableType()->SetKind(NKikimrMiniKQL::Tuple);
        auto &tuple = *range.MutableTo()->MutableType()->MutableTuple();
        for (auto &s : to) {
            if (s)
                range.MutableTo()->MutableValue()->AddTuple()->MutableOptional()->SetText(s);
            auto &elem = *tuple.AddElement();
            elem.SetKind(NKikimrMiniKQL::Optional);
            auto &item = *elem.MutableOptional()->MutableItem();
            item.SetKind(NKikimrMiniKQL::Data);
            item.MutableData()->SetScheme(NUdf::TDataType<NUdf::TUtf8>::Id);
        }
    }
    range.SetFromInclusive(fromInclusive);
    range.SetToInclusive(toInclusive);

    return range;
}
