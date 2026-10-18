#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>
#include <util/system/shellcommand.h>

#include <ydb/core/testlib/test_client.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>

using namespace NKikimr;
using namespace NKikimr::Tests;

namespace {

void RunPython(ui16 port, const TString& database, const TString& script, const TString& fileName) {
    {
        TFileOutput out(fileName);
        out << script;
    }
    const TString root = ArcadiaSourceRoot();
    const TString pythonPath = root + "/contrib/python/pg8000:"
        + root + "/contrib/python/scramp:"
        + root + "/contrib/python/asn1crypto:"
        + root + "/contrib/python/python-dateutil/py3:"
        + root + "/contrib/python/six/py3";
    TShellCommand command("python3", {fileName, ToString(port), database, pythonPath});
    command.Run().Wait();
    UNIT_ASSERT_C(command.GetExitCode() && *command.GetExitCode() == 0,
        command.GetOutput() + "\n" + command.GetError());
}

} // namespace

Y_UNIT_TEST_SUITE(SelectPoint) {
    Y_UNIT_TEST(PrimaryKeyReturnsOneRow) {
        TPortManager ports;
        const ui16 sqlPort = ports.GetTcpPort();
        TServerSettings settings(ports.GetTcpPort());
        settings.AppConfig->MutableSqlConfig()->SetEnable(true);
        settings.AppConfig->MutableSqlConfig()->SetListeningPort(sqlPort);
        settings.AppConfig->MutableSqlConfig()->SetListeningAddress("::");
        TServer server(settings);
        TClient client(settings);
        client.InitRootScheme();
        const TString database = "/" + settings.DomainName;

        RunPython(sqlPort, database, R"PY(
import sys
sys.path[:0] = sys.argv[3].split(":")
import importlib.metadata as metadata
metadata.version = lambda name: "1.31.5"
import pg8000.dbapi
conn = pg8000.dbapi.connect(user="test", host="::1", port=int(sys.argv[1]), database=sys.argv[2], timeout=60)
conn.autocommit = True
conn.cursor().execute("CREATE TABLE items (id int8 PRIMARY KEY, note text)")
)PY", "pg8000_select_create.py");

        NKikimrMiniKQL::TResult result;
        const TString query = TString::Join(
            "(",
            "(let row '('('id (Int64 '7))))",
            "(let myUpd '('('note (Utf8 '\"hello\"))))",
            "(let pgmReturn (AsList (UpdateRow '", database, "/items row myUpd)))",
            "(return pgmReturn)",
            ")");
        UNIT_ASSERT(client.FlatQuery(server.GetRuntime(), query, result));

        RunPython(sqlPort, database, R"PY(
import sys
sys.path[:0] = sys.argv[3].split(":")
import importlib.metadata as metadata
metadata.version = lambda name: "1.31.5"
import pg8000.dbapi
conn = pg8000.dbapi.connect(user="test", host="::1", port=int(sys.argv[1]), database=sys.argv[2], timeout=60)
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT note FROM items WHERE id = 7")
row = cur.fetchone()
if list(row) != ["hello"] or cur.rowcount != 1:
    raise SystemExit(repr(row) + " rowcount=" + str(cur.rowcount))
)PY", "pg8000_select_point.py");
    }
}
