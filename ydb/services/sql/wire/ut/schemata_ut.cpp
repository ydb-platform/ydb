#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>
#include <util/system/shellcommand.h>

#include <ydb/core/testlib/test_client.h>

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

Y_UNIT_TEST_SUITE(InformationSchema) {
    Y_UNIT_TEST(SchemataListsDirectories) {
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
cur = conn.cursor()
cur.execute("CREATE SCHEMA app")
cur.execute("CREATE TABLE items (id int8 PRIMARY KEY)")
cur.execute("SELECT schema_name FROM information_schema.schemata")
names = sorted(row[0] for row in cur.fetchall())
if "app" not in names or "items" in names:
    raise SystemExit(repr(names))
cur.execute("SELECT catalog_name, schema_name FROM information_schema.schemata WHERE schema_name = 'app'")
row = cur.fetchone()
if list(row) != ["dc-1", "app"] or cur.rowcount != 1:
    raise SystemExit(repr(row) + " rowcount=" + str(cur.rowcount))
try:
    cur.execute("SELECT schema_owner FROM information_schema.schemata")
except pg8000.dbapi.DatabaseError:
    pass
else:
    raise SystemExit("extra column was accepted")
)PY", "pg8000_schemata.py");
    }

    Y_UNIT_TEST(TablesListsTables) {
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
cur = conn.cursor()
cur.execute("CREATE SCHEMA app")
cur.execute("CREATE TABLE items (id int8 PRIMARY KEY)")
cur.execute("CREATE TABLE app.kept (id int8 PRIMARY KEY)")
cur.execute("CREATE SEQUENCE seq")
cur.execute("SELECT table_schema, table_name, table_type FROM information_schema.tables")
rows = sorted(tuple(row) for row in cur.fetchall())
names = [row[1] for row in rows]
if ("", "items", "BASE TABLE") not in rows or ("app", "kept", "BASE TABLE") not in rows or "seq" in names:
    raise SystemExit(repr(rows))
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'app'")
one = [row[0] for row in cur.fetchall()]
if one != ["kept"]:
    raise SystemExit(repr(one))
try:
    cur.execute("SELECT extra FROM information_schema.tables")
except pg8000.dbapi.DatabaseError:
    pass
else:
    raise SystemExit("extra column was accepted")
)PY", "pg8000_tables.py");
    }
}
