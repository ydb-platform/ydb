#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/file.h>
#include <util/system/shellcommand.h>

#include <ydb/core/base/path.h>
#include <ydb/core/scheme/scheme_type_info.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

using namespace NKikimr;
using namespace NKikimr::Tests;

namespace {

const char* PythonScript = R"PY(
import sys
sys.path[:0] = sys.argv[3].split(":")
import importlib.metadata as metadata
metadata.version = lambda name: "1.31.5"
import pg8000.dbapi

port = int(sys.argv[1])
database = sys.argv[2]
conn = pg8000.dbapi.connect(user="test", host="::1", port=port, database=database, timeout=60)
conn.autocommit = True
cur = conn.cursor()
cur.execute("""CREATE TABLE IF NOT EXISTS types (id int8 PRIMARY KEY, ts timestamp, tiny "Int8", note text DEFAULT 'hi')""")
cur.execute("""CREATE TABLE IF NOT EXISTS types (id int8 PRIMARY KEY, ts timestamp, tiny "Int8", note text DEFAULT 'hi')""")

def must_fail(sql):
    try:
        cur.execute(sql)
    except pg8000.dbapi.DatabaseError:
        return
    raise SystemExit("table was created: " + sql)

must_fail("CREATE TABLE parted (id int4 PRIMARY KEY) PARTITION BY RANGE (id)")
must_fail("CREATE TABLE checked (id int4 PRIMARY KEY, CHECK (id > 0))")
must_fail("CREATE TABLE nodate (id int4 PRIMARY KEY, d date)")
cur.execute("""CREATE TABLE dates (id int4 PRIMARY KEY, d "Date")""")
)PY";

struct TSeenColumn {
    TString TypeName;
    bool NotNull = false;
    bool HasDefault = false;
    TString DefaultText;
};

THashMap<TString, TSeenColumn> Describe(TServer& server, const TString& path, bool& exists) {
    auto* runtime = server.GetRuntime();
    auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
    NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = SplitPath(path);
    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
    request->ResultSet.emplace_back(std::move(entry));
    const TActorId edge = runtime->AllocateEdgeActor();
    runtime->Send(new IEventHandle(MakeSchemeCacheID(), edge, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release())), 0, true);
    auto ev = runtime->GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(edge);
    const auto* navigate = ev->Get()->Request.Get();
    exists = navigate && navigate->ResultSet.size() == 1
        && navigate->ResultSet[0].Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
    THashMap<TString, TSeenColumn> columns;
    if (!exists) {
        return columns;
    }
    for (const auto& [id, column] : navigate->ResultSet[0].Columns) {
        Y_UNUSED(id);
        TSeenColumn seen;
        seen.TypeName = NScheme::TypeName(column.PType, column.PTypeMod);
        seen.NotNull = column.IsNotNullColumn;
        seen.HasDefault = column.DefaultKind == NKikimr::TSysTables::TTableColumnInfo::DEFAULT_LITERAL;
        if (seen.HasDefault) {
            seen.DefaultText = column.DefaultFromLiteral.Getvalue().Gettext_value();
        }
        columns[column.Name] = std::move(seen);
    }
    return columns;
}

THashMap<TString, TSeenColumn> WaitTable(TServer& server, const TString& path) {
    const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
    THashMap<TString, TSeenColumn> columns;
    do {
        bool exists = false;
        columns = Describe(server, path, exists);
        if (exists) {
            return columns;
        }
        Sleep(TDuration::MilliSeconds(100));
    } while (TInstant::Now() < deadline);
    return {};
}

} // namespace

Y_UNIT_TEST_SUITE(CreateTable) {
    Y_UNIT_TEST(Pg8000CreatesMappedColumns) {
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

        const TString scriptPath = "pg8000_create_table.py";
        {
            TFileOutput out(scriptPath);
            out << PythonScript;
        }

        const TString root = ArcadiaSourceRoot();
        const TString pythonPath = root + "/contrib/python/pg8000:"
            + root + "/contrib/python/scramp:"
            + root + "/contrib/python/asn1crypto:"
            + root + "/contrib/python/python-dateutil/py3:"
            + root + "/contrib/python/six/py3";
        TShellCommand command("python3", {scriptPath, ToString(sqlPort), database, pythonPath});
        command.Run().Wait();
        UNIT_ASSERT_C(command.GetExitCode() && *command.GetExitCode() == 0,
            command.GetOutput() + "\n" + command.GetError());

        const auto types = WaitTable(server, database + "/types");
        const TSeenColumn* id = types.FindPtr("id");
        const TSeenColumn* ts = types.FindPtr("ts");
        const TSeenColumn* tiny = types.FindPtr("tiny");
        const TSeenColumn* note = types.FindPtr("note");
        UNIT_ASSERT(id && ts && tiny && note);
        UNIT_ASSERT_VALUES_EQUAL(id->TypeName, "Int64");
        UNIT_ASSERT(id->NotNull);
        UNIT_ASSERT_VALUES_EQUAL(ts->TypeName, "Timestamp64");
        UNIT_ASSERT_VALUES_EQUAL(tiny->TypeName, "Int8");
        UNIT_ASSERT_VALUES_EQUAL(note->TypeName, "Utf8");
        UNIT_ASSERT(note->HasDefault);
        UNIT_ASSERT_VALUES_EQUAL(note->DefaultText, "hi");

        bool parted = true;
        Describe(server, database + "/parted", parted);
        UNIT_ASSERT(!parted);
        bool checked = true;
        Describe(server, database + "/checked", checked);
        UNIT_ASSERT(!checked);
        bool nodate = true;
        Describe(server, database + "/nodate", nodate);
        UNIT_ASSERT(!nodate);

        const auto dates = WaitTable(server, database + "/dates");
        const TSeenColumn* date = dates.FindPtr("d");
        UNIT_ASSERT(date);
        UNIT_ASSERT_VALUES_EQUAL(date->TypeName, "Date");
    }

    Y_UNIT_TEST(UniqueIndexIsSynchronous) {
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

        const char* script = R"PY(
import sys
sys.path[:0] = sys.argv[3].split(":")
import importlib.metadata as metadata
metadata.version = lambda name: "1.31.5"
import pg8000.dbapi
conn = pg8000.dbapi.connect(user="test", host="::1", port=int(sys.argv[1]), database=sys.argv[2], timeout=60)
conn.autocommit = True
conn.cursor().execute("CREATE TABLE uniq (id int4 PRIMARY KEY, email text, UNIQUE (email))")
)PY";
        const TString scriptPath = "pg8000_unique.py";
        {
            TFileOutput out(scriptPath);
            out << script;
        }
        const TString root = ArcadiaSourceRoot();
        const TString pythonPath = root + "/contrib/python/pg8000:"
            + root + "/contrib/python/scramp:"
            + root + "/contrib/python/asn1crypto:"
            + root + "/contrib/python/python-dateutil/py3:"
            + root + "/contrib/python/six/py3";
        TShellCommand command("python3", {scriptPath, ToString(sqlPort), database, pythonPath});
        command.Run().Wait();
        UNIT_ASSERT_C(command.GetExitCode() && *command.GetExitCode() == 0,
            command.GetOutput() + "\n" + command.GetError());

        auto* runtime = server.GetRuntime();
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
        NKikimrSchemeOp::TIndexDescription unique;
        bool found = false;
        do {
            auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = SplitPath(database + "/uniq");
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
            request->ResultSet.emplace_back(std::move(entry));
            const TActorId edge = runtime->AllocateEdgeActor();
            runtime->Send(new IEventHandle(MakeSchemeCacheID(), edge, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release())), 0, true);
            auto ev = runtime->GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(edge);
            const auto* navigate = ev->Get()->Request.Get();
            if (navigate && navigate->ResultSet.size() == 1
                && navigate->ResultSet[0].Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok
                && navigate->ResultSet[0].Indexes.size() == 1)
            {
                unique = navigate->ResultSet[0].Indexes[0];
                found = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        } while (TInstant::Now() < deadline);
        UNIT_ASSERT(found);
        UNIT_ASSERT_VALUES_EQUAL(unique.GetType(), NKikimrSchemeOp::EIndexTypeGlobalUnique);
        UNIT_ASSERT_VALUES_EQUAL(unique.GetState(), NKikimrSchemeOp::EIndexStateReady);
        UNIT_ASSERT_VALUES_EQUAL(unique.KeyColumnNamesSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(unique.GetKeyColumnNames(0), "email");
    }

    Y_UNIT_TEST(AddColumnLeavesDateUnchanged) {
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

        const char* script = R"PY(
import sys
sys.path[:0] = sys.argv[3].split(":")
import importlib.metadata as metadata
metadata.version = lambda name: "1.31.5"
import pg8000.dbapi
conn = pg8000.dbapi.connect(user="test", host="::1", port=int(sys.argv[1]), database=sys.argv[2], timeout=60)
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE TABLE base (id int4 PRIMARY KEY)")
cur.execute("ALTER TABLE base ADD COLUMN note text")
try:
    cur.execute("ALTER TABLE base ADD COLUMN d date")
except pg8000.dbapi.DatabaseError:
    pass
else:
    raise SystemExit("date column was added")
)PY";
        const TString scriptPath = "pg8000_alter.py";
        {
            TFileOutput out(scriptPath);
            out << script;
        }
        const TString root = ArcadiaSourceRoot();
        const TString pythonPath = root + "/contrib/python/pg8000:"
            + root + "/contrib/python/scramp:"
            + root + "/contrib/python/asn1crypto:"
            + root + "/contrib/python/python-dateutil/py3:"
            + root + "/contrib/python/six/py3";
        TShellCommand command("python3", {scriptPath, ToString(sqlPort), database, pythonPath});
        command.Run().Wait();
        UNIT_ASSERT_C(command.GetExitCode() && *command.GetExitCode() == 0,
            command.GetOutput() + "\n" + command.GetError());

        const auto columns = WaitTable(server, database + "/base");
        const TSeenColumn* id = columns.FindPtr("id");
        const TSeenColumn* note = columns.FindPtr("note");
        UNIT_ASSERT(id && note);
        UNIT_ASSERT_VALUES_EQUAL(id->TypeName, "Int32");
        UNIT_ASSERT_VALUES_EQUAL(note->TypeName, "Utf8");
        UNIT_ASSERT(!columns.FindPtr("d"));
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 2u);
    }

    Y_UNIT_TEST(DropColumnRemovesIt) {
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

        const char* script = R"PY(
import sys
sys.path[:0] = sys.argv[3].split(":")
import importlib.metadata as metadata
metadata.version = lambda name: "1.31.5"
import pg8000.dbapi
conn = pg8000.dbapi.connect(user="test", host="::1", port=int(sys.argv[1]), database=sys.argv[2], timeout=60)
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE TABLE base (id int4 PRIMARY KEY, note text)")
cur.execute("ALTER TABLE base DROP COLUMN note")

def must_fail(sql):
    try:
        cur.execute(sql)
    except pg8000.dbapi.DatabaseError:
        return
    raise SystemExit("drop succeeded: " + sql)

must_fail("ALTER TABLE base DROP COLUMN note")
must_fail("ALTER TABLE base DROP COLUMN id")
)PY";
        const TString scriptPath = "pg8000_drop_column.py";
        {
            TFileOutput out(scriptPath);
            out << script;
        }
        const TString root = ArcadiaSourceRoot();
        const TString pythonPath = root + "/contrib/python/pg8000:"
            + root + "/contrib/python/scramp:"
            + root + "/contrib/python/asn1crypto:"
            + root + "/contrib/python/python-dateutil/py3:"
            + root + "/contrib/python/six/py3";
        TShellCommand command("python3", {scriptPath, ToString(sqlPort), database, pythonPath});
        command.Run().Wait();
        UNIT_ASSERT_C(command.GetExitCode() && *command.GetExitCode() == 0,
            command.GetOutput() + "\n" + command.GetError());

        const auto columns = WaitTable(server, database + "/base");
        UNIT_ASSERT(columns.FindPtr("id"));
        UNIT_ASSERT(!columns.FindPtr("note"));
        UNIT_ASSERT_VALUES_EQUAL(columns.size(), 1u);
    }

    Y_UNIT_TEST(AlterColumnKeepsType) {
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

        const char* script = R"PY(
import sys
sys.path[:0] = sys.argv[3].split(":")
import importlib.metadata as metadata
metadata.version = lambda name: "1.31.5"
import pg8000.dbapi
conn = pg8000.dbapi.connect(user="test", host="::1", port=int(sys.argv[1]), database=sys.argv[2], timeout=60)
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE TABLE attrs (id int4 PRIMARY KEY, note text)")
cur.execute("ALTER TABLE attrs ALTER COLUMN note SET NOT NULL")
cur.execute("ALTER TABLE attrs ALTER COLUMN note SET DEFAULT 'hi'")
try:
    cur.execute("ALTER TABLE attrs ALTER COLUMN note TYPE int4")
except pg8000.dbapi.DatabaseError:
    pass
else:
    raise SystemExit("type change was accepted")
)PY";
        const TString scriptPath = "pg8000_alter_column.py";
        {
            TFileOutput out(scriptPath);
            out << script;
        }
        const TString root = ArcadiaSourceRoot();
        const TString pythonPath = root + "/contrib/python/pg8000:"
            + root + "/contrib/python/scramp:"
            + root + "/contrib/python/asn1crypto:"
            + root + "/contrib/python/python-dateutil/py3:"
            + root + "/contrib/python/six/py3";
        TShellCommand command("python3", {scriptPath, ToString(sqlPort), database, pythonPath});
        command.Run().Wait();
        UNIT_ASSERT_C(command.GetExitCode() && *command.GetExitCode() == 0,
            command.GetOutput() + "\n" + command.GetError());

        const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
        TSeenColumn note;
        bool ready = false;
        do {
            const auto columns = WaitTable(server, database + "/attrs");
            const TSeenColumn* found = columns.FindPtr("note");
            if (found && found->NotNull && found->HasDefault && found->TypeName == "Utf8") {
                note = *found;
                ready = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        } while (TInstant::Now() < deadline);
        UNIT_ASSERT(ready);
        UNIT_ASSERT_VALUES_EQUAL(note.DefaultText, "hi");
        UNIT_ASSERT_VALUES_EQUAL(note.TypeName, "Utf8");
    }

    Y_UNIT_TEST(CreateSequenceStoresBounds) {
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

        const char* script = R"PY(
import sys
sys.path[:0] = sys.argv[3].split(":")
import importlib.metadata as metadata
metadata.version = lambda name: "1.31.5"
import pg8000.dbapi
conn = pg8000.dbapi.connect(user="test", host="::1", port=int(sys.argv[1]), database=sys.argv[2], timeout=60)
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE SEQUENCE bounds AS integer INCREMENT BY 2 MINVALUE 1 MAXVALUE 20 START WITH 5 CACHE 3 CYCLE")
try:
    cur.execute("CREATE TEMPORARY SEQUENCE hidden")
except pg8000.dbapi.DatabaseError:
    pass
else:
    raise SystemExit("temporary sequence was accepted")
)PY";
        const TString scriptPath = "pg8000_sequence.py";
        {
            TFileOutput out(scriptPath);
            out << script;
        }
        const TString root = ArcadiaSourceRoot();
        const TString pythonPath = root + "/contrib/python/pg8000:"
            + root + "/contrib/python/scramp:"
            + root + "/contrib/python/asn1crypto:"
            + root + "/contrib/python/python-dateutil/py3:"
            + root + "/contrib/python/six/py3";
        TShellCommand command("python3", {scriptPath, ToString(sqlPort), database, pythonPath});
        command.Run().Wait();
        UNIT_ASSERT_C(command.GetExitCode() && *command.GetExitCode() == 0,
            command.GetOutput() + "\n" + command.GetError());

        auto* runtime = server.GetRuntime();
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
        NKikimrSchemeOp::TSequenceDescription description;
        bool found = false;
        do {
            auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = SplitPath(database + "/bounds");
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            request->ResultSet.emplace_back(std::move(entry));
            const TActorId edge = runtime->AllocateEdgeActor();
            runtime->Send(new IEventHandle(MakeSchemeCacheID(), edge, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release())), 0, true);
            auto ev = runtime->GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(edge);
            const auto* navigate = ev->Get()->Request.Get();
            if (navigate && navigate->ResultSet.size() == 1 && navigate->ResultSet[0].SequenceInfo) {
                description = navigate->ResultSet[0].SequenceInfo->Description;
                found = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        } while (TInstant::Now() < deadline);
        UNIT_ASSERT(found);
        UNIT_ASSERT_VALUES_EQUAL(description.GetDataType(), "Int32");
        UNIT_ASSERT_VALUES_EQUAL(description.GetIncrement(), 2);
        UNIT_ASSERT_VALUES_EQUAL(description.GetMinValue(), 1);
        UNIT_ASSERT_VALUES_EQUAL(description.GetMaxValue(), 20);
        UNIT_ASSERT_VALUES_EQUAL(description.GetStartValue(), 5);
        UNIT_ASSERT_VALUES_EQUAL(description.GetCache(), 3u);
        UNIT_ASSERT(description.GetCycle());

        bool hidden = true;
        Describe(server, database + "/hidden", hidden);
        UNIT_ASSERT(!hidden);
    }

    Y_UNIT_TEST(AlterSequenceRestartChangesDescription) {
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

        const char* script = R"PY(
import sys
sys.path[:0] = sys.argv[3].split(":")
import importlib.metadata as metadata
metadata.version = lambda name: "1.31.5"
import pg8000.dbapi
conn = pg8000.dbapi.connect(user="test", host="::1", port=int(sys.argv[1]), database=sys.argv[2], timeout=60)
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE SEQUENCE restartable START WITH 1")
cur.execute("ALTER SEQUENCE restartable RESTART WITH 9")
try:
    cur.execute("ALTER SEQUENCE restartable OWNED BY t.id")
except pg8000.dbapi.DatabaseError:
    pass
else:
    raise SystemExit("owned by was accepted")
)PY";
        const TString scriptPath = "pg8000_alter_sequence.py";
        {
            TFileOutput out(scriptPath);
            out << script;
        }
        const TString root = ArcadiaSourceRoot();
        const TString pythonPath = root + "/contrib/python/pg8000:"
            + root + "/contrib/python/scramp:"
            + root + "/contrib/python/asn1crypto:"
            + root + "/contrib/python/python-dateutil/py3:"
            + root + "/contrib/python/six/py3";
        TShellCommand command("python3", {scriptPath, ToString(sqlPort), database, pythonPath});
        command.Run().Wait();
        UNIT_ASSERT_C(command.GetExitCode() && *command.GetExitCode() == 0,
            command.GetOutput() + "\n" + command.GetError());

        auto* runtime = server.GetRuntime();
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
        bool found = false;
        do {
            auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = SplitPath(database + "/restartable");
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            request->ResultSet.emplace_back(std::move(entry));
            const TActorId edge = runtime->AllocateEdgeActor();
            runtime->Send(new IEventHandle(MakeSchemeCacheID(), edge, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release())), 0, true);
            auto ev = runtime->GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(edge);
            const auto* navigate = ev->Get()->Request.Get();
            if (navigate && navigate->ResultSet.size() == 1 && navigate->ResultSet[0].SequenceInfo
                && navigate->ResultSet[0].SequenceInfo->Description.GetVersion() >= 2)
            {
                const auto& description = navigate->ResultSet[0].SequenceInfo->Description;
                if (description.HasSetVal()) {
                    UNIT_ASSERT_VALUES_EQUAL(description.GetSetVal().GetNextValue(), 9);
                }
                UNIT_ASSERT_VALUES_EQUAL(description.GetMinValue(), 1);
                found = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        } while (TInstant::Now() < deadline);
        UNIT_ASSERT(found);
    }

    Y_UNIT_TEST(DropTableRemovesPath) {
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

        const char* script = R"PY(
import sys
sys.path[:0] = sys.argv[3].split(":")
import importlib.metadata as metadata
metadata.version = lambda name: "1.31.5"
import pg8000.dbapi
conn = pg8000.dbapi.connect(user="test", host="::1", port=int(sys.argv[1]), database=sys.argv[2], timeout=60)
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE TABLE doomed (id int4 PRIMARY KEY)")
cur.execute("DROP TABLE doomed")
cur.execute("DROP TABLE IF EXISTS doomed")
try:
    cur.execute("DROP TABLE doomed")
except pg8000.dbapi.DatabaseError:
    pass
else:
    raise SystemExit("missing table was dropped")
)PY";
        const TString scriptPath = "pg8000_drop_table.py";
        {
            TFileOutput out(scriptPath);
            out << script;
        }
        const TString root = ArcadiaSourceRoot();
        const TString pythonPath = root + "/contrib/python/pg8000:"
            + root + "/contrib/python/scramp:"
            + root + "/contrib/python/asn1crypto:"
            + root + "/contrib/python/python-dateutil/py3:"
            + root + "/contrib/python/six/py3";
        TShellCommand command("python3", {scriptPath, ToString(sqlPort), database, pythonPath});
        command.Run().Wait();
        UNIT_ASSERT_C(command.GetExitCode() && *command.GetExitCode() == 0,
            command.GetOutput() + "\n" + command.GetError());

        bool exists = true;
        Describe(server, database + "/doomed", exists);
        UNIT_ASSERT(!exists);
    }

    Y_UNIT_TEST(DropSequenceRemovesPath) {
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

        const char* script = R"PY(
import sys
sys.path[:0] = sys.argv[3].split(":")
import importlib.metadata as metadata
metadata.version = lambda name: "1.31.5"
import pg8000.dbapi
conn = pg8000.dbapi.connect(user="test", host="::1", port=int(sys.argv[1]), database=sys.argv[2], timeout=60)
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE SEQUENCE doomed_seq")
cur.execute("DROP SEQUENCE doomed_seq")
cur.execute("DROP SEQUENCE IF EXISTS doomed_seq")
try:
    cur.execute("DROP SEQUENCE doomed_seq")
except pg8000.dbapi.DatabaseError:
    pass
else:
    raise SystemExit("missing sequence was dropped")
)PY";
        const TString scriptPath = "pg8000_drop_sequence.py";
        {
            TFileOutput out(scriptPath);
            out << script;
        }
        const TString root = ArcadiaSourceRoot();
        const TString pythonPath = root + "/contrib/python/pg8000:"
            + root + "/contrib/python/scramp:"
            + root + "/contrib/python/asn1crypto:"
            + root + "/contrib/python/python-dateutil/py3:"
            + root + "/contrib/python/six/py3";
        TShellCommand command("python3", {scriptPath, ToString(sqlPort), database, pythonPath});
        command.Run().Wait();
        UNIT_ASSERT_C(command.GetExitCode() && *command.GetExitCode() == 0,
            command.GetOutput() + "\n" + command.GetError());

        auto* runtime = server.GetRuntime();
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(database + "/doomed_seq");
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        request->ResultSet.emplace_back(std::move(entry));
        const TActorId edge = runtime->AllocateEdgeActor();
        runtime->Send(new IEventHandle(MakeSchemeCacheID(), edge, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release())), 0, true);
        auto ev = runtime->GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(edge);
        const auto* navigate = ev->Get()->Request.Get();
        const bool exists = navigate && navigate->ResultSet.size() == 1
            && navigate->ResultSet[0].Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok
            && navigate->ResultSet[0].SequenceInfo;
        UNIT_ASSERT(!exists);
    }

    Y_UNIT_TEST(CreateSchemaMakesDirectory) {
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

        const char* script = R"PY(
import sys
sys.path[:0] = sys.argv[3].split(":")
import importlib.metadata as metadata
metadata.version = lambda name: "1.31.5"
import pg8000.dbapi
conn = pg8000.dbapi.connect(user="test", host="::1", port=int(sys.argv[1]), database=sys.argv[2], timeout=60)
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE SCHEMA app")
try:
    cur.execute("CREATE SCHEMA nested CREATE TABLE t (id int4 PRIMARY KEY)")
except pg8000.dbapi.DatabaseError:
    pass
else:
    raise SystemExit("schema body was accepted")
)PY";
        const TString scriptPath = "pg8000_schema.py";
        {
            TFileOutput out(scriptPath);
            out << script;
        }
        const TString root = ArcadiaSourceRoot();
        const TString pythonPath = root + "/contrib/python/pg8000:"
            + root + "/contrib/python/scramp:"
            + root + "/contrib/python/asn1crypto:"
            + root + "/contrib/python/python-dateutil/py3:"
            + root + "/contrib/python/six/py3";
        TShellCommand command("python3", {scriptPath, ToString(sqlPort), database, pythonPath});
        command.Run().Wait();
        UNIT_ASSERT_C(command.GetExitCode() && *command.GetExitCode() == 0,
            command.GetOutput() + "\n" + command.GetError());

        auto* runtime = server.GetRuntime();
        const TInstant deadline = TInstant::Now() + TDuration::Seconds(10);
        bool found = false;
        do {
            auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
            NSchemeCache::TSchemeCacheNavigate::TEntry entry;
            entry.Path = SplitPath(database + "/app");
            entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
            request->ResultSet.emplace_back(std::move(entry));
            const TActorId edge = runtime->AllocateEdgeActor();
            runtime->Send(new IEventHandle(MakeSchemeCacheID(), edge, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release())), 0, true);
            auto ev = runtime->GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(edge);
            const auto* navigate = ev->Get()->Request.Get();
            if (navigate && navigate->ResultSet.size() == 1
                && navigate->ResultSet[0].Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok
                && navigate->ResultSet[0].Kind == NSchemeCache::TSchemeCacheNavigate::KindPath)
            {
                found = true;
                break;
            }
            Sleep(TDuration::MilliSeconds(100));
        } while (TInstant::Now() < deadline);
        UNIT_ASSERT(found);
        bool nested = true;
        Describe(server, database + "/nested", nested);
        UNIT_ASSERT(!nested);
    }

    Y_UNIT_TEST(DropSchemaKeepsNonEmpty) {
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

        const char* script = R"PY(
import sys
sys.path[:0] = sys.argv[3].split(":")
import importlib.metadata as metadata
metadata.version = lambda name: "1.31.5"
import pg8000.dbapi
conn = pg8000.dbapi.connect(user="test", host="::1", port=int(sys.argv[1]), database=sys.argv[2], timeout=60)
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE SCHEMA empty")
cur.execute("DROP SCHEMA empty")
cur.execute("DROP SCHEMA IF EXISTS empty")
cur.execute("CREATE SCHEMA full")
cur.execute("CREATE TABLE full.t (id int4 PRIMARY KEY)")
try:
    cur.execute("DROP SCHEMA full CASCADE")
except pg8000.dbapi.DatabaseError:
    pass
else:
    raise SystemExit("cascade drop was accepted")
try:
    cur.execute("DROP SCHEMA full")
except pg8000.dbapi.DatabaseError:
    pass
else:
    raise SystemExit("non-empty schema was dropped")
)PY";
        const TString scriptPath = "pg8000_drop_schema.py";
        {
            TFileOutput out(scriptPath);
            out << script;
        }
        const TString root = ArcadiaSourceRoot();
        const TString pythonPath = root + "/contrib/python/pg8000:"
            + root + "/contrib/python/scramp:"
            + root + "/contrib/python/asn1crypto:"
            + root + "/contrib/python/python-dateutil/py3:"
            + root + "/contrib/python/six/py3";
        TShellCommand command("python3", {scriptPath, ToString(sqlPort), database, pythonPath});
        command.Run().Wait();
        UNIT_ASSERT_C(command.GetExitCode() && *command.GetExitCode() == 0,
            command.GetOutput() + "\n" + command.GetError());

        const auto columns = WaitTable(server, database + "/full/t");
        UNIT_ASSERT(columns.FindPtr("id"));
        auto* runtime = server.GetRuntime();
        auto request = MakeHolder<NSchemeCache::TSchemeCacheNavigate>();
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(database + "/full");
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
        request->ResultSet.emplace_back(std::move(entry));
        const TActorId edge = runtime->AllocateEdgeActor();
        runtime->Send(new IEventHandle(MakeSchemeCacheID(), edge, new TEvTxProxySchemeCache::TEvNavigateKeySet(request.Release())), 0, true);
        auto ev = runtime->GrabEdgeEventRethrow<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(edge);
        const auto* navigate = ev->Get()->Request.Get();
        UNIT_ASSERT(navigate && navigate->ResultSet.size() == 1
            && navigate->ResultSet[0].Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok);
        bool empty = true;
        Describe(server, database + "/empty", empty);
        UNIT_ASSERT(!empty);
    }
}
