#include <ydb/core/kqp/common/simple/temp_tables.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/sys_view/ut_common.h>
#include <ydb/core/ydb_convert/table_description.h>

#include <ydb/library/testlib/common/test_utils.h>

#include <ydb/public/lib/ydb_cli/dump/util/query_utils.h>

namespace NKikimr {
namespace NSysView {

using namespace NYdb;
using namespace NYdb::NDump;

namespace {

class TShowCreateChecker {
public:

    explicit TShowCreateChecker(TTestEnv& env)
        : Env(env)
        , Runtime(*Env.GetServer().GetRuntime())
        , QueryClient(NQuery::TQueryClient(Env.GetDriver()))
        , Session(QueryClient.GetSession().GetValueSync().GetSession())
    {
        CreateTier("tier1");
        CreateTier("tier2");
    }

    void WaitForCdcStreamReady(const std::string& streamPath) {
        for (int i = 0; i < 60; ++i) {
            auto pathDesc = DescribePath(Runtime, TString(streamPath));
            if (pathDesc.HasCdcStreamDescription()
                && pathDesc.GetCdcStreamDescription().GetState() == NKikimrSchemeOp::ECdcStreamStateReady)
            {
                return;
            }
            Sleep(TDuration::MilliSeconds(500));
        }
        UNIT_FAIL("Timed out waiting for CDC stream to become ready: " << streamPath);
    }

    std::string ShowCreateTable(NQuery::TSession& session, const std::string& tableName) {
        return ShowCreate(session, "TABLE", tableName);
    }

    void CheckShowCreateTable(const std::string& query, const std::string& tableName, TString formatQuery = "", bool temporary = false, bool initialScan = false) {
        auto session = QueryClient.GetSession().GetValueSync().GetSession();

        ExecuteQuery(session, query);
        auto showCreateTableQuery = ShowCreateTable(session, tableName);

        if (formatQuery) {
            TString normalizedFormatQuery = NormalizeWhitespaceInQuery(UnescapeC(formatQuery));
            TString normalizedShowCreateTableQuery = NormalizeWhitespaceInQuery(UnescapeC(showCreateTableQuery));
            UNIT_ASSERT_STRINGS_EQUAL(normalizedFormatQuery, normalizedShowCreateTableQuery);
        }

        if (initialScan) {
            return;
        }

        std::optional<TString> tempDir = std::nullopt;
        if (temporary) {
            auto res = Env.GetClient().Ls("/Root/.tmp/sessions");
            UNIT_ASSERT(res);
            UNIT_ASSERT(res->Record.HasPathDescription());
            UNIT_ASSERT(res->Record.GetPathDescription().ChildrenSize() == 1);
            tempDir = res->Record.GetPathDescription().GetChildren(0).GetName();
        }

        auto describeResultOrig = DescribeTable(tableName, tempDir);

        DropTable(session, tableName);

        ExecuteQuery(session, showCreateTableQuery);
        auto describeResultNew = DescribeTable(tableName, tempDir);

        DropTable(session, tableName);

        CompareDescriptions(describeResultOrig, describeResultNew, showCreateTableQuery);
    }

    // Checks that the view created from the description provided by the `SHOW CREATE VIEW` statement
    // can be used to create a view with a description equal to the original.
    void CheckShowCreateView(const std::string& query, const std::string& viewName, const std::string& formatQuery = "") {
        ExecuteQuery(Session, query);
        auto showCreateViewResult = ShowCreateView(Session, viewName);

        if (!formatQuery.empty()) {
            TString normalizedFormatQuery = NormalizeWhitespaceInQuery(UnescapeC(formatQuery));
            TString normalizedShowCreateViewQuery = NormalizeWhitespaceInQuery(UnescapeC(showCreateViewResult));
            UNIT_ASSERT_STRINGS_EQUAL(normalizedFormatQuery, normalizedShowCreateViewQuery);
        }

        const auto originalDescription = CanonizeViewDescription(DescribeView(viewName));

        DropView(Session, viewName);
        ExecuteQuery(Session, showCreateViewResult);

        const auto newDescription = CanonizeViewDescription(DescribeView(viewName));

        CompareDescriptions(originalDescription, newDescription, showCreateViewResult);
        DropView(Session, viewName);
    }

private:

    void CreateTier(const std::string& tierName) {
        ExecuteQuery(Session, std::format(R"(
            UPSERT OBJECT `accessKey` (TYPE SECRET) WITH (value = `secretAccessKey`);
            UPSERT OBJECT `secretKey` (TYPE SECRET) WITH (value = `fakeSecret`);
            CREATE EXTERNAL DATA SOURCE `{}` WITH (
                SOURCE_TYPE = "ObjectStorage",
                LOCATION = "http://fake.fake/olap-{}",
                AUTH_METHOD = "AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME = "accessKey",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME = "secretKey",
                AWS_REGION = "ru-central1"
            );
        )", tierName, tierName));
    }

    Ydb::Table::DescribeTableResult DescribeTable(const std::string& tableName, std::optional<TString> sessionId = std::nullopt) {

        auto describeTable = [this](TString&& path) {
            auto pathDescription = DescribePath(Runtime, std::move(path));

            if (pathDescription.HasColumnTableDescription()) {
                const auto& tableDescription = pathDescription.GetColumnTableDescription();
                return *GetScheme(tableDescription);
            }

            if (!pathDescription.HasTable()) {
                UNIT_FAIL("Invalid path type: " << pathDescription.GetSelf().GetPathType());
            }

            const auto& tableDescription = pathDescription.GetTable();
            return *GetScheme(tableDescription);
        };

        auto tablePath = TString(tableName);
        if (!IsStartWithSlash(tablePath)) {
            tablePath = CanonizePath(JoinPath({"/Root", tablePath}));
        }
        if (sessionId.has_value()) {
            tablePath = NKqp::GetTempTablePath("Root", sessionId.value(), tablePath);
        }
        auto tableDesc = describeTable(std::move(tablePath));

        return tableDesc;
    }

    NKikimrSchemeOp::TViewDescription DescribeView(const std::string& viewName) {
        auto pathDescription = DescribePath(Runtime, TString(viewName));
        UNIT_ASSERT_C(pathDescription.HasViewDescription(), pathDescription.DebugString());
        return pathDescription.GetViewDescription();
    }

    NKikimrSchemeOp::TViewDescription CanonizeViewDescription(NKikimrSchemeOp::TViewDescription&& description) {
        description.ClearVersion();
        description.ClearPathId();

        TString queryText;
        NYql::TIssues issues;
        UNIT_ASSERT_C(NDump::Format(description.GetQueryText(), queryText, issues), issues.ToString());
        *description.MutableQueryText() = queryText;

        return description;
    }

    std::string ShowCreate(NQuery::TSession& session, std::string_view type, const std::string& path) {
        const auto result = ExecuteQuery(session, std::format("SHOW CREATE {} `{}`;", type, path));

        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
        auto resultSet = result.GetResultSet(0);
        auto columnsMeta = resultSet.GetColumnsMeta();
        UNIT_ASSERT_VALUES_EQUAL(columnsMeta.size(), 3);

        TResultSetParser parser(resultSet);
        UNIT_ASSERT(parser.TryNextRow());

        TString createQuery = "";

        for (const auto& column : columnsMeta) {
            TValueParser parserValue(parser.GetValue(column.Name));
            parserValue.OpenOptional();
            const auto& value = parserValue.GetUtf8();

            if (column.Name == "Path") {
                UNIT_ASSERT_VALUES_EQUAL(value, path);
            } else if (column.Name == "PathType") {
                auto actualType = to_upper(TString(value));
                UNIT_ASSERT_VALUES_EQUAL(actualType, type);
            } else if (column.Name == "CreateQuery") {
                createQuery = value;
            } else {
                UNIT_FAIL("Invalid column name: " << column.Name);
            }
        }
        UNIT_ASSERT(createQuery);

        return createQuery;
    }

    std::string ShowCreateView(NQuery::TSession& session, const std::string& viewName) {
        return ShowCreate(session, "VIEW", viewName);
    }

    void DropTable(NQuery::TSession& session, const std::string& tableName) {
        ExecuteQuery(session, std::format("DROP TABLE `{}`;", tableName));
    }

    void DropView(NQuery::TSession& session, const std::string& viewName) {
        ExecuteQuery(session, std::format("DROP VIEW `{}`;", viewName));
    }

    template <typename TProtobufDescription>
    void CompareDescriptions(const TProtobufDescription& describeResultOrig, const TProtobufDescription& describeResultNew, const std::string& showCreateTableQuery) {
        TString first;
        ::google::protobuf::TextFormat::PrintToString(describeResultOrig, &first);
        TString second;
        ::google::protobuf::TextFormat::PrintToString(describeResultNew, &second);

        UNIT_ASSERT_VALUES_EQUAL_C(first, second, showCreateTableQuery);
    }

    TMaybe<Ydb::Table::DescribeTableResult> GetScheme(const NKikimrSchemeOp::TTableDescription& tableDesc) {
        Ydb::Table::DescribeTableResult scheme;

        NKikimrMiniKQL::TType mkqlKeyType;

        try {
            FillColumnDescription(scheme, mkqlKeyType, tableDesc);
        } catch (const yexception&) {
            return Nothing();
        }

        scheme.mutable_primary_key()->CopyFrom(tableDesc.GetKeyColumnNames());

        try {
            FillTableBoundary(scheme, tableDesc, mkqlKeyType);
            FillIndexDescription(scheme, tableDesc);
        } catch (const yexception&) {
            return Nothing();
        }

        FillChangefeedDescription(scheme, tableDesc);

        FillStorageSettings(scheme, tableDesc);
        FillColumnFamilies(scheme, tableDesc);
        FillPartitioningSettings(scheme, tableDesc);
        FillKeyBloomFilter(scheme, tableDesc);
        FillReadReplicasSettings(scheme, tableDesc);

        TString error;
        Ydb::StatusIds::StatusCode status;
        if (!FillSequenceDescription(scheme, tableDesc, status, error)) {
            return Nothing();
        }

        return scheme;
    }

    TMaybe<Ydb::Table::DescribeTableResult> GetScheme(const NKikimrSchemeOp::TColumnTableDescription& tableDesc) {
        Ydb::Table::DescribeTableResult scheme;

        FillColumnDescription(scheme, tableDesc);
        FillColumnFamilies(scheme, tableDesc);

        return scheme;
    }

    TString NormalizeWhitespaceInQuery(const TString& query) {
        TString result;
        result.reserve(query.size());
        for (char c : query) {
            if (!std::isspace(static_cast<unsigned char>(c))) {
                result.push_back(c);
            }
        }
        return result;
    };


private:
    TTestEnv& Env;
    TTestActorRuntime& Runtime;
    NQuery::TQueryClient QueryClient;
    NQuery::TSession Session;
};

} // namespace

Y_UNIT_TEST_SUITE(ShowCreateSystemView) {

Y_UNIT_TEST(ViewBasic) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});
    NQuery::TQueryClient queryClient(env.GetDriver());
    NQuery::TSession session(queryClient.GetSession().GetValueSync().GetSession());
    TShowCreateChecker checker(env);

    checker.CheckShowCreateView(R"(
            CREATE VIEW `test_view` WITH security_invoker = TRUE AS SELECT 1;
        )",
        "test_view",
R"(CREATE VIEW `test_view` WITH (security_invoker = TRUE) AS
SELECT
    1
;
)"
    );
}

Y_UNIT_TEST(ViewFromTable) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});
    NQuery::TQueryClient queryClient(env.GetDriver());
    NQuery::TSession session(queryClient.GetSession().GetValueSync().GetSession());
    TShowCreateChecker checker(env);

    ExecuteQuery(session, R"(
        CREATE TABLE t (
            key int,
            value utf8,
            PRIMARY KEY(key)
        );
    )");

    checker.CheckShowCreateView(
        R"(
            CREATE VIEW test_view WITH security_invoker = TRUE AS
                SELECT * FROM t;
        )", "test_view",
        R"(
            CREATE VIEW `test_view` WITH (security_invoker = TRUE) AS
                SELECT
                    *
                FROM
                    t
                ;
        )"
    );
}

Y_UNIT_TEST(ViewWithTablePathPrefix) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});
    NQuery::TQueryClient queryClient(env.GetDriver());
    NQuery::TSession session(queryClient.GetSession().GetValueSync().GetSession());
    TShowCreateChecker checker(env);

    ExecuteQuery(session, R"(
        CREATE TABLE `a/b/c/t` (
            key int,
            value utf8,
            PRIMARY KEY(key)
        );
    )");

    checker.CheckShowCreateView(
        R"(
            PRAGMA TablePathPrefix = "/Root/a/b/c";
            CREATE VIEW test_view WITH security_invoker = TRUE AS
                SELECT * FROM t;
        )", "a/b/c/test_view",
        R"(
            PRAGMA TablePathPrefix = '/Root/a/b/c';

            CREATE VIEW `test_view` WITH (security_invoker = TRUE) AS
                SELECT
                    *
                FROM
                    t
                ;
        )"
    );
}

Y_UNIT_TEST(ViewWithSingleQuotedTablePathPrefix) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});
    NQuery::TQueryClient queryClient(env.GetDriver());
    NQuery::TSession session(queryClient.GetSession().GetValueSync().GetSession());
    TShowCreateChecker checker(env);

    ExecuteQuery(session, R"(
        CREATE TABLE `a/b/c/t` (
            key int,
            value utf8,
            PRIMARY KEY(key)
        );
    )");

    checker.CheckShowCreateView(
        R"(
            -- the case of the pragma identifier does not matter, but is preserved
            pragma tabLEpathPRefix = '/Root/a/b';
            CREATE VIEW `../../test_view` WITH security_invoker = TRUE AS
                SELECT * FROM `c/t`;
        )", "test_view",
        R"(
            -- the case of the pragma identifier does not matter, but is preserved
            PRAGMA tabLEpathPRefix = '/Root/a/b';

            CREATE VIEW `../../test_view` WITH (security_invoker = TRUE) AS
                SELECT
                    *
                FROM
                    `c/t`
                ;
        )"
    );
}

Y_UNIT_TEST(ViewWithPairedTablePathPrefix) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});
    NQuery::TQueryClient queryClient(env.GetDriver());
    NQuery::TSession session(queryClient.GetSession().GetValueSync().GetSession());
    TShowCreateChecker checker(env);

    ExecuteQuery(session, R"(
        CREATE TABLE `a/b/c/t` (
            key int,
            value utf8,
            PRIMARY KEY(key)
        );
    )");

    checker.CheckShowCreateView(
        R"(
            PRAGMA TablePathPrefix ("db", "/Root/a/b/c");
            CREATE VIEW `test_view` WITH security_invoker = TRUE AS
                SELECT * FROM t;
        )", "a/b/c/test_view",
        R"(
            PRAGMA TablePathPrefix('db', '/Root/a/b/c');

            CREATE VIEW `test_view` WITH (security_invoker = TRUE) AS
                SELECT
                    *
                FROM
                    t
                ;
        )"
    );
}

Y_UNIT_TEST(ViewWithTwoTablePathPrefixes) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});
    NQuery::TQueryClient queryClient(env.GetDriver());
    NQuery::TSession session(queryClient.GetSession().GetValueSync().GetSession());
    TShowCreateChecker checker(env);

    ExecuteQuery(session, R"(
        CREATE TABLE `some/other/folder/t` (
            key int,
            value utf8,
            PRIMARY KEY(key)
        );
    )");

    checker.CheckShowCreateView(
        R"(
            PRAGMA TablePathPrefix = "/Root/a/b/c";
            PRAGMA TablePathPrefix = "/Root/some/other/folder";
            CREATE VIEW `test_view` WITH security_invoker = TRUE AS
                SELECT * FROM t;
        )", "some/other/folder/test_view",
        R"(
            PRAGMA TablePathPrefix = '/Root/a/b/c';
            PRAGMA TablePathPrefix = '/Root/some/other/folder';

            CREATE VIEW `test_view` WITH (security_invoker = TRUE) AS
                SELECT
                    *
                FROM
                    t
                ;
        )"
    );
}

Y_UNIT_TEST(TableDefaultLiteral) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Bool DEFAULT true,
                PRIMARY KEY (Key)
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` Uint32,
                `Value` Bool DEFAULT TRUE,
                PRIMARY KEY (`Key`)
            );
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE `/Root/test_show_create` (
                Key Uint32 DEFAULT 1,
                Value Int32 DEFAULT -100,
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint64 DEFAULT 100,
                Value Int64 DEFAULT -100,
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Double DEFAULT 0.5,
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Float DEFAULT CAST(4.0 AS FLOAT),
                PRIMARY KEY (Key)
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` Uint32,
                `Value` Float DEFAULT 4,
                PRIMARY KEY (`Key`)
            );
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Double DEFAULT 0.075,
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Date DEFAULT CAST('2000-01-02' as DATE),
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Datetime DEFAULT CAST('2000-01-02T02:26:51Z' as DATETIME),
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Timestamp DEFAULT CAST('2000-01-02T02:26:50.999900Z' as TIMESTAMP),
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Uuid DEFAULT Uuid("afcbef30-9ac3-481a-aa6a-8d9b785dbb0a"),
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Json DEFAULT "[12]",
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Yson DEFAULT "[13]",
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value String DEFAULT "string",
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Utf8 DEFAULT "utf8",
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Interval DEFAULT Interval("P10D"),
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Date32 DEFAULT Date32('1970-01-05'),
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Datetime64 DEFAULT Datetime64('1970-01-01T00:00:00Z'),
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Timestamp64 DEFAULT Timestamp64('1970-01-01T00:00:00Z'),
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Interval64 DEFAULT Interval64('P222D'),
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Decimal(22, 15) DEFAULT CAST("11.11" AS Decimal(22, 15)),
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Decimal(35, 10) DEFAULT CAST("110.111" AS Decimal(35, 10)),
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );
}

Y_UNIT_TEST(TablePartitionAtKeys) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 Uint64,
                Key2 String,
                Value String,
                PRIMARY KEY (Key1, Key2)
            )
            WITH (
                PARTITION_AT_KEYS = ((10), (100, "123"), (1000, "cde"))
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 Uint64,
                Key2 String,
                Key3 Utf8,
                PRIMARY KEY (Key1, Key2)
            )
            WITH (
                PARTITION_AT_KEYS = (10)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 Uint64,
                Key2 String,
                Key3 Utf8,
                PRIMARY KEY (Key1, Key2)
            )
            WITH (
                PARTITION_AT_KEYS = (10, 20, 30)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 Uint64,
                Key2 String,
                Key3 Utf8,
                PRIMARY KEY (Key1, Key2, Key3)
            )
            WITH (
                PARTITION_AT_KEYS = ((10, "str"), (10, "str", "utf"))
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                BoolValue Bool,
                Int32Value Int32,
                Uint32Value Uint32,
                Int64Value Int64,
                Uint64Value Uint64,
                StringValue String,
                Utf8Value Utf8,
                Value1 Int32 Family family1,
                Value2 Int64 Family family1,
                FAMILY family1 (),
                PRIMARY KEY (BoolValue, Int32Value, Uint32Value, Int64Value, Uint64Value, StringValue, Utf8Value)
            ) WITH (
                PARTITION_AT_KEYS = ((false), (false, 1, 2), (true, 1, 1, 1, 1, "str"), (true, 1, 1, 100, 0, "str", "utf"))
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `BoolValue` Bool,
                `Int32Value` Int32,
                `Uint32Value` Uint32,
                `Int64Value` Int64,
                `Uint64Value` Uint64,
                `StringValue` String,
                `Utf8Value` Utf8,
                `Value1` Int32 FAMILY `family1`,
                `Value2` Int64 FAMILY `family1`,
                FAMILY `family1` (),
                PRIMARY KEY (`BoolValue`, `Int32Value`, `Uint32Value`, `Int64Value`, `Uint64Value`, `StringValue`, `Utf8Value`)
            ) WITH (
                PARTITION_AT_KEYS = ((FALSE), (FALSE, 1, 2), (TRUE, 1, 1, 1, 1, 'str'), (TRUE, 1, 1, 100, 0, 'str', 'utf'))
            );
        )"
    );
}

Y_UNIT_TEST(TablePartitionByHash) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 Uint64 NOT NULL,
                Key2 String NOT NULL,
                Value String,
                PRIMARY KEY (Key1, Key2)
            )
            PARTITION BY HASH(Key1, Key2)
            WITH (
                STORE = COLUMN
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key1` Uint64 NOT NULL,
                `Key2` String NOT NULL,
                `Value` String,
                PRIMARY KEY (`Key1`, `Key2`)
            )
            PARTITION BY HASH (`Key1`, `Key2`)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 64
            );
        )"
    );
}

Y_UNIT_TEST(TableColumn) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true, .EnableOlapCompression = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 Uint64 NOT NULL,
                Key2 Utf8 NOT NULL COMPRESSION (),
                Key3 Int32 NOT NULL COMPRESSION (algorithm = off),
                Value1 Utf8 COMPRESSION (algorithm = lz4),
                Value2 Int16 COMPRESSION (algorithm = zstd),
                Value3 String COMPRESSION (algorithm = zstd, level = 10),
                PRIMARY KEY (Key1, Key2, Key3),
            )
            PARTITION BY HASH(`Key1`, `Key2`)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100,
                TTL =
                    Interval("PT10S") TO EXTERNAL DATA SOURCE `/Root/tier1`,
                    Interval("PT1H") DELETE
                    ON Key1 AS SECONDS
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key1` Uint64 NOT NULL,
                `Key2` Utf8 NOT NULL,
                `Key3` Int32 NOT NULL COMPRESSION (algorithm = off),
                `Value1` Utf8 COMPRESSION (algorithm = lz4),
                `Value2` Int16 COMPRESSION (algorithm = zstd, level = 1),
                `Value3` String COMPRESSION (algorithm = zstd, level = 10),
                PRIMARY KEY (`Key1`, `Key2`, `Key3`)
            )
            PARTITION BY HASH (`Key1`, `Key2`)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 100,
                TTL =
                    INTERVAL('PT10S') TO EXTERNAL DATA SOURCE `/Root/tier1`,
                    INTERVAL('PT1H') DELETE
                ON Key1 AS SECONDS
            );
        )"
    );
}

Y_UNIT_TEST(TablePartitionSettings) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint64 NOT NULL,
                Value1 String NOT NULL,
                Value2 Int32 NOT NULL,
                PRIMARY KEY (Key)
            )
            WITH (
                UNIFORM_PARTITIONS = 10,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 10
            );
        )", "test_show_create"
    );
}

Y_UNIT_TEST(TableReadReplicas) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint64 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            )
            WITH (
                READ_REPLICAS_SETTINGS = "PER_AZ:2"
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint64 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            )
            WITH (
                READ_REPLICAS_SETTINGS = "ANY_AZ:3"
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` Uint64 NOT NULL,
                `Value` String NOT NULL,
                PRIMARY KEY (`Key`)
            )
            WITH (READ_REPLICAS_SETTINGS = 'ANY_AZ:3');
        )"
    );
}

Y_UNIT_TEST(TableKeyBloomFilter) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint64 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            )
            WITH (
                KEY_BLOOM_FILTER = ENABLED
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint64 NOT NULL,
                Value String NOT NULL,
                PRIMARY KEY (Key)
            )
            WITH (
                KEY_BLOOM_FILTER = DISABLED
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` Uint64 NOT NULL,
                `Value` String NOT NULL,
                PRIMARY KEY (`Key`)
            )
            WITH (KEY_BLOOM_FILTER = DISABLED);
        )"
    );
}

Y_UNIT_TEST(TableTtlSettings) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Timestamp NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
            WITH (
                TTL = Interval("P1D") DELETE ON Key
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32 NOT NULL,
                PRIMARY KEY (Key)
            )
            WITH (
                TTL =
                    Interval("PT1H") DELETE ON Key AS SECONDS
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` Uint32 NOT NULL,
                PRIMARY KEY (`Key`)
            )
            WITH (TTL = INTERVAL('PT1H') DELETE ON Key AS SECONDS);
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(`Key`)
            WITH (
                STORE = COLUMN,
                TTL = INTERVAL('PT1H') DELETE ON Key AS MILLISECONDS
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(`Key`)
            WITH (
                STORE = COLUMN,
                TTL =
                    INTERVAL('PT1H') TO EXTERNAL DATA SOURCE `/Root/tier2`,
                    INTERVAL('PT3H') DELETE
                ON Key AS NANOSECONDS
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint64 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(`Key`)
            WITH (
                STORE = COLUMN,
                TTL = INTERVAL('PT1H') TO EXTERNAL DATA SOURCE `/Root/tier2` ON Key AS MICROSECONDS
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Timestamp NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(`Key`)
            WITH (
                STORE = COLUMN,
                TTL =
                    Interval("PT10S") TO EXTERNAL DATA SOURCE `/Root/tier1`,
                    Interval("PT1M") TO EXTERNAL DATA SOURCE `/Root/tier2`,
                    Interval("PT1H") DELETE
                    ON Key
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` Timestamp NOT NULL,
                `Value` String,
                PRIMARY KEY (`Key`)
            )
            PARTITION BY HASH (`Key`)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 64,
                TTL =
                    INTERVAL('PT10S') TO EXTERNAL DATA SOURCE `/Root/tier1`,
                    INTERVAL('PT1M') TO EXTERNAL DATA SOURCE `/Root/tier2`,
                    INTERVAL('PT1H') DELETE
                ON Key
            );
        )"
    );
}

Y_UNIT_TEST(TableTemporary) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TEMPORARY TABLE test_show_create (
                Key Int32 NOT NULL,
                Value String,
                PRIMARY KEY (Key)
            );
        )", "test_show_create",
        R"(
            CREATE TEMPORARY TABLE `test_show_create` (
                `Key` Int32 NOT NULL,
                `Value` String,
                PRIMARY KEY (`Key`)
            );
        )", true
    );
}

Y_UNIT_TEST(Table) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true, .EnableFulltextIndex = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE `/Root/test_show_create` (
                Key Uint32,
                Value Uint32,
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint32,
                Value Uint32,
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 Int64 NOT NULL,
                Key2 Utf8 NOT NULL,
                Key3 PgInt2 NOT NULL,
                Value1 Utf8,
                Value2 Bool,
                Value3 String,
                PRIMARY KEY (Key1, Key2, Key3),
                INDEX Index1 GLOBAL USING vector_kmeans_tree ON (`Value3`) WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2)
            );
            ALTER TABLE test_show_create ADD INDEX Index2 GLOBAL SYNC ON (Key2, Value1, Value2);
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint64,
                Text String,
                Data String,
                PRIMARY KEY (Key),
                INDEX fulltext_idx GLOBAL USING fulltext_plain ON (Text) WITH (tokenizer=standard, use_filter_lowercase=true, use_filter_length=true, filter_length_min=3)
            );
            ALTER TABLE test_show_create ADD INDEX Index2 GLOBAL SYNC ON (Data);
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint64,
                BoolValue Bool,
                Int32Value Int32,
                Uint32Value Uint32,
                Int64Value Int64,
                Uint64Value Uint64,
                FloatValue Float,
                DoubleValue Double,
                StringValue String,
                Utf8Value Utf8,
                DateValue Date,
                DatetimeValue Datetime,
                TimestampValue Timestamp,
                IntervalValue Interval,
                DecimalValue1 Decimal(22,9),
                DecimalValue2 Decimal(35,10),
                JsonValue Json,
                YsonValue Yson,
                JsonDocumentValue JsonDocument,
                DyNumberValue DyNumber,
                Int32NotNullValue Int32 NOT NULL,
                PRIMARY KEY (Key)
            );
        )", "test_show_create"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 Int64 NOT NULL DEFAULT -100,
                Key2 Utf8 NOT NULL,
                Key3 BigSerial NOT NULL,
                Value1 Utf8 FAMILY Family1,
                Value2 Bool FAMILY Family2,
                Value3 String FAMILY Family2,
                INDEX Index1 GLOBAL USING vector_kmeans_tree ON (`Value3`) WITH (distance=cosine, vector_type="uint8", vector_dimension=2, levels=1, clusters=2),
                PRIMARY KEY (Key1, Key2, Key3),
                FAMILY Family1 (
                    DATA = "test0",
                    COMPRESSION = "off"
                ),
                FAMILY Family2 (
                    DATA = "test1",
                    COMPRESSION = "lz4"
                )
            ) WITH (
                AUTO_PARTITIONING_PARTITION_SIZE_MB = 1000
            );
            ALTER TABLE test_show_create ADD INDEX Index2 GLOBAL ASYNC ON (Key2, Value1, Value2);
            ALTER TABLE test_show_create ADD INDEX Index3 GLOBAL ASYNC ON (Key3, Value2) COVER (Value1, Value3);
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key1` Int64 NOT NULL DEFAULT -100,
                `Key2` Utf8 NOT NULL,
                `Key3` Serial8 NOT NULL,
                `Value1` Utf8 FAMILY `Family1`,
                `Value2` Bool FAMILY `Family2`,
                `Value3` String FAMILY `Family2`,
                INDEX `Index1` GLOBAL USING vector_kmeans_tree ON (`Value3`) WITH (distance = cosine, vector_type = 'uint8', vector_dimension = 2, clusters = 2, levels = 1),
                INDEX `Index2` GLOBAL ASYNC ON (`Key2`, `Value1`, `Value2`),
                INDEX `Index3` GLOBAL ASYNC ON (`Key3`, `Value2`) COVER (`Value1`, `Value3`),
                FAMILY `Family1` (DATA = 'test0', COMPRESSION = 'off'),
                FAMILY `Family2` (DATA = 'test1', COMPRESSION = 'lz4'),
                PRIMARY KEY (`Key1`, `Key2`, `Key3`)
            )
            WITH (
                AUTO_PARTITIONING_BY_SIZE = ENABLED,
                AUTO_PARTITIONING_PARTITION_SIZE_MB = 1000
            );
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 Int64 NOT NULL DEFAULT -100,
                Key2 Utf8 NOT NULL,
                Key3 BigSerial NOT NULL,
                Value1 Utf8,
                Value2 Bool,
                Value3 STRING,
                Value4 Timestamp DEFAULT CAST('2000-01-02T02:26:50.999900Z' as TIMESTAMP),
                Value5 String,
                INDEX Index2 GLOBAL USING vector_kmeans_tree ON (Value5) COVER (Value1, Value3) WITH (distance=manhattan, vector_type=float, vector_dimension=2, clusters=2, levels=1),
                PRIMARY KEY (Key1, Key2, Key3),
            ) WITH (
                TTL = Interval("PT1H") DELETE ON Value4,
                KEY_BLOOM_FILTER = ENABLED,
                PARTITION_AT_KEYS = ((10), (100, "123"), (1000, "cde")),
                AUTO_PARTITIONING_BY_LOAD = ENABLED
            );
            ALTER TABLE test_show_create ADD INDEX Index1 GLOBAL ASYNC ON (Key2, Value1, Value2) COVER (Value5, Value3);
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key1` Int64 NOT NULL DEFAULT -100,
                `Key2` Utf8 NOT NULL,
                `Key3` Serial8 NOT NULL,
                `Value1` Utf8,
                `Value2` Bool,
                `Value3` String,
                `Value4` Timestamp DEFAULT TIMESTAMP('2000-01-02T02:26:50.999900Z'),
                `Value5` String,
                INDEX `Index1` GLOBAL ASYNC ON (`Key2`, `Value1`, `Value2`) COVER (`Value5`, `Value3`),
                INDEX `Index2` GLOBAL USING vector_kmeans_tree ON (`Value5`) COVER (`Value1`, `Value3`) WITH (distance = manhattan, vector_type = 'float', vector_dimension = 2, clusters = 2, levels = 1),
                PRIMARY KEY (`Key1`, `Key2`, `Key3`)
            )
            WITH (
                AUTO_PARTITIONING_BY_LOAD = ENABLED,
                PARTITION_AT_KEYS = ((10), (100, '123'), (1000, 'cde')),
                KEY_BLOOM_FILTER = ENABLED,
                TTL = INTERVAL('PT1H') DELETE ON Value4
            );
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 Uint32,
                Key2 BigSerial,
                Key3 SmallSerial,
                Value1 Serial,
                Value2 String,
                PRIMARY KEY (Key1, Key2, Key3)
            );
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_1` WITH (MODE = 'OLD_IMAGE', FORMAT = 'DEBEZIUM_JSON', RETENTION_PERIOD = Interval("PT1H"));
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_2` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON', TOPIC_MIN_ACTIVE_PARTITIONS = 10, RETENTION_PERIOD = Interval("PT3H"), VIRTUAL_TIMESTAMPS = TRUE);
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_3` WITH (MODE = 'KEYS_ONLY', TOPIC_MIN_ACTIVE_PARTITIONS = 3, FORMAT = 'JSON', RETENTION_PERIOD = Interval("PT30M"));
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key2`
                START WITH 150
                INCREMENT BY 300;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key2`
                INCREMENT 1;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key3`
                RESTART WITH 5;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Value1`
                START WITH 101;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Value1`
                INCREMENT 404
                RESTART;
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key1` Uint32,
                `Key2` Serial8 NOT NULL,
                `Key3` Serial2 NOT NULL,
                `Value1` Serial4 NOT NULL,
                `Value2` String,
                PRIMARY KEY (`Key1`, `Key2`, `Key3`)
            );

            ALTER TABLE `test_show_create`
                ADD CHANGEFEED `feed_1` WITH (MODE = 'OLD_IMAGE', FORMAT = 'DEBEZIUM_JSON', RETENTION_PERIOD = INTERVAL('PT1H'), TOPIC_MIN_ACTIVE_PARTITIONS = 1)
            ;

            ALTER TABLE `test_show_create`
                ADD CHANGEFEED `feed_2` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON', VIRTUAL_TIMESTAMPS = TRUE, RETENTION_PERIOD = INTERVAL('PT3H'), TOPIC_MIN_ACTIVE_PARTITIONS = 10)
            ;

            ALTER TABLE `test_show_create`
                ADD CHANGEFEED `feed_3` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = INTERVAL('PT30M'), TOPIC_MIN_ACTIVE_PARTITIONS = 3)
            ;

            ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key2` START WITH 150;

            ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key3` RESTART WITH 5;

            ALTER SEQUENCE `/Root/test_show_create/_serial_column_Value1` START WITH 101 INCREMENT BY 404 RESTART;
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 BigSerial,
                Key2 SmallSerial,
                Value1 Serial,
                Value2 String,
                PRIMARY KEY (Key1, Key2)
            ) WITH (
                AUTO_PARTITIONING_BY_LOAD = ENABLED,
                PARTITION_AT_KEYS = ((10), (100, 1000), (1000, 20))
            );
            ALTER TABLE test_show_create ADD CHANGEFEED `feed1` WITH (
                MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = Interval("PT1H")
            );
            ALTER TABLE test_show_create ADD CHANGEFEED `feed2` WITH (
                MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = Interval("PT2H")
            );
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key1`
                START WITH 150
                INCREMENT BY 300;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key1`
                INCREMENT 1;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key2`
                RESTART WITH 5;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Value1`
                START WITH 101;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Value1`
                INCREMENT 404
                RESTART;
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key1` Serial8 NOT NULL,
                `Key2` Serial2 NOT NULL,
                `Value1` Serial4 NOT NULL,
                `Value2` String,
                PRIMARY KEY (`Key1`, `Key2`)
            )
            WITH (
                AUTO_PARTITIONING_BY_LOAD = ENABLED,
                PARTITION_AT_KEYS = ((10), (100, 1000), (1000, 20))
            );

            ALTER TABLE `test_show_create`
                ADD CHANGEFEED `feed1` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = INTERVAL('PT1H'))
            ;

            ALTER TABLE `test_show_create`
                ADD CHANGEFEED `feed2` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = INTERVAL('PT2H'))
            ;

            ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key1` START WITH 150;

            ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key2` RESTART WITH 5;

            ALTER SEQUENCE `/Root/test_show_create/_serial_column_Value1` START WITH 101 INCREMENT BY 404 RESTART;
        )"
    );
}

Y_UNIT_TEST(TableChangefeeds) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            ALTER TABLE test_show_create ADD CHANGEFEED `feed` WITH (
                MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = Interval("PT1H")
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` Uint64,
                `Value` String,
                PRIMARY KEY (`Key`)
            );

            ALTER TABLE `test_show_create`
                ADD CHANGEFEED `feed` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = INTERVAL('PT1H'), TOPIC_MIN_ACTIVE_PARTITIONS = 1)
            ;
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_1` WITH (MODE = 'OLD_IMAGE', FORMAT = 'DEBEZIUM_JSON', RETENTION_PERIOD = Interval("PT1H"));
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_2` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON', TOPIC_MIN_ACTIVE_PARTITIONS = 10, RETENTION_PERIOD = Interval("PT3H"), VIRTUAL_TIMESTAMPS = TRUE);
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` Uint64,
                `Value` String,
                PRIMARY KEY (`Key`)
            );

            ALTER TABLE `test_show_create`
                ADD CHANGEFEED `feed_1` WITH (MODE = 'OLD_IMAGE', FORMAT = 'DEBEZIUM_JSON', RETENTION_PERIOD = INTERVAL('PT1H'), TOPIC_MIN_ACTIVE_PARTITIONS = 1)
            ;

            ALTER TABLE `test_show_create`
                ADD CHANGEFEED `feed_2` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON', VIRTUAL_TIMESTAMPS = TRUE, RETENTION_PERIOD = INTERVAL('PT3H'), TOPIC_MIN_ACTIVE_PARTITIONS = 10)
            ;
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key String,
                Value String,
                PRIMARY KEY (Key)
            );
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON');
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` String,
                `Value` String,
                PRIMARY KEY (`Key`)
            );

            ALTER TABLE `test_show_create`
                ADD CHANGEFEED `feed` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = INTERVAL('P1D'))
            ;
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key String,
                Value String,
                PRIMARY KEY (Key)
            );
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', SCHEMA_CHANGES = TRUE);
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` String,
                `Value` String,
                PRIMARY KEY (`Key`)
            );

            ALTER TABLE `test_show_create`
                ADD CHANGEFEED `feed` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', SCHEMA_CHANGES = TRUE, RETENTION_PERIOD = INTERVAL('P1D'))
            ;
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_1` WITH (MODE = 'OLD_IMAGE', FORMAT = 'DEBEZIUM_JSON', RETENTION_PERIOD = Interval("PT1H"));
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_2` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON', TOPIC_MIN_ACTIVE_PARTITIONS = 10, RETENTION_PERIOD = Interval("PT3H"), VIRTUAL_TIMESTAMPS = TRUE);
            ALTER TABLE test_show_create
                ADD CHANGEFEED `feed_3` WITH (MODE = 'KEYS_ONLY', TOPIC_MIN_ACTIVE_PARTITIONS = 3, FORMAT = 'JSON', RETENTION_PERIOD = Interval("PT30M"), INITIAL_SCAN = TRUE);
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` Uint64,
                `Value` String,
                PRIMARY KEY (`Key`)
            );

            ALTER TABLE `test_show_create`
                ADD CHANGEFEED `feed_1` WITH (MODE = 'OLD_IMAGE', FORMAT = 'DEBEZIUM_JSON', RETENTION_PERIOD = INTERVAL('PT1H'), TOPIC_MIN_ACTIVE_PARTITIONS = 1)
            ;

            ALTER TABLE `test_show_create`
                ADD CHANGEFEED `feed_2` WITH (MODE = 'NEW_IMAGE', FORMAT = 'JSON', VIRTUAL_TIMESTAMPS = TRUE, RETENTION_PERIOD = INTERVAL('PT3H'), TOPIC_MIN_ACTIVE_PARTITIONS = 10)
            ;

            ALTER TABLE `test_show_create`
                ADD CHANGEFEED `feed_3` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = INTERVAL('PT30M'), TOPIC_MIN_ACTIVE_PARTITIONS = 3, INITIAL_SCAN = TRUE)
            ;
        )", false, true
    );
}

Y_UNIT_TEST(TableChangefeedAfterInitialScan) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    auto session = NQuery::TQueryClient(env.GetDriver()).GetSession().GetValueSync().GetSession();

    ExecuteQuery(session, R"(
        CREATE TABLE test_show_create (
            Key Uint64,
            Value String,
            PRIMARY KEY (Key)
        );
        ALTER TABLE test_show_create
            ADD CHANGEFEED `feed` WITH (MODE = 'KEYS_ONLY', FORMAT = 'JSON', RETENTION_PERIOD = Interval("PT30M"), INITIAL_SCAN = TRUE);
    )");

    // Wait for the initial scan to complete (state transitions from ECdcStreamStateScan to ECdcStreamStateReady)
    checker.WaitForCdcStreamReady("/Root/test_show_create/feed");

    // After the scan is complete, SHOW CREATE TABLE must still include INITIAL_SCAN = TRUE
    auto showCreateTableQuery = checker.ShowCreateTable(session, "test_show_create");
    UNIT_ASSERT_C(showCreateTableQuery.contains("INITIAL_SCAN = TRUE"),
        "INITIAL_SCAN = TRUE must be present in SHOW CREATE TABLE output after initial scan completes: "
        << showCreateTableQuery);

    ExecuteQuery(session, "DROP TABLE `test_show_create`;");
}

Y_UNIT_TEST(TableSequences) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key Serial,
                Value String,
                PRIMARY KEY (Key)
            );
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key`
                START 50
                INCREMENT BY 11;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key`
                RESTART;
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` Serial4 NOT NULL,
                `Value` String,
                PRIMARY KEY (`Key`)
            );

            ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key` START WITH 50 INCREMENT BY 11 RESTART;
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 BigSerial,
                Key2 SmallSerial,
                Value String,
                PRIMARY KEY (Key1, Key2)
            );
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key1`
                START WITH 50
                INCREMENT BY 11;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key2`
                RESTART WITH 5;
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key1` Serial8 NOT NULL,
                `Key2` Serial2 NOT NULL,
                `Value` String,
                PRIMARY KEY (`Key1`, `Key2`)
            );

            ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key1` START WITH 50 INCREMENT BY 11;

            ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key2` RESTART WITH 5;
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 BigSerial,
                Key2 SmallSerial,
                Value1 Serial,
                Value2 String,
                PRIMARY KEY (Key1, Key2)
            );
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key1`
                START WITH 150
                INCREMENT BY 300;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key1`
                INCREMENT 1;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Key2`
                RESTART WITH 5;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Value1`
                START WITH 101;
            ALTER SEQUENCE IF EXISTS `/Root/test_show_create/_serial_column_Value1`
                INCREMENT 404
                RESTART;
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key1` Serial8 NOT NULL,
                `Key2` Serial2 NOT NULL,
                `Value1` Serial4 NOT NULL,
                `Value2` String,
                PRIMARY KEY (`Key1`, `Key2`)
            );

            ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key1` START WITH 150;

            ALTER SEQUENCE `/Root/test_show_create/_serial_column_Key2` RESTART WITH 5;

            ALTER SEQUENCE `/Root/test_show_create/_serial_column_Value1` START WITH 101 INCREMENT BY 404 RESTART;
        )"
    );
}

Y_UNIT_TEST(TablePartitionPolicyIndexTable) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 Int64 NOT NULL,
                Key2 Utf8 NOT NULL,
                Key3 PgInt2 NOT NULL,
                Value1 Utf8,
                Value2 Bool,
                Value3 String,
                INDEX Index1 GLOBAL SYNC ON (Key2, Value1, Value2),
                PRIMARY KEY (Key1, Key2, Key3)
            );
            ALTER TABLE test_show_create ALTER INDEX Index1 SET (
                AUTO_PARTITIONING_BY_LOAD = ENABLED,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 5000
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key1` Int64 NOT NULL,
                `Key2` Utf8 NOT NULL,
                `Key3` pgint2 NOT NULL,
                `Value1` Utf8,
                `Value2` Bool,
                `Value3` String,
                INDEX `Index1` GLOBAL SYNC ON (`Key2`, `Value1`, `Value2`),
                PRIMARY KEY (`Key1`, `Key2`, `Key3`)
            );

            ALTER TABLE `test_show_create`
                ALTER INDEX `Index1` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 5000)
            ;
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 Int64 NOT NULL DEFAULT -100,
                Key2 Utf8 NOT NULL,
                Key3 BigSerial NOT NULL,
                Value1 Utf8 FAMILY Family1,
                Value2 Bool FAMILY Family2,
                Value3 String FAMILY Family2,
                INDEX Index1 GLOBAL ASYNC ON (Key2, Value1, Value2),
                INDEX Index2 GLOBAL ASYNC ON (Key3, Value2) COVER (Value1, Value3),
                PRIMARY KEY (Key1, Key2, Key3),
                FAMILY Family1 (
                    DATA = "test0",
                    COMPRESSION = "off"
                ),
                FAMILY Family2 (
                    DATA = "test1",
                    COMPRESSION = "lz4"
                )
            ) WITH (
                AUTO_PARTITIONING_PARTITION_SIZE_MB = 1000
            );
            ALTER TABLE test_show_create ALTER INDEX Index1 SET (
                AUTO_PARTITIONING_BY_LOAD = ENABLED,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2000
            );
            ALTER TABLE test_show_create ALTER INDEX Index2 SET (
                AUTO_PARTITIONING_BY_SIZE = ENABLED,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key1` Int64 NOT NULL DEFAULT -100,
                `Key2` Utf8 NOT NULL,
                `Key3` Serial8 NOT NULL,
                `Value1` Utf8 FAMILY `Family1`,
                `Value2` Bool FAMILY `Family2`,
                `Value3` String FAMILY `Family2`,
                INDEX `Index1` GLOBAL ASYNC ON (`Key2`, `Value1`, `Value2`),
                INDEX `Index2` GLOBAL ASYNC ON (`Key3`, `Value2`) COVER (`Value1`, `Value3`),
                FAMILY `Family1` (DATA = 'test0', COMPRESSION = 'off'),
                FAMILY `Family2` (DATA = 'test1', COMPRESSION = 'lz4'),
                PRIMARY KEY (`Key1`, `Key2`, `Key3`)
            )
            WITH (
                AUTO_PARTITIONING_BY_SIZE = ENABLED,
                AUTO_PARTITIONING_PARTITION_SIZE_MB = 1000
            );

            ALTER TABLE `test_show_create`
                ALTER INDEX `Index1` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2000)
            ;

            ALTER TABLE `test_show_create`
                ALTER INDEX `Index2` SET (AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 100)
            ;
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE test_show_create (
                Key1 Int64 NOT NULL,
                Key2 Utf8 NOT NULL,
                Key3 PgInt2 NOT NULL,
                Value1 Utf8,
                Value2 Bool,
                Value3 String,
                INDEX Index1 GLOBAL SYNC ON (Key2, Value1, Value2),
                INDEX Index2 GLOBAL ASYNC ON (Key3, Value1) COVER (Value2, Value3),
                INDEX Index3 GLOBAL SYNC ON (Key1, Key2, Value1),
                PRIMARY KEY (Key1, Key2, Key3)
            );
            ALTER TABLE test_show_create ALTER INDEX Index1 SET (
                AUTO_PARTITIONING_BY_LOAD = ENABLED,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 5000,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1000
            );
            ALTER TABLE test_show_create ALTER INDEX Index2 SET (
                AUTO_PARTITIONING_BY_SIZE = ENABLED,
                AUTO_PARTITIONING_PARTITION_SIZE_MB = 10000,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 2700
            );
            ALTER TABLE test_show_create ALTER INDEX Index3 SET (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3500
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key1` Int64 NOT NULL,
                `Key2` Utf8 NOT NULL,
                `Key3` pgint2 NOT NULL,
                `Value1` Utf8,
                `Value2` Bool,
                `Value3` String,
                INDEX `Index1` GLOBAL SYNC ON (`Key2`, `Value1`, `Value2`),
                INDEX `Index2` GLOBAL ASYNC ON (`Key3`, `Value1`) COVER (`Value2`, `Value3`),
                INDEX `Index3` GLOBAL SYNC ON (`Key1`, `Key2`, `Value1`),
                PRIMARY KEY (`Key1`, `Key2`, `Key3`)
            );

            ALTER TABLE `test_show_create`
                ALTER INDEX `Index1` SET (AUTO_PARTITIONING_BY_LOAD = ENABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1000, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 5000)
            ;

            ALTER TABLE `test_show_create`
                ALTER INDEX `Index2` SET (AUTO_PARTITIONING_BY_SIZE = ENABLED, AUTO_PARTITIONING_PARTITION_SIZE_MB = 10000, AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 2700)
            ;

            ALTER TABLE `test_show_create`
                ALTER INDEX `Index3` SET (AUTO_PARTITIONING_BY_SIZE = DISABLED, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 3500)
            ;
        )"
    );
}

Y_UNIT_TEST(TableColumnAlterColumn) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true, .AlterObjectEnabled = true, .EnableSparsedColumns = true, .EnableOlapCompression = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE `/Root/test_show_create` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                Col3 Uint32,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `FORCE_SIMD_PARSING`=`true`, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `OTHERS_ALLOWED_FRACTION`=`0.5`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `ENCODING.DICTIONARY.ENABLED`=`true`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col3, `DEFAULT_VALUE`=`5`);
            ALTER TABLE `/Root/test_show_create` ALTER COLUMN Col2 SET COMPRESSION (algorithm=zstd, level=4);
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Col1` Uint64 NOT NULL,
                `Col2` JsonDocument COMPRESSION (algorithm = zstd, level = 4),
                `Col3` Uint32,
                PRIMARY KEY (`Col1`)
            )
            PARTITION BY HASH (`Col1`)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2
            );

            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = ALTER_COLUMN, NAME = Col2, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME` = `SUB_COLUMNS`, `SPARSED_DETECTOR_KFF` = `20`, `COLUMNS_LIMIT` = `1024`, `MEM_LIMIT_CHUNK` = `52428800`, `OTHERS_ALLOWED_FRACTION` = `0.5`, `DATA_EXTRACTOR_CLASS_NAME` = `JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY` = `false`, `FORCE_SIMD_PARSING` = `true`, `ENCODING.DICTIONARY.ENABLED` = `true`);

            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = ALTER_COLUMN, NAME = Col3, `DEFAULT_VALUE` = `5`);
        )"
    );
}

Y_UNIT_TEST(TableColumnUpsertOptions) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true, .AlterObjectEnabled = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE `/Root/test_show_create` (
                Col1 Uint64 NOT NULL,
                Col2 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`,
                `COMPACTION_PLANNER.FEATURES`=`{"levels" : [{"class_name" : "Zero", "portions_live_duration" : "5s", "expected_blobs_size" : 1000000000000, "portions_count_available" : 2},
                                {"class_name" : "Zero"}]}`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `METADATA_MEMORY_MANAGER.CLASS_NAME`=`local_db`,
                    `METADATA_MEMORY_MANAGER.FEATURES`=`{"memory_cache_size" : 0}`);
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Col1` Uint64 NOT NULL,
                `Col2` JsonDocument,
                PRIMARY KEY (`Col1`)
            )
            PARTITION BY HASH (`Col1`)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2
            );

            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME` = 'SIMPLE', `COMPACTION_PLANNER.CLASS_NAME` = 'lc-buckets', `COMPACTION_PLANNER.FEATURES` = `{"levels":[{"portions_count_available":2,"portions_live_duration":"5.000000s","class_name":"Zero","expected_blobs_size":1000000000000},{"class_name":"Zero"}]}`, `METADATA_MEMORY_MANAGER.CLASS_NAME` = 'local_db', `METADATA_MEMORY_MANAGER.FEATURES` = `{"memory_cache_size":0,"fetch_on_start":false}`);
        )"
    );
}

Y_UNIT_TEST(TableColumnUpsertIndex) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true, .AlterObjectEnabled = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE `/Root/test_show_create` (
                Col1 Uint64 NOT NULL,
                Col2 Uint32 NOT NULL,
                Col3 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=count_min_sketch_index, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['Col2']}`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=bloom_ngramm_filter_index, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "Col3", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096,
                        "records_count" : 1024, "case_sensitive" : false, "data_extractor" : {"class_name" : "SUB_COLUMN", "sub_column_name" : '"b.c.d"'}}`);
            ALTER OBJECT `Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=bloom_filter_index, TYPE=BLOOM_FILTER,
                    FEATURES=`{"column_name" : "Col2", "false_positive_probability" : 0.01, "bits_storage_type": "BITSET"}`);
            ALTER OBJECT `Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=max_index, TYPE=MAX, FEATURES=`{"column_name": "Col2"}`);
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Col1` Uint64 NOT NULL,
                `Col2` Uint32 NOT NULL,
                `Col3` JsonDocument,
                PRIMARY KEY (`Col1`)
            )
            PARTITION BY HASH (`Col1`)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2
            );

            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = max_index, TYPE = MAX, FEATURES = `{"column_name":"Col2"}`);

            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = count_min_sketch_index, TYPE = COUNT_MIN_SKETCH, FEATURES = `{"column_names":["Col2"]}`);

            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = bloom_ngramm_filter_index, TYPE = BLOOM_NGRAMM_FILTER, FEATURES = `{"bits_storage_type":"SIMPLE_STRING","records_count":1024,"case_sensitive":false,"ngramm_size":3,"filter_size_bytes":4096,"data_extractor":{"class_name":"SUB_COLUMN","sub_column_name":"\\\"b.c.d\\\""},"hashes_count":2,"column_name":"Col3"}`);

            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = bloom_filter_index, TYPE = BLOOM_FILTER, FEATURES = `{"false_positive_probability":0.01,"data_extractor":{"class_name":"DEFAULT"},"bits_storage_type":"BITSET","column_name":"Col2"}`);
        )"
    );
}

Y_UNIT_TEST(TableColumnAlterObject) {
    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true, .AlterObjectEnabled = true, .EnableSparsedColumns = true, .EnableOlapCompression = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE `/Root/test_show_create` (
                Col1 Uint64 NOT NULL,
                Col2 Uint32 NOT NULL,
                Col3 JsonDocument,
                PRIMARY KEY (Col1)
            )
            PARTITION BY HASH(Col1)
            WITH (STORE = COLUMN, AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=count_min_sketch_index, TYPE=COUNT_MIN_SKETCH,
                    FEATURES=`{"column_names" : ['Col2']}`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=bloom_ngramm_filter_index, TYPE=BLOOM_NGRAMM_FILTER,
                FEATURES=`{"column_name" : "Col2", "ngramm_size" : 3, "hashes_count" : 2, "filter_size_bytes" : 4096,
                        "records_count" : 1024, "case_sensitive" : true, "data_extractor" : {"class_name" : "SUB_COLUMN", "sub_column_name" : "a"}}`);
            ALTER OBJECT `Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=bloom_filter_index, TYPE=BLOOM_FILTER,
                FEATURES=`{"column_name" : "Col2", "false_positive_probability" : 0.01}`);
            ALTER OBJECT `Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=max_index, TYPE=MAX, FEATURES=`{"column_name": "Col2"}`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`,
                `COMPACTION_PLANNER.FEATURES`=`{"levels" : [{"class_name" : "Zero", "portions_live_duration" : "180s", "expected_blobs_size" : 2048000},
                            {"class_name" : "Zero", "expected_blobs_size" : 2048000}, {"class_name" : "Zero"}]}`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `METADATA_MEMORY_MANAGER.CLASS_NAME`=`local_db`,
                    `METADATA_MEMORY_MANAGER.FEATURES`=`{"memory_cache_size" : 0}`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col3, `FORCE_SIMD_PARSING`=`true`, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`, `OTHERS_ALLOWED_FRACTION`=`0.5`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col3, `ENCODING.DICTIONARY.ENABLED`=`true`);
            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Col2, `DEFAULT_VALUE`=`100`);
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Col1` Uint64 NOT NULL,
                `Col2` Uint32 NOT NULL,
                `Col3` JsonDocument,
                PRIMARY KEY (`Col1`)
            )
            PARTITION BY HASH (`Col1`)
            WITH (
                STORE = COLUMN,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 2
            );

            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = ALTER_COLUMN, NAME = Col2, `DEFAULT_VALUE` = `100`);

            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = ALTER_COLUMN, NAME = Col3, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME` = `SUB_COLUMNS`, `SPARSED_DETECTOR_KFF` = `20`, `COLUMNS_LIMIT` = `1024`, `MEM_LIMIT_CHUNK` = `52428800`, `OTHERS_ALLOWED_FRACTION` = `0.5`, `DATA_EXTRACTOR_CLASS_NAME` = `JSON_SCANNER`, `SCAN_FIRST_LEVEL_ONLY` = `false`, `FORCE_SIMD_PARSING` = `true`, `ENCODING.DICTIONARY.ENABLED` = `true`);

            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = max_index, TYPE = MAX, FEATURES = `{"column_name":"Col2"}`);

            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = count_min_sketch_index, TYPE = COUNT_MIN_SKETCH, FEATURES = `{"column_names":["Col2"]}`);

            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = bloom_ngramm_filter_index, TYPE = BLOOM_NGRAMM_FILTER, FEATURES = `{"bits_storage_type":"SIMPLE_STRING","records_count":1024,"case_sensitive":true,"ngramm_size":3,"filter_size_bytes":4096,"data_extractor":{"class_name":"SUB_COLUMN","sub_column_name":"a"},"hashes_count":2,"column_name":"Col2"}`);

            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_INDEX, NAME = bloom_filter_index, TYPE = BLOOM_FILTER, FEATURES = `{"false_positive_probability":0.01,"data_extractor":{"class_name":"DEFAULT"},"bits_storage_type":"SIMPLE_STRING","column_name":"Col2"}`);

            ALTER OBJECT `/Root/test_show_create` (TYPE TABLE) SET (ACTION = UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME` = 'SIMPLE', `COMPACTION_PLANNER.CLASS_NAME` = 'lc-buckets', `COMPACTION_PLANNER.FEATURES` = `{"levels":[{"portions_live_duration":"180.000000s","class_name":"Zero","expected_blobs_size":2048000},{"class_name":"Zero","expected_blobs_size":2048000},{"class_name":"Zero"}]}`, `METADATA_MEMORY_MANAGER.CLASS_NAME` = 'local_db', `METADATA_MEMORY_MANAGER.FEATURES` = `{"memory_cache_size":0,"fetch_on_start":false}`);
        )"
    );
}

Y_UNIT_TEST(TableFamilyParameters) {
    TTestEnv env(1, 4, {.StoragePools = 4, .ShowCreateTable = true, .EnableTableCacheModes = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_YQL, NActors::NLog::PRI_TRACE);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    TShowCreateChecker checker(env);

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE `/Root/test_show_create` (
                Key Uint32,
                Value0 String,
                Value1 String FAMILY Family1,
                Value2 String FAMILY Family2,
                Value3 String FAMILY Family3,
                Value4 String FAMILY Family4,
                Value5 String FAMILY Family5,
                Value6 String FAMILY Family6,
                PRIMARY KEY (Key),
                FAMILY default (
                    DATA = "test0",
                    COMPRESSION = "lz4",
                    CACHE_MODE = "in_memory"
                ),
                FAMILY Family1 (
                    COMPRESSION = "off",
                    CACHE_MODE = "regular"
                ),
                FAMILY Family2 (
                    DATA = "test1",
                    CACHE_MODE = "in_memory"
                ),
                FAMILY Family3 (
                    DATA = "test2",
                    COMPRESSION = "lz4"
                ),
                FAMILY Family4 (
                    DATA = "test3"
                ),
                FAMILY Family5 (
                    COMPRESSION = "off"
                ),
                FAMILY Family6 (
                    CACHE_MODE = "regular"
                )
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` Uint32,
                `Value0` String,
                `Value1` String FAMILY `Family1`,
                `Value2` String FAMILY `Family2`,
                `Value3` String FAMILY `Family3`,
                `Value4` String FAMILY `Family4`,
                `Value5` String FAMILY `Family5`,
                `Value6` String FAMILY `Family6`,
                FAMILY `default` (DATA = 'test0', COMPRESSION = 'lz4', CACHE_MODE = 'in_memory'),
                FAMILY `Family1` (COMPRESSION = 'off', CACHE_MODE = 'regular'),
                FAMILY `Family2` (DATA = 'test1', CACHE_MODE = 'in_memory'),
                FAMILY `Family3` (DATA = 'test2', COMPRESSION = 'lz4'),
                FAMILY `Family4` (DATA = 'test3'),
                FAMILY `Family5` (COMPRESSION = 'off'),
                FAMILY `Family6` (CACHE_MODE = 'regular'),
                PRIMARY KEY (`Key`)
            );
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE `/Root/test_show_create` (
                Key Uint32,
                Value0 String,
                PRIMARY KEY (Key),
                FAMILY default (
                    DATA = "test0"
                )
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` Uint32,
                `Value0` String,
                FAMILY `default` (DATA = 'test0'),
                PRIMARY KEY (`Key`)
            );
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE `/Root/test_show_create` (
                Key Uint32,
                Value0 String,
                PRIMARY KEY (Key),
                FAMILY default (
                    COMPRESSION = "lz4"
                )
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` Uint32,
                `Value0` String,
                FAMILY `default` (COMPRESSION = 'lz4'),
                PRIMARY KEY (`Key`)
            );
        )"
    );

    checker.CheckShowCreateTable(
        R"(
            CREATE TABLE `/Root/test_show_create` (
                Key Uint32,
                Value0 String,
                PRIMARY KEY (Key),
                FAMILY default (
                    CACHE_MODE = "in_memory"
                )
            );
        )", "test_show_create",
        R"(
            CREATE TABLE `test_show_create` (
                `Key` Uint32,
                `Value0` String,
                FAMILY `default` (CACHE_MODE = 'in_memory'),
                PRIMARY KEY (`Key`)
            );
        )"
    );
}

Y_UNIT_TEST(TableSystemTableWithEmptyKeyColumnIds) {
    // This test reproduces the crash from issue #30332
    // When trying to SHOW CREATE TABLE on a system table that has empty key column IDs,
    // the formatter crashes at line 347 with: Y_ENSURE(!tableDesc.GetKeyColumnIds().empty())
    //
    // The issue specifically mentions `.sys/tables` as causing the crash.
    // This test verifies that SHOW CREATE TABLE on system tables either succeeds
    // or returns a proper error status, but does not crash the server.

    TTestEnv env(1, 4, {.StoragePools = 3, .ShowCreateTable = true});

    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_EXECUTER, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
    env.GetServer().GetRuntime()->SetLogPriority(NKikimrServices::SYSTEM_VIEWS, NActors::NLog::PRI_DEBUG);

    NQuery::TQueryClient queryClient(env.GetDriver());
    auto session = queryClient.GetSession().GetValueSync().GetSession();

    // The issue specifically mentions .sys/tables as the problematic table
    // We test this and a few other system tables to ensure robustness
    TVector<TString> systemTablesToTest = {
        "/Root/.sys/tables",  // The specific table mentioned in issue #30332
        "/Root/.sys/partition_stats",
        "/Root/.sys/nodes"
    };

    for (const auto& systemTable : systemTablesToTest) {
        Cerr << "Testing SHOW CREATE TABLE on " << systemTable << Endl;

        // Try to execute SHOW CREATE TABLE on the system table
        // Before the fix, this would crash with Y_ENSURE at line 347
        // After the fix, this should either succeed or return a proper error status
        auto result = session.ExecuteQuery(
            TStringBuilder() << "SHOW CREATE TABLE `" << systemTable << "`;",
            NQuery::TTxControl::NoTx()
        ).GetValueSync();

        if (!result.IsSuccess()) {
            // If it fails, verify it's a proper error status, not an internal error from a crash
            // The formatter should handle empty key column IDs gracefully and return UNSUPPORTED
            // or SCHEME_ERROR, not crash with an internal error
            UNIT_ASSERT_C(
                result.GetStatus() == EStatus::SCHEME_ERROR ||
                result.GetStatus() == EStatus::BAD_REQUEST,
                "SHOW CREATE TABLE on " << systemTable
                << " should return a proper error status (SCHEME_ERROR or BAD_REQUEST), "
                << "not an internal error from a crash. Got status: " << result.GetStatus()
                << ", issues: " << result.GetIssues().ToString()
            );

            // Verify the error message is meaningful
            UNIT_ASSERT_C(
                !result.GetIssues().ToString().empty(),
                "Error message should not be empty for " << systemTable
            );

            Cerr << "SHOW CREATE TABLE on " << systemTable << " returned expected error: "
                    << result.GetStatus() << " - " << result.GetIssues().ToString() << Endl;
        } else {
            // If it succeeds, verify we got a valid result with the expected structure
            UNIT_ASSERT_C(
                result.GetResultSets().size() > 0,
                "SHOW CREATE TABLE on " << systemTable << " should return at least one result set"
            );

            auto resultSet = result.GetResultSet(0);
            auto columnsMeta = resultSet.GetColumnsMeta();
            UNIT_ASSERT_C(
                columnsMeta.size() == 3,
                "SHOW CREATE TABLE result should have 3 columns (Path, PathType, CreateQuery), got: "
                << columnsMeta.size()
            );

            Cerr << "SHOW CREATE TABLE on " << systemTable << " succeeded" << Endl;
        }
    }
}

}

} // NSysView
} // NKikimr
