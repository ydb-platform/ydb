#include "sql_complete.h"
#include <yql/essentials/sql/v1/complete/syntax/grammar.h>

#include <yql/essentials/sql/v1/complete/name/cache/local/cache.h>
#include <yql/essentials/sql/v1/complete/name/cluster/static/discovery.h>
#include <yql/essentials/sql/v1/complete/name/object/simple/schema.h>
#include <yql/essentials/sql/v1/complete/name/object/simple/cached/schema.h>
#include <yql/essentials/sql/v1/complete/name/object/simple/static/schema.h>
#include <yql/essentials/sql/v1/complete/name/object/simple/static/schema_json.h>
#include <yql/essentials/sql/v1/complete/name/service/ranking/frequency.h>
#include <yql/essentials/sql/v1/complete/name/service/ranking/ranking.h>
#include <yql/essentials/sql/v1/complete/name/service/cluster/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/impatient/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/schema/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/static/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/union/name_service.h>

#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/iterator/iterate_keys.h>
#include <library/cpp/iterator/functools.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>

#include <util/charset/utf8.h>

using namespace NSQLComplete;

class TDummyException: public yexception {
public:
    TDummyException() {
        Append("T_T");
    }
};

class TFailingNameService: public INameService {
public:
    NThreading::TFuture<TNameResponse> Lookup(const TNameRequest&) const override {
        auto e = std::make_exception_ptr(TDummyException());
        return NThreading::MakeErrorFuture<TNameResponse>(e);
    }
};

Y_UNIT_TEST_SUITE(SqlCompleteTests) {
using ECandidateKind::BindingName;
using ECandidateKind::ClusterName;
using ECandidateKind::ColumnName;
using ECandidateKind::FolderName;
using ECandidateKind::FunctionName;
using ECandidateKind::HintName;
using ECandidateKind::Keyword;
using ECandidateKind::PragmaName;
using ECandidateKind::TableName;
using ECandidateKind::TypeName;
using ECandidateKind::UnknownName;

TLexerSupplier MakePureLexerSupplier() {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
    lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();
    return [lexers = std::move(lexers)](bool ansi) {
        return NSQLTranslationV1::MakeLexer(
            lexers, ansi, NSQLTranslationV1::ELexerFlavor::Pure);
    };
}

ISqlCompletionEngine::TPtr MakeSqlCompletionEngineUT() {
    TLexerSupplier lexer = MakePureLexerSupplier();

    TNameSet names = {
        .Pragmas = {
            "yson.CastToString",
            "yt.RuntimeCluster",
            "yt.RuntimeClusterSelection",
        },
        .Types = {"Uint64"},
        .Functions = {
            "StartsWith",
            "DateTime::Split",
            "Python::__private",
        },
        .Hints = {
            {EStatementKind::Select, {"XLOCK"}},
            {EStatementKind::Insert, {"EXPIRATION"}},
        },
    };

    TString clustersText = R"({
            "": { "type": "Folder", "entries": {
                "local": { "type": "Folder", "entries": {
                    "example": { "type": "Table", "columns": {} },
                    "account": { "type": "Table", "columns": {} },
                    "abacaba": { "type": "Table", "columns": {} }
                }},
                "test": { "type": "Folder", "entries": {
                    "service": { "type": "Folder", "entries": {
                        "example": { "type": "Table", "columns": {} }
                    }},
                    "meta": { "type": "Table", "columns": {} }
                }},
                "prod": { "type": "Folder", "entries": {
                }},
                ".sys": { "type": "Folder", "entries": {
                    "status": { "type": "Table", "columns": {} }
                }}
            }},
            "example": { "type": "Folder", "entries": {
                "people": { "type": "Table", "columns": {
                    "Name": {},
                    "Age": {}
                }},
                "yql": { "type": "Folder", "entries": {
                    "tutorial": { "type": "Table", "columns": {
                       "course": {},
                       "room": {},
                       "time": {}
                    }}
                }},
                "link": { "type": "LINK" },
                "topic": { "type": "Topic" }
            }},
            "saurus": { "type": "Folder", "entries": {
                "maxim": { "type": "Table", "columns": {
                   "Y Q L": {},
                   "o``o": {}
                }}
            }},
            "loggy": { "type": "Folder", "entries": {
                "yql": { "type": "Folder", "entries": {
                    "2025-01": { "type": "Table", "columns": {
                        "timestamp": {},
                        "message": {}
                    }},
                    "2025-02": { "type": "Table", "columns": {
                        "timestamp": {},
                        "message": {}
                    }},
                    "2025-03": { "type": "Table", "columns": {
                        "timestamp": {},
                        "message": {}
                    }}
                }}
            }}
        })";

    NJson::TJsonMap clustersJson;
    Y_ENSURE(NJson::ReadJsonTree(clustersText, &clustersJson));

    auto clustersIt = NFuncTools::Filter(
        [](const auto& x) { return !x.empty(); }, IterateKeys(clustersJson.GetMapSafe()));
    TVector<TString> clusters(begin(clustersIt), end(clustersIt));

    TFrequencyData frequency;
    IRanking::TPtr ranking = MakeDefaultRanking(frequency);

    TVector<INameService::TPtr> children = {
        MakeStaticNameService(std::move(names), frequency),
        MakeImpatientNameService(
            MakeSchemaNameService(
                MakeSimpleSchema(
                    MakeStaticSimpleSchema(clustersJson)))),
        MakeImpatientNameService(
            MakeClusterNameService(
                MakeStaticClusterDiscovery(std::move(clusters)))),
    };
    INameService::TPtr service = MakeUnionNameService(std::move(children), ranking);

    TConfiguration config;
    return MakeSqlCompletionEngine(std::move(lexer), std::move(service), config, ranking);
}

TVector<TCandidate> Complete(ISqlCompletionEngine::TPtr& engine, TString sharped, TEnvironment env = {}) {
    return engine->Complete(SharpedInput(sharped), std::move(env)).GetValueSync().Candidates;
}

TVector<TCandidate> CompleteTop(size_t limit, ISqlCompletionEngine::TPtr& engine, TString sharped, TEnvironment env = {}) {
    auto candidates = Complete(engine, std::move(sharped), std::move(env));
    candidates.crop(limit);
    return candidates;
}

Y_UNIT_TEST(Beginning) {
    TVector<TCandidate> expected = {
        {.Kind = Keyword, .Content = "ALTER"},
        {.Kind = Keyword, .Content = "ANALYZE"},
        {.Kind = Keyword, .Content = "BACKUP"},
        {.Kind = Keyword, .Content = "BATCH"},
        {.Kind = Keyword, .Content = "COMMIT"},
        {.Kind = Keyword, .Content = "CREATE"},
        {.Kind = Keyword, .Content = "DECLARE"},
        {.Kind = Keyword, .Content = "DEFINE"},
        {.Kind = Keyword, .Content = "DELETE FROM"},
        {.Kind = Keyword, .Content = "DISCARD"},
        {.Kind = Keyword, .Content = "DO"},
        {.Kind = Keyword, .Content = "DROP"},
        {.Kind = Keyword, .Content = "EVALUATE"},
        {.Kind = Keyword, .Content = "EXPLAIN"},
        {.Kind = Keyword, .Content = "EXPORT"},
        {.Kind = Keyword, .Content = "FOR"},
        {.Kind = Keyword, .Content = "FROM"},
        {.Kind = Keyword, .Content = "GRANT"},
        {.Kind = Keyword, .Content = "IF"},
        {.Kind = Keyword, .Content = "IMPORT"},
        {.Kind = Keyword, .Content = "INSERT"},
        {.Kind = Keyword, .Content = "PARALLEL"},
        {.Kind = Keyword, .Content = "PRAGMA"},
        {.Kind = Keyword, .Content = "PROCESS"},
        {.Kind = Keyword, .Content = "REDUCE"},
        {.Kind = Keyword, .Content = "REPLACE"},
        {.Kind = Keyword, .Content = "RESTORE"},
        {.Kind = Keyword, .Content = "REVOKE"},
        {.Kind = Keyword, .Content = "ROLLBACK"},
        {.Kind = Keyword, .Content = "SELECT"},
        {.Kind = Keyword, .Content = "SHOW CREATE"},
        {.Kind = Keyword, .Content = "TRUNCATE TABLE"},
        {.Kind = Keyword, .Content = "UPDATE"},
        {.Kind = Keyword, .Content = "UPSERT"},
        {.Kind = Keyword, .Content = "USE"},
        {.Kind = Keyword, .Content = "VALUES"},
    };

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, ""), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, " "), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "  "), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, ";"), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "; "), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, " ; "), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "#SELECT"), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "#SELECT * FROM"), expected);
}

Y_UNIT_TEST(Use) {
    TVector<TCandidate> expected = {
        {.Kind = ClusterName, .Content = "example"},
        {.Kind = ClusterName, .Content = "loggy"},
        {.Kind = ClusterName, .Content = "saurus"},
    };
    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "USE "), expected);
}

Y_UNIT_TEST(UseClusterResultion) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TString query = R"sql(
                DECLARE $cluster_name AS String;
                USE yt:$cluster_name;
                SELECT * FROM #
            )sql";

        TEnvironment env = {
            .Parameters = {{"$cluster_name", "saurus"}},
        };

        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "`maxim`"},
            {.Kind = BindingName, .Content = "$cluster_name"},
            {.Kind = ClusterName, .Content = "example"},
            {.Kind = ClusterName, .Content = "loggy"},
            {.Kind = ClusterName, .Content = "saurus"},
        };

        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(5, engine, query, env), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = FolderName, .Content = "`.sys/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`local/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`prod/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`test/`", .CursorShift = 1},
            {.Kind = ClusterName, .Content = "example"},
            {.Kind = ClusterName, .Content = "loggy"},
            {.Kind = ClusterName, .Content = "saurus"},
            {.Kind = Keyword, .Content = "ANY"},
        };
        UNIT_ASSERT_VALUES_EQUAL(
            CompleteTop(
                8,
                engine,
                "USE yt:$cluster_name; SELECT * FROM ",
                {.Parameters = {}}),
            expected);
    }
}

Y_UNIT_TEST(Alter) {
    TVector<TCandidate> expected = {
        {.Kind = Keyword, .Content = "ASYNC REPLICATION"},
        {.Kind = Keyword, .Content = "BACKUP COLLECTION"},
        {.Kind = Keyword, .Content = "DATABASE"},
        {.Kind = Keyword, .Content = "EXTERNAL"},
        {.Kind = Keyword, .Content = "GROUP"},
        {.Kind = Keyword, .Content = "OBJECT"},
        {.Kind = Keyword, .Content = "RESOURCE POOL"},
        {.Kind = Keyword, .Content = "SECRET"},
        {.Kind = Keyword, .Content = "SEQUENCE"},
        {.Kind = Keyword, .Content = "STREAMING QUERY"},
        {.Kind = Keyword, .Content = "TABLE"},
        {.Kind = Keyword, .Content = "TABLESTORE"},
        {.Kind = Keyword, .Content = "TOPIC"},
        {.Kind = Keyword, .Content = "TRANSFER"},
        {.Kind = Keyword, .Content = "USER"},
    };

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "ALTER "), expected);
}

Y_UNIT_TEST(Create) {
    TVector<TCandidate> expected = {
        {.Kind = Keyword, .Content = "ASYNC REPLICATION"},
        {.Kind = Keyword, .Content = "BACKUP COLLECTION"},
        {.Kind = Keyword, .Content = "EXTERNAL"},
        {.Kind = Keyword, .Content = "GROUP"},
        {.Kind = Keyword, .Content = "OBJECT"},
        {.Kind = Keyword, .Content = "OR REPLACE"},
        {.Kind = Keyword, .Content = "RESOURCE POOL"},
        {.Kind = Keyword, .Content = "SECRET"},
        {.Kind = Keyword, .Content = "STREAMING QUERY"},
        {.Kind = Keyword, .Content = "TABLE"},
        {.Kind = Keyword, .Content = "TABLESTORE"},
        {.Kind = Keyword, .Content = "TEMP TABLE"},
        {.Kind = Keyword, .Content = "TEMPORARY TABLE"},
        {.Kind = Keyword, .Content = "TOPIC"},
        {.Kind = Keyword, .Content = "TRANSFER"},
        {.Kind = Keyword, .Content = "USER"},
        {.Kind = Keyword, .Content = "VIEW"},
    };

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "CREATE "), expected);
}

Y_UNIT_TEST(CreateTable) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TCandidate> expected = {
            {.Kind = FolderName, .Content = "`.sys/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`local/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`prod/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`test/`", .CursorShift = 1},
            {.Kind = ClusterName, .Content = "example"},
            {.Kind = ClusterName, .Content = "loggy"},
            {.Kind = ClusterName, .Content = "saurus"},
            {.Kind = Keyword, .Content = "IF NOT EXISTS"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "CREATE TABLE #"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = FolderName, .Content = "service/"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "CREATE TABLE `test/#`"), expected);
    }
}

Y_UNIT_TEST(Delete) {
    TVector<TCandidate> expected = {
        {.Kind = Keyword, .Content = "FROM"},
    };

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "DELETE "), expected);
}

Y_UNIT_TEST(Drop) {
    TVector<TCandidate> expected = {
        {.Kind = Keyword, .Content = "ASYNC REPLICATION"},
        {.Kind = Keyword, .Content = "BACKUP COLLECTION"},
        {.Kind = Keyword, .Content = "EXTERNAL"},
        {.Kind = Keyword, .Content = "GROUP"},
        {.Kind = Keyword, .Content = "OBJECT"},
        {.Kind = Keyword, .Content = "RESOURCE POOL"},
        {.Kind = Keyword, .Content = "SECRET"},
        {.Kind = Keyword, .Content = "STREAMING QUERY"},
        {.Kind = Keyword, .Content = "TABLE"},
        {.Kind = Keyword, .Content = "TABLESTORE"},
        {.Kind = Keyword, .Content = "TOPIC"},
        {.Kind = Keyword, .Content = "TRANSFER"},
        {.Kind = Keyword, .Content = "USER"},
        {.Kind = Keyword, .Content = "VIEW"},
    };

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "DROP "), expected);
}

Y_UNIT_TEST(DropObject) {
    TVector<TCandidate> expected = {
        {.Kind = FolderName, .Content = "`.sys/`", .CursorShift = 1},
        {.Kind = FolderName, .Content = "`local/`", .CursorShift = 1},
        {.Kind = FolderName, .Content = "`prod/`", .CursorShift = 1},
        {.Kind = FolderName, .Content = "`test/`", .CursorShift = 1},
        {.Kind = ClusterName, .Content = "example"},
        {.Kind = ClusterName, .Content = "loggy"},
        {.Kind = ClusterName, .Content = "saurus"},
        {.Kind = Keyword, .Content = "IF EXISTS"},
    };
    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "DROP TABLE "), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "DROP VIEW "), expected);
}

Y_UNIT_TEST(Explain) {
    TVector<TCandidate> expected = {
        {.Kind = Keyword, .Content = "ALTER"},
        {.Kind = Keyword, .Content = "ANALYZE"},
        {.Kind = Keyword, .Content = "BACKUP"},
        {.Kind = Keyword, .Content = "BATCH"},
        {.Kind = Keyword, .Content = "COMMIT"},
        {.Kind = Keyword, .Content = "CREATE"},
        {.Kind = Keyword, .Content = "DECLARE"},
        {.Kind = Keyword, .Content = "DEFINE"},
        {.Kind = Keyword, .Content = "DELETE FROM"},
        {.Kind = Keyword, .Content = "DISCARD"},
        {.Kind = Keyword, .Content = "DO"},
        {.Kind = Keyword, .Content = "DROP"},
        {.Kind = Keyword, .Content = "EVALUATE"},
        {.Kind = Keyword, .Content = "EXPORT"},
        {.Kind = Keyword, .Content = "FOR"},
        {.Kind = Keyword, .Content = "FROM"},
        {.Kind = Keyword, .Content = "GRANT"},
        {.Kind = Keyword, .Content = "IF"},
        {.Kind = Keyword, .Content = "IMPORT"},
        {.Kind = Keyword, .Content = "INSERT"},
        {.Kind = Keyword, .Content = "PARALLEL"},
        {.Kind = Keyword, .Content = "PRAGMA"},
        {.Kind = Keyword, .Content = "PROCESS"},
        {.Kind = Keyword, .Content = "QUERY PLAN"},
        {.Kind = Keyword, .Content = "REDUCE"},
        {.Kind = Keyword, .Content = "REPLACE"},
        {.Kind = Keyword, .Content = "RESTORE"},
        {.Kind = Keyword, .Content = "REVOKE"},
        {.Kind = Keyword, .Content = "ROLLBACK"},
        {.Kind = Keyword, .Content = "SELECT"},
        {.Kind = Keyword, .Content = "SHOW CREATE"},
        {.Kind = Keyword, .Content = "TRUNCATE TABLE"},
        {.Kind = Keyword, .Content = "UPDATE"},
        {.Kind = Keyword, .Content = "UPSERT"},
        {.Kind = Keyword, .Content = "USE"},
        {.Kind = Keyword, .Content = "VALUES"},
    };

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "EXPLAIN "), expected);
}

Y_UNIT_TEST(Grant) {
    TVector<TCandidate> expected = {
        {.Kind = Keyword, .Content = "ALL"},
        {.Kind = Keyword, .Content = "ALTER SCHEMA"},
        {.Kind = Keyword, .Content = "CONNECT"},
        {.Kind = Keyword, .Content = "CREATE"},
        {.Kind = Keyword, .Content = "DESCRIBE SCHEMA"},
        {.Kind = Keyword, .Content = "DROP"},
        {.Kind = Keyword, .Content = "ERASE ROW"},
        {.Kind = Keyword, .Content = "FULL"},
        {.Kind = Keyword, .Content = "GRANT"},
        {.Kind = Keyword, .Content = "INSERT"},
        {.Kind = Keyword, .Content = "LIST"},
        {.Kind = Keyword, .Content = "MANAGE"},
        {.Kind = Keyword, .Content = "MODIFY"},
        {.Kind = Keyword, .Content = "REMOVE SCHEMA"},
        {.Kind = Keyword, .Content = "SELECT"},
        {.Kind = Keyword, .Content = "UPDATE ROW"},
        {.Kind = Keyword, .Content = "USE"},
    };

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "GRANT "), expected);
}

Y_UNIT_TEST(Insert) {
    TVector<TCandidate> expected = {
        {.Kind = Keyword, .Content = "INTO"},
        {.Kind = Keyword, .Content = "OR"},
    };

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "INSERT "), expected);
}

Y_UNIT_TEST(Pragma) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TCandidate> expected = {
            {.Kind = Keyword, .Content = "ANSI"},
            {.Kind = PragmaName, .Content = "yson.CastToString"},
            {.Kind = PragmaName, .Content = "yt.RuntimeCluster"},
            {.Kind = PragmaName, .Content = "yt.RuntimeClusterSelection"}};
        auto completion = engine->Complete({.Text = "PRAGMA "}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "");
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = PragmaName, .Content = "yson.CastToString"}};
        auto completion = engine->Complete({.Text = "PRAGMA ys"}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "ys");
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = PragmaName, .Content = "yson.CastToString"}};
        auto completion = engine->Complete({.Text = "PRAGMA yson"}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "yson");
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = PragmaName, .Content = "CastToString"}};
        auto completion = engine->Complete({.Text = "PRAGMA yson."}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "");
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = PragmaName, .Content = "CastToString"}};
        auto completion = engine->Complete({.Text = "PRAGMA yson.cast"}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "cast");
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = PragmaName, .Content = "RuntimeCluster"},
            {.Kind = PragmaName, .Content = "RuntimeClusterSelection"}};
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "pragma yt."), expected);
        UNIT_ASSERT_VALUES_EQUAL(
            Complete(engine, "pragma yt.RuntimeClusterSelection='force';\npragma yt.Ru"),
            expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = PragmaName, .Content = "RuntimeCluster"},
            {.Kind = PragmaName, .Content = "RuntimeClusterSelection"}};
        UNIT_ASSERT_VALUES_EQUAL(
            Complete(engine, "pragma yt.Ru#\n"),
            expected);
    }
}

Y_UNIT_TEST(Select) {
    TVector<TCandidate> expected = {
        {.Kind = Keyword, .Content = "ALL"},
        {.Kind = Keyword, .Content = "BITCAST()", .CursorShift = 1},
        {.Kind = Keyword, .Content = "CASE"},
        {.Kind = Keyword, .Content = "CAST()", .CursorShift = 1},
        {.Kind = Keyword, .Content = "CURRENT_DATE"},
        {.Kind = Keyword, .Content = "CURRENT_TIME"},
        {.Kind = Keyword, .Content = "CURRENT_TIMESTAMP"},
        {.Kind = TypeName, .Content = "Callable<>", .CursorShift = 1},
        {.Kind = Keyword, .Content = "DISTINCT"},
        {.Kind = FunctionName, .Content = "DateTime::Split()", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Decimal()", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Dict<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "DynamicLinear<>", .CursorShift = 1},
        {.Kind = Keyword, .Content = "EMPTY_ACTION"},
        {.Kind = Keyword, .Content = "EXISTS()", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Enum<>", .CursorShift = 1},
        {.Kind = Keyword, .Content = "FALSE"},
        {.Kind = TypeName, .Content = "Flow<>", .CursorShift = 1},
        {.Kind = Keyword, .Content = "JSON_EXISTS()", .CursorShift = 1},
        {.Kind = Keyword, .Content = "JSON_QUERY()", .CursorShift = 1},
        {.Kind = Keyword, .Content = "JSON_VALUE()", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Linear<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "List<>", .CursorShift = 1},
        {.Kind = Keyword, .Content = "NOT"},
        {.Kind = Keyword, .Content = "NULL"},
        {.Kind = TypeName, .Content = "Optional<>", .CursorShift = 1},
        {.Kind = FunctionName, .Content = "Python::__private()", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Resource<>", .CursorShift = 1},
        {.Kind = Keyword, .Content = "STREAM"},
        {.Kind = TypeName, .Content = "Set<>", .CursorShift = 1},
        {.Kind = FunctionName, .Content = "StartsWith()", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Stream<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Struct<>", .CursorShift = 1},
        {.Kind = Keyword, .Content = "TRUE"},
        {.Kind = TypeName, .Content = "Tagged<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Tuple<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Uint64"},
        {.Kind = TypeName, .Content = "Variant<>", .CursorShift = 1},
    };

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT "), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT # FROM"), expected);
}

Y_UNIT_TEST(SelectFrom) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TCandidate> expected = {
            {.Kind = FolderName, .Content = "`.sys/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`local/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`prod/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`test/`", .CursorShift = 1},
            {.Kind = ClusterName, .Content = "example"},
            {.Kind = ClusterName, .Content = "loggy"},
            {.Kind = ClusterName, .Content = "saurus"},
            {.Kind = Keyword, .Content = "ANY"},
            {.Kind = FunctionName, .Content = "CONCAT()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "EACH()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "FILTER()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "FOLDER()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "LIKE()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "PARTITIONS()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "PARTITION_LIST()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "RANGE()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "REGEXP()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "WalkFolders()", .CursorShift = 1},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM "), expected);
    }
    {
        TString input = "SELECT * FROM pr";
        TVector<TCandidate> expected = {
            {.Kind = FolderName, .Content = "`prod/`", .CursorShift = 1},
        };
        TCompletion actual = engine->Complete(SharpedInput(input)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, "pr");
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = FolderName, .Content = ".sys/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "local/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "prod/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "test/`", .CursorShift = 1},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM `#"), expected);
    }
    {
        TString input = "SELECT * FROM `#`";
        TVector<TCandidate> expected = {
            {.Kind = FolderName, .Content = ".sys/"},
            {.Kind = FolderName, .Content = "local/"},
            {.Kind = FolderName, .Content = "prod/"},
            {.Kind = FolderName, .Content = "test/"},
        };
        TCompletion actual = engine->Complete(SharpedInput(input)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, "");
    }
    {
        TString input = "SELECT * FROM `local/#`";
        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "abacaba"},
            {.Kind = TableName, .Content = "account"},
            {.Kind = TableName, .Content = "example"},
        };
        TCompletion actual = engine->Complete(SharpedInput(input)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, "");
    }
    {
        TString input = "SELECT * FROM `local/a#`";
        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "abacaba"},
            {.Kind = TableName, .Content = "account"},
        };
        TCompletion actual = engine->Complete(SharpedInput(input)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, "a");
    }
    {
        TString input = "SELECT * FROM `.sy#`";
        TVector<TCandidate> expected = {
            {.Kind = FolderName, .Content = ".sys/"},
        };
        TCompletion actual = engine->Complete(SharpedInput(input)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, ".sy");
    }
    {
        TString input = "SELECT * FROM `/test/ser#vice/`";
        TVector<TCandidate> expected = {
            {.Kind = FolderName, .Content = "service/"},
        };
        TCompletion actual = engine->Complete(SharpedInput(input)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, "ser");
    }
}

Y_UNIT_TEST(SelectFromUnclosedIdQuoted) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TCandidate> expected = {
            {.Kind = FolderName, .Content = ".sys/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "local/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "prod/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "test/`", .CursorShift = 1},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(4, engine, "SELECT * FROM `#"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "meta`"},
            {.Kind = FolderName, .Content = "service/`", .CursorShift = 1},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM `test/"), expected);
    }
}

Y_UNIT_TEST(SelectFromCluster) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TCandidate> expected = {
            {.Kind = ClusterName, .Content = "example"},
            {.Kind = ClusterName, .Content = "loggy"},
            {.Kind = ClusterName, .Content = "saurus"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM yt:"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = ClusterName, .Content = "saurus"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM yt:saurus#"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "`maxim`"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, "SELECT * FROM yt:saurus."), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "`people`"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, "SELECT * FROM example."), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "tutorial"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, "SELECT * FROM example.`/yql/t#`"), expected);
    }
}

Y_UNIT_TEST(SelectFromWithUse) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "`maxim`"},
            {.Kind = ClusterName, .Content = "example"},
            {.Kind = ClusterName, .Content = "loggy"},
            {.Kind = ClusterName, .Content = "saurus"},
            {.Kind = Keyword, .Content = "ANY"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(5, engine, "USE yt:saurus; SELECT * FROM "), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "`people`"},
            {.Kind = FolderName, .Content = "`yql/`", .CursorShift = 1},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "USE yt:saurus; SELECT * FROM example."), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "`maxim`"},
            {.Kind = ClusterName, .Content = "example"},
            {.Kind = ClusterName, .Content = "loggy"},
            {.Kind = ClusterName, .Content = "saurus"},
            {.Kind = Keyword, .Content = "ANY"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(5, engine, "USE example; USE yt:saurus; SELECT * FROM "), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "`maxim`"},
            {.Kind = ClusterName, .Content = "example"},
            {.Kind = ClusterName, .Content = "loggy"},
            {.Kind = ClusterName, .Content = "saurus"},
            {.Kind = Keyword, .Content = "ANY"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(5, engine, R"(
                USE example;
                DEFINE ACTION $hello() AS
                    USE yt:saurus;
                    SELECT * FROM #;
                END DEFINE;
            )"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "`people`"},
            {.Kind = FolderName, .Content = "`yql/`", .CursorShift = 1},
            {.Kind = BindingName, .Content = "$action"},
            {.Kind = ClusterName, .Content = "example"},
            {.Kind = ClusterName, .Content = "loggy"},
            {.Kind = ClusterName, .Content = "saurus"},
            {.Kind = Keyword, .Content = "ANY"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(7, engine, R"(
                USE example;

                DEFINE ACTION $action() AS
                    USE yt:saurus;
                    SELECT * FROM test;
                END DEFINE;

                SELECT * FROM #
            )"), expected);
    }
}

Y_UNIT_TEST(SelectWhere) {
    TVector<TCandidate> expected = {
        {.Kind = Keyword, .Content = "BITCAST()", .CursorShift = 1},
        {.Kind = Keyword, .Content = "CASE"},
        {.Kind = Keyword, .Content = "CAST()", .CursorShift = 1},
        {.Kind = Keyword, .Content = "CURRENT_DATE"},
        {.Kind = Keyword, .Content = "CURRENT_TIME"},
        {.Kind = Keyword, .Content = "CURRENT_TIMESTAMP"},
        {.Kind = TypeName, .Content = "Callable<>", .CursorShift = 1},
        {.Kind = FunctionName, .Content = "DateTime::Split()", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Decimal()", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Dict<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "DynamicLinear<>", .CursorShift = 1},
        {.Kind = Keyword, .Content = "EMPTY_ACTION"},
        {.Kind = Keyword, .Content = "EXISTS()", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Enum<>", .CursorShift = 1},
        {.Kind = Keyword, .Content = "FALSE"},
        {.Kind = TypeName, .Content = "Flow<>", .CursorShift = 1},
        {.Kind = Keyword, .Content = "JSON_EXISTS()", .CursorShift = 1},
        {.Kind = Keyword, .Content = "JSON_QUERY()", .CursorShift = 1},
        {.Kind = Keyword, .Content = "JSON_VALUE()", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Linear<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "List<>", .CursorShift = 1},
        {.Kind = Keyword, .Content = "NOT"},
        {.Kind = Keyword, .Content = "NULL"},
        {.Kind = TypeName, .Content = "Optional<>", .CursorShift = 1},
        {.Kind = FunctionName, .Content = "Python::__private()", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Resource<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Set<>", .CursorShift = 1},
        {.Kind = FunctionName, .Content = "StartsWith()", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Stream<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Struct<>", .CursorShift = 1},
        {.Kind = Keyword, .Content = "TRUE"},
        {.Kind = TypeName, .Content = "Tagged<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Tuple<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Uint64"},
        {.Kind = TypeName, .Content = "Variant<>", .CursorShift = 1},
    };

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM a WHERE "), expected);
}

Y_UNIT_TEST(SelectSubquery) {
    auto engine = MakeSqlCompletionEngineUT();

    TVector<TCandidate> expected = {
        {.Kind = Keyword, .Content = "SELECT"},
    };

    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "$x = sel#"), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "$x = (sel#)"), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT (sel#)"), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM t WHERE (sel#)"), expected);
}

Y_UNIT_TEST(Upsert) {
    TVector<TCandidate> expected = {
        {.Kind = Keyword, .Content = "INTO"},
        {.Kind = Keyword, .Content = "OBJECT"},
    };

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "UPSERT "), expected);
}

Y_UNIT_TEST(UpsertInto) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TCandidate> expected = {
            {.Kind = FolderName, .Content = "`.sys/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`local/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`prod/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`test/`", .CursorShift = 1},
            {.Kind = ClusterName, .Content = "example"},
            {.Kind = ClusterName, .Content = "loggy"},
            {.Kind = ClusterName, .Content = "saurus"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "UPSERT INTO "), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "meta"},
            {.Kind = FolderName, .Content = "service/"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "UPSERT INTO `test/#`"), expected);
    }
}

Y_UNIT_TEST(AlterObject) {
    auto engine = MakeSqlCompletionEngineUT();

    TString query = R"sql(
            ALTER OBJECT example.`#`
        )sql";

    TVector<TCandidate> expected = {
        {.Kind = FolderName, .Content = "yql/"},
        {.Kind = UnknownName, .Content = "link"},
        {.Kind = UnknownName, .Content = "topic"},
    };
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, query), expected);
}

Y_UNIT_TEST(TypeName) {
    TVector<TCandidate> expected = {
        {.Kind = TypeName, .Content = "Callable<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Decimal()", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Dict<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "DynamicLinear<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Enum<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Flow<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Linear<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "List<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Optional<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Resource<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Set<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Stream<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Struct<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Tagged<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Tuple<>", .CursorShift = 1},
        {.Kind = TypeName, .Content = "Uint64"},
        {.Kind = TypeName, .Content = "Variant<>", .CursorShift = 1},
    };

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "CREATE TABLE table (id "), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT CAST (1 AS "), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT Optional<"), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT Optional<#>"), expected);
}

Y_UNIT_TEST(TypeNameAsArgument) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TCandidate> expected = {
            {.Kind = TypeName, .Content = "Uint64"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT Nothing(Uint"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = TypeName, .Content = "Optional<>", .CursorShift = 1},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT Nothing(Option"), expected);
    }
}

Y_UNIT_TEST(FunctionName) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TCandidate> expected = {
            {.Kind = FunctionName, .Content = "DateTime::Split()", .CursorShift = 1},
        };
        auto completion = engine->Complete({.Text = "SELECT Date"}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "Date");
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = FunctionName, .Content = "Split()", .CursorShift = 1},
        };
        auto completion = engine->Complete({.Text = "SELECT DateTime:"}).GetValueSync();
        UNIT_ASSERT(completion.Candidates.empty());
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = FunctionName, .Content = "Split()", .CursorShift = 1},
        };
        auto completion = engine->Complete({.Text = "SELECT DateTime::"}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "");
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = FunctionName, .Content = "Split()", .CursorShift = 1},
        };
        auto completion = engine->Complete({.Text = "SELECT DateTime::s"}).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
        UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "s");
    }
}

Y_UNIT_TEST(SelectTableHintName) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TCandidate> expected = {
            {.Kind = HintName, .Content = "XLOCK"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "PROCESS my_table USING $udf(TableRows()) WITH "), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = Keyword, .Content = "COLUMNS"},
            {.Kind = Keyword, .Content = "SCHEMA"},
            {.Kind = Keyword, .Content = "WATERMARK"},
            {.Kind = HintName, .Content = "XLOCK"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "REDUCE my_table WITH "), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = Keyword, .Content = "COLUMNS"},
            {.Kind = Keyword, .Content = "SCHEMA"},
            {.Kind = Keyword, .Content = "WATERMARK"},
            {.Kind = HintName, .Content = "XLOCK"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT key FROM my_table WITH "), expected);
    }
}

Y_UNIT_TEST(InsertTableHintName) {
    TVector<TCandidate> expected = {
        {.Kind = Keyword, .Content = "COLUMNS"},
        {.Kind = HintName, .Content = "EXPIRATION"},
        {.Kind = Keyword, .Content = "SCHEMA"},
        {.Kind = Keyword, .Content = "WATERMARK"},
    };

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "INSERT INTO my_table WITH "), expected);
}

Y_UNIT_TEST(CursorPosition) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TCandidate> expected = {
            {.Kind = Keyword, .Content = "AND"},
            {.Kind = Keyword, .Content = "AS"},
            {.Kind = Keyword, .Content = "ASSUME"},
            {.Kind = Keyword, .Content = "BETWEEN"},
            {.Kind = Keyword, .Content = "COLLATE"},
            {.Kind = Keyword, .Content = "EXCEPT"},
            {.Kind = Keyword, .Content = "FROM"},
            {.Kind = Keyword, .Content = "GLOB"},
            {.Kind = Keyword, .Content = "GROUP"},
            {.Kind = Keyword, .Content = "HAVING"},
            {.Kind = Keyword, .Content = "ILIKE"},
            {.Kind = Keyword, .Content = "IN"},
            {.Kind = Keyword, .Content = "INTERSECT"},
            {.Kind = Keyword, .Content = "INTO RESULT"},
            {.Kind = Keyword, .Content = "IS"},
            {.Kind = Keyword, .Content = "ISNULL"},
            {.Kind = Keyword, .Content = "LIKE"},
            {.Kind = Keyword, .Content = "LIMIT"},
            {.Kind = Keyword, .Content = "MATCH"},
            {.Kind = Keyword, .Content = "NOT"},
            {.Kind = Keyword, .Content = "NOTNULL"},
            {.Kind = Keyword, .Content = "OR"},
            {.Kind = Keyword, .Content = "ORDER BY"},
            {.Kind = Keyword, .Content = "REGEXP"},
            {.Kind = Keyword, .Content = "RLIKE"},
            {.Kind = Keyword, .Content = "UNION"},
            {.Kind = Keyword, .Content = "WHERE"},
            {.Kind = Keyword, .Content = "WINDOW"},
            {.Kind = Keyword, .Content = "WITHOUT"},
            {.Kind = Keyword, .Content = "XOR"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT `a`"), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT `a`#FROM"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = Keyword, .Content = "FROM"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM# "), expected);
    }
}

Y_UNIT_TEST(Enclosed) {
    TVector<TCandidate> empty = {};

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT \"#\""), empty);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT `#`"), empty);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT 21#21"), empty);

    UNIT_ASSERT(FindPtr(Complete(engine, "SELECT `name`#"), TCandidate{Keyword, "FROM"}) != nullptr);
    UNIT_ASSERT(FindPtr(Complete(engine, "SELECT #`name`"), TCandidate{FunctionName, "StartsWith()", 1}) != nullptr);

    UNIT_ASSERT_GT_C(Complete(engine, "SELECT \"a\"#\"b\"").size(), 0, "Between tokens");
    UNIT_ASSERT_VALUES_EQUAL_C(Complete(engine, "SELECT `a`#`b`"), empty, "Solid ID_QUOTED");
    UNIT_ASSERT_VALUES_EQUAL_C(Complete(engine, "SELECT `a#\\`b`"), empty, "Solid ID_QUOTED");
    UNIT_ASSERT_VALUES_EQUAL_C(Complete(engine, "SELECT `a\\#`b`"), empty, "Solid ID_QUOTED");
    UNIT_ASSERT_VALUES_EQUAL_C(Complete(engine, "SELECT `a\\`#b`"), empty, "Solid ID_QUOTED");
}

Y_UNIT_TEST(SemiEnclosed) {
    TVector<TCandidate> expected = {};

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT \""), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT `"), expected);
}

Y_UNIT_TEST(UTF8Wide) {
    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "\xF0\x9F\x98\x8A").size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "编码").size(), 0);
}

Y_UNIT_TEST(WordBreak) {
    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_GE(Complete(engine, "SELECT (").size(), 29);
    UNIT_ASSERT_GE(Complete(engine, "SELECT (1)").size(), 30);
    UNIT_ASSERT_GE(Complete(engine, "SELECT 1;").size(), 35);
}

Y_UNIT_TEST(Bindings) {
    auto engine = MakeSqlCompletionEngineUT();

    TString query = R"(
            $=0;
            $a=0;
            $abac=0;
            SELECT
        )";

    {
        TVector<TCandidate> expected = {
            {.Kind = BindingName, .Content = "$a"},
            {.Kind = BindingName, .Content = "$abac"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = BindingName, .Content = "$abac"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, query + "ab"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = BindingName, .Content = "$a"},
            {.Kind = BindingName, .Content = "$abac"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, query + "$"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = BindingName, .Content = "$abac"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, query + "$ab"), expected);
    }
}

Y_UNIT_TEST(BindingEditRange) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TString query = R"sql(
                $abacaba=0;
                SELECT #
            )sql";
        TCompletion c = engine->Complete(SharpedInput(query)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(c.CompletedToken.Content, "");
    }
    {
        TString query = R"sql(
                $abacaba=0;
                SELECT $#
            )sql";
        TCompletion c = engine->Complete(SharpedInput(query)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(c.CompletedToken.Content, "$");
    }
    {
        TString query = R"sql(
                $abacaba=0;
                SELECT $aba#
            )sql";
        TCompletion c = engine->Complete(SharpedInput(query)).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL(c.CompletedToken.Content, "$aba");
    }
}

Y_UNIT_TEST(TableFunction) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TCandidate> expected = {
            {.Kind = FunctionName, .Content = "CONCAT()", .CursorShift = 1},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, "SELECT * FROM Conca#"), expected);
    }
    {
        TVector<TCandidate> expected = {};
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, "SELECT Conca#"), expected);
    }
}

Y_UNIT_TEST(BeforeTableFunction) {
    auto engine = MakeSqlCompletionEngineUT();

    TString query = R"sql(
        USE example; --абвгдабвгдаб
        FROM (SELECT a FROM # RANGE(`t/`, ''));
    )sql";

    TVector<TCandidate> expected = {
        {.Kind = TableName, .Content = "`people`"},
        {.Kind = FolderName, .Content = "`yql/`", .CursorShift = 1},
        {.Kind = ClusterName, .Content = "example"},
        {.Kind = ClusterName, .Content = "loggy"},
        {.Kind = ClusterName, .Content = "saurus"},
        {.Kind = Keyword, .Content = "ANY"},
        {.Kind = FunctionName, .Content = "CONCAT()", .CursorShift = 1},
        {.Kind = FunctionName, .Content = "EACH()", .CursorShift = 1},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(8, engine, query), expected);
}

Y_UNIT_TEST(TableAsFunctionArgument) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TCandidate> expected = {
            {.Kind = FolderName, .Content = "`.sys/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`local/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`prod/`", .CursorShift = 1},
            {.Kind = FolderName, .Content = "`test/`", .CursorShift = 1},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(4, engine, "SELECT * FROM CONCAT(#)"), expected);
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(4, engine, "SELECT * FROM CONCAT(a, #)"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "`people`"},
            {.Kind = FolderName, .Content = "`yql/`", .CursorShift = 1},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "USE example; SELECT * FROM Concat(#)"), expected);
    }
    {
        UNIT_ASSERT_VALUES_EQUAL(
            CompleteTop(1, engine, "SELECT * FROM Concat(`#`)").at(0).Kind, FolderName);
    }
    {
        UNIT_ASSERT_VALUES_EQUAL(
            CompleteTop(1, engine, "SELECT * FROM Range(#)").at(0).Kind, FolderName);
        UNIT_ASSERT_VALUES_EQUAL(
            CompleteTop(1, engine, "SELECT * FROM Range(``, #)").at(0).Kind, FolderName);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = TableName, .Content = "example"},
        };
        UNIT_ASSERT_VALUES_EQUAL(
            CompleteTop(10, engine, "SELECT * FROM Range(`test/service`, `#`)"), expected);
        UNIT_ASSERT_VALUES_EQUAL(
            CompleteTop(10, engine, "SELECT * FROM Range(`test/service`, ``, `#`)"), expected);
    }
    {
        UNIT_ASSERT_VALUES_UNEQUAL(CompleteTop(1, engine, "SELECT Max(#)").at(0).Kind, FolderName);
        UNIT_ASSERT_VALUES_UNEQUAL(CompleteTop(1, engine, "SELECT Concat(#)").at(0).Kind, FolderName);
    }
}

Y_UNIT_TEST(TableFunctionCluster) {
    auto engine = MakeSqlCompletionEngineUT();
    TVector<TCandidate> expected = {
        {.Kind = TableName, .Content = "`people`"},
        {.Kind = FolderName, .Content = "`yql/`", .CursorShift = 1},
    };
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "SELECT * FROM example.Concat(#)"), expected);
}

Y_UNIT_TEST(ColumnsAtSimpleSelect) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "Age"},
            {.Kind = ColumnName, .Content = "Name"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "SELECT # FROM example.`/people`"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "Age"},
            {.Kind = Keyword, .Content = "ALL"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "SELECT a# FROM example.`/people`"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "Age"},
            {.Kind = ColumnName, .Content = "Name"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "USE example; SELECT # FROM `/people`"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "Age"},
            {.Kind = ColumnName, .Content = "Name"},
            {.Kind = ColumnName, .Content = "x.Age"},
            {.Kind = ColumnName, .Content = "x.Name"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, "SELECT # FROM example.`/people` AS x"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "Age"},
            {.Kind = ColumnName, .Content = "Name"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(3, engine, "SELECT x.# FROM example.`/people` AS x"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "Age"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "SELECT x.a# FROM example.`/people` AS x"), expected);
    }
    {
        TVector<TCandidate> expected = {};
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "SELECT y.a# FROM example.`/people` AS x"), expected);
    }
}

Y_UNIT_TEST(ColumnsAtJoin) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TString query = R"(
                SELECT #
                FROM example.`/people` AS ep
                JOIN example.`/yql/tutorial` AS et ON 1 = 1
            )";

        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "Age"},
            {.Kind = ColumnName, .Content = "Name"},
            {.Kind = ColumnName, .Content = "course"},
            {.Kind = ColumnName, .Content = "room"},
            {.Kind = ColumnName, .Content = "time"},
            {.Kind = ColumnName, .Content = "ep.Age"},
            {.Kind = ColumnName, .Content = "ep.Name"},
            {.Kind = ColumnName, .Content = "et.course"},
            {.Kind = ColumnName, .Content = "et.room"},
            {.Kind = ColumnName, .Content = "et.time"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
    }
}

Y_UNIT_TEST(ColumnsAtSubquery) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TString query = R"(
                SELECT #
                FROM (SELECT * FROM example.`/people`) AS ep
                JOIN (SELECT room AS Room, time FROM example.`/yql/tutorial`) AS et ON 1 = 1
            )";

        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "Age"},
            {.Kind = ColumnName, .Content = "Name"},
            {.Kind = ColumnName, .Content = "Room"},
            {.Kind = ColumnName, .Content = "time"},
            {.Kind = ColumnName, .Content = "ep.Age"},
            {.Kind = ColumnName, .Content = "ep.Name"},
            {.Kind = ColumnName, .Content = "et.Room"},
            {.Kind = ColumnName, .Content = "et.time"},
            {.Kind = Keyword, .Content = "ALL"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
    }
    {
        TString query = R"(
                SELECT #
                FROM example.`/yql/tutorial` AS x
                JOIN example.`/yql/tutorial` AS y ON 1 = 1
            )";

        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "x.course"},
            {.Kind = ColumnName, .Content = "x.room"},
            {.Kind = ColumnName, .Content = "x.time"},
            {.Kind = ColumnName, .Content = "y.course"},
            {.Kind = ColumnName, .Content = "y.room"},
            {.Kind = ColumnName, .Content = "y.time"},
            {.Kind = Keyword, .Content = "ALL"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
    }
    {
        TString query = "SELECT # FROM (SELECT 1 AS x)";

        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "x"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, query), expected);
    }
    {
        TString query = R"(
                SELECT #
                FROM (
                    SELECT epp.*, test
                    FROM example.`/people` AS epp
                    JOIN example.`/yql/tutorial` AS eqt ON TRUE
                    JOIN testing ON TRUE
                ) AS ep
            )";

        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "Age"},
            {.Kind = ColumnName, .Content = "Name"},
            {.Kind = ColumnName, .Content = "test"},
            {.Kind = ColumnName, .Content = "ep.Age"},
            {.Kind = ColumnName, .Content = "ep.Name"},
            {.Kind = ColumnName, .Content = "ep.test"},
            {.Kind = Keyword, .Content = "ALL"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
    }
    {
        TString query = R"(
                SELECT #
                FROM (
                    SELECT * WITHOUT Age, eqt.course
                    FROM example.`/people` AS epp
                    JOIN example.`/yql/tutorial` AS eqt ON TRUE
                    JOIN testing ON TRUE
                ) AS ep
                JOIN example.`/people`      ON TRUE
                JOIN example.`/people`      ON TRUE
                JOIN example.`/people` AS x ON TRUE
            )";

        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "Age"},
            {.Kind = ColumnName, .Content = "room"},
            {.Kind = ColumnName, .Content = "time"},
            {.Kind = ColumnName, .Content = "ep.Name"},
            {.Kind = ColumnName, .Content = "ep.room"},
            {.Kind = ColumnName, .Content = "ep.time"},
            {.Kind = ColumnName, .Content = "x.Age"},
            {.Kind = ColumnName, .Content = "x.Name"},
            {.Kind = Keyword, .Content = "ALL"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
    }
}

Y_UNIT_TEST(ColumnsFromNamedExpr) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TVector<TString> queries = {
            R"sql(SELECT # FROM $)sql",
            R"sql(SELECT # FROM $$)sql",
            R"sql(SELECT # FROM $x)sql",
        };

        TVector<TCandidate> expected = {
            {.Kind = Keyword, .Content = "ALL"},
        };

        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, queries[0]), expected);
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, queries[1]), expected);
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, queries[2]), expected);
    }
    {
        TString declare = R"sql(DECLARE $x AS String;)sql";

        TVector<TString> queries = {
            declare + R"sql(SELECT # FROM example.$x)sql",
            declare + R"sql(USE example; SELECT # FROM $x)sql",
        };

        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "Age"},
            {.Kind = ColumnName, .Content = "Name"},
            {.Kind = BindingName, .Content = "$x"},
        };

        TEnvironment env = {
            .Parameters = {{"$x", "/people"}},
        };

        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(3, engine, queries[0], env), expected);
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(3, engine, queries[1], env), expected);
    }
    {
        TString query = R"sql(
                USE example;
                SELECT # FROM $x;
            )sql";

        TEnvironment env = {
            .Parameters = {{"$x", "/people"}},
        };

        TVector<TCandidate> expected = {
            {.Kind = Keyword, .Content = "ALL"},
        };

        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, query, {}), expected);
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, query, env), expected);
    }
}

Y_UNIT_TEST(ColumnFromTableFunction) {
    auto engine = MakeSqlCompletionEngineUT();

    TVector<TString> input = {
        R"sql(SELECT # FROM loggy.CONCAT(`yql/2025-01`))sql",
        R"sql(SELECT # FROM loggy.CONCAT(`yql/2025-01`, `yql/2025-02`))sql",
        R"sql(SELECT # FROM loggy.RANGE(`yql`, `2025-01`, `2025-03`))sql",
    };

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "message"},
        {.Kind = ColumnName, .Content = "timestamp"},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, input[0]), expected);
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, input[1]), expected);
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, input[2]), expected);
}

Y_UNIT_TEST(ColumnPositions) {
    auto engine = MakeSqlCompletionEngineUT();

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "Age"},
        {.Kind = ColumnName, .Content = "Name"},
    };

    {
        TString query = "SELECT # FROM example.`/people`";
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
    }
    {
        TString query = "SELECT f(#) FROM example.`/people`";
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
    }
    {
        TString query = "SELECT * FROM example.`/people` WHERE #";
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
    }
    {
        TString query = "SELECT * FROM example.`/people` WHERE a#";
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, query).at(0).Content, "Age");
    }
    {
        TString query = "SELECT * FROM example.`/people` WHERE f(#)";
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
    }
    {
        TString query = "SELECT * FROM example.`/people` ORDER BY #";
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
    }
    {
        TString query = "SELECT * FROM example.`/people` ORDER BY a#";
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, query).at(0).Content, "Age");
    }
    {
        TString query = "SELECT * FROM example.`/people` ORDER BY f(#)";
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
    }
    {
        TString query = "SELECT * FROM example.`/people` GROUP BY #";
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
    }
    {
        TString query = "SELECT * FROM example.`/people` GROUP BY a#";
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, query).at(0).Content, "Age");
    }
    {
        TString query = R"(
                SELECT *
                FROM example.`/people` AS a
                JOIN example.`/people` AS b ON a.#
            )";
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
    }
}

Y_UNIT_TEST(ColumnFiltration) {
    auto engine = MakeSqlCompletionEngineUT();

    TString query = R"sql(
            SELECT roo#
            FROM example.`/people` AS roommate
            JOIN example.`/yql/tutorial` AS query ON 1 = 1;
        )sql";

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "room"},
        {.Kind = ColumnName, .Content = "query.room"},
        {.Kind = ColumnName, .Content = "roommate.Age"},
        {.Kind = ColumnName, .Content = "roommate.Name"},
    };
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
}

Y_UNIT_TEST(QualifiedColumnAtWhere) {
    auto engine = MakeSqlCompletionEngineUT();

    TString prefix = R"sql(SELECT * FROM example.`/people` AS x )sql";

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "Age"},
        {.Kind = ColumnName, .Content = "Name"},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, prefix + "WHERE x.#"), expected);
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, prefix + "GROUP BY x.#"), expected);
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, prefix + "HAVING x.#"), expected);
}

Y_UNIT_TEST(ColumnFromQualifiedAtWhere) {
    auto engine = MakeSqlCompletionEngineUT();

    TString prefix = R"sql(SELECT * FROM example.`/people` AS x )sql";

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "Age"},
        {.Kind = ColumnName, .Content = "Name"},
        {.Kind = ColumnName, .Content = "x.Age"},
        {.Kind = ColumnName, .Content = "x.Name"},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, prefix + "WHERE #"), expected);
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, prefix + "GROUP BY #"), expected);
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, prefix + "HAVING #"), expected);
}

Y_UNIT_TEST(ColumnFromQuotedAlias) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TString query;

        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "Age"},
            {.Kind = ColumnName, .Content = "Name"},
            {.Kind = ColumnName, .Content = "`per son`.Age"},
            {.Kind = ColumnName, .Content = "`per son`.Name"},
        };

        query = R"sql(SELECT # FROM example.`/people` AS `per son`)sql";
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
    }
    {
        TString query = R"sql(SELECT per# FROM example.`/people` AS `per son`)sql";

        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "`per son`.Age"},
            {.Kind = ColumnName, .Content = "`per son`.Name"},
        };

        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
    }
    {
        TString query = R"sql(SELECT `per son`.# FROM example.`/people` AS `per son`)sql";

        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "Age"},
            {.Kind = ColumnName, .Content = "Name"},
        };

        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
    }
}

Y_UNIT_TEST(ColumnFromFolderLikeQuotedAlias) {
    auto engine = MakeSqlCompletionEngineUT();

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "Age"},
        {.Kind = ColumnName, .Content = "Name"},
        {.Kind = ColumnName, .Content = "`people/`.Age"},
        {.Kind = ColumnName, .Content = "`people/`.Name"},
    };

    TString query;

    query = R"sql(SELECT # FROM example.`/people` AS `people/`)sql";
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);

    query = R"sql(SELECT # FROM (SELECT * FROM example.`/people`) AS `people/`)sql";
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);

    query = R"sql(SELECT # FROM (SELECT Age, Name FROM example.`/people`) AS `people/`)sql";
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
}

Y_UNIT_TEST(ProjectionVisibility) {
    auto engine = MakeSqlCompletionEngineUT();
    {
        TString query = "SELECT Age as a, # FROM example.`/people`";

        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "Age"},
            {.Kind = ColumnName, .Content = "Name"},
        };

        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
    }
    {
        TString query = "SELECT Age as a, b FROM example.`/people` ORDER BY #";

        TVector<TCandidate> expected = {
            {.Kind = ColumnName, .Content = "a"},
            {.Kind = ColumnName, .Content = "b"},
        };

        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
    }
}

Y_UNIT_TEST(ColumnFromNamedNode) {
    auto engine = MakeSqlCompletionEngineUT();

    TString prefix = R"sql(
            USE example;
            $source = '/peo' || 'ple';
        )sql";

    TVector<TString> input = {
        prefix + R"sql(SELECT # FROM $source)sql",
        prefix + R"sql(SELECT * FROM $source WHERE #)sql",
        prefix + R"sql(SELECT * FROM $source GROUP BY #)sql",
    };

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "Age"},
        {.Kind = ColumnName, .Content = "Name"},
        {.Kind = BindingName, .Content = "$source"},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(3, engine, input[0]), expected);
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(3, engine, input[1]), expected);
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(3, engine, input[2]), expected);
}

Y_UNIT_TEST(ColumnFromIndirectNamedNode) {
    auto engine = MakeSqlCompletionEngineUT();

    TString query = R"sql(
            $x = (SELECT 1 AS a);
            $y = $x;
            SELECT # FROM $y;
        )sql";

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "a"},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
}

Y_UNIT_TEST(TableFromRedefinedNamedNode) {
    auto engine = MakeSqlCompletionEngineUT();

    TString query = R"sql(
        USE example;
        $x = '/pe';
        $x = $x || 'op';
        $x = $x || 'le';
        SELECT # FROM $x;
    )sql";

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "Age"},
        {.Kind = ColumnName, .Content = "Name"},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
}

Y_UNIT_TEST(TableAtRedefinedNamedNode) {
    auto engine = MakeSqlCompletionEngineUT();

    TString query = R"sql(
        USE example;
        $x = '/people';
        $x = SELECT a# FROM $x;
    )sql";

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "Age"},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
}

Y_UNIT_TEST(ColumnQuoted) {
    auto engine = MakeSqlCompletionEngineUT();

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "`Y Q L`"},
        {.Kind = ColumnName, .Content = "`o````o`"},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "SELECT # FROM saurus.maxim"), expected);
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, "SELECT Y# FROM saurus.maxim").at(0), expected.at(0));
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT `# FROM saurus.maxim").size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT `#` FROM saurus.maxim").size(), 0);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT `Y #` FROM saurus.maxim").size(), 0);
}

Y_UNIT_TEST(ColumnReplicationConflict) {
    auto engine = MakeSqlCompletionEngineUT();

    TString query = R"sql(
            SELECT #
            FROM (SELECT a, b) AS x
            JOIN (SELECT a, b) AS y
        )sql";

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "x.a"},
        {.Kind = ColumnName, .Content = "x.b"},
        {.Kind = ColumnName, .Content = "y.a"},
        {.Kind = ColumnName, .Content = "y.b"},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
}

Y_UNIT_TEST(ColumnReplication) {
    auto engine = MakeSqlCompletionEngineUT();

    TString query = R"sql(
            SELECT #
            FROM (SELECT a, b) AS x
            JOIN (SELECT b, c) AS y
        )sql";

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "a"},
        {.Kind = ColumnName, .Content = "c"},
        {.Kind = ColumnName, .Content = "x.a"},
        {.Kind = ColumnName, .Content = "x.b"},
        {.Kind = ColumnName, .Content = "y.b"},
        {.Kind = ColumnName, .Content = "y.c"},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
}

Y_UNIT_TEST(ColumnReplicationCaseSensivity) {
    auto engine = MakeSqlCompletionEngineUT();

    TString query = R"sql(
            SELECT #
            FROM (SELECT A, B) AS x
            JOIN (SELECT a, b) AS y
        )sql";

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "A"},
        {.Kind = ColumnName, .Content = "B"},
        {.Kind = ColumnName, .Content = "a"},
        {.Kind = ColumnName, .Content = "b"},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
}

Y_UNIT_TEST(ColumnReplicationFiltration) {
    auto engine = MakeSqlCompletionEngineUT();

    TVector<TString> queries = {
        R"sql(SELECT x# FROM (SELECT XXX) AS xxx)sql",
        R"sql(SELECT X# FROM (SELECT XXX) AS xxx)sql",
    };

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "XXX"},
        {.Kind = ColumnName, .Content = "xxx.XXX"},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, queries[0]), expected);
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, queries[1]), expected);
}

Y_UNIT_TEST(ColumnAtWhereInSubquery) {
    auto engine = MakeSqlCompletionEngineUT();

    TString query = R"sql(
            SELECT * FROM (
                SELECT a AS b
                FROM (SELECT 1 AS a) AS x
                WHERE x.#
            );
        )sql";

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "a"},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, query), expected);
}

Y_UNIT_TEST(ColumnAtSubqueryExpresson) {
    auto engine = MakeSqlCompletionEngineUT();

    TVector<TString> input = {
        R"sql(SELECT (SELECT # FROM (SELECT 1 AS a));)sql",
        R"sql(SELECT 1 + (SELECT # FROM (SELECT 1 AS a));)sql",
        R"sql(SELECT * FROM t WHERE (SELECT # FROM (SELECT 1 AS a));)sql",
        R"sql(SELECT * FROM t WHERE 1 < (SELECT # FROM (SELECT 1 AS a));)sql",
    };

    TVector<TCandidate> expected = {
        {.Kind = ColumnName, .Content = "a"},
    };

    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, input[0]), expected);
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, input[1]), expected);
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, input[2]), expected);
    UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, input[3]), expected);
}

Y_UNIT_TEST(NoBindingAtQuoted) {
    auto engine = MakeSqlCompletionEngineUT();

    TVector<TCandidate> expected = {
        {.Kind = FolderName, .Content = ".sys/"},
        {.Kind = FolderName, .Content = "local/"},
        {.Kind = FolderName, .Content = "prod/"},
        {.Kind = FolderName, .Content = "test/"},
    };

    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "$x = 1; SELECT * FROM `#`"), expected);
}

Y_UNIT_TEST(Typing) {
    const auto queryUtf16 = TUtf16String::FromUtf8(
        "SELECT \n"
        "  123467, \"Hello, {name}! 编码\"}, \n"
        "  (1 + (5 * 1 / 0)), MIN(identifier), \n"
        "  Bool(field), Math::Sin(var) \n"
        "FROM `local/test/space/table` JOIN test;");

    auto engine = MakeSqlCompletionEngineUT();

    for (std::size_t size = 0; size <= queryUtf16.size(); ++size) {
        const TWtringBuf prefixUtf16(queryUtf16, 0, size);
        TCompletion completion = engine->Complete({.Text = TString::FromUtf16(prefixUtf16)}).GetValueSync();
        Y_DO_NOT_OPTIMIZE_AWAY(completion);
    }
}

Y_UNIT_TEST(Tabbing) {
    TString query = R"(
USE example;

SELECT
    123467, \"Hello, {name}! 编码\"},
    (1 + (5 * 1 / 0)), MIN(identifier),
    Bool(field), Math::Sin(var)
FROM `local/test/space/table`
JOIN yt:$cluster_name.test;
)";

    query += query + ";";
    query += query + ";";

    auto engine = MakeSqlCompletionEngineUT();

    const auto* begin = reinterpret_cast<const unsigned char*>(query.c_str());
    const auto* end = reinterpret_cast<const unsigned char*>(begin + query.size());
    const auto* ptr = begin;

    wchar32 rune;
    while (ptr < end) {
        Y_ENSURE(ReadUTF8CharAndAdvance(rune, ptr, end) == RECODE_OK);
        TCompletionInput input = {
            .Text = query,
            .CursorPosition = static_cast<size_t>(std::distance(begin, ptr)),
        };
        TCompletion completion = engine->Complete(input).GetValueSync();
        Y_DO_NOT_OPTIMIZE_AWAY(completion);
    }
}

Y_UNIT_TEST(CaseInsensitivity) {
    TVector<TCandidate> expected = {
        {.Kind = Keyword, .Content = "SELECT"},
    };

    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "se"), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "sE"), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "Se"), expected);
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SE"), expected);
}

Y_UNIT_TEST(InvalidStatementsRecovery) {
    auto engine = MakeSqlCompletionEngineUT();
    UNIT_ASSERT_GE(Complete(engine, "select select; ").size(), 35);
    UNIT_ASSERT_GE(Complete(engine, "select select;").size(), 35);
    UNIT_ASSERT_GE(Complete(engine, "#;select select;").size(), 35);
    UNIT_ASSERT_GE(Complete(engine, "# ;select select;").size(), 35);
    UNIT_ASSERT_GE(Complete(engine, ";#;").size(), 35);
    UNIT_ASSERT_GE(Complete(engine, "#;;").size(), 35);
    UNIT_ASSERT_GE(Complete(engine, ";;#").size(), 35);
    UNIT_ASSERT_VALUES_EQUAL_C(Complete(engine, "!;").size(), 0, "Lexer failing");
}

Y_UNIT_TEST(InvalidCursorPosition) {
    auto engine = MakeSqlCompletionEngineUT();

    UNIT_ASSERT_NO_EXCEPTION(engine->Complete({"", 0}).GetValueSync());
    UNIT_ASSERT_EXCEPTION(engine->Complete({"", 1}).GetValueSync(), yexception);

    UNIT_ASSERT_NO_EXCEPTION(engine->Complete({"s", 0}).GetValueSync());
    UNIT_ASSERT_NO_EXCEPTION(engine->Complete({"s", 1}).GetValueSync());

    UNIT_ASSERT_NO_EXCEPTION(engine->Complete({"ы", 0}).GetValueSync());
    UNIT_ASSERT_EXCEPTION(engine->Complete({"ы", 1}).GetValueSync(), yexception);
    UNIT_ASSERT_NO_EXCEPTION(engine->Complete({"ы", 2}).GetValueSync());
}

Y_UNIT_TEST(DefaultNameService) {
    auto service = MakeStaticNameService(LoadDefaultNameSet(), LoadFrequencyData());
    auto engine = MakeSqlCompletionEngine(MakePureLexerSupplier(), std::move(service));
    {
        TVector<TCandidate> expected = {
            {.Kind = TypeName, .Content = "Uint64"},
            {.Kind = TypeName, .Content = "Uint32"},
            {.Kind = TypeName, .Content = "Utf8"},
            {.Kind = TypeName, .Content = "Uuid"},
            {.Kind = TypeName, .Content = "Uint8"},
            {.Kind = TypeName, .Content = "Unit"},
            {.Kind = TypeName, .Content = "Uint16"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT Optional<U"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = PragmaName, .Content = "yson.DisableStrict"},
            {.Kind = PragmaName, .Content = "yson.AutoConvert"},
            {.Kind = PragmaName, .Content = "yson.Strict"},
            {.Kind = PragmaName, .Content = "yson.CastToString"},
            {.Kind = PragmaName, .Content = "yson.DisableCastToString"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "PRAGMA yson"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = HintName, .Content = "IGNORE_TYPE_V3"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "REDUCE a WITH ig"), expected);
    }
}

Y_UNIT_TEST(OnFailingNameService) {
    auto service = MakeIntrusive<TFailingNameService>();
    auto engine = MakeSqlCompletionEngine(MakePureLexerSupplier(), std::move(service));
    UNIT_ASSERT_EXCEPTION(Complete(engine, ""), TDummyException);
    UNIT_ASSERT_EXCEPTION(Complete(engine, "SELECT Optional<U"), TDummyException);
    UNIT_ASSERT_EXCEPTION(Complete(engine, "SELECT CAST (1 AS ").size(), TDummyException);
}

Y_UNIT_TEST(NameNormalization) {
    auto service = MakeStaticNameService(LoadDefaultNameSet(), LoadFrequencyData());
    auto engine = MakeSqlCompletionEngine(MakePureLexerSupplier(), std::move(service));
    TVector<TCandidate> expected = {
        {.Kind = HintName, .Content = "IGNORE_TYPE_V3"},
    };
    UNIT_ASSERT_VALUES_EQUAL(Complete(engine, {"REDUCE a WITH ignoret"}), expected);
}

Y_UNIT_TEST(Ranking) {
    TFrequencyData frequency = {
        .Keywords = {
            {"select", 2},
            {"insert", 4},
        },
        .Pragmas = {
            {"yt.defaultmemorylimit", 16},
            {"yt.annotations", 8},
        },
        .Types = {
            {"int32", 128},
            {"int64", 64},
            {"interval", 32},
            {"interval64", 32},
        },
        .Functions = {
            {"min", 128},
            {"max", 64},
            {"maxof", 64},
            {"minby", 32},
            {"maxby", 32},
        },
        .Hints = {
            {"xlock", 4},
            {"unordered", 2},
        },
    };
    auto service = MakeStaticNameService(
        Pruned(LoadDefaultNameSet(), LoadFrequencyData()),
        std::move(frequency));
    auto engine = MakeSqlCompletionEngine(MakePureLexerSupplier(), std::move(service));
    {
        TVector<TCandidate> expected = {
            {.Kind = Keyword, .Content = "INSERT"},
            {.Kind = Keyword, .Content = "SELECT"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, ""), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = PragmaName, .Content = "DefaultMemoryLimit"},
            {.Kind = PragmaName, .Content = "Annotations"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, "PRAGMA yt."), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = TypeName, .Content = "Int32"},
            {.Kind = TypeName, .Content = "Int64"},
            {.Kind = TypeName, .Content = "Interval"},
            {.Kind = TypeName, .Content = "Interval64"},
            {.Kind = TypeName, .Content = "Int16"},
            {.Kind = TypeName, .Content = "Int8"},
        };
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT Optional<I"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = FunctionName, .Content = "Min()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "Max()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "MaxOf()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "MaxBy()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "MinBy()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "Math::Abs()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "Math::Acos()", .CursorShift = 1},
            {.Kind = FunctionName, .Content = "Math::Asin()", .CursorShift = 1},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, "SELECT m"), expected);
    }
    {
        TVector<TCandidate> expected = {
            {.Kind = HintName, .Content = "XLOCK"},
            {.Kind = HintName, .Content = "UNORDERED"},
            {.Kind = Keyword, .Content = "COLUMNS"},
            {.Kind = HintName, .Content = "FORCE_INFER_SCHEMA"},
        };
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, "SELECT * FROM a WITH "), expected);
    }
}

Y_UNIT_TEST(IgnoredRules) {
    auto lexer = MakePureLexerSupplier();

    TNameSet names;
    TFrequencyData frequency;
    auto service = MakeStaticNameService(names, MakeDefaultRanking(frequency));

    {
        auto engine = MakeSqlCompletionEngine(lexer, service);
        UNIT_ASSERT_UNEQUAL(Complete(engine, {"UPDA"}).size(), 0);
        UNIT_ASSERT_UNEQUAL(Complete(engine, {"DELE"}).size(), 0);
        UNIT_ASSERT_UNEQUAL(Complete(engine, {"ROLL"}).size(), 0);
        UNIT_ASSERT_UNEQUAL(Complete(engine, {"INSE"}).size(), 0);
        UNIT_ASSERT_UNEQUAL(Complete(engine, {"SELE"}).size(), 0);
    }

    auto config = MakeYQLConfiguration();
    auto engine = MakeSqlCompletionEngine(lexer, std::move(service), config);

    UNIT_ASSERT_EQUAL(Complete(engine, {"UPDA"}).size(), 0);
    UNIT_ASSERT_EQUAL(Complete(engine, {"DELE"}).size(), 0);
    UNIT_ASSERT_EQUAL(Complete(engine, {"ROLL"}).size(), 0);
    UNIT_ASSERT_UNEQUAL(Complete(engine, {"INSE"}).size(), 0);
    UNIT_ASSERT_UNEQUAL(Complete(engine, {"SELE"}).size(), 0);
}

Y_UNIT_TEST(IgnoredTokens) {
    auto lexer = MakePureLexerSupplier();

    TNameSet names;
    TFrequencyData frequency;
    auto service = MakeStaticNameService(names, MakeDefaultRanking(frequency));

    auto config = MakeYQLConfiguration();
    auto engine = MakeSqlCompletionEngine(lexer, std::move(service), config);

    UNIT_ASSERT(!FindPtr(Complete(engine, {""}), TCandidate{Keyword, "FOR"}));
    UNIT_ASSERT(!FindPtr(Complete(engine, {""}), TCandidate{Keyword, "PARALLEL"}));

    UNIT_ASSERT(FindPtr(Complete(engine, {"EVALUATE "}), TCandidate{Keyword, "FOR"}));
    UNIT_ASSERT(FindPtr(Complete(engine, {"EVALUATE  "}), TCandidate{Keyword, "FOR"}));
    UNIT_ASSERT(FindPtr(Complete(engine, {"EVALUATE /**/"}), TCandidate{Keyword, "FOR"}));
}

Y_UNIT_TEST(CachedSchema) {
    TLexerSupplier lexer = MakePureLexerSupplier();

    auto time = NMonotonic::CreateDefaultMonotonicTimeProvider();
    TSchemaCaches caches = {
        .List = MakeLocalCache<
            TSchemaDescribeCacheKey, TVector<TFolderEntry>>(time, {}),
        .DescribeTable = MakeLocalCache<
            TSchemaDescribeCacheKey, TMaybe<TTableDetails>>(time, {}),
    };

    auto aliceService = MakeSchemaNameService(
        MakeSimpleSchema(
            MakeCachedSimpleSchema(
                caches, "alice",
                MakeStaticSimpleSchema(TSchemaData{
                    .Folders = {{"", {{"/", {{.Type = "Table", .Name = "alice"}}}}}},
                    .Tables = {{"", {{"/alice", {{"alice"}}}}}},
                }))));

    auto petyaService = MakeSchemaNameService(
        MakeSimpleSchema(
            MakeCachedSimpleSchema(
                caches, "petya",
                MakeStaticSimpleSchema(TSchemaData{
                    .Folders = {{"", {{"/", {{.Type = "Table", .Name = "petya"}}}}}},
                    .Tables = {{"", {{"/petya", {{"petya"}}}}}},
                }))));

    auto aliceEngine = MakeSqlCompletionEngine(lexer, std::move(aliceService));
    auto petyaEngine = MakeSqlCompletionEngine(lexer, std::move(petyaService));

    TVector<TCandidate> empty;
    {
        TVector<TCandidate> aliceExpected = {{.Kind = TableName, .Content = "`alice`"}};
        TVector<TCandidate> petyaExpected = {{.Kind = TableName, .Content = "`petya`"}};

        // Updates in backround
        UNIT_ASSERT_VALUES_EQUAL(Complete(aliceEngine, "SELECT * FROM "), aliceExpected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(petyaEngine, "SELECT * FROM "), petyaExpected);
    }
    {
        TVector<TCandidate> aliceExpected = {{.Kind = ColumnName, .Content = "alice"}};
        TVector<TCandidate> petyaExpected = {{.Kind = ColumnName, .Content = "petya"}};

        // Updates in backround
        UNIT_ASSERT_VALUES_EQUAL(Complete(aliceEngine, "SELECT a# FROM alice"), aliceExpected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(petyaEngine, "SELECT p# FROM petya"), petyaExpected);
    }
}

} // Y_UNIT_TEST_SUITE(SqlCompleteTests)
