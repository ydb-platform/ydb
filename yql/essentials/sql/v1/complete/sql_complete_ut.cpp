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

    TLexerSupplier MakePureLexerSupplier() {
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4Pure = NSQLTranslationV1::MakeAntlr4PureLexerFactory();
        lexers.Antlr4PureAnsi = NSQLTranslationV1::MakeAntlr4PureAnsiLexerFactory();
        return [lexers = std::move(lexers)](bool ansi) {
            return NSQLTranslationV1::MakeLexer(
                lexers, ansi, /* antlr4 = */ true,
                NSQLTranslationV1::ELexerFlavor::Pure);
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
                }}
            }},
            "saurus": { "type": "Folder", "entries": {
                "maxim": { "type": "Table", "columns": {
                   "Y Q L": {},
                   "o``o": {}
                }}
            }}
        })";

        NJson::TJsonMap clustersJson;
        Y_ENSURE(NJson::ReadJsonTree(clustersText, &clustersJson));

        auto clustersIt = NFuncTools::Filter(
            [](const auto& x) { return !x.empty(); }, IterateKeys(clustersJson.GetMapSafe()));
        TVector<TString> clusters(begin(clustersIt), end(clustersIt));

        TFrequencyData frequency;

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
        INameService::TPtr service = MakeUnionNameService(
            std::move(children), MakeDefaultRanking(frequency));

        return MakeSqlCompletionEngine(std::move(lexer), std::move(service));
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
            {Keyword, "ALTER"},
            {Keyword, "ANALYZE"},
            {Keyword, "BACKUP"},
            {Keyword, "BATCH"},
            {Keyword, "COMMIT"},
            {Keyword, "CREATE"},
            {Keyword, "DECLARE"},
            {Keyword, "DEFINE"},
            {Keyword, "DELETE FROM"},
            {Keyword, "DISCARD"},
            {Keyword, "DO"},
            {Keyword, "DROP"},
            {Keyword, "EVALUATE"},
            {Keyword, "EXPLAIN"},
            {Keyword, "EXPORT"},
            {Keyword, "FOR"},
            {Keyword, "FROM"},
            {Keyword, "GRANT"},
            {Keyword, "IF"},
            {Keyword, "IMPORT"},
            {Keyword, "INSERT"},
            {Keyword, "PARALLEL"},
            {Keyword, "PRAGMA"},
            {Keyword, "PROCESS"},
            {Keyword, "REDUCE"},
            {Keyword, "REPLACE"},
            {Keyword, "RESTORE"},
            {Keyword, "REVOKE"},
            {Keyword, "ROLLBACK"},
            {Keyword, "SELECT"},
            {Keyword, "SHOW CREATE"},
            {Keyword, "UPDATE"},
            {Keyword, "UPSERT"},
            {Keyword, "USE"},
            {Keyword, "VALUES"},
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
            {ClusterName, "example"},
            {ClusterName, "saurus"},
        };
        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "USE "), expected);
    }

    Y_UNIT_TEST(UseClusterResultion) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {TableName, "`maxim`"},
                {ClusterName, "example"},
                {ClusterName, "saurus"},
                {Keyword, "ANY"},
            };
            UNIT_ASSERT_VALUES_EQUAL(
                CompleteTop(
                    4,
                    engine,
                    "USE yt:$cluster_name; SELECT * FROM ",
                    {.Parameters = {{"$cluster_name", "saurus"}}}),
                expected);
        }
        {
            TVector<TCandidate> expected = {
                {FolderName, "`.sys/`", 1},
                {FolderName, "`local/`", 1},
                {FolderName, "`prod/`", 1},
                {FolderName, "`test/`", 1},
                {ClusterName, "example"},
                {ClusterName, "saurus"},
                {Keyword, "ANY"},
            };
            UNIT_ASSERT_VALUES_EQUAL(
                CompleteTop(
                    7,
                    engine,
                    "USE yt:$cluster_name; SELECT * FROM ",
                    {.Parameters = {}}),
                expected);
        }
    }

    Y_UNIT_TEST(Alter) {
        TVector<TCandidate> expected = {
            {Keyword, "ASYNC REPLICATION"},
            {Keyword, "BACKUP COLLECTION"},
            {Keyword, "DATABASE"},
            {Keyword, "EXTERNAL"},
            {Keyword, "GROUP"},
            {Keyword, "OBJECT"},
            {Keyword, "RESOURCE POOL"},
            {Keyword, "SEQUENCE"},
            {Keyword, "TABLE"},
            {Keyword, "TABLESTORE"},
            {Keyword, "TOPIC"},
            {Keyword, "TRANSFER"},
            {Keyword, "USER"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "ALTER "), expected);
    }

    Y_UNIT_TEST(Create) {
        TVector<TCandidate> expected = {
            {Keyword, "ASYNC REPLICATION"},
            {Keyword, "BACKUP COLLECTION"},
            {Keyword, "EXTERNAL"},
            {Keyword, "GROUP"},
            {Keyword, "OBJECT"},
            {Keyword, "OR REPLACE"},
            {Keyword, "RESOURCE POOL"},
            {Keyword, "TABLE"},
            {Keyword, "TABLESTORE"},
            {Keyword, "TEMP TABLE"},
            {Keyword, "TEMPORARY TABLE"},
            {Keyword, "TOPIC"},
            {Keyword, "TRANSFER"},
            {Keyword, "USER"},
            {Keyword, "VIEW"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "CREATE "), expected);
    }

    Y_UNIT_TEST(CreateTable) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {FolderName, "`.sys/`", 1},
                {FolderName, "`local/`", 1},
                {FolderName, "`prod/`", 1},
                {FolderName, "`test/`", 1},
                {ClusterName, "example"},
                {ClusterName, "saurus"},
                {Keyword, "IF NOT EXISTS"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "CREATE TABLE #"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {FolderName, "service/"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "CREATE TABLE `test/#`"), expected);
        }
    }

    Y_UNIT_TEST(Delete) {
        TVector<TCandidate> expected = {
            {Keyword, "FROM"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "DELETE "), expected);
    }

    Y_UNIT_TEST(Drop) {
        TVector<TCandidate> expected = {
            {Keyword, "ASYNC REPLICATION"},
            {Keyword, "BACKUP COLLECTION"},
            {Keyword, "EXTERNAL"},
            {Keyword, "GROUP"},
            {Keyword, "OBJECT"},
            {Keyword, "RESOURCE POOL"},
            {Keyword, "TABLE"},
            {Keyword, "TABLESTORE"},
            {Keyword, "TOPIC"},
            {Keyword, "TRANSFER"},
            {Keyword, "USER"},
            {Keyword, "VIEW"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "DROP "), expected);
    }

    Y_UNIT_TEST(DropObject) {
        TVector<TCandidate> expected = {
            {FolderName, "`.sys/`", 1},
            {FolderName, "`local/`", 1},
            {FolderName, "`prod/`", 1},
            {FolderName, "`test/`", 1},
            {ClusterName, "example"},
            {ClusterName, "saurus"},
            {Keyword, "IF EXISTS"},
        };
        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "DROP TABLE "), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "DROP VIEW "), expected);
    }

    Y_UNIT_TEST(Explain) {
        TVector<TCandidate> expected = {
            {Keyword, "ALTER"},
            {Keyword, "ANALYZE"},
            {Keyword, "BACKUP"},
            {Keyword, "BATCH"},
            {Keyword, "COMMIT"},
            {Keyword, "CREATE"},
            {Keyword, "DECLARE"},
            {Keyword, "DEFINE"},
            {Keyword, "DELETE FROM"},
            {Keyword, "DISCARD"},
            {Keyword, "DO"},
            {Keyword, "DROP"},
            {Keyword, "EVALUATE"},
            {Keyword, "EXPORT"},
            {Keyword, "FOR"},
            {Keyword, "FROM"},
            {Keyword, "GRANT"},
            {Keyword, "IF"},
            {Keyword, "IMPORT"},
            {Keyword, "INSERT"},
            {Keyword, "PARALLEL"},
            {Keyword, "PRAGMA"},
            {Keyword, "PROCESS"},
            {Keyword, "QUERY PLAN"},
            {Keyword, "REDUCE"},
            {Keyword, "REPLACE"},
            {Keyword, "RESTORE"},
            {Keyword, "REVOKE"},
            {Keyword, "ROLLBACK"},
            {Keyword, "SELECT"},
            {Keyword, "SHOW CREATE"},
            {Keyword, "UPDATE"},
            {Keyword, "UPSERT"},
            {Keyword, "USE"},
            {Keyword, "VALUES"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "EXPLAIN "), expected);
    }

    Y_UNIT_TEST(Grant) {
        TVector<TCandidate> expected = {
            {Keyword, "ALL"},
            {Keyword, "ALTER SCHEMA"},
            {Keyword, "CONNECT"},
            {Keyword, "CREATE"},
            {Keyword, "DESCRIBE SCHEMA"},
            {Keyword, "DROP"},
            {Keyword, "ERASE ROW"},
            {Keyword, "FULL"},
            {Keyword, "GRANT"},
            {Keyword, "INSERT"},
            {Keyword, "LIST"},
            {Keyword, "MANAGE"},
            {Keyword, "MODIFY"},
            {Keyword, "REMOVE SCHEMA"},
            {Keyword, "SELECT"},
            {Keyword, "UPDATE ROW"},
            {Keyword, "USE"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "GRANT "), expected);
    }

    Y_UNIT_TEST(Insert) {
        TVector<TCandidate> expected = {
            {Keyword, "INTO"},
            {Keyword, "OR"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "INSERT "), expected);
    }

    Y_UNIT_TEST(Pragma) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {Keyword, "ANSI"},
                {PragmaName, "yson.CastToString"},
                {PragmaName, "yt.RuntimeCluster"},
                {PragmaName, "yt.RuntimeClusterSelection"}};
            auto completion = engine->Complete({"PRAGMA "}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "");
        }
        {
            TVector<TCandidate> expected = {
                {PragmaName, "yson.CastToString"}};
            auto completion = engine->Complete({"PRAGMA ys"}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "ys");
        }
        {
            TVector<TCandidate> expected = {
                {PragmaName, "yson.CastToString"}};
            auto completion = engine->Complete({"PRAGMA yson"}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "yson");
        }
        {
            TVector<TCandidate> expected = {
                {PragmaName, "CastToString"}};
            auto completion = engine->Complete({"PRAGMA yson."}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "");
        }
        {
            TVector<TCandidate> expected = {
                {PragmaName, "CastToString"}};
            auto completion = engine->Complete({"PRAGMA yson.cast"}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "cast");
        }
        {
            TVector<TCandidate> expected = {
                {PragmaName, "RuntimeCluster"},
                {PragmaName, "RuntimeClusterSelection"}};
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "pragma yt."), expected);
            UNIT_ASSERT_VALUES_EQUAL(
                Complete(engine, "pragma yt.RuntimeClusterSelection='force';\npragma yt.Ru"),
                expected);
        }
        {
            TVector<TCandidate> expected = {
                {PragmaName, "RuntimeCluster"},
                {PragmaName, "RuntimeClusterSelection"}};
            UNIT_ASSERT_VALUES_EQUAL(
                Complete(engine, "pragma yt.Ru#\n"),
                expected);
        }
    }

    Y_UNIT_TEST(Select) {
        TVector<TCandidate> expected = {
            {Keyword, "ALL"},
            {Keyword, "BITCAST()", 1},
            {Keyword, "CASE"},
            {Keyword, "CAST()", 1},
            {Keyword, "CURRENT_DATE"},
            {Keyword, "CURRENT_TIME"},
            {Keyword, "CURRENT_TIMESTAMP"},
            {TypeName, "Callable<>", 1},
            {Keyword, "DISTINCT"},
            {FunctionName, "DateTime::Split()", 1},
            {TypeName, "Decimal()", 1},
            {TypeName, "Dict<>", 1},
            {Keyword, "EMPTY_ACTION"},
            {Keyword, "EXISTS()", 1},
            {TypeName, "Enum<>", 1},
            {Keyword, "FALSE"},
            {TypeName, "Flow<>", 1},
            {Keyword, "JSON_EXISTS()", 1},
            {Keyword, "JSON_QUERY()", 1},
            {Keyword, "JSON_VALUE()", 1},
            {TypeName, "List<>", 1},
            {Keyword, "NOT"},
            {Keyword, "NULL"},
            {TypeName, "Optional<>", 1},
            {FunctionName, "Python::__private()", 1},
            {TypeName, "Resource<>", 1},
            {Keyword, "STREAM"},
            {TypeName, "Set<>", 1},
            {FunctionName, "StartsWith()", 1},
            {TypeName, "Stream<>", 1},
            {TypeName, "Struct<>", 1},
            {Keyword, "TRUE"},
            {TypeName, "Tagged<>", 1},
            {TypeName, "Tuple<>", 1},
            {TypeName, "Uint64"},
            {TypeName, "Variant<>", 1},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT "), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT # FROM"), expected);
    }

    Y_UNIT_TEST(SelectFrom) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {FolderName, "`.sys/`", 1},
                {FolderName, "`local/`", 1},
                {FolderName, "`prod/`", 1},
                {FolderName, "`test/`", 1},
                {ClusterName, "example"},
                {ClusterName, "saurus"},
                {Keyword, "ANY"},
                {FunctionName, "CONCAT()", 1},
                {FunctionName, "EACH()", 1},
                {FunctionName, "FILTER()", 1},
                {FunctionName, "FOLDER()", 1},
                {FunctionName, "LIKE()", 1},
                {FunctionName, "RANGE()", 1},
                {FunctionName, "REGEXP()", 1},
                {FunctionName, "WalkFolders()", 1},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM "), expected);
        }
        {
            TString input = "SELECT * FROM pr";
            TVector<TCandidate> expected = {
                {FolderName, "`prod/`", 1},
            };
            TCompletion actual = engine->Complete(SharpedInput(input)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, "pr");
        }
        {
            TVector<TCandidate> expected = {
                {FolderName, ".sys/`", 1},
                {FolderName, "local/`", 1},
                {FolderName, "prod/`", 1},
                {FolderName, "test/`", 1},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM `#"), expected);
        }
        {
            TString input = "SELECT * FROM `#`";
            TVector<TCandidate> expected = {
                {FolderName, ".sys/"},
                {FolderName, "local/"},
                {FolderName, "prod/"},
                {FolderName, "test/"},
            };
            TCompletion actual = engine->Complete(SharpedInput(input)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, "");
        }
        {
            TString input = "SELECT * FROM `local/#`";
            TVector<TCandidate> expected = {
                {TableName, "abacaba"},
                {TableName, "account"},
                {TableName, "example"},
            };
            TCompletion actual = engine->Complete(SharpedInput(input)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, "");
        }
        {
            TString input = "SELECT * FROM `local/a#`";
            TVector<TCandidate> expected = {
                {TableName, "abacaba"},
                {TableName, "account"},
            };
            TCompletion actual = engine->Complete(SharpedInput(input)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, "a");
        }
        {
            TString input = "SELECT * FROM `.sy#`";
            TVector<TCandidate> expected = {
                {FolderName, ".sys/"},
            };
            TCompletion actual = engine->Complete(SharpedInput(input)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, ".sy");
        }
        {
            TString input = "SELECT * FROM `/test/ser#vice/`";
            TVector<TCandidate> expected = {
                {FolderName, "service/"},
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
                {FolderName, ".sys/`", 1},
                {FolderName, "local/`", 1},
                {FolderName, "prod/`", 1},
                {FolderName, "test/`", 1},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(4, engine, "SELECT * FROM `#"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {TableName, "meta`"},
                {FolderName, "service/`", 1},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM `test/"), expected);
        }
    }

    Y_UNIT_TEST(SelectFromCluster) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {ClusterName, "example"},
                {ClusterName, "saurus"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM yt:"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {ClusterName, "saurus"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM yt:saurus#"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {TableName, "`maxim`"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, "SELECT * FROM yt:saurus."), expected);
        }
        {
            TVector<TCandidate> expected = {
                {TableName, "`people`"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, "SELECT * FROM example."), expected);
        }
        {
            TVector<TCandidate> expected = {
                {TableName, "tutorial"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, "SELECT * FROM example.`/yql/t#`"), expected);
        }
    }

    Y_UNIT_TEST(SelectFromWithUse) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {TableName, "`maxim`"},
                {ClusterName, "example"},
                {ClusterName, "saurus"},
                {Keyword, "ANY"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(4, engine, "USE yt:saurus; SELECT * FROM "), expected);
        }
        {
            TVector<TCandidate> expected = {
                {TableName, "`people`"},
                {FolderName, "`yql/`", 1},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "USE yt:saurus; SELECT * FROM example."), expected);
        }
        {
            TVector<TCandidate> expected = {
                {TableName, "`maxim`"},
                {ClusterName, "example"},
                {ClusterName, "saurus"},
                {Keyword, "ANY"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(4, engine, "USE example; USE yt:saurus; SELECT * FROM "), expected);
        }
        {
            TVector<TCandidate> expected = {
                {BindingName, "$hello"},
                {TableName, "`maxim`"},
                {ClusterName, "example"},
                {ClusterName, "saurus"},
                {Keyword, "ANY"},
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
                {BindingName, "$action"},
                {TableName, "`people`"},
                {FolderName, "`yql/`", 1},
                {ClusterName, "example"},
                {ClusterName, "saurus"},
                {Keyword, "ANY"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(6, engine, R"(
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
            {Keyword, "BITCAST()", 1},
            {Keyword, "CASE"},
            {Keyword, "CAST()", 1},
            {Keyword, "CURRENT_DATE"},
            {Keyword, "CURRENT_TIME"},
            {Keyword, "CURRENT_TIMESTAMP"},
            {TypeName, "Callable<>", 1},
            {FunctionName, "DateTime::Split()", 1},
            {TypeName, "Decimal()", 1},
            {TypeName, "Dict<>", 1},
            {Keyword, "EMPTY_ACTION"},
            {Keyword, "EXISTS()", 1},
            {TypeName, "Enum<>", 1},
            {Keyword, "FALSE"},
            {TypeName, "Flow<>", 1},
            {Keyword, "JSON_EXISTS()", 1},
            {Keyword, "JSON_QUERY()", 1},
            {Keyword, "JSON_VALUE()", 1},
            {TypeName, "List<>", 1},
            {Keyword, "NOT"},
            {Keyword, "NULL"},
            {TypeName, "Optional<>", 1},
            {FunctionName, "Python::__private()", 1},
            {TypeName, "Resource<>", 1},
            {TypeName, "Set<>", 1},
            {FunctionName, "StartsWith()", 1},
            {TypeName, "Stream<>", 1},
            {TypeName, "Struct<>", 1},
            {Keyword, "TRUE"},
            {TypeName, "Tagged<>", 1},
            {TypeName, "Tuple<>", 1},
            {TypeName, "Uint64"},
            {TypeName, "Variant<>", 1},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM a WHERE "), expected);
    }

    Y_UNIT_TEST(Upsert) {
        TVector<TCandidate> expected = {
            {Keyword, "INTO"},
            {Keyword, "OBJECT"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "UPSERT "), expected);
    }

    Y_UNIT_TEST(UpsertInto) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {FolderName, "`.sys/`", 1},
                {FolderName, "`local/`", 1},
                {FolderName, "`prod/`", 1},
                {FolderName, "`test/`", 1},
                {ClusterName, "example"},
                {ClusterName, "saurus"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "UPSERT INTO "), expected);
        }
        {
            TVector<TCandidate> expected = {
                {TableName, "meta"},
                {FolderName, "service/"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "UPSERT INTO `test/#`"), expected);
        }
    }

    Y_UNIT_TEST(TypeName) {
        TVector<TCandidate> expected = {
            {TypeName, "Callable<>", 1},
            {TypeName, "Decimal()", 1},
            {TypeName, "Dict<>", 1},
            {TypeName, "Enum<>", 1},
            {TypeName, "Flow<>", 1},
            {TypeName, "List<>", 1},
            {TypeName, "Optional<>", 1},
            {TypeName, "Resource<>", 1},
            {TypeName, "Set<>", 1},
            {TypeName, "Stream<>", 1},
            {TypeName, "Struct<>", 1},
            {TypeName, "Tagged<>", 1},
            {TypeName, "Tuple<>", 1},
            {TypeName, "Uint64"},
            {TypeName, "Variant<>", 1},
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
                {TypeName, "Uint64"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT Nothing(Uint"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {TypeName, "Optional<>", 1},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT Nothing(Option"), expected);
        }
    }

    Y_UNIT_TEST(FunctionName) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {FunctionName, "DateTime::Split()", 1},
            };
            auto completion = engine->Complete({"SELECT Date"}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "Date");
        }
        {
            TVector<TCandidate> expected = {
                {FunctionName, "Split()", 1},
            };
            auto completion = engine->Complete({"SELECT DateTime:"}).GetValueSync();
            UNIT_ASSERT(completion.Candidates.empty());
        }
        {
            TVector<TCandidate> expected = {
                {FunctionName, "Split()", 1},
            };
            auto completion = engine->Complete({"SELECT DateTime::"}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "");
        }
        {
            TVector<TCandidate> expected = {
                {FunctionName, "Split()", 1},
            };
            auto completion = engine->Complete({"SELECT DateTime::s"}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "s");
        }
    }

    Y_UNIT_TEST(SelectTableHintName) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {BindingName, "$udf"},
                {HintName, "XLOCK"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "PROCESS my_table USING $udf(TableRows()) WITH "), expected);
        }
        {
            TVector<TCandidate> expected = {
                {Keyword, "COLUMNS"},
                {Keyword, "SCHEMA"},
                {HintName, "XLOCK"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "REDUCE my_table WITH "), expected);
        }
        {
            TVector<TCandidate> expected = {
                {Keyword, "COLUMNS"},
                {Keyword, "SCHEMA"},
                {HintName, "XLOCK"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT key FROM my_table WITH "), expected);
        }
    }

    Y_UNIT_TEST(InsertTableHintName) {
        TVector<TCandidate> expected = {
            {Keyword, "COLUMNS"},
            {HintName, "EXPIRATION"},
            {Keyword, "SCHEMA"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "INSERT INTO my_table WITH "), expected);
    }

    Y_UNIT_TEST(CursorPosition) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {Keyword, "AND"},
                {Keyword, "AS"},
                {Keyword, "ASSUME"},
                {Keyword, "BETWEEN"},
                {Keyword, "COLLATE"},
                {Keyword, "EXCEPT"},
                {Keyword, "FROM"},
                {Keyword, "GLOB"},
                {Keyword, "GROUP"},
                {Keyword, "HAVING"},
                {Keyword, "ILIKE"},
                {Keyword, "IN"},
                {Keyword, "INTERSECT"},
                {Keyword, "INTO RESULT"},
                {Keyword, "IS"},
                {Keyword, "ISNULL"},
                {Keyword, "LIKE"},
                {Keyword, "LIMIT"},
                {Keyword, "MATCH"},
                {Keyword, "NOT"},
                {Keyword, "NOTNULL"},
                {Keyword, "OR"},
                {Keyword, "ORDER BY"},
                {Keyword, "REGEXP"},
                {Keyword, "RLIKE"},
                {Keyword, "UNION"},
                {Keyword, "WHERE"},
                {Keyword, "WINDOW"},
                {Keyword, "WITHOUT"},
                {Keyword, "XOR"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT `a`"), expected);
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT `a`#FROM"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {Keyword, "FROM"},
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
                {BindingName, "$a"},
                {BindingName, "$abac"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
        }
        {
            TVector<TCandidate> expected = {
                {BindingName, "$abac"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, query + "ab"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {BindingName, "a"},
                {BindingName, "abac"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, query + "$"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {BindingName, "abac"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, query + "$ab"), expected);
        }
    }

    Y_UNIT_TEST(TableFunction) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {FunctionName, "CONCAT()", 1},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, "SELECT * FROM Conca#"), expected);
        }
        {
            TVector<TCandidate> expected = {};
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, "SELECT Conca#"), expected);
        }
    }

    Y_UNIT_TEST(TableAsFunctionArgument) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {FolderName, "`.sys/`", 1},
                {FolderName, "`local/`", 1},
                {FolderName, "`prod/`", 1},
                {FolderName, "`test/`", 1},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(4, engine, "SELECT * FROM CONCAT(#)"), expected);
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(4, engine, "SELECT * FROM CONCAT(a, #)"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {TableName, "`people`"},
                {FolderName, "`yql/`", 1},
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
            UNIT_ASSERT_VALUES_UNEQUAL(
                CompleteTop(1, engine, "SELECT * FROM Range(``, #)").at(0).Kind, FolderName);
        }
        {
            UNIT_ASSERT_VALUES_UNEQUAL(CompleteTop(1, engine, "SELECT Max(#)").at(0).Kind, FolderName);
            UNIT_ASSERT_VALUES_UNEQUAL(CompleteTop(1, engine, "SELECT Concat(#)").at(0).Kind, FolderName);
        }
    }

    Y_UNIT_TEST(ColumnsAtSimpleSelect) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {ColumnName, "Age"},
                {ColumnName, "Name"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "SELECT # FROM example.`/people`"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {ColumnName, "Age"},
                {Keyword, "ALL"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "SELECT a# FROM example.`/people`"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {ColumnName, "Age"},
                {ColumnName, "Name"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "USE example; SELECT # FROM `/people`"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {ColumnName, "x.Age"},
                {ColumnName, "x.Name"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "SELECT # FROM example.`/people` AS x"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {ColumnName, "Age"},
                {ColumnName, "Name"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "SELECT x.# FROM example.`/people` AS x"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {ColumnName, "Age"},
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
                {ColumnName, "ep.Age"},
                {ColumnName, "ep.Name"},
                {ColumnName, "et.course"},
                {ColumnName, "et.room"},
                {ColumnName, "et.time"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(5, engine, query), expected);
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
                {ColumnName, "et.Room"},
                {ColumnName, "et.time"},
                {ColumnName, "ep.Age"},
                {ColumnName, "ep.Name"},
                {Keyword, "ALL"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(5, engine, query), expected);
        }
        {
            TString query = R"(
                SELECT #
                FROM example.`/yql/tutorial` AS x
                JOIN example.`/yql/tutorial` AS y ON 1 = 1
            )";

            TVector<TCandidate> expected = {
                {ColumnName, "y.course"},
                {ColumnName, "x.course"},
                {ColumnName, "x.room"},
                {ColumnName, "y.room"},
                {ColumnName, "x.time"},
                {ColumnName, "y.time"},
                {Keyword, "ALL"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(7, engine, query), expected);
        }
        {
            TString query = "SELECT # FROM (SELECT 1 AS x)";

            TVector<TCandidate> expected = {
                {ColumnName, "x"},
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
                {ColumnName, "ep.test"},
                {ColumnName, "ep.Age"},
                {ColumnName, "ep.Name"},
                {Keyword, "ALL"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(4, engine, query), expected);
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
                {ColumnName, "x.Age"},
                {ColumnName, "x.Name"},
                {ColumnName, "ep.Name"},
                {ColumnName, "ep.room"},
                {ColumnName, "ep.time"},
                {Keyword, "ALL"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(6, engine, query), expected);
        }
    }

    Y_UNIT_TEST(ColumnPositions) {
        auto engine = MakeSqlCompletionEngineUT();

        TVector<TCandidate> expected = {
            {ColumnName, "Age"},
            {ColumnName, "Name"},
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
            TString query = "SELECT * FROM example.`/people` WHERE f(#)";
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
        }
        {
            TString query = "SELECT * FROM example.`/people` ORDER BY #";
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
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
            TString query = R"(
                SELECT *
                FROM example.`/people` AS a
                JOIN example.`/people` AS b ON a.#
            )";
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
        }
    }

    Y_UNIT_TEST(ProjectionVisibility) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TString query = "SELECT Age as a, # FROM example.`/people`";

            TVector<TCandidate> expected = {
                {ColumnName, "Age"},
                {ColumnName, "Name"},
            };

            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
        }
        {
            TString query = "SELECT Age as a, b FROM example.`/people` WHERE #";

            TVector<TCandidate> expected = {
                {ColumnName, "a"},
                {ColumnName, "b"},
            };

            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, query), expected);
        }
    }

    Y_UNIT_TEST(ColumnQuoted) {
        auto engine = MakeSqlCompletionEngineUT();

        TVector<TCandidate> expected = {
            {ColumnName, "`Y Q L`"},
            {ColumnName, "`o````o`"},
        };

        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(2, engine, "SELECT # FROM saurus.maxim"), expected);
        UNIT_ASSERT_VALUES_EQUAL(CompleteTop(1, engine, "SELECT Y# FROM saurus.maxim").at(0), expected.at(0));
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT `# FROM saurus.maxim").size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT `#` FROM saurus.maxim").size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT `Y #` FROM saurus.maxim").size(), 0);
    }

    Y_UNIT_TEST(NoBindingAtQuoted) {
        auto engine = MakeSqlCompletionEngineUT();

        TVector<TCandidate> expected = {
            {FolderName, ".sys/"},
            {FolderName, "local/"},
            {FolderName, "prod/"},
            {FolderName, "test/"},
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
            TCompletion completion = engine->Complete({TString::FromUtf16(prefixUtf16)}).GetValueSync();
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
            {Keyword, "SELECT"},
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
                {TypeName, "Uint64"},
                {TypeName, "Uint32"},
                {TypeName, "Utf8"},
                {TypeName, "Uuid"},
                {TypeName, "Uint8"},
                {TypeName, "Unit"},
                {TypeName, "Uint16"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT Optional<U"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {PragmaName, "yson.DisableStrict"},
                {PragmaName, "yson.AutoConvert"},
                {PragmaName, "yson.Strict"},
                {PragmaName, "yson.CastToString"},
                {PragmaName, "yson.DisableCastToString"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "PRAGMA yson"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {HintName, "IGNORE_TYPE_V3"},
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
            {HintName, "IGNORE_TYPE_V3"},
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
                {Keyword, "INSERT"},
                {Keyword, "SELECT"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, ""), expected);
        }
        {
            TVector<TCandidate> expected = {
                {PragmaName, "DefaultMemoryLimit"},
                {PragmaName, "Annotations"},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, "PRAGMA yt."), expected);
        }
        {
            TVector<TCandidate> expected = {
                {TypeName, "Int32"},
                {TypeName, "Int64"},
                {TypeName, "Interval"},
                {TypeName, "Interval64"},
                {TypeName, "Int16"},
                {TypeName, "Int8"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT Optional<I"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {FunctionName, "Min()", 1},
                {FunctionName, "Max()", 1},
                {FunctionName, "MaxOf()", 1},
                {FunctionName, "MaxBy()", 1},
                {FunctionName, "MinBy()", 1},
                {FunctionName, "Math::Abs()", 1},
                {FunctionName, "Math::Acos()", 1},
                {FunctionName, "Math::Asin()", 1},
            };
            UNIT_ASSERT_VALUES_EQUAL(CompleteTop(expected.size(), engine, "SELECT m"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {HintName, "XLOCK"},
                {HintName, "UNORDERED"},
                {Keyword, "COLUMNS"},
                {HintName, "FORCE_INFER_SCHEMA"},
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
                        .Folders = {{"", {{"/", {{"Table", "alice"}}}}}},
                        .Tables = {{"", {{"/alice", {{"alice"}}}}}},
                    }))));

        auto petyaService = MakeSchemaNameService(
            MakeSimpleSchema(
                MakeCachedSimpleSchema(
                    caches, "petya",
                    MakeStaticSimpleSchema(TSchemaData{
                        .Folders = {{"", {{"/", {{"Table", "petya"}}}}}},
                        .Tables = {{"", {{"/petya", {{"petya"}}}}}},
                    }))));

        auto aliceEngine = MakeSqlCompletionEngine(lexer, std::move(aliceService));
        auto petyaEngine = MakeSqlCompletionEngine(lexer, std::move(petyaService));

        TVector<TCandidate> empty;
        {
            TVector<TCandidate> aliceExpected = {{TableName, "`alice`"}};
            TVector<TCandidate> petyaExpected = {{TableName, "`petya`"}};

            // Updates in backround
            UNIT_ASSERT_VALUES_EQUAL(Complete(aliceEngine, "SELECT * FROM "), aliceExpected);
            UNIT_ASSERT_VALUES_EQUAL(Complete(petyaEngine, "SELECT * FROM "), petyaExpected);
        }
        {
            TVector<TCandidate> aliceExpected = {{ColumnName, "alice"}};
            TVector<TCandidate> petyaExpected = {{ColumnName, "petya"}};

            // Updates in backround
            UNIT_ASSERT_VALUES_EQUAL(Complete(aliceEngine, "SELECT a# FROM alice"), aliceExpected);
            UNIT_ASSERT_VALUES_EQUAL(Complete(petyaEngine, "SELECT p# FROM petya"), petyaExpected);
        }
    }

} // Y_UNIT_TEST_SUITE(SqlCompleteTests)
