#include "sql_complete.h"

#include <yql/essentials/sql/v1/complete/name/cluster/static/discovery.h>
#include <yql/essentials/sql/v1/complete/name/object/dispatch/schema.h>
#include <yql/essentials/sql/v1/complete/name/object/simple/schema.h>
#include <yql/essentials/sql/v1/complete/name/object/simple/static/schema.h>
#include <yql/essentials/sql/v1/complete/name/service/ranking/frequency.h>
#include <yql/essentials/sql/v1/complete/name/service/ranking/ranking.h>
#include <yql/essentials/sql/v1/complete/name/service/cluster/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/schema/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/static/name_service.h>
#include <yql/essentials/sql/v1/complete/name/service/union/name_service.h>

#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_pure_ansi/lexer.h>

#include <library/cpp/testing/unittest/registar.h>

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
    NThreading::TFuture<TNameResponse> Lookup(TNameRequest) const override {
        auto e = std::make_exception_ptr(TDummyException());
        return NThreading::MakeErrorFuture<TNameResponse>(e);
    }
};

Y_UNIT_TEST_SUITE(SqlCompleteTests) {
    using ECandidateKind::ClusterName;
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

        THashMap<TString, THashMap<TString, TVector<TFolderEntry>>> fss = {
            {"", {{"/", {{"Folder", "local"},
                         {"Folder", "test"},
                         {"Folder", "prod"},
                         {"Folder", ".sys"}}},
                  {"/local/", {{"Table", "example"},
                               {"Table", "account"},
                               {"Table", "abacaba"}}},
                  {"/test/", {{"Folder", "service"},
                              {"Table", "meta"}}},
                  {"/test/service/", {{"Table", "example"}}},
                  {"/.sys/", {{"Table", "status"}}}}},
            {"example",
             {{"/", {{"Table", "people"},
                     {"Folder", "yql"}}},
              {"/yql/", {{"Table", "tutorial"}}}}},
            {"yt:saurus",
             {{"/", {{"Table", "maxim"}}}}},
        };

        TVector<TString> clusters;
        for (const auto& [cluster, _] : fss) {
            clusters.emplace_back(cluster);
        }
        EraseIf(clusters, [](const auto& s) { return s.empty(); });

        TFrequencyData frequency;

        IRanking::TPtr ranking = MakeDefaultRanking(frequency);

        THashMap<TString, ISchema::TPtr> schemasByCluster;
        for (auto& [cluster, fs] : fss) {
            schemasByCluster[std::move(cluster)] =
                MakeSimpleSchema(
                    MakeStaticSimpleSchema(std::move(fs)));
        }

        TVector<INameService::TPtr> children = {
            MakeStaticNameService(std::move(names), frequency),
            MakeSchemaNameService(MakeDispatchSchema(std::move(schemasByCluster))),
            MakeClusterNameService(MakeStaticClusterDiscovery(std::move(clusters))),
        };

        INameService::TPtr service = MakeUnionNameService(std::move(children), ranking);

        return MakeSqlCompletionEngine(std::move(lexer), std::move(service));
    }

    TVector<TCandidate> Complete(ISqlCompletionEngine::TPtr& engine, TString sharped) {
        return engine->CompleteAsync(SharpedInput(sharped)).GetValueSync().Candidates;
    }

    TVector<TCandidate> CompleteTop(size_t limit, ISqlCompletionEngine::TPtr& engine, TString sharped) {
        auto candidates = Complete(engine, sharped);
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
            {ClusterName, "yt:saurus"},
        };
        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "USE "), expected);
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
                {FolderName, "`.sys/`"},
                {FolderName, "`local/`"},
                {FolderName, "`prod/`"},
                {FolderName, "`test/`"},
                {ClusterName, "example"},
                {ClusterName, "yt:saurus"},
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
            {FolderName, "`.sys/`"},
            {FolderName, "`local/`"},
            {FolderName, "`prod/`"},
            {FolderName, "`test/`"},
            {ClusterName, "example"},
            {ClusterName, "yt:saurus"},
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
            auto completion = engine->CompleteAsync({"PRAGMA "}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "");
        }
        {
            TVector<TCandidate> expected = {
                {PragmaName, "yson.CastToString"}};
            auto completion = engine->CompleteAsync({"PRAGMA ys"}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "ys");
        }
        {
            TVector<TCandidate> expected = {
                {PragmaName, "yson.CastToString"}};
            auto completion = engine->CompleteAsync({"PRAGMA yson"}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "yson");
        }
        {
            TVector<TCandidate> expected = {
                {PragmaName, "CastToString"}};
            auto completion = engine->CompleteAsync({"PRAGMA yson."}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "");
        }
        {
            TVector<TCandidate> expected = {
                {PragmaName, "CastToString"}};
            auto completion = engine->CompleteAsync({"PRAGMA yson.cast"}).GetValueSync();
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
            {Keyword, "BITCAST("},
            {Keyword, "CALLABLE"},
            {Keyword, "CASE"},
            {Keyword, "CAST("},
            {Keyword, "CURRENT_DATE"},
            {Keyword, "CURRENT_TIME"},
            {Keyword, "CURRENT_TIMESTAMP"},
            {Keyword, "DICT<"},
            {Keyword, "DISTINCT"},
            {FunctionName, "DateTime::Split("},
            {Keyword, "EMPTY_ACTION"},
            {Keyword, "ENUM"},
            {Keyword, "EXISTS("},
            {Keyword, "FALSE"},
            {Keyword, "FLOW<"},
            {Keyword, "JSON_EXISTS("},
            {Keyword, "JSON_QUERY("},
            {Keyword, "JSON_VALUE("},
            {Keyword, "LIST<"},
            {Keyword, "NOT"},
            {Keyword, "NULL"},
            {Keyword, "OPTIONAL<"},
            {FunctionName, "Python::__private("},
            {Keyword, "RESOURCE<"},
            {Keyword, "SET<"},
            {Keyword, "STREAM"},
            {Keyword, "STRUCT"},
            {FunctionName, "StartsWith("},
            {Keyword, "TAGGED<"},
            {Keyword, "TRUE"},
            {Keyword, "TUPLE"},
            {Keyword, "VARIANT"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT "), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT # FROM"), expected);
    }

    Y_UNIT_TEST(SelectFrom) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {FolderName, "`.sys/`"},
                {FolderName, "`local/`"},
                {FolderName, "`prod/`"},
                {FolderName, "`test/`"},
                {ClusterName, "example"},
                {ClusterName, "yt:saurus"},
                {Keyword, "ANY"},
                {Keyword, "CALLABLE"},
                {Keyword, "DICT"},
                {Keyword, "ENUM"},
                {Keyword, "FLOW"},
                {Keyword, "LIST"},
                {Keyword, "OPTIONAL"},
                {Keyword, "RESOURCE"},
                {Keyword, "SET"},
                {Keyword, "STRUCT"},
                {Keyword, "TAGGED"},
                {Keyword, "TUPLE"},
                {Keyword, "VARIANT"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM "), expected);
        }
        {
            TVector<TCandidate> expected = {};
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
            TCompletion actual = engine->Complete(SharpedInput(input));
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
            TCompletion actual = engine->Complete(SharpedInput(input));
            UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, "");
        }
        {
            TString input = "SELECT * FROM `local/a#`";
            TVector<TCandidate> expected = {
                {TableName, "abacaba"},
                {TableName, "account"},
            };
            TCompletion actual = engine->Complete(SharpedInput(input));
            UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, "a");
        }
        {
            TString input = "SELECT * FROM `.sy#`";
            TVector<TCandidate> expected = {
                {FolderName, ".sys/"},
            };
            TCompletion actual = engine->Complete(SharpedInput(input));
            UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, ".sy");
        }
        {
            TString input = "SELECT * FROM `/test/ser#vice/`";
            TVector<TCandidate> expected = {
                {FolderName, "service/"},
            };
            TCompletion actual = engine->Complete(SharpedInput(input));
            UNIT_ASSERT_VALUES_EQUAL(actual.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(actual.CompletedToken.Content, "ser");
        }
    }

    Y_UNIT_TEST(SelectFromCluster) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {ClusterName, "yt:saurus"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM yt#"), expected);
        }
        {
            TVector<TCandidate> expected = {
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
                {Keyword, "CALLABLE"},
                {Keyword, "DICT"},
                {Keyword, "ENUM"},
                {Keyword, "FLOW"},
                {Keyword, "LIST"},
                {Keyword, "OPTIONAL"},
                {Keyword, "RESOURCE"},
                {Keyword, "SET"},
                {Keyword, "STRUCT"},
                {Keyword, "TAGGED"},
                {Keyword, "TUPLE"},
                {Keyword, "VARIANT"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT * FROM yt:saurus."), expected);
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

    Y_UNIT_TEST(SelectWhere) {
        TVector<TCandidate> expected = {
            {Keyword, "BITCAST("},
            {Keyword, "CALLABLE"},
            {Keyword, "CASE"},
            {Keyword, "CAST("},
            {Keyword, "CURRENT_DATE"},
            {Keyword, "CURRENT_TIME"},
            {Keyword, "CURRENT_TIMESTAMP"},
            {Keyword, "DICT<"},
            {FunctionName, "DateTime::Split("},
            {Keyword, "EMPTY_ACTION"},
            {Keyword, "ENUM"},
            {Keyword, "EXISTS("},
            {Keyword, "FALSE"},
            {Keyword, "FLOW<"},
            {Keyword, "JSON_EXISTS("},
            {Keyword, "JSON_QUERY("},
            {Keyword, "JSON_VALUE("},
            {Keyword, "LIST<"},
            {Keyword, "NOT"},
            {Keyword, "NULL"},
            {Keyword, "OPTIONAL<"},
            {FunctionName, "Python::__private("},
            {Keyword, "RESOURCE<"},
            {Keyword, "SET<"},
            {Keyword, "STREAM<"},
            {Keyword, "STRUCT"},
            {FunctionName, "StartsWith("},
            {Keyword, "TAGGED<"},
            {Keyword, "TRUE"},
            {Keyword, "TUPLE"},
            {Keyword, "VARIANT"},
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
                {FolderName, "`.sys/`"},
                {FolderName, "`local/`"},
                {FolderName, "`prod/`"},
                {FolderName, "`test/`"},
                {ClusterName, "example"},
                {ClusterName, "yt:saurus"},
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
            {Keyword, "CALLABLE<("},
            {Keyword, "DECIMAL("},
            {Keyword, "DICT<"},
            {Keyword, "ENUM<"},
            {Keyword, "FLOW<"},
            {Keyword, "LIST<"},
            {Keyword, "OPTIONAL<"},
            {Keyword, "RESOURCE<"},
            {Keyword, "SET<"},
            {Keyword, "STREAM<"},
            {Keyword, "STRUCT"},
            {Keyword, "TAGGED<"},
            {Keyword, "TUPLE"},
            {TypeName, "Uint64"},
            {Keyword, "VARIANT<"},
        };

        auto engine = MakeSqlCompletionEngineUT();
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "CREATE TABLE table (id "), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT CAST (1 AS "), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT OPTIONAL<"), expected);
        UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT OPTIONAL<#>"), expected);
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
                {Keyword, "OPTIONAL<"},
            };
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT Nothing(Option"), expected);
        }
    }

    Y_UNIT_TEST(FunctionName) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
                {FunctionName, "DateTime::Split("},
            };
            auto completion = engine->CompleteAsync({"SELECT Date"}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "Date");
        }
        {
            TVector<TCandidate> expected = {
                {FunctionName, "Split("},
            };
            auto completion = engine->CompleteAsync({"SELECT DateTime:"}).GetValueSync();
            UNIT_ASSERT(completion.Candidates.empty());
        }
        {
            TVector<TCandidate> expected = {
                {FunctionName, "Split("},
            };
            auto completion = engine->CompleteAsync({"SELECT DateTime::"}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "");
        }
        {
            TVector<TCandidate> expected = {
                {FunctionName, "Split("},
            };
            auto completion = engine->CompleteAsync({"SELECT DateTime::s"}).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL(completion.Candidates, expected);
            UNIT_ASSERT_VALUES_EQUAL(completion.CompletedToken.Content, "s");
        }
    }

    Y_UNIT_TEST(SelectTableHintName) {
        auto engine = MakeSqlCompletionEngineUT();
        {
            TVector<TCandidate> expected = {
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
        UNIT_ASSERT(FindPtr(Complete(engine, "SELECT #`name`"), TCandidate{FunctionName, "StartsWith("}) != nullptr);

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
            TCompletion completion = engine->CompleteAsync({TString::FromUtf16(prefixUtf16)}).GetValueSync();
            Y_DO_NOT_OPTIMIZE_AWAY(completion);
        }
    }

    Y_UNIT_TEST(Tabbing) {
        TString query =
            "SELECT \n"
            "  123467, \"Hello, {name}! 编码\"}, \n"
            "  (1 + (5 * 1 / 0)), MIN(identifier), \n"
            "  Bool(field), Math::Sin(var) \n"
            "FROM `local/test/space/table` JOIN test;";
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
            TCompletion completion = engine->CompleteAsync(input).GetValueSync();
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

        UNIT_ASSERT_NO_EXCEPTION(engine->CompleteAsync({"", 0}).GetValueSync());
        UNIT_ASSERT_EXCEPTION(engine->CompleteAsync({"", 1}).GetValueSync(), yexception);

        UNIT_ASSERT_NO_EXCEPTION(engine->CompleteAsync({"s", 0}).GetValueSync());
        UNIT_ASSERT_NO_EXCEPTION(engine->CompleteAsync({"s", 1}).GetValueSync());

        UNIT_ASSERT_NO_EXCEPTION(engine->CompleteAsync({"ы", 0}).GetValueSync());
        UNIT_ASSERT_EXCEPTION(engine->CompleteAsync({"ы", 1}).GetValueSync(), yexception);
        UNIT_ASSERT_NO_EXCEPTION(engine->CompleteAsync({"ы", 2}).GetValueSync());
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
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT OPTIONAL<U"), expected);
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
        UNIT_ASSERT_EXCEPTION(Complete(engine, "SELECT OPTIONAL<U"), TDummyException);
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
            UNIT_ASSERT_VALUES_EQUAL(Complete(engine, "SELECT OPTIONAL<I"), expected);
        }
        {
            TVector<TCandidate> expected = {
                {FunctionName, "Min("},
                {FunctionName, "Max("},
                {FunctionName, "MaxOf("},
                {FunctionName, "MaxBy("},
                {FunctionName, "MinBy("},
                {FunctionName, "Math::Abs("},
                {FunctionName, "Math::Acos("},
                {FunctionName, "Math::Asin("},
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

} // Y_UNIT_TEST_SUITE(SqlCompleteTests)
