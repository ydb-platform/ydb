#include <ydb/library/workload/imdb/imdb.h>
#include <ydb/library/workload/abstract/workload_factory.h>

#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/folder/tempdir.h>
#include <util/generic/hash_set.h>
#include <util/stream/file.h>

using namespace NYdbWorkload;

namespace {

void AssertTranslates(const std::string& query) {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
    NSQLTranslation::TTranslationSettings settings;
    settings.SyntaxVersion = 1;
    settings.DefaultCluster = "db";
    settings.ClusterMapping["db"] = "kikimr";
    google::protobuf::Arena arena;
    settings.Arena = &arena;
    const auto result = NSQLTranslationV1::SqlToYql(lexers, parsers, TString(query), settings);
    UNIT_ASSERT_C(result.IsOk(), result.Issues.ToString() << "\n" << query);
}

void SetInput(TWorkloadDataInitializer& initializer, const TString& path) {
    NLastGetopt::TOpts opts;
    initializer.ConfigureOpts(opts);
    const char* argv[] = {"files", "--input", path.c_str()};
    NLastGetopt::TOptsParseResultException parsed(&opts, Y_ARRAY_SIZE(argv), argv);
}

} // namespace

Y_UNIT_TEST_SUITE(ImdbWorkload) {
    Y_UNIT_TEST(Registered) {
        THolder<TWorkloadParams> params(TWorkloadFactory::Construct("imdb"));
        UNIT_ASSERT(params);
        UNIT_ASSERT_VALUES_EQUAL(params->GetWorkloadName(), "IMDb");
        const auto types = params->CreateGenerator()->GetSupportedWorkloadTypes();
        UNIT_ASSERT_VALUES_EQUAL(types.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(types.front().CommandName, "bench");
        UNIT_ASSERT(types.front().Kind == IWorkloadQueryGenerator::TWorkloadType::EKind::Benchmark);
    }

    Y_UNIT_TEST(AllQueriesTranslate) {
        TImdbWorkloadParams params;
        params.DbPath = "/Root";
        params.SetPath("benchmarks/imdb");
        const auto generator = params.CreateGenerator();
        const auto queries = generator->GetWorkload(0);
        UNIT_ASSERT_VALUES_EQUAL(queries.size(), 113);
        UNIT_ASSERT_VALUES_EQUAL(queries.front().QueryName, "1a");
        UNIT_ASSERT_VALUES_EQUAL(queries.back().QueryName, "33c");
        THashSet<TString> names;
        for (const auto& query: queries) {
            UNIT_ASSERT(names.insert(query.QueryName).second);
            UNIT_ASSERT_STRING_CONTAINS(query.Query, "PRAGMA TablePathPrefix = \"/Root/benchmarks/imdb\";");
            AssertTranslates(query.Query);
        }
        UNIT_ASSERT(generator->GetWorkload(1).empty());
    }

    Y_UNIT_TEST(SchemaAndCleanupPaths) {
        for (const auto* store: {"row", "column", "external-s3"}) {
            TImdbWorkloadParams params;
            params.DbPath = "/Root";
            params.SetPath("benchmarks/imdb");
            NLastGetopt::TOpts opts;
            params.ConfigureOpts(opts, TWorkloadParams::ECommandType::Init, 0);
            const char* argv[] = {"init", "--store", store};
            NLastGetopt::TOptsParseResultException parsed(&opts, Y_ARRAY_SIZE(argv), argv);
            const auto generator = params.CreateGenerator();
            const auto ddl = generator->GetDDLQueries();
            const auto paths = generator->GetCleanPaths();
            UNIT_ASSERT_VALUES_EQUAL(paths.size(), 21);
            THashSet<TString> unique;
            for (const auto& path: paths) {
                UNIT_ASSERT(unique.insert(path).second);
                UNIT_ASSERT_STRING_CONTAINS(ddl, "TABLE `/Root/" + path + "`");
            }
            UNIT_ASSERT_STRING_CONTAINS(ddl, "production_year Int32");
            UNIT_ASSERT_STRING_CONTAINS(ddl, "title Utf8");
            AssertTranslates(ddl);
        }
    }

    Y_UNIT_TEST(AnalyzeAllTablesAndColumns) {
        for (const auto* path: {"", "benchmarks/imdb", "/Root/benchmarks/imdb"}) {
            TImdbWorkloadParams params;
            params.DbPath = "/Root";
            params.SetPath(path);
            const TImdbWorkloadGenerator generator(params);
            const auto queries = generator.GetAnalyzeQueries();
            UNIT_ASSERT_VALUES_EQUAL(queries.size(), 21);
            THashSet<TString> unique;
            for (const auto& query: queries) {
                UNIT_ASSERT(unique.insert(query).second);
                UNIT_ASSERT(query.StartsWith("ANALYZE `/Root/"));
                // Omitting the column list requests statistics for every column.
                UNIT_ASSERT(query.EndsWith("`;"));
                UNIT_ASSERT(query.find('(') == TString::npos);
                AssertTranslates(std::string(query));
            }
            UNIT_ASSERT_VALUES_EQUAL(queries.front(),
                "ANALYZE `" + params.GetFullTableName("aka_name") + "`;");
            UNIT_ASSERT_VALUES_EQUAL(queries.back(),
                "ANALYZE `" + params.GetFullTableName("title") + "`;");
        }
    }

    Y_UNIT_TEST(AnalyzeQuotesTablePaths) {
        TImdbWorkloadParams params;
        params.DbPath = "/Root";
        params.SetPath("bench`mark\\imdb");
        const auto queries = TImdbWorkloadGenerator(params).GetAnalyzeQueries();
        UNIT_ASSERT_VALUES_EQUAL(queries.front(), "ANALYZE `/Root/bench\\`mark\\\\imdb/aka_name`;");
        AssertTranslates(std::string(queries.front()));
    }

    Y_UNIT_TEST(ImportRoutesFilesAndSuppliesHeaders) {
        TTempDir directory;
        TImdbWorkloadParams params;
        params.DbPath = "/Root";
        params.SetPath("custom/imdb");
        const auto paths = params.CreateGenerator()->GetCleanPaths();
        // Empty files stand in for all tables except two small lookup tables.
        for (const auto& path: paths) {
            const auto name = TFsPath(path).GetName();
            TFileOutput output(TFsPath(directory.Name()) / (name + ".csv"));
            if (name == "company_type") {
                output << "1,production companies\n";
            } else if (name == "kind_type") {
                output << "1,movie\n";
            }
        }
        const auto initializers = params.CreateDataInitializers();
        UNIT_ASSERT_VALUES_EQUAL(initializers.size(), 1);
        auto& initializer = *initializers.front();
        SetInput(initializer, directory.Name());
        const auto generators = initializer.GetBulkInitialData();
        UNIT_ASSERT_VALUES_EQUAL(generators.size(), 21);
        size_t portions = 0;
        for (const auto& generator: generators) {
            for (const auto& portion: generator->GenerateDataPortion()) {
                ++portions;
                UNIT_ASSERT_VALUES_EQUAL(portion->GetTable(), "/Root/custom/imdb/" + generator->GetName());
                const auto* csv = std::get_if<IBulkDataGenerator::TDataPortion::TCsv>(&portion->MutableData());
                UNIT_ASSERT(csv);
                UNIT_ASSERT_STRING_CONTAINS(csv->Data, "id,kind\n1,");
            }
            UNIT_ASSERT(generator->GenerateDataPortion().empty());
        }
        UNIT_ASSERT_VALUES_EQUAL(portions, 2);
    }

    Y_UNIT_TEST(ImportRejectsMissingAndUnsupportedFiles) {
        TTempDir directory;
        TImdbWorkloadParams params;
        auto initializers = params.CreateDataInitializers();
        auto& initializer = *initializers.front();
        UNIT_ASSERT_EXCEPTION_CONTAINS(initializer.GetBulkInitialData(), yexception, "existing directory");
        SetInput(initializer, directory.Name());
        UNIT_ASSERT_EXCEPTION_CONTAINS(initializer.GetBulkInitialData(), yexception, "aka_name not found");
        const auto tableDir = TFsPath(directory.Name()) / "aka_name";
        tableDir.MkDir();
        TFileOutput(tableDir / "data.txt").Finish();
        UNIT_ASSERT_EXCEPTION_CONTAINS(initializer.GetBulkInitialData(), yexception, "Unsupported IMDb data file");
    }

    Y_UNIT_TEST(CsvEscapingMultilineAndResume) {
        TTempDir directory;
        TImdbWorkloadParams params;
        params.DbPath = "/Root";
        params.BulkSize = 1;
        for (const auto& path: params.CreateGenerator()->GetCleanPaths()) {
            TFileOutput(TFsPath(directory.Name()) / (TFsPath(path).GetName() + ".csv")).Finish();
        }
        {
            TFileOutput output(TFsPath(directory.Name()) / "aka_name.csv");
            output << "1,1,\"Name \\\"quoted\\\" \\\\ path\nsecond line\",,,,,\n"
                << "2,2,\"\",,,,,\n";
        }
        const auto statePath = (TFsPath(directory.Name()) / "state.json").GetPath();
        auto makeInitializer = [&] {
            auto initializer = params.CreateDataInitializers().front();
            NLastGetopt::TOpts opts;
            initializer->ConfigureOpts(opts);
            const char* argv[] = {"files", "--input", directory.Name().c_str(), "--state", statePath.c_str()};
            NLastGetopt::TOptsParseResultException parsed(&opts, Y_ARRAY_SIZE(argv), argv);
            return initializer;
        };
        {
            auto initializer = makeInitializer();
            auto generators = initializer->GetBulkInitialData();
            const auto portions = generators.front()->GenerateDataPortion();
            UNIT_ASSERT_VALUES_EQUAL(portions.size(), 1);
            auto& csv = std::get<IBulkDataGenerator::TDataPortion::TCsv>(portions.front()->MutableData());
            UNIT_ASSERT_STRING_CONTAINS(csv.Data, "1,1,\"Name \"\"quoted\"\" \\ path\nsecond line\",,,,,\n");
            UNIT_ASSERT(csv.Data.find("2,2,") == TString::npos);
            portions.front()->SetSendResult(NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues()));
        }
        {
            auto initializer = makeInitializer();
            auto generators = initializer->GetBulkInitialData();
            const auto portions = generators.front()->GenerateDataPortion();
            UNIT_ASSERT_VALUES_EQUAL(portions.size(), 1);
            const auto& csv = std::get<IBulkDataGenerator::TDataPortion::TCsv>(portions.front()->MutableData());
            UNIT_ASSERT_STRING_CONTAINS(csv.Data, "2,2,\"\",,,,,\n");
            UNIT_ASSERT(csv.Data.find("1,1,") == TString::npos);
            UNIT_ASSERT(generators.front()->GenerateDataPortion().empty());
        }
    }

    Y_UNIT_TEST(CanonicalCheckIsNotSilentlyIgnored) {
        TImdbWorkloadParams params;
        NLastGetopt::TOpts opts;
        params.ConfigureOpts(opts, TWorkloadParams::ECommandType::Run, 0);
        const char* argv[] = {"bench", "--check-canonical"};
        NLastGetopt::TOptsParseResultException parsed(&opts, Y_ARRAY_SIZE(argv), argv);
        UNIT_ASSERT_EXCEPTION_CONTAINS(params.CreateGenerator()->GetWorkload(0), yexception, "Canonical results");
    }
}
