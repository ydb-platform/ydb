#include "mock_table.h"

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/string/cast.h>

#include <atomic>

namespace {

class TWorkloadScheme : public TSchemeImpl {
public:
    grpc::Status DescribePath(grpc::ServerContext* context, const Ydb::Scheme::DescribePathRequest* request, Ydb::Scheme::DescribePathResponse* response) override {
        ++Describes;
        CheckClientMetadata(context, "x-ydb-database", ExpectedDatabase);
        CHECK_EXP(request->path() == Path, "Incorrect workload path: " << request->path());
        auto* operation = response->mutable_operation();
        operation->set_ready(true);
        if (Exists) {
            Ydb::Scheme::DescribePathResult result;
            result.mutable_self()->set_name("bench");
            result.mutable_self()->set_type(Ydb::Scheme::Entry::TABLE);
            operation->set_status(Ydb::StatusIds::SUCCESS);
            operation->mutable_result()->PackFrom(result);
        } else {
            operation->set_status(Ydb::StatusIds::SCHEME_ERROR);
            operation->add_issues()->set_message("Path not found");
        }
        return grpc::Status::OK;
    }

    TString Path;
    std::atomic<bool> Exists{true};
    std::atomic<size_t> Describes{0};
};

class TWorkloadTable : public TMockTableService {
public:
    explicit TWorkloadTable(TWorkloadScheme& scheme)
        : Scheme(scheme)
    {}

    grpc::Status DropTable(grpc::ServerContext*, const Ydb::Table::DropTableRequest* request, Ydb::Table::DropTableResponse* response) override {
        CHECK_EXP(request->path() == Scheme.Path, "Incorrect cleanup path: " << request->path());
        Scheme.Exists = false;
        ++Drops;
        response->mutable_operation()->set_ready(true);
        response->mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
        return grpc::Status::OK;
    }

    grpc::Status ExecuteSchemeQuery(grpc::ServerContext*, const Ydb::Table::ExecuteSchemeQueryRequest* request, Ydb::Table::ExecuteSchemeQueryResponse* response) override {
        const TString prefix = "CREATE TABLE `" + DdlPath + "`";
        CHECK_EXP(request->yql_text().find(prefix.c_str()) != std::string::npos, "Incorrect workload DDL: " << request->yql_text());
        Scheme.Exists = true;
        ++Creates;
        response->mutable_operation()->set_ready(true);
        response->mutable_operation()->set_status(Ydb::StatusIds::SUCCESS);
        return grpc::Status::OK;
    }

    TString DdlPath;
    std::atomic<size_t> Drops{0};
    std::atomic<size_t> Creates{0};

private:
    TWorkloadScheme& Scheme;
};

class TWorkloadFixture : public TCliTestFixture {
    void AddServices() override {
        AddService<TDiscoveryImpl>("localhost", FromString<ui16>(TStringBuf(GetAddress()).RNextTok(':')));
        AddService<TWorkloadScheme>();
        AddService<TWorkloadTable>(Service<TWorkloadScheme>());
    }
};

class TDeniedQueries : public TMockGrpcServiceBase<Ydb::Query::V1::QueryService::Service> {
public:
    grpc::Status CreateSession(grpc::ServerContext*, const Ydb::Query::CreateSessionRequest*, Ydb::Query::CreateSessionResponse* response) override {
        response->set_status(Ydb::StatusIds::UNAUTHORIZED);
        return grpc::Status::OK;
    }
};

class TInferenceFixture : public TCliTestFixture {
    void AddServices() override {
        AddService<TDiscoveryImpl>("localhost", FromString<ui16>(TStringBuf(GetAddress()).RNextTok(':')));
        AddService<TDeniedQueries>();
    }
};

} // namespace

Y_UNIT_TEST_SUITE(RelativePathCompatibility) {
    Y_UNIT_TEST(DotSqlPathsUseTheDatabaseRoot) {
        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(GetYdbEndpoint()).SetDatabase(GetYdbDatabase()));
        NYdb::NTable::TTableClient client(driver);
        auto result = client.RetryOperationSync([](NYdb::NTable::TSession session) {
            return session.ExecuteSchemeQuery("CREATE TABLE `./RelativeSqlPaths` (key Uint64, PRIMARY KEY(key));").GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        result = client.RetryOperationSync([](NYdb::NTable::TSession session) {
            return session.DropTable(GetYdbDatabase() + "/RelativeSqlPaths").GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST(RelativeSqlPrefixesUseTheDatabaseRoot) {
        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(GetYdbEndpoint()).SetDatabase(GetYdbDatabase()));
        NYdb::NTable::TTableClient client(driver);
        NYdb::NScheme::TSchemeClient scheme(driver);
        const TString directory = GetYdbDatabase() + "/RelativeSqlPrefixPaths";
        const auto made = scheme.MakeDirectory(directory).GetValueSync();
        UNIT_ASSERT_C(made.IsSuccess(), made.GetIssues().ToString());
        struct TCase {
            TString Pragma;
            TString Prefix;
            TString Target;
        };
        for (const auto& testCase : TVector<TCase>{
            {"RelativePathPrefix", ".", GetYdbDatabase()},
            {"RelativePathPrefix", "RelativeSqlPrefixPaths", directory},
            {"TablePathPrefix", directory, directory},
        }) {
            const TString query = "PRAGMA " + testCase.Pragma + "(\"" + testCase.Prefix
                + "\"); CREATE TABLE `RelativeSqlPrefixTable` (key Uint64, PRIMARY KEY(key));";
            auto result = client.RetryOperationSync([&](NYdb::NTable::TSession session) {
                return session.ExecuteSchemeQuery(query).GetValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), query << Endl << result.GetIssues().ToString());
            result = client.RetryOperationSync([&](NYdb::NTable::TSession session) {
                return session.DropTable(testCase.Target + "/RelativeSqlPrefixTable").GetValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        const auto removed = scheme.RemoveDirectory(directory).GetValueSync();
        UNIT_ASSERT_C(removed.IsSuccess(), removed.GetIssues().ToString());
    }

    Y_UNIT_TEST_F(InferenceKeepsRelativeDirectoryNames, TInferenceFixture) {
        const auto csv = EnvFile("key,name\n1,value\n", "input.csv");
        for (const TString& database : {GetDatabase(), TString("tenant"), TString{}}) {
            const auto output = RunCli({"-d", database, "tools", "infer", "csv", csv, "--header", "--path", "tenant/orders"});
            UNIT_ASSERT_STRING_CONTAINS(output, "CREATE TABLE `tenant/orders`");
        }
        const auto output = RunCli({"-d", GetDatabase(), "tools", "infer", "csv", csv, "--header", "--path", GetDatabase() + "/orders"});
        UNIT_ASSERT_STRING_CONTAINS(output, "CREATE TABLE `" + GetDatabase() + "/orders`");
        for (const TString& database : {GetDatabase(), TString("tenant")}) {
            TString error;
            ExpectFail();
            RunCli({"-e", GetEndpoint(), "-d", database, "tools", "infer", "csv", csv,
                "--header", "--path", "tenant/orders", "--execute"}, {}, {}, &error);
            UNIT_ASSERT_STRING_CONTAINS(error, "CREATE TABLE `tenant/orders`");
            UNIT_ASSERT_STRING_CONTAINS(error, "UNAUTHORIZED");
        }
        TString error;
        ExpectFail();
        RunCli({"-e", GetEndpoint(), "-d", GetDatabase(), "tools", "infer", "csv", csv,
            "--header", "--path", GetDatabase() + "/orders", "--execute"}, {}, {}, &error);
        UNIT_ASSERT_STRING_CONTAINS(error, "CREATE TABLE orders (");
        UNIT_ASSERT_STRING_CONTAINS(error, "UNAUTHORIZED");
    }

    Y_UNIT_TEST_F(BenchmarkKeepsExplicitAbsolutePaths, TCliTestFixture) {
        const auto output = RunCli({"-e", GetEndpoint(), "-d", "tenant", "workload", "clickbench",
            "--path", "/test_database/bench", "run", "--dry-run", "--verbose"});
        UNIT_ASSERT_STRING_CONTAINS(output, "/test_database/bench");
    }

    Y_UNIT_TEST_F(BenchmarkQueriesUseTheSelectedDatabasePrefix, TCliTestFixture) {
        struct TCase {
            TString Database;
            TString Path;
            TString Prefix;
        };
        for (const auto& testCase : TVector<TCase>{
            {GetDatabase(), "bench/nested", "PRAGMA TablePathPrefix = \"" + GetDatabase() + "/bench/nested\";"},
            {"tenant", "bench/nested", "PRAGMA RelativePathPrefix = \"./bench/nested\";"},
            {"tenant", "/test_database/bench/nested", "PRAGMA TablePathPrefix = \"/test_database/bench/nested\";"},
        }) {
            const auto output = RunCli({"-e", GetEndpoint(), "-d", testCase.Database, "workload", "query",
                "--path", testCase.Path, "run", "--dry-run", "--verbose", "--query", "SELECT 1;"});
            UNIT_ASSERT_STRING_CONTAINS(output, testCase.Prefix);
        }
    }

    Y_UNIT_TEST_F(QueryInitComposesOnePrefixBeforeAllStatements, TCliTestFixture) {
        const TString columns = " (key Uint64, PRIMARY KEY(key));";
        const auto first = EnvHomeFile("suite/init/first.sql", "CREATE TABLE `{db}/RelativeInitFirst`" + columns);
        EnvHomeFile("suite/init/nested/second.sql", "CREATE TABLE `RelativeInitSecond`" + columns);
        const auto output = RunCli({"-e", GetEndpoint(), "-d", "tenant", "workload", "query",
            "--path", "RelativeQueryInitPaths", "init", "--suite-path", TFsPath(first).Parent().Parent().GetPath(),
            "--query", "CREATE TABLE RelativeInitCustom" + columns, "--dry-run"});
        const auto begin = output.find("PRAGMA ");
        const auto end = output.rfind(';');
        UNIT_ASSERT(begin != TString::npos && end != TString::npos && end > begin);
        const auto query = output.substr(begin, end - begin + 1);
        NYdb::TDriver driver(NYdb::TDriverConfig().SetEndpoint(GetYdbEndpoint()).SetDatabase(GetYdbDatabase()));
        NYdb::NTable::TTableClient client(driver);
        NYdb::NScheme::TSchemeClient scheme(driver);
        const TString directory = GetYdbDatabase() + "/RelativeQueryInitPaths";
        const auto made = scheme.MakeDirectory(directory).GetValueSync();
        UNIT_ASSERT_C(made.IsSuccess(), made.GetIssues().ToString());
        auto result = client.RetryOperationSync([&](NYdb::NTable::TSession session) {
            return session.ExecuteSchemeQuery(query).GetValueSync();
        });
        UNIT_ASSERT_C(result.IsSuccess(), query << Endl << result.GetIssues().ToString());
        for (const TString table : {"RelativeInitCustom", "RelativeInitFirst", "RelativeInitSecond"}) {
            result = client.RetryOperationSync([&](NYdb::NTable::TSession session) {
                return session.DropTable(directory + "/" + table).GetValueSync();
            });
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        const auto removed = scheme.RemoveDirectory(directory).GetValueSync();
        UNIT_ASSERT_C(removed.IsSuccess(), removed.GetIssues().ToString());
    }

    Y_UNIT_TEST_F(VectorUsesOneTablePath, TCliTestFixture) {
        struct TCase {
            TString Database;
            TString Table;
            TString Target;
        };
        for (const auto& testCase : TVector<TCase>{
            {GetDatabase(), "tenant/vectors", GetDatabase() + "/tenant/vectors"},
            {"tenant", "vectors", "./vectors"},
            {"tenant", "tenant/vectors", "./tenant/vectors"},
            {"tenant", "/test_database/tenant/vectors", "/test_database/tenant/vectors"},
        }) {
            for (const TString command : {"init", "build-index", "drop-index"}) {
                const auto output = RunCli({"-e", GetEndpoint(), "-d", testCase.Database,
                    "workload", "vector", command, "--table", testCase.Table, "--dry-run"});
                const TString prefix = command == "init" ? "CREATE TABLE `" : "ALTER TABLE `";
                UNIT_ASSERT_STRING_CONTAINS(output, prefix + testCase.Target + "`");
            }
        }
    }

    Y_UNIT_TEST_F(KvInitAndCleanUseOneTarget, TWorkloadFixture) {
        for (const bool relative : {false, true}) {
            for (const TString path : {"bench", "tenant/bench"}) {
                auto& scheme = Service<TWorkloadScheme>();
                auto& table = Service<TWorkloadTable>();
                const TString database = relative ? TString("tenant") : GetDatabase();
                scheme.Path = relative ? path : database + "/" + path;
                scheme.Exists = true;
                scheme.Describes = table.Drops = table.Creates = 0;
                table.DdlPath = relative ? "./" + path : scheme.Path;
                const TString parent = relative ? TString("tenant") : database + "/tenant";
                auto expectParent = [&] {
                    scheme.ExpectedDatabase = database;
                    if (path.Contains('/')) {
                        scheme.ExpectListDirectory(parent).ExpectChild(parent, "other", Ydb::Scheme::Entry::TABLE);
                    }
                };
                expectParent();
                RunCli({"-e", GetEndpoint(), "-d", database, "workload", "kv", "init",
                    "--path", path, "--init-upserts", "0", "--clear"});
                UNIT_ASSERT(scheme.Exists);
                UNIT_ASSERT_VALUES_EQUAL(scheme.Describes.load(), 2);
                UNIT_ASSERT_VALUES_EQUAL(table.Drops.load(), 1);
                UNIT_ASSERT_VALUES_EQUAL(table.Creates.load(), 1);
                expectParent();
                RunCli({"-e", GetEndpoint(), "-d", database, "workload", "kv", "clean", "--path", path});
                UNIT_ASSERT(!scheme.Exists);
                UNIT_ASSERT_VALUES_EQUAL(table.Drops.load(), 2);
            }
        }
    }
}
