#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/helpers/helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/thread/pool.h>

#include <thread>


void ExecutorExample(const std::string& endpoint, const std::string& database) {
    auto driverConfig = NYdb::CreateFromEnvironment(endpoint + "/?database=" + database)
        .SetExecutor(NYdb::CreateThreadPoolExecutorAdapter(
            std::make_shared<TThreadPool>(TThreadPool::TParams()
                .SetBlocking(true)
                .SetCatching(false)
                .SetForkAware(false)),
            std::thread::hardware_concurrency())
        );

    NYdb::TDriver driver(driverConfig);
    NYdb::NQuery::TQueryClient client(driver);

    try {
        auto result = client.ExecuteQuery("SELECT 1", NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        NYdb::NStatusHelpers::ThrowOnError(result);
        auto parser = result.GetResultSetParser(0);
        parser.TryNextRow();
        std::cout << "Result: " << parser.ColumnParser(0).GetInt32() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Execution failed: " << e.what() << std::endl;
    }

    driver.Stop(true);
}

int main(int argc, char** argv) {
    std::string endpoint;
    std::string database;

    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT").StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database").Required().RequiredArgument("DATABASE").StoreResult(&database);

    opts.SetFreeArgsMin(0);
    NLastGetopt::TOptsParseResult result(&opts, argc, argv);

    ExecutorExample(endpoint, database);
    return 0;
}
