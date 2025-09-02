#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/helpers/helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/getopt/last_getopt.h>


void TimeExample(const std::string& endpoint, const std::string& database) {
    auto driverConfig = NYdb::CreateFromEnvironment(endpoint + "/?database=" + database);
    NYdb::TDriver driver(driverConfig);
    NYdb::NQuery::TQueryClient client(driver);

    NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([](NYdb::NQuery::TSession session) -> NYdb::TStatus {
        std::string query = R"(
            $ts1 = DateTime::MakeTimestamp64(Timestamp64("2019-05-12T15:30:18.123456Z"));
            $ts2 = DateTime::MakeTimestamp64(Timestamp64("2019-05-12T15:30:19.012345Z"));
            SELECT $ts1 as ts1, $ts2 as ts2, $ts2 - $ts1 as interval
        )";

        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        if (!result.IsSuccess()) {
            return result;
        }

        NYdb::TResultSetParser parser(result.GetResultSet(0));
        while (parser.TryNextRow()) {
            auto ts1 = parser.ColumnParser("ts1").GetTimestamp64();
            auto ts2 = parser.ColumnParser("ts2").GetTimestamp64();
            auto interval = parser.ColumnParser("interval").GetInterval64();

            std::cout << "ts1: " << ts1 << std::endl;
            std::cout << "ts2: " << ts2 << std::endl;
            std::cout << "interval: " << std::chrono::duration_cast<std::chrono::milliseconds>(interval) << std::endl;

            std::cout << "ts1 + interval (" << ts1 + interval << ") = ts2 (" << ts2 << ")" << std::endl;
        }
        return result;
    }));

    driver.Stop(true);
}

int main(int argc, char** argv) {
    NLastGetopt::TOpts opts = NLastGetopt::TOpts::Default();

    std::string endpoint;
    std::string database;

    opts.AddLongOption('e', "endpoint", "YDB endpoint").Required().RequiredArgument("HOST:PORT").StoreResult(&endpoint);
    opts.AddLongOption('d', "database", "YDB database").Required().RequiredArgument("DATABASE").StoreResult(&database);

    opts.SetFreeArgsMin(0);

    NLastGetopt::TOptsParseResult result(&opts, argc, argv);

    try {
        TimeExample(endpoint, database);
    } catch (const std::exception& e) {
        std::cerr << "Execution failed: " << e.what() << std::endl;
    }

    return 0;
}
