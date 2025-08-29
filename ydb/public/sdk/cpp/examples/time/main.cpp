#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/helpers/helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/getopt/last_getopt.h>


template <class Duration>
std::string FormatDatetime(const std::chrono::sys_time<Duration>& tp) {
    auto timeT = std::chrono::system_clock::to_time_t(tp);
    std::tm* tm = std::gmtime(&timeT);

    std::stringstream ss;
    ss << std::put_time(tm, "%FT%TZ");

    return ss.str();
}

std::string FormatTimestamp(const std::chrono::sys_time<NYdb::TWideMicroseconds>& tp) {
    auto timeT = std::chrono::system_clock::to_time_t(tp);
    std::tm* tm = std::gmtime(&timeT);

    std::stringstream ss;
    ss << std::put_time(tm, "%FT%T");

    auto micros = tp.time_since_epoch() % NYdb::TWideSeconds(1);
    if (micros.count() < 0) {
        micros += NYdb::TWideSeconds(1);
    }

    ss << '.' << std::setfill('0') << std::setw(6) << micros.count() << 'Z';

    return ss.str();
}

void TimeExample(const std::string& endpoint, const std::string& database) {
    auto driverConfig = NYdb::CreateFromEnvironment(endpoint + "/?database=" + database);
    NYdb::TDriver driver(driverConfig);
    NYdb::NQuery::TQueryClient client(driver);

    NYdb::NStatusHelpers::ThrowOnError(client.RetryQuerySync([](NYdb::NQuery::TSession session) -> NYdb::TStatus {
        std::string query = R"(
            $ts1 = DateTime::MakeTimestamp64(Timestamp64("2019-05-12T15:30:18.123456Z"));
            $ts2 = DateTime::MakeTimestamp64(Timestamp64("2019-05-12T15:30:19.012345Z"));
            SELECT $ts1 as ts1, $ts2 as ts2
        )";

        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        if (!result.IsSuccess()) {
            return result;
        }

        NYdb::TResultSetParser parser(result.GetResultSet(0));
        while (parser.TryNextRow()) {
            auto ts1 = parser.ColumnParser("ts1").GetTimestamp64();
            auto ts2 = parser.ColumnParser("ts2").GetTimestamp64();

            std::cout << "ts1: " << FormatDatetime(ts1) << std::endl;
            std::cout << "ts2: " << FormatTimestamp(ts2) << std::endl;
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
