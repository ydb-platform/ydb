#include "util.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <format>
#include <sstream>
#include <iomanip>

#if defined(_linux_)
#include <sched.h>
#endif

#include <util/system/info.h>

namespace NYdb::NTPCC {

std::string GetFormattedSize(size_t size) {
    constexpr size_t TiB = 1024ULL * 1024 * 1024 * 1024;
    constexpr size_t GiB = 1024ULL * 1024 * 1024;
    constexpr size_t MiB = 1024ULL * 1024;

    std::stringstream ss;
    ss << std::fixed << std::setprecision(2);

    if (size >= TiB) {
        ss << static_cast<double>(size) / TiB << " TiB";
    } else if (size >= GiB) {
        ss << static_cast<double>(size) / GiB << " GiB";
    } else if (size >= MiB) {
        ss << static_cast<double>(size) / MiB << " MiB";
    } else if (size >= 1024) {
        ss << static_cast<double>(size) / 1024 << " KiB";
    } else {
        ss << size << " B";
    }

    return ss.str();
}

void ExitIfError(const TStatus& status, const TString& what) {
    if (status.GetStatus() == EStatus::SUCCESS) {
        return;
    }

    TStringStream ss;
    ss << what << ": " << ToString(status.GetStatus());
    const auto& issues = status.GetIssues();
    if (issues) {
        ss << ", issues: ";
        issues.PrintTo(ss, true);
    }

    Cerr << ss.Str() << Endl;
    std::exit(1);
}

void ThrowIfError(const TStatus& status, const TString& what) {
    if (status.GetStatus() == EStatus::SUCCESS) {
        return;
    }

    TStringStream ss;
    ss << what << ": " << ToString(status.GetStatus());
    const auto& issues = status.GetIssues();
    if (issues) {
        ss << ", issues: ";
        issues.PrintTo(ss, true);
    }

    ythrow yexception() << ss.Str();
}

std::stop_source& GetGlobalInterruptSource() {
    static std::stop_source StopByInterrupt;
    return StopByInterrupt;
}

#if defined(_linux_)
size_t NumberOfMyCpus() {
    cpu_set_t set;
    CPU_ZERO(&set);
    if (sched_getaffinity(0, sizeof(set), &set) == -1) {
        return NSystemInfo::CachedNumberOfCpus();
    }

    int count = 0;
    for (int i = 0; i < CPU_SETSIZE; i++) {
        if (CPU_ISSET(i, &set))
            count++;
    }

    return count;
}

#else // not Linux

size_t NumberOfMyCpus() {
    return NSystemInfo::CachedNumberOfCpus();
}

#endif // _linux_

size_t NumberOfComputeCpus(TDriver& driver) {
    using namespace NYdb::NQuery;

    TQueryClient client(driver);

    std::string query = std::format(R"(
        SELECT SUM(CpuThreads) from `.sys/nodes`;
    )");

    auto result = client.RetryQuery([&query](TSession session) {
        return session.ExecuteQuery(query, TTxControl::NoTx());
    }).GetValueSync();

    if (!result.IsSuccess()) {
        return 0;
    }

    TResultSetParser parser(result.GetResultSet(0));
    if (!parser.TryNextRow()) {
        return 0;
    }

    return parser.ColumnParser("column0").GetOptionalUint64().value_or(0);
}

} // namespace NYdb::NTPCC
