#include "options.h"
#include "server.h"

#include <library/cpp/colorizer/colors.h>

using namespace NKikimr;

void PrintResults(const TOptions& opts, const TRequestStats& stats) {
    opts.PrintOpts();

    auto PrintValue = [](const char* name, const auto& value) {
        Cout << NColorizer::StdOut().Blue()
             << name << NColorizer::StdOut().Default() << ": "
             << NColorizer::StdOut().Green() << value << NColorizer::StdOut().Default() << Endl;
    };

    const size_t reqs = stats.RequestsCount;
    const size_t resps = stats.ResponsesCount;
    PrintValue("Requests sent", reqs);
    PrintValue("Responses received", resps);
    PrintValue("OK responses", stats.OkResponses.load());
    PrintValue("Deadline responses", stats.DeadlineResponses.load());
    const double seconds = static_cast<double>(opts.TestTime.MicroSeconds()) / 1000000.0;
    const size_t reqsPerSecond = static_cast<size_t>(static_cast<double>(resps) / seconds);
    const size_t respsPerSecond = static_cast<size_t>(static_cast<double>(resps) / seconds);
    PrintValue("Requests per second", reqsPerSecond);
    PrintValue("Responses per second", respsPerSecond);

    const size_t lostRequests = reqs - resps;
    PrintValue("Requests lost (no response)", lostRequests);
    PrintValue("Percent of lost requests", static_cast<double>(lostRequests) / static_cast<double>(reqs) * 100.0);
}

int main(int argc, const char* argv[]) {
    TOptions opts(argc, argv);
    TTestServer server(opts);
    TRequestStats stats;
    server.RunQuotaRequesters(stats);
    PrintResults(opts, stats);
}
