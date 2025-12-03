#include "sqs_workload_stats_collector.h"

#include <chrono>
#include <thread>

namespace NYdb::NConsoleClient {

TSqsWorkloadStatsCollector::TSqsWorkloadStatsCollector(TDuration windowSec, TDuration totalSec)
    : WindowSec(windowSec), TotalSec(totalSec) {}

void TSqsWorkloadStatsCollector::PrintHeader() {
    Cout << "Time\tEnd-to-end latency\tRead messages\tRead bytes\tRead time\tWrite messages\tWrite bytes\tWrite time"
         << Endl;
}

void TSqsWorkloadStatsCollector::PrintWindowStats() {}

void TSqsWorkloadStatsCollector::Run(const TClientCommand::TConfig&) {
    if (Quiet) {
        return;
    }

    auto endTime = Now() + TotalSec;
    while (Now() < endTime) {
        PrintWindowStats();
        std::this_thread::sleep_for(std::chrono::milliseconds(WindowSec.MilliSeconds()));
    }
}

}  // namespace NYdb::NConsoleClient
