#pragma once

#include "sqs_workload_scenario.h"
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

struct TSqsWorkloadMessageReceivedEvent {
    double EndToEndLatency;
    TInstant Timestamp;
};

class TSqsWorkloadStats {
public:
    template<typename T>
    void AddEvent(T&& event);
};


class TSqsWorkloadStatsCollector : public TSqsWorkloadScenario {
public:
    void Run(const TClientCommand::TConfig& config);

    TSqsWorkloadStatsCollector(TDuration windowSec, TDuration totalSec);

private:
    TDuration WindowSec = TDuration::Seconds(1);
    TDuration TotalSec = TDuration::Seconds(60);

    void PrintHeader();
    void PrintWindowStats();
};

} // namespace NYdb::NConsoleClient
