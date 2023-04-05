#include "topic_workload_stats_collector.h"

#include "topic_workload_defines.h"

using namespace NYdb::NConsoleClient;

TTopicWorkloadStatsCollector::TTopicWorkloadStatsCollector(
    bool producer, bool consumer,
    bool quiet, bool printTimestamp,
    ui32 windowDurationSec, ui32 totalDurationSec,
    std::shared_ptr<std::atomic_bool> errorFlag)
    : Producer(producer)
    , Consumer(consumer)
    , Quiet(quiet)
    , PrintTimestamp(printTimestamp)
    , WindowDurationSec(windowDurationSec)
    , TotalDurationSec(totalDurationSec)
    , ErrorFlag(errorFlag)
    , WindowStats(MakeHolder<TTopicWorkloadStats>())
{
}

void TTopicWorkloadStatsCollector::PrintHeader() const {
    if (Quiet)
        return;

    Cout << "Window\t" << (Producer ? "Write speed\tWrite time\tInflight\t" : "") << (Consumer ? "Lag\tLag time\tRead speed\tFull time\t" : "") << (PrintTimestamp ? "Timestamp" : "") << Endl;
    Cout << "#\t" << (Producer ? "msg/s\tMB/s\tP99(ms)\t\tmax msg\t\t" : "") << (Consumer ? "max msg\tP99(ms)\t\tmsg/s\tMB/s\tP99(ms)" : "");
    Cout << Endl;
}

void TTopicWorkloadStatsCollector::PrintWindowStatsLoop() {
    auto StartTime = Now();
    auto StopTime = StartTime + TDuration::Seconds(TotalDurationSec + 1);
    int windowIt = 1;
    auto windowDuration = TDuration::Seconds(WindowDurationSec);
    while (Now() < StopTime && !*ErrorFlag) {
        if (Now() > StartTime + windowIt * windowDuration && !*ErrorFlag) {
            PrintWindowStats(windowIt++);
        }
        Sleep(std::max(TDuration::Zero(), Now() - StartTime - windowIt * windowDuration));
    }
}

void TTopicWorkloadStatsCollector::PrintWindowStats(ui32 windowIt) {
    PrintStats(windowIt);

    with_lock (Lock) {
        WindowStats = MakeHolder<TTopicWorkloadStats>();
    }
}
void TTopicWorkloadStatsCollector::PrintTotalStats() const {
    PrintHeader();
    PrintStats({});
}

void TTopicWorkloadStatsCollector::PrintStats(TMaybe<ui32> windowIt) const {
    if (Quiet)
        return;

    with_lock (Lock) {
        const auto& stats = windowIt.Empty() ? TotalStats : *WindowStats;
        double seconds = windowIt.Empty() ? TotalDurationSec : WindowDurationSec;
        TString totalIt = windowIt.Empty() ? "Total" : std::to_string(windowIt.GetRef());

        Cout << totalIt;
        if (Producer) {
            Cout << "\t" << (int)(stats.WriteMessages / seconds)
                 << "\t" << (int)(stats.WriteBytes / seconds / 1024 / 1024)
                 << "\t" << stats.WriteTimeHist.GetValueAtPercentile(99) << "\t"
                 << "\t" << stats.InflightMessages << "\t";
        }
        if (Consumer) {
            Cout << "\t" << stats.LagMessages
                 << "\t" << stats.LagTimeHist.GetValueAtPercentile(99) << "\t"
                 << "\t" << (int)(stats.ReadMessages / seconds)
                 << "\t" << (int)(stats.ReadBytes / seconds / 1024 / 1024)
                 << "\t" << stats.FullTimeHist.GetValueAtPercentile(99) << "\t";
        }
        if (PrintTimestamp) {
            Cout << "\t" << Now().ToStringUpToSeconds();
        }
        Cout << Endl;
    }
}

ui64 TTopicWorkloadStatsCollector::GetTotalReadMessages() const {
    with_lock (Lock) {
        return TotalStats.ReadMessages;
    }
}

ui64 TTopicWorkloadStatsCollector::GetTotalWriteMessages() const {
    with_lock (Lock) {
        return TotalStats.WriteMessages;
    }
}

void TTopicWorkloadStatsCollector::AddWriterEvent(ui64 messageSize, ui64 writeTime, ui64 inflightMessages)
{
    with_lock (Lock) {
        WindowStats->AddWriterEvent(messageSize, writeTime, inflightMessages);
        TotalStats.AddWriterEvent(messageSize, writeTime, inflightMessages);
    }
}

void TTopicWorkloadStatsCollector::AddReaderEvent(ui64 messageSize, ui64 fullTime)
{
    with_lock (Lock) {
        WindowStats->AddReaderEvent(messageSize, fullTime);
        TotalStats.AddReaderEvent(messageSize, fullTime);
    }
}

void TTopicWorkloadStatsCollector::AddLagEvent(ui64 lagMessages, ui64 lagTime)
{
    with_lock (Lock) {
        WindowStats->AddLagEvent(lagMessages, lagTime);
        TotalStats.AddLagEvent(lagMessages, lagTime);
    }
}
