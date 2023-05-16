#include "topic_workload_stats_collector.h"

#include "topic_workload_defines.h"

using namespace NYdb::NConsoleClient;

TTopicWorkloadStatsCollector::TTopicWorkloadStatsCollector(
    size_t writerCount, size_t readerCount,
    bool quiet, bool printTimestamp,
    ui32 windowDurationSec, ui32 totalDurationSec,
    std::shared_ptr<std::atomic_bool> errorFlag)
    : WriterCount(writerCount)
    , ReaderCount(readerCount)
    , Quiet(quiet)
    , PrintTimestamp(printTimestamp)
    , WindowDurationSec(windowDurationSec)
    , TotalDurationSec(totalDurationSec)
    , ErrorFlag(errorFlag)
    , WindowStats(MakeHolder<TTopicWorkloadStats>())
{
    for (size_t writerIdx = 0; writerIdx < writerCount; writerIdx++) {
        auto writerQueue = MakeHolder<TAutoLockFreeQueue<TTopicWorkloadStats::WriterEvent>>();
        WriterEventQueues.emplace_back(std::move(writerQueue));
    }

    for (size_t readerIdx = 0; readerIdx < readerCount; readerIdx++) {
        auto readerQueue = MakeHolder<TAutoLockFreeQueue<TTopicWorkloadStats::ReaderEvent>>();
        ReaderEventQueues.emplace_back(std::move(readerQueue));
        auto lagQueue = MakeHolder<TAutoLockFreeQueue<TTopicWorkloadStats::LagEvent>>();
        LagEventQueues.emplace_back(std::move(lagQueue));
    }
}

void TTopicWorkloadStatsCollector::PrintHeader(bool total) const {
    if (Quiet && !total)
        return;

    Cout << "Window\t" << (WriterCount > 0 ? "Write speed\tWrite time\tInflight\t" : "") << (ReaderCount > 0 ? "Lag\t\tLag time\tRead speed\tFull time\t" : "") << (PrintTimestamp ? "Timestamp" : "") << Endl;
    Cout << "#\t" << (WriterCount > 0 ? "msg/s\tMB/s\tP99(ms)\t\tP99(msg)\t" : "") << (ReaderCount > 0 ? "P99(msg)\tP99(ms)\t\tmsg/s\tMB/s\tP99(ms)" : "");
    Cout << Endl;
}

void TTopicWorkloadStatsCollector::PrintWindowStatsLoop() {
    auto StartTime = Now();
    auto StopTime = StartTime + TDuration::Seconds(TotalDurationSec + 1);
    int windowIt = 1;
    auto windowDuration = TDuration::Seconds(WindowDurationSec);
    while (Now() < StopTime && !*ErrorFlag) {
        if (Now() > StartTime + windowIt * windowDuration && !*ErrorFlag) {
            CollectThreadEvents();
            PrintWindowStats(windowIt++);
        }
        Sleep(std::max(TDuration::Zero(), Now() - StartTime - windowIt * windowDuration));
    }
    CollectThreadEvents();
}

void TTopicWorkloadStatsCollector::PrintWindowStats(ui32 windowIt) {
    PrintStats(windowIt);

    WindowStats = MakeHolder<TTopicWorkloadStats>();
}
void TTopicWorkloadStatsCollector::PrintTotalStats() const {
    PrintHeader(true);
    PrintStats({});
}

void TTopicWorkloadStatsCollector::PrintStats(TMaybe<ui32> windowIt) const {
    if (Quiet && windowIt.Defined())
        return;

    const auto& stats = windowIt.Empty() ? TotalStats : *WindowStats;
    double seconds = windowIt.Empty() ? TotalDurationSec : WindowDurationSec;
    TString totalIt = windowIt.Empty() ? "Total" : std::to_string(windowIt.GetRef());

    Cout << totalIt;
    if (WriterCount > 0) {
        Cout << "\t" << (int)(stats.WriteMessages / seconds)
             << "\t" << (int)(stats.WriteBytes / seconds / 1024 / 1024)
             << "\t" << stats.WriteTimeHist.GetValueAtPercentile(99) << "\t"
             << "\t" << stats.InflightMessagesHist.GetValueAtPercentile(99) << "\t";
    }
    if (ReaderCount > 0) {
        Cout << "\t" << stats.LagMessagesHist.GetValueAtPercentile(99) << "\t"
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

void TTopicWorkloadStatsCollector::CollectThreadEvents()
{
    for (auto& queue : WriterEventQueues) {
        TTopicWorkloadStats::WriterEventRef event;
        while (queue->Dequeue(&event)) {
            WindowStats->AddWriterEvent(*event);
            TotalStats.AddWriterEvent(*event);
        }
    }
    for (auto& queue : ReaderEventQueues)
    {
        TTopicWorkloadStats::ReaderEventRef event;
        while (queue->Dequeue(&event)) {
            WindowStats->AddReaderEvent(*event);
            TotalStats.AddReaderEvent(*event);
        }
    }
    for (auto& queue : LagEventQueues)
    {
        TTopicWorkloadStats::LagEventRef event;
        while (queue->Dequeue(&event)) {
            WindowStats->AddLagEvent(*event);
            TotalStats.AddLagEvent(*event);
        }
    }
}

ui64 TTopicWorkloadStatsCollector::GetTotalReadMessages() const {
    return TotalStats.ReadMessages;
}

ui64 TTopicWorkloadStatsCollector::GetTotalWriteMessages() const {
    return TotalStats.WriteMessages;
}

void TTopicWorkloadStatsCollector::AddWriterEvent(size_t writerIdx, const TTopicWorkloadStats::WriterEvent& event)
{
    auto ref = MakeHolder<TTopicWorkloadStats::WriterEvent>(event);
    WriterEventQueues[writerIdx]->Enqueue(ref);
}

void TTopicWorkloadStatsCollector::AddReaderEvent(size_t readerIdx, const TTopicWorkloadStats::ReaderEvent& event)
{
    auto ref = MakeHolder<TTopicWorkloadStats::ReaderEvent>(event);
    ReaderEventQueues[readerIdx]->Enqueue(ref);
}

void TTopicWorkloadStatsCollector::AddLagEvent(size_t readerIdx, const TTopicWorkloadStats::LagEvent& event)
{
    auto ref = MakeHolder<TTopicWorkloadStats::LagEvent>(event);
    LagEventQueues[readerIdx]->Enqueue(ref);
}
