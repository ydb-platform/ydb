#include "topic_workload_stats_collector.h"

#include "topic_workload_defines.h"

#include "util/stream/format.h"

using namespace NYdb::NConsoleClient;

TTopicWorkloadStatsCollector::TTopicWorkloadStatsCollector(
    size_t writerCount, size_t readerCount,
    bool quiet, bool printTimestamp,
    ui32 windowDurationSec, ui32 totalDurationSec, ui32 warmupSec,
    double percentile,
    std::shared_ptr<std::atomic_bool> errorFlag)
    : WriterCount(writerCount)
    , ReaderCount(readerCount)
    , Quiet(quiet)
    , PrintTimestamp(printTimestamp)
    , WindowSec(windowDurationSec)
    , TotalSec(totalDurationSec)
    , WarmupSec(warmupSec)
    , Percentile(percentile)
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

    TStringBuilder header;

    header << "Window\t";
    if (WriterCount > 0)
        header << "Write speed\tWrite time\tInflight\t";
    if (ReaderCount > 0)
        header << "Lag\t\tLag time\tRead speed\tFull time\t";
    if (PrintTimestamp)
        header << "Timestamp";
    header << "\n";

    header << "#\t";
    if (WriterCount > 0)
        header << "msg/s\tMB/s\tpercentile,ms\tpercentile,msg\t";
    if (ReaderCount > 0)
        header << "percentile,msg\tpercentile,ms\tmsg/s\tMB/s\tpercentile,ms";
    header << "\n";

    Cout << header << Flush;
}

void TTopicWorkloadStatsCollector::PrintWindowStatsLoop() {
    auto startTime = Now();
    WarmupTime = startTime + TDuration::Seconds(WarmupSec);
    auto stopTime = startTime + TDuration::Seconds(TotalSec + 1);
    int windowIt = 1;
    auto windowDuration = TDuration::Seconds(WindowSec);
    while (Now() < stopTime && !*ErrorFlag) {
        auto windowTime = [startTime, windowDuration](int index) {
            return startTime + index * windowDuration;
        };
        if (Now() > windowTime(windowIt) && !*ErrorFlag) {
            CollectThreadEvents();
            PrintWindowStats(windowIt++);
        }
        Sleep(std::max(TDuration::Zero(), Now() - windowTime(windowIt)));
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
    double seconds = windowIt.Empty() ? TotalSec - WarmupSec : WindowSec;
    TString totalIt = windowIt.Empty() ? "Total" : std::to_string(windowIt.GetRef());

    Cout << totalIt;
    if (WriterCount > 0) {
        Cout << "\t" << (int)(stats.WriteMessages / seconds)
             << "\t" << (int)(stats.WriteBytes / seconds / 1024 / 1024)
             << "\t" << stats.WriteTimeHist.GetValueAtPercentile(Percentile) << "\t"
             << "\t" << stats.InflightMessagesHist.GetValueAtPercentile(Percentile) << "\t";
    }
    if (ReaderCount > 0) {
        Cout << "\t" << stats.LagMessagesHist.GetValueAtPercentile(Percentile) << "\t"
             << "\t" << stats.LagTimeHist.GetValueAtPercentile(Percentile) << "\t"
             << "\t" << (int)(stats.ReadMessages / seconds)
             << "\t" << (int)(stats.ReadBytes / seconds / 1024 / 1024)
             << "\t" << stats.FullTimeHist.GetValueAtPercentile(Percentile) << "\t";
    }
    if (PrintTimestamp) {
        Cout << "\t" << Now().ToStringUpToSeconds();
    }
    Cout << Endl;
}

void TTopicWorkloadStatsCollector::CollectThreadEvents()
{
    CollectThreadEvents(WriterEventQueues);
    CollectThreadEvents(ReaderEventQueues);
    CollectThreadEvents(LagEventQueues);
}

template<class T>
void TTopicWorkloadStatsCollector::CollectThreadEvents(TEventQueues<T>& queues)
{
    for (auto& queue : queues) {
        THolder<T> event;
        while (queue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
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
    AddEvent(writerIdx, WriterEventQueues, event);
}

void TTopicWorkloadStatsCollector::AddReaderEvent(size_t readerIdx, const TTopicWorkloadStats::ReaderEvent& event)
{
    AddEvent(readerIdx, ReaderEventQueues, event);
}

void TTopicWorkloadStatsCollector::AddLagEvent(size_t readerIdx, const TTopicWorkloadStats::LagEvent& event)
{
    AddEvent(readerIdx, LagEventQueues, event);
}

template<class T>
void TTopicWorkloadStatsCollector::AddEvent(size_t index, TEventQueues<T>& queues, const T& event)
{
    if ((WarmupTime != TInstant()) && (Now() >= WarmupTime)) {
        queues[index]->Enqueue(MakeHolder<T>(event));
    }
}
