#include "topic_workload_stats_collector.h"

#include "topic_workload_defines.h"

#include "util/stream/format.h"

using namespace NYdb::NConsoleClient;

TTopicWorkloadStatsCollector::TTopicWorkloadStatsCollector(
    size_t writerCount, size_t readerCount,
    bool quiet, bool printTimestamp,
    ui32 windowDurationSec, ui32 totalDurationSec, ui32 warmupSec,
    double percentile,
    std::shared_ptr<std::atomic_bool> errorFlag,
    bool transferMode)
    : WriterCount(writerCount)
    , ReaderCount(readerCount)
    , TransferMode(transferMode)
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
        AddQueue(WriterEventQueues);
        AddQueue(WriterSelectEventQueues);
        AddQueue(WriterUpsertEventQueues);
        AddQueue(WriterCommitTxEventQueues);
    }

    for (size_t readerIdx = 0; readerIdx < readerCount; readerIdx++) {
        AddQueue(ReaderEventQueues);
        AddQueue(LagEventQueues);
        AddQueue(ReaderSelectEventQueues);
        AddQueue(ReaderUpsertEventQueues);
        AddQueue(ReaderCommitTxEventQueues);
    }
}

void TTopicWorkloadStatsCollector::PrintHeader(bool total) const {
    if (Quiet && !total)
        return;

    TStringBuilder header;

    header << "Window\t";
    if (WriterCount > 0)
        header << "Write speed\tWrite time\tInflight\t";
    if (ReaderCount > 0) {
        if (!TransferMode)
            header << "Lag\t\tLag time\t";
        header << "Read speed\t";
        if (TransferMode) {
            header << "Topic time\t";
        } else {
            header << "Full time\t";
        }
    }
    if (TransferMode) {
        header << "Select time\t";
        header << "Upsert time\t";
        header << "Commit time\t";
    }
    if (PrintTimestamp)
        header << "Timestamp";

    header << "\n";

    header << "#\t";
    if (WriterCount > 0)
        header << "msg/s\tMB/s\tpercentile,ms\tpercentile,msg\t";
    if (ReaderCount > 0) {
        if (!TransferMode)
            header << "percentile,msg\tpercentile,ms\t";
        header << "msg/s\tMB/s\tpercentile,ms\t";
    }
    if (TransferMode) {
        header << "percentile,ms\t";
        header << "percentile,ms\t";
        header << "percentile,ms\t";
    }
    header << "\n";

    Cout << header << Flush;
}

void TTopicWorkloadStatsCollector::PrintWindowStatsLoop() {
    PrintHeader();

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
        if (!TransferMode) {
            Cout << "\t" << stats.LagMessagesHist.GetValueAtPercentile(Percentile) << "\t"
                 << "\t" << stats.LagTimeHist.GetValueAtPercentile(Percentile) << "\t";
        }
        Cout << "\t" << (int)(stats.ReadMessages / seconds)
             << "\t" << (int)(stats.ReadBytes / seconds / 1024 / 1024)
             << "\t" << stats.FullTimeHist.GetValueAtPercentile(Percentile) << "\t";
    }
    if (TransferMode) {
        Cout << "\t" << stats.SelectTimeHist.GetValueAtPercentile(Percentile) << "\t";
        Cout << "\t" << stats.UpsertTimeHist.GetValueAtPercentile(Percentile) << "\t";
        Cout << "\t" << stats.CommitTxTimeHist.GetValueAtPercentile(Percentile) << "\t";
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
    CollectThreadEvents(ReaderSelectEventQueues);
    CollectThreadEvents(ReaderUpsertEventQueues);
    CollectThreadEvents(ReaderCommitTxEventQueues);
    CollectThreadEvents(WriterSelectEventQueues);
    CollectThreadEvents(WriterUpsertEventQueues);
    CollectThreadEvents(WriterCommitTxEventQueues);
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

template<class T>
void TTopicWorkloadStatsCollector::AddQueue(TEventQueues<T>& queues)
{
    auto queue = MakeHolder<TAutoLockFreeQueue<T>>();
    queues.emplace_back(std::move(queue));
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

void TTopicWorkloadStatsCollector::AddReaderSelectEvent(size_t readerIdx, const TTopicWorkloadStats::SelectEvent& event)
{
    AddEvent(readerIdx, ReaderSelectEventQueues, event);
}

void TTopicWorkloadStatsCollector::AddReaderUpsertEvent(size_t readerIdx, const TTopicWorkloadStats::UpsertEvent& event)
{
    AddEvent(readerIdx, ReaderUpsertEventQueues, event);
}

void TTopicWorkloadStatsCollector::AddReaderCommitTxEvent(size_t readerIdx, const TTopicWorkloadStats::CommitTxEvent& event)
{
    AddEvent(readerIdx, ReaderCommitTxEventQueues, event);
}

void TTopicWorkloadStatsCollector::AddWriterSelectEvent(size_t writerIdx, const TTopicWorkloadStats::SelectEvent& event)
{
    AddEvent(writerIdx, WriterSelectEventQueues, event);
}

void TTopicWorkloadStatsCollector::AddWriterUpsertEvent(size_t writerIdx, const TTopicWorkloadStats::UpsertEvent& event)
{
    AddEvent(writerIdx, WriterUpsertEventQueues, event);
}

void TTopicWorkloadStatsCollector::AddWriterCommitTxEvent(size_t writerIdx, const TTopicWorkloadStats::CommitTxEvent& event)
{
    AddEvent(writerIdx, WriterCommitTxEventQueues, event);
}

template<class T>
void TTopicWorkloadStatsCollector::AddEvent(size_t index, TEventQueues<T>& queues, const T& event)
{
    if ((WarmupTime != TInstant()) && (Now() >= WarmupTime)) {
        queues[index]->Enqueue(MakeHolder<T>(event));
    }
}
