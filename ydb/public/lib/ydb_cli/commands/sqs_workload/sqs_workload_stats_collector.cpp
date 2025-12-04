#include "sqs_workload_stats_collector.h"

#include <fmt/format.h>

using namespace NYdb::NConsoleClient;

TSqsWorkloadStatsCollector::TSqsWorkloadStatsCollector(
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
    , WindowStats(MakeHolder<TSqsWorkloadStats>())
{
    SendRequestDoneEventQueue = MakeHolder<TAutoLockFreeQueue<TSqsWorkloadStats::SendRequestDoneEvent>>();
    ReceiveRequestDoneEventQueue = MakeHolder<TAutoLockFreeQueue<TSqsWorkloadStats::ReceiveRequestDoneEvent>>();
    DeleteRequestDoneEventQueue = MakeHolder<TAutoLockFreeQueue<TSqsWorkloadStats::DeleteRequestDoneEvent>>();
    GotMessageEventQueue = MakeHolder<TAutoLockFreeQueue<TSqsWorkloadStats::GotMessageEvent>>();
    UnreadRequests = MakeHolder<THashMap<TString, std::pair<TSqsWorkloadStats::SendRequestStartEvent, size_t>>>();
}

void TSqsWorkloadStatsCollector::PrintHeader(bool total) const {
    if (Quiet && !total)
        return;

    TStringBuilder header;

    if (WriterCount > 0 && ReaderCount == 0) {
        header << fmt::format("{:<6} {:>16} {:>17} {:>13}",
                              "Window",
                              "Write speed",
                              "Write bytes",
                              "Send time");
        if (PrintTimestamp) {
            header << " " << "Timestamp";
        }
        header << "\n";

        header << fmt::format("{:<6} {:>16} {:>17} {:>13}",
                              "#",
                              "msg/s",
                              "MB/s",
                              "percentile,ms");
        header << "\n";
    } else if (WriterCount == 0 && ReaderCount > 0) {
        header << fmt::format("{:<6} {:>16} {:>17} {:>22} {:>15} {:>15}",
                              "Window",
                              "Read speed",
                              "Read bytes",
                              "End-to-end latency",
                              "Receive time",
                              "Delete time");
        if (PrintTimestamp) {
            header << " " << "Timestamp";
        }
        header << "\n";

        header << fmt::format("{:<6} {:>16} {:>17} {:>22} {:>15} {:>15}",
                              "#",
                              "msg/s",
                              "MB/s",
                              "percentile,ms",
                              "percentile,ms",
                              "percentile,ms");
        header << "\n";
    } else {
        header << "Window\t";
        if (WriterCount > 0)
            header << "Write speed\tWrite bytes\tSend time\t";
        if (ReaderCount > 0) {
            header << "Read speed\tRead bytes\tEnd-to-end latency\t";
            header << "Receive time\tDelete time\t";
        }
        if (PrintTimestamp)
            header << "Timestamp";

        header << "\n";

        header << "#\t";
        if (WriterCount > 0)
            header << "msg/s\tMB/s\tpercentile,ms\t";
        if (ReaderCount > 0) {
            header << "msg/s\tMB/s\tpercentile,ms\t";
            header << "percentile,ms\tpercentile,ms\t";
        }
        header << "\n";
    }

    Cout << header << Flush;
}

void TSqsWorkloadStatsCollector::PrintWindowStatsLoop() {
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
            CollectEvents();
            PrintWindowStats(windowIt++);
        }
        Sleep(std::max(TDuration::Zero(), Now() - windowTime(windowIt)));
    }

    CollectEvents();
}

void TSqsWorkloadStatsCollector::PrintWindowStats(ui32 windowIt) {
    PrintStats(windowIt);

    WindowStats = MakeHolder<TSqsWorkloadStats>();
}

void TSqsWorkloadStatsCollector::PrintTotalStats() const {
    PrintHeader(true);
    PrintStats({});
}

void TSqsWorkloadStatsCollector::PrintStats(TMaybe<ui32> windowIt) const {
    if (Quiet && windowIt.Defined())
        return;

    const auto& stats = windowIt.Empty() ? TotalStats : *WindowStats;
    double seconds = windowIt.Empty() ? TotalSec - WarmupSec : WindowSec;
    TString totalIt = windowIt.Empty() ? "Total" : std::to_string(windowIt.GetRef());

    if (WriterCount > 0 && ReaderCount == 0) {
        const auto writeSpeed = (int)(stats.WriteMessages / seconds);
        const long double writeMbPerSec = (long double)stats.WriteBytes / seconds / 1024.0 / 1024.0;
        const auto sendMs = (i64)stats.SendRequestTimeHist.GetValueAtPercentile(Percentile);
        const auto mbStr = fmt::format("{:.3f}", writeMbPerSec);

        Cout << fmt::format("{:<6} {:>16} {:>17} {:>13}",
                            totalIt,
                            writeSpeed,
                            mbStr,
                            sendMs);
        if (PrintTimestamp) {
            Cout << " " << Now().ToStringUpToSeconds();
        }
        Cout << Endl;
        return;
    }

    if (WriterCount == 0 && ReaderCount > 0) {
        const auto readSpeed = (int)(stats.ReadMessages / seconds);
        const long double readMbPerSec = (long double)stats.ReadBytes / seconds / 1024.0 / 1024.0;
        const auto e2eMs = (i64)stats.EndToEndLatencyHist.GetValueAtPercentile(Percentile);
        const auto recvMs = (i64)stats.ReceiveRequestTimeHist.GetValueAtPercentile(Percentile);
        const auto delMs = (i64)stats.DeleteRequestTimeHist.GetValueAtPercentile(Percentile);
        const auto mbStr = fmt::format("{:.3f}", readMbPerSec);

        Cout << fmt::format("{:<6} {:>16} {:>17} {:>22} {:>15} {:>15}",
                            totalIt,
                            readSpeed,
                            mbStr,
                            e2eMs,
                            recvMs,
                            delMs);
        if (PrintTimestamp) {
            Cout << " " << Now().ToStringUpToSeconds();
        }
        Cout << Endl;
        return;
    }
}

void TSqsWorkloadStatsCollector::CollectEvents()
{
    CollectEvents<TSqsWorkloadStats::SendRequestDoneEvent>();
    CollectEvents<TSqsWorkloadStats::ReceiveRequestDoneEvent>();
    CollectEvents<TSqsWorkloadStats::DeleteRequestDoneEvent>();
    CollectEvents<TSqsWorkloadStats::GotMessageEvent>();
}

template<class T>
void TSqsWorkloadStatsCollector::CollectEvents()
{
    THolder<T> event;
    if constexpr (std::is_same_v<T, TSqsWorkloadStats::SendRequestDoneEvent>) {
        while (SendRequestDoneEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    } else if constexpr (std::is_same_v<T, TSqsWorkloadStats::ReceiveRequestDoneEvent>) {
        while (ReceiveRequestDoneEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    } else if constexpr (std::is_same_v<T, TSqsWorkloadStats::DeleteRequestDoneEvent>) {
        while (DeleteRequestDoneEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    } else if constexpr (std::is_same_v<T, TSqsWorkloadStats::GotMessageEvent>) {
        while (GotMessageEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    }
}

ui64 TSqsWorkloadStatsCollector::GetTotalReadMessages() const {
    return TotalStats.ReadMessages;
}

ui64 TSqsWorkloadStatsCollector::GetTotalWriteMessages() const {
    return TotalStats.WriteMessages;
}

void TSqsWorkloadStatsCollector::AddSendRequestDoneEvent(const TSqsWorkloadStats::SendRequestDoneEvent& event)
{
    SendRequestDoneEventQueue->Enqueue(MakeHolder<TSqsWorkloadStats::SendRequestDoneEvent>(event));
}

void TSqsWorkloadStatsCollector::AddReceiveRequestDoneEvent(const TSqsWorkloadStats::ReceiveRequestDoneEvent& event)
{
    ReceiveRequestDoneEventQueue->Enqueue(MakeHolder<TSqsWorkloadStats::ReceiveRequestDoneEvent>(event));
}

void TSqsWorkloadStatsCollector::AddDeleteRequestDoneEvent(const TSqsWorkloadStats::DeleteRequestDoneEvent& event)
{
    DeleteRequestDoneEventQueue->Enqueue(MakeHolder<TSqsWorkloadStats::DeleteRequestDoneEvent>(event));
}

void TSqsWorkloadStatsCollector::AddGotMessageEvent(TSqsWorkloadStats::GotMessageEvent& event)
{
    GotMessageEventQueue->Enqueue(MakeHolder<TSqsWorkloadStats::GotMessageEvent>(event));
}

template<class T>
void TSqsWorkloadStatsCollector::AddEvent(THolder<TAutoLockFreeQueue<T>>& queue, const T& event)
{
    if ((WarmupTime != TInstant()) && (Now() >= WarmupTime)) {
        queue->Enqueue(MakeHolder<T>(event));
    }
}
