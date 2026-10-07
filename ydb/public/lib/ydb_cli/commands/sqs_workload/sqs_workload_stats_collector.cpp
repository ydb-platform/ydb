#include "sqs_workload_stats_collector.h"

#include <fmt/format.h>

using namespace NYdb::NConsoleClient;

TSqsWorkloadStatsCollector::TSqsWorkloadStatsCollector(
    size_t writerCount, size_t readerCount, bool quiet, bool printTimestamp,
    ui32 windowDurationSec, ui32 totalDurationSec, ui32 warmupSec,
    double percentile, std::shared_ptr<std::atomic_bool> errorFlag)
    : WriterCount(writerCount),
    ReaderCount(readerCount),
    Quiet(quiet),
    PrintTimestamp(printTimestamp),
    WindowSec(windowDurationSec),
    TotalSec(totalDurationSec),
    WarmupSec(warmupSec),
    Percentile(percentile),
    ErrorFlag(errorFlag),
    WindowStats(std::make_unique<TSqsWorkloadStats>())
{
    SendRequestDoneEventQueue = std::make_unique<
        TAutoLockFreeQueue<TSqsWorkloadStats::SendRequestDoneEvent>>();
    ReceiveRequestDoneEventQueue = std::make_unique<
        TAutoLockFreeQueue<TSqsWorkloadStats::ReceiveRequestDoneEvent>>();
    DeleteRequestDoneEventQueue = std::make_unique<
        TAutoLockFreeQueue<TSqsWorkloadStats::DeleteRequestDoneEvent>>();
    GotMessageEventQueue =
        std::make_unique<TAutoLockFreeQueue<TSqsWorkloadStats::GotMessageEvent>>();
    SendRequestErrorEventQueue = std::make_unique<
        TAutoLockFreeQueue<TSqsWorkloadStats::SendRequestErrorEvent>>();
    ReceiveRequestErrorEventQueue = std::make_unique<
        TAutoLockFreeQueue<TSqsWorkloadStats::ReceiveRequestErrorEvent>>();
    DeleteRequestErrorEventQueue = std::make_unique<
        TAutoLockFreeQueue<TSqsWorkloadStats::DeleteRequestErrorEvent>>();
    SentMessagesEventQueue =
        std::make_unique<TAutoLockFreeQueue<TSqsWorkloadStats::SentMessagesEvent>>();
    DeletedMessagesEventQueue = std::make_unique<
        TAutoLockFreeQueue<TSqsWorkloadStats::DeletedMessagesEvent>>();
    FinishProcessMessagesEventQueue = std::make_unique<
        TAutoLockFreeQueue<TSqsWorkloadStats::FinishProcessMessagesEvent>>();
    PushAsyncRequestTaskToQueueEventQueue = std::make_unique<TAutoLockFreeQueue<
        TSqsWorkloadStats::PushAsyncRequestTaskToQueueEvent>>();
    ErrorWhileProcessingMessagesEventQueue = std::make_unique<TAutoLockFreeQueue<
        TSqsWorkloadStats::ErrorWhileProcessingMessagesEvent>>();
}

void TSqsWorkloadStatsCollector::PrintHeader(bool total) const {
    if (Quiet && !total) {
        return;
    }

    TStringBuilder header;

    if (WriterCount > 0 && ReaderCount == 0) {
        header << fmt::format("{:<6} {:>10} {:>10} {:>8} {:>10} {:>10} {:>12}",
                              "Window", "Wr speed", "Wr bytes", "Send ms",
                              "Send err", "Send ok", "Async tasks");
        if (PrintTimestamp) {
            header << " " << "Timestamp";
        }
        header << "\n";

        header << fmt::format("{:<6} {:>10} {:>10} {:>8} {:>10} {:>10} {:>12}",
                              "#", "msg/s", "MB/s",
                              "p" + std::to_string((int)(Percentile)), "count",
                              "count", "count");
        header << "\n";
    } else if (WriterCount == 0 && ReaderCount > 0) {
        header << fmt::format(
            "{:<6} {:>10} {:>10} {:>10} {:>8} {:>8} {:>10} "
            "{:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12}", "Window", "Rd speed",
            "Rd bytes", "E2E ms", "Recv ms", "Del ms", "Recv err", "Recv ok",
            "Del err", "Del ok", "Del speed", "InFlight", "Proc err", "Async tasks");
        if (PrintTimestamp) {
            header << " " << "Timestamp";
        }
        header << "\n";

        header << fmt::format(
            "{:<6} {:>10} {:>10} {:>10} {:>8} {:>8} {:>10} "
            "{:>10} {:>10} {:>10} {:>10} {:>10} {:>10} {:>12}", "#", "msg/s", "MB/s",
            "p" + std::to_string((int)(Percentile)),
            "p" + std::to_string((int)(Percentile)),
            "p" + std::to_string((int)(Percentile)), "count", "count", "count",
            "count", "msg/s", "count", "count", "count");
        header << "\n";
    } else {
        header << "Window\t";
        if (WriterCount > 0) {
            header << "Wr speed\tWr bytes\tSend ms\tSend err\tSend ok\tAsync "
                      "tasks\t";
        }
        if (ReaderCount > 0) {
            header << "Rd speed\tRd bytes\tE2E ms\t";
            header
                << "Recv ms\tDel ms\tRecv err\tRecv ok\tDel err\tDel ok\tDel "
                   "speed\tInFlight\tProc err\tAsync tasks\t";
        }
        if (PrintTimestamp) {
            header << "Timestamp";
        }

        header << "\n";

        header << "#\t";
        if (WriterCount > 0) {
            header << "msg/s\tMB/s\tp" << (int)(Percentile)
                   << "\tcount\tcount\tcount\t";
        }
        if (ReaderCount > 0) {
            header << "msg/s\tMB/s\tp" << (int)(Percentile) << "\t";
            header << "p" << (int)(Percentile) << "\tp" << (int)(Percentile)
                   << "\tcount\tcount\tcount\tcount\tmsg/s\tcount\tcount\tcount\t";
        }
        header << "\n";
    }

    Cout << header << Flush;
}

void TSqsWorkloadStatsCollector::PrintWindowStatsLoop(
    std::shared_ptr<std::atomic_bool> finishedFlag) {
    PrintHeader();

    auto startTime = Now();
    WarmupTime = startTime + TDuration::Seconds(WarmupSec);

    int windowIt = 1;
    auto windowDuration = TDuration::Seconds(WindowSec);
    while (!*ErrorFlag && !finishedFlag->load()) {
        auto windowTime = [startTime, windowDuration](int index) {
            return startTime + index * windowDuration;
        };
        if (Now() > windowTime(windowIt) && !*ErrorFlag) {
            PrintWindowStats(windowIt++);
        }
        Sleep(std::max(TDuration::Zero(), Now() - windowTime(windowIt)));
    }

    CollectEvents();
}

void TSqsWorkloadStatsCollector::PrintWindowStats(ui32 windowIt) {
    CollectEvents();
    PrintStats(windowIt);

    auto messagesInFlight = WindowStats->MessagesInFlight;
    auto asyncRequestTasks = WindowStats->AsyncRequestTasks;
    WindowStats = std::make_unique<TSqsWorkloadStats>();
    WindowStats->MessagesInFlight = messagesInFlight;
    WindowStats->MessagesInFlightHist.RecordValue(messagesInFlight);
    WindowStats->AsyncRequestTasks = asyncRequestTasks;
}

void TSqsWorkloadStatsCollector::PrintTotalStats() const {
    PrintHeader(true);
    PrintStats({});
}

void TSqsWorkloadStatsCollector::PrintStats(TMaybe<ui32> windowIt) const {
    if (Quiet && windowIt.Defined()) {
        return;
    }

    const auto& stats = windowIt.Empty() ? TotalStats : *WindowStats;
    double seconds = windowIt.Empty() ? TotalSec - WarmupSec : WindowSec;
    TString totalIt =
        windowIt.Empty() ? "Total" : std::to_string(windowIt.GetRef());

    if (WriterCount > 0 && ReaderCount == 0) {
        const auto writeSpeed = (int)(stats.WriteMessages / seconds);
        const long double writeMbPerSec =
            (long double)stats.WriteBytes / seconds / 1024.0 / 1024.0;
        const auto sendMs =
            (i64)stats.SendRequestTimeHist.GetValueAtPercentile(Percentile);
        const auto mbStr = fmt::format("{:.3f}", writeMbPerSec);

        Cout << fmt::format("{:<6} {:>10} {:>10} {:>8} {:>10} {:>10} {:>12}",
                            totalIt, writeSpeed, mbStr, sendMs,
                            stats.SendRequestErrors, stats.SendRequestsSuccess,
                            stats.AsyncRequestTasks);
        if (PrintTimestamp) {
            Cout << " " << Now().ToStringUpToSeconds();
        }
        Cout << Endl;
        return;
    }

    if (WriterCount == 0 && ReaderCount > 0) {
        const auto readSpeed = (int)(stats.ReadMessages / seconds);
        const long double readMbPerSec =
            (long double)stats.ReadBytes / seconds / 1024.0 / 1024.0;
        const auto e2eMs =
            (i64)stats.EndToEndLatencyHist.GetValueAtPercentile(Percentile);
        const auto recvMs =
            (i64)stats.ReceiveRequestTimeHist.GetValueAtPercentile(Percentile);
        const auto delMs =
            (i64)stats.DeleteRequestTimeHist.GetValueAtPercentile(Percentile);
        const auto inFlight =
            (i64)stats.MessagesInFlightHist.GetValueAtPercentile(Percentile);
        const auto delSpeed = (int)(stats.DeleteMessages / seconds);
        const auto mbStr = fmt::format("{:.3f}", readMbPerSec);

        Cout << fmt::format(
            "{:<6} {:>10} {:>10} {:>10} {:>8} {:>8} {:>10} {:>10} {:>10} "
            "{:>10} "
            "{:>10} {:>10} {:>10} {:>12}", totalIt, readSpeed, mbStr, e2eMs, recvMs,
            delMs, stats.ReceiveRequestErrors, stats.ReceiveRequestsSuccess,
            stats.DeleteRequestErrors, stats.DeleteRequestsSuccess, delSpeed,
            inFlight, stats.ErrorsWhileProcessingMessages, stats.AsyncRequestTasks);
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
    CollectEvents<TSqsWorkloadStats::GotMessageEvent>();
    CollectEvents<TSqsWorkloadStats::DeleteRequestDoneEvent>();
    CollectEvents<TSqsWorkloadStats::SendRequestErrorEvent>();
    CollectEvents<TSqsWorkloadStats::ReceiveRequestErrorEvent>();
    CollectEvents<TSqsWorkloadStats::DeleteRequestErrorEvent>();
    CollectEvents<TSqsWorkloadStats::SentMessagesEvent>();
    CollectEvents<TSqsWorkloadStats::DeletedMessagesEvent>();
    CollectEvents<TSqsWorkloadStats::FinishProcessMessagesEvent>();
    CollectEvents<TSqsWorkloadStats::PushAsyncRequestTaskToQueueEvent>();
    CollectEvents<TSqsWorkloadStats::ErrorWhileProcessingMessagesEvent>();
}

template <class T>
void TSqsWorkloadStatsCollector::CollectEvents()
{
    std::unique_ptr<T> event;
    if constexpr (std::is_same_v<T, TSqsWorkloadStats::SendRequestDoneEvent>) {
        while (SendRequestDoneEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    } else if constexpr (std::is_same_v<
                             T, TSqsWorkloadStats::ReceiveRequestDoneEvent>) {
        while (ReceiveRequestDoneEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    } else if constexpr (std::is_same_v<T,
                                        TSqsWorkloadStats::GotMessageEvent>) {
        while (GotMessageEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    } else if constexpr (std::is_same_v<
                             T, TSqsWorkloadStats::DeleteRequestDoneEvent>) {
        while (DeleteRequestDoneEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    } else if constexpr (std::is_same_v<
                             T, TSqsWorkloadStats::SendRequestErrorEvent>) {
        while (SendRequestErrorEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    } else if constexpr (std::is_same_v<
                             T, TSqsWorkloadStats::ReceiveRequestErrorEvent>) {
        while (ReceiveRequestErrorEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    } else if constexpr (std::is_same_v<
                             T, TSqsWorkloadStats::DeleteRequestErrorEvent>) {
        while (DeleteRequestErrorEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    } else if constexpr (std::is_same_v<T,
                                        TSqsWorkloadStats::SentMessagesEvent>) {
        while (SentMessagesEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    } else if constexpr (std::is_same_v<
                             T, TSqsWorkloadStats::DeletedMessagesEvent>) {
        while (DeletedMessagesEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    } else if constexpr (std::is_same_v<
                             T,
                             TSqsWorkloadStats::FinishProcessMessagesEvent>) {
        while (FinishProcessMessagesEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    } else if constexpr (
        std::is_same_v<T, TSqsWorkloadStats::PushAsyncRequestTaskToQueueEvent>) {
        while (PushAsyncRequestTaskToQueueEventQueue->Dequeue(&event)) {
            WindowStats->AddEvent(*event);
            TotalStats.AddEvent(*event);
        }
    } else if constexpr (
        std::is_same_v<T, TSqsWorkloadStats::ErrorWhileProcessingMessagesEvent>) {
        while (ErrorWhileProcessingMessagesEventQueue->Dequeue(&event)) {
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

void TSqsWorkloadStatsCollector::AddSendRequestDoneEvent(
    const TSqsWorkloadStats::SendRequestDoneEvent& event)
{
    SendRequestDoneEventQueue->Enqueue(
        std::make_unique<TSqsWorkloadStats::SendRequestDoneEvent>(event));
}

void TSqsWorkloadStatsCollector::AddReceiveRequestDoneEvent(
    const TSqsWorkloadStats::ReceiveRequestDoneEvent& event)
{
    ReceiveRequestDoneEventQueue->Enqueue(
        std::make_unique<TSqsWorkloadStats::ReceiveRequestDoneEvent>(event));
}

void TSqsWorkloadStatsCollector::AddDeleteRequestDoneEvent(
    const TSqsWorkloadStats::DeleteRequestDoneEvent& event)
{
    DeleteRequestDoneEventQueue->Enqueue(
        std::make_unique<TSqsWorkloadStats::DeleteRequestDoneEvent>(event));
}

void TSqsWorkloadStatsCollector::AddGotMessageEvent(
    TSqsWorkloadStats::GotMessageEvent& event)
{
    GotMessageEventQueue->Enqueue(
        std::make_unique<TSqsWorkloadStats::GotMessageEvent>(event));
}

void TSqsWorkloadStatsCollector::AddSendRequestErrorEvent(
    const TSqsWorkloadStats::SendRequestErrorEvent& event)
{
    SendRequestErrorEventQueue->Enqueue(
        std::make_unique<TSqsWorkloadStats::SendRequestErrorEvent>(event));
}

void TSqsWorkloadStatsCollector::AddReceiveRequestErrorEvent(
    const TSqsWorkloadStats::ReceiveRequestErrorEvent& event)
{
    ReceiveRequestErrorEventQueue->Enqueue(
        std::make_unique<TSqsWorkloadStats::ReceiveRequestErrorEvent>(event));
}

void TSqsWorkloadStatsCollector::AddDeleteRequestErrorEvent(
    const TSqsWorkloadStats::DeleteRequestErrorEvent& event)
{
    DeleteRequestErrorEventQueue->Enqueue(
        std::make_unique<TSqsWorkloadStats::DeleteRequestErrorEvent>(event));
}

void TSqsWorkloadStatsCollector::AddSentMessagesEvent(
    const TSqsWorkloadStats::SentMessagesEvent& event)
{
    SentMessagesEventQueue->Enqueue(
        std::make_unique<TSqsWorkloadStats::SentMessagesEvent>(event));
}

void TSqsWorkloadStatsCollector::AddDeletedMessagesEvent(
    const TSqsWorkloadStats::DeletedMessagesEvent& event)
{
    DeletedMessagesEventQueue->Enqueue(
        std::make_unique<TSqsWorkloadStats::DeletedMessagesEvent>(event));
}

void TSqsWorkloadStatsCollector::AddFinishProcessMessagesEvent(
    const TSqsWorkloadStats::FinishProcessMessagesEvent& event)
{
    FinishProcessMessagesEventQueue->Enqueue(
        std::make_unique<TSqsWorkloadStats::FinishProcessMessagesEvent>(event));
}

void TSqsWorkloadStatsCollector::AddPushAsyncRequestTaskToQueueEvent(
    const TSqsWorkloadStats::PushAsyncRequestTaskToQueueEvent& event)
{
    PushAsyncRequestTaskToQueueEventQueue->Enqueue(
        std::make_unique<TSqsWorkloadStats::PushAsyncRequestTaskToQueueEvent>(event));
}

void TSqsWorkloadStatsCollector::AddErrorWhileProcessingMessagesEvent(
    const TSqsWorkloadStats::ErrorWhileProcessingMessagesEvent& event)
{
    ErrorWhileProcessingMessagesEventQueue->Enqueue(
        std::make_unique<TSqsWorkloadStats::ErrorWhileProcessingMessagesEvent>(event));
}

template <class T>
void TSqsWorkloadStatsCollector::AddEvent(std::unique_ptr<TAutoLockFreeQueue<T>>& queue,
                                          const T& event)
{
    if ((WarmupTime != TInstant()) && (Now() >= WarmupTime)) {
        queue->Enqueue(std::make_unique<T>(event));
    }
}
