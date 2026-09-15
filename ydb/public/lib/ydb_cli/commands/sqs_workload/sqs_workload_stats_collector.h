#pragma once

#include "sqs_workload_stats.h"

#include <util/thread/lfqueue.h>
#include <ydb/public/lib/ydb_cli/common/command.h>

namespace NYdb::NConsoleClient {

    class TSqsWorkloadStatsCollector {
    public:
        TSqsWorkloadStatsCollector(size_t writerCount, size_t readerCount,
                                   bool quiet, bool printTimestamp,
                                   ui32 windowDurationSec, ui32 totalDurationSec,
                                   ui32 warmupSec, double percentile,
                                   std::shared_ptr<std::atomic_bool> errorFlag);

        void PrintWindowStatsLoop(std::shared_ptr<std::atomic_bool> finishedFlag);

        void PrintHeader(bool total = false) const;
        void PrintTotalStats() const;

        void AddSendRequestDoneEvent(
            const TSqsWorkloadStats::SendRequestDoneEvent& event);
        void AddReceiveRequestDoneEvent(
            const TSqsWorkloadStats::ReceiveRequestDoneEvent& event);
        void AddDeleteRequestDoneEvent(
            const TSqsWorkloadStats::DeleteRequestDoneEvent& event);
        void AddGotMessageEvent(TSqsWorkloadStats::GotMessageEvent& event);
        void AddSendRequestErrorEvent(
            const TSqsWorkloadStats::SendRequestErrorEvent& event);
        void AddReceiveRequestErrorEvent(
            const TSqsWorkloadStats::ReceiveRequestErrorEvent& event);
        void AddDeleteRequestErrorEvent(
            const TSqsWorkloadStats::DeleteRequestErrorEvent& event);
        void AddDeletedMessagesEvent(
            const TSqsWorkloadStats::DeletedMessagesEvent& event);
        void
        AddSentMessagesEvent(const TSqsWorkloadStats::SentMessagesEvent& event);
        void AddFinishProcessMessagesEvent(
            const TSqsWorkloadStats::FinishProcessMessagesEvent& event);
        void AddPushAsyncRequestTaskToQueueEvent(
            const TSqsWorkloadStats::PushAsyncRequestTaskToQueueEvent& event);
        void AddErrorWhileProcessingMessagesEvent(
            const TSqsWorkloadStats::ErrorWhileProcessingMessagesEvent& event);

        ui64 GetTotalReadMessages() const;
        ui64 GetTotalWriteMessages() const;

    private:
        template <typename T>
        using TEventQueues = std::vector<THolder<TAutoLockFreeQueue<T>>>;

        void CollectEvents();

        template <typename T>
        void CollectEvents();

        template <typename T>
        void AddEvent(THolder<TAutoLockFreeQueue<T>>& queue, const T& event);

        void PrintWindowStats(ui32 windowIt);
        void PrintStats(TMaybe<ui32> windowIt) const;

        size_t WriterCount;
        size_t ReaderCount;

        THolder<TAutoLockFreeQueue<TSqsWorkloadStats::SendRequestDoneEvent>>
            SendRequestDoneEventQueue;
        THolder<TAutoLockFreeQueue<TSqsWorkloadStats::ReceiveRequestDoneEvent>>
            ReceiveRequestDoneEventQueue;
        THolder<TAutoLockFreeQueue<TSqsWorkloadStats::DeleteRequestDoneEvent>>
            DeleteRequestDoneEventQueue;
        THolder<TAutoLockFreeQueue<TSqsWorkloadStats::GotMessageEvent>>
            GotMessageEventQueue;
        THolder<TAutoLockFreeQueue<TSqsWorkloadStats::SendRequestErrorEvent>>
            SendRequestErrorEventQueue;
        THolder<TAutoLockFreeQueue<TSqsWorkloadStats::ReceiveRequestErrorEvent>>
            ReceiveRequestErrorEventQueue;
        THolder<TAutoLockFreeQueue<TSqsWorkloadStats::DeleteRequestErrorEvent>>
            DeleteRequestErrorEventQueue;
        THolder<TAutoLockFreeQueue<TSqsWorkloadStats::SentMessagesEvent>>
            SentMessagesEventQueue;
        THolder<TAutoLockFreeQueue<TSqsWorkloadStats::DeletedMessagesEvent>>
            DeletedMessagesEventQueue;
        THolder<TAutoLockFreeQueue<TSqsWorkloadStats::FinishProcessMessagesEvent>>
            FinishProcessMessagesEventQueue;
        THolder<
            TAutoLockFreeQueue<TSqsWorkloadStats::PushAsyncRequestTaskToQueueEvent>>
            PushAsyncRequestTaskToQueueEventQueue;
        THolder<TAutoLockFreeQueue<TSqsWorkloadStats::ErrorWhileProcessingMessagesEvent>>
            ErrorWhileProcessingMessagesEventQueue;

        bool Quiet;
        bool PrintTimestamp;

        double WindowSec;
        double TotalSec;
        double WarmupSec;
        double Percentile;

        std::shared_ptr<std::atomic_bool> ErrorFlag;

        THolder<TSqsWorkloadStats> WindowStats;
        TSqsWorkloadStats TotalStats;

        TInstant WarmupTime;
    };

} // namespace NYdb::NConsoleClient
