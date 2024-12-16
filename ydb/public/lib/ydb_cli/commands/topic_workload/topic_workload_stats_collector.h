#pragma once

#include "topic_workload_stats.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>

#include <util/thread/lfqueue.h>

namespace NYdb {
    namespace NConsoleClient {
        class TTopicWorkloadStatsCollector {
        public:
            TTopicWorkloadStatsCollector(
                size_t producerCount, size_t consumerCount,
                bool quiet, bool printTimestamp,
                ui32 windowDurationSec, ui32 totalDurationSec, ui32 warmupSec,
                double Percentile,
                std::shared_ptr<std::atomic_bool> errorFlag,
                bool transferMode);

            void PrintWindowStatsLoop();

            void PrintHeader(bool total = false) const;
            void PrintTotalStats() const;

            void AddWriterEvent(size_t writerIdx, const TTopicWorkloadStats::WriterEvent& event);
            void AddReaderEvent(size_t readerIdx, const TTopicWorkloadStats::ReaderEvent& event);
            void AddLagEvent(size_t readerIdx, const TTopicWorkloadStats::LagEvent& event);
            void AddReaderSelectEvent(size_t readerIdx, const TTopicWorkloadStats::SelectEvent& event);
            void AddReaderUpsertEvent(size_t readerIdx, const TTopicWorkloadStats::UpsertEvent& event);
            void AddReaderCommitTxEvent(size_t readerIdx, const TTopicWorkloadStats::CommitTxEvent& event);
            void AddWriterSelectEvent(size_t readerIdx, const TTopicWorkloadStats::SelectEvent& event);
            void AddWriterUpsertEvent(size_t readerIdx, const TTopicWorkloadStats::UpsertEvent& event);
            void AddWriterCommitTxEvent(size_t readerIdx, const TTopicWorkloadStats::CommitTxEvent& event);

            ui64 GetTotalReadMessages() const;
            ui64 GetTotalWriteMessages() const;

        private:
            template<class T>
            using TEventQueues = std::vector<THolder<TAutoLockFreeQueue<T>>>;

            void CollectThreadEvents();
            template<class T>
            void CollectThreadEvents(TEventQueues<T>& queues);

            template<class T>
            static void AddQueue(TEventQueues<T>& queues);

            template<class T>
            void AddEvent(size_t index, TEventQueues<T>& queues, const T& event);

            void PrintWindowStats(ui32 windowIt);
            void PrintStats(TMaybe<ui32> windowIt) const;

            size_t WriterCount;
            size_t ReaderCount;
            bool TransferMode;

            TEventQueues<TTopicWorkloadStats::WriterEvent> WriterEventQueues;
            TEventQueues<TTopicWorkloadStats::ReaderEvent> ReaderEventQueues;
            TEventQueues<TTopicWorkloadStats::LagEvent> LagEventQueues;
            TEventQueues<TTopicWorkloadStats::SelectEvent> ReaderSelectEventQueues;
            TEventQueues<TTopicWorkloadStats::UpsertEvent> ReaderUpsertEventQueues;
            TEventQueues<TTopicWorkloadStats::CommitTxEvent> ReaderCommitTxEventQueues;
            TEventQueues<TTopicWorkloadStats::SelectEvent> WriterSelectEventQueues;
            TEventQueues<TTopicWorkloadStats::UpsertEvent> WriterUpsertEventQueues;
            TEventQueues<TTopicWorkloadStats::CommitTxEvent> WriterCommitTxEventQueues;

            bool Quiet;
            bool PrintTimestamp;

            double WindowSec;
            double TotalSec;
            double WarmupSec;

            double Percentile;

            std::shared_ptr<std::atomic_bool> ErrorFlag;

            THolder<TTopicWorkloadStats> WindowStats;
            TTopicWorkloadStats TotalStats;

            TInstant WarmupTime;
        };
    }
}
