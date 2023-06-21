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
                std::shared_ptr<std::atomic_bool> errorFlag);

            void PrintWindowStatsLoop();

            void PrintHeader(bool total = false) const;
            void PrintTotalStats() const;

            void AddWriterEvent(size_t writerIdx, const TTopicWorkloadStats::WriterEvent& event);
            void AddReaderEvent(size_t readerIdx, const TTopicWorkloadStats::ReaderEvent& event);
            void AddLagEvent(size_t readerIdx, const TTopicWorkloadStats::LagEvent& event);

            ui64 GetTotalReadMessages() const;
            ui64 GetTotalWriteMessages() const;

        private:
            template<class T>
            using TEventQueues = std::vector<THolder<TAutoLockFreeQueue<T>>>;

            void CollectThreadEvents(ui32 windowIt);
            template<class T>
            void CollectThreadEvents(ui32 windowIt, TEventQueues<T>& queues);

            template<class T>
            static void AddEvent(size_t index, TEventQueues<T>& queues, const T& event);

            void PrintWindowStats(ui32 windowIt);
            void PrintStats(TMaybe<ui32> windowIt) const;

            size_t WriterCount;
            size_t ReaderCount;

            TEventQueues<TTopicWorkloadStats::WriterEvent> WriterEventQueues;
            TEventQueues<TTopicWorkloadStats::ReaderEvent> ReaderEventQueues;
            TEventQueues<TTopicWorkloadStats::LagEvent> LagEventQueues;

            bool Quiet;
            bool PrintTimestamp;

            double WindowDurationSec;
            double TotalDurationSec;
            double WarmupSec;

            double Percentile;

            std::shared_ptr<std::atomic_bool> ErrorFlag;

            THolder<TTopicWorkloadStats> WindowStats;
            TTopicWorkloadStats TotalStats;
        };
    }
}
