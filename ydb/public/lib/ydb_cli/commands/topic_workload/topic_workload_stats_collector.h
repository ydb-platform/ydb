#pragma once

#include "topic_workload_stats.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_workload.h>

namespace NYdb {
    namespace NConsoleClient {
        class TTopicWorkloadStatsCollector {
        public:
            TTopicWorkloadStatsCollector(
                bool producer, bool consumer,
                bool quiet, bool printTimestamp,
                ui32 windowDurationSec, ui32 totalDurationSec,
                std::shared_ptr<std::atomic_bool> errorFlag);

            void PrintWindowStatsLoop();

            void PrintHeader() const;
            void PrintTotalStats() const;

            void AddWriterEvent(ui64 messageSize, ui64 writeTime, ui64 inflightMessages);
            void AddReaderEvent(ui64 messageSize, ui64 fullTime);
            void AddLagEvent(ui64 lagMessages, ui64 lagTime);

            ui64 GetTotalReadMessages() const;
            ui64 GetTotalWriteMessages() const;

        private:
            void PrintWindowStats(ui32 windowIt);
            void PrintStats(TMaybe<ui32> windowIt) const;

            bool Producer;
            bool Consumer;

            bool Quiet;
            bool PrintTimestamp;

            double WindowDurationSec;
            double TotalDurationSec;

            TSpinLock Lock;
            std::shared_ptr<std::atomic_bool> ErrorFlag;

            THolder<TTopicWorkloadStats> WindowStats;
            TTopicWorkloadStats TotalStats;
        };
    }
}