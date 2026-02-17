#pragma once

#include "topic_workload_defines.h"
#include "topic_workload_stats_collector.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/logger/log.h>
#include <util/system/types.h>
#include <util/string/type.h>
#include <util/generic/size_literals.h>

namespace NYdb {
    namespace NConsoleClient {
        struct TTopicWorkloadReaderParams {
            size_t TotalSec;
            const NYdb::TDriver& Driver;
            std::shared_ptr<TLog> Log;
            std::shared_ptr<TTopicWorkloadStatsCollector> StatsCollector;
            std::shared_ptr<std::atomic_bool> ErrorFlag;
            std::shared_ptr<std::atomic_uint> StartedCount;
            TString Database;
            TString TopicName;
            TString TableName;
            TString ReadOnlyTableName;
            ui32 ConsumerIdx;
            TString ConsumerPrefix;
            ui64 ReaderIdx;
            bool UseTransactions = false;
            bool UseTopicCommit = false;
            bool UseTableSelect = false;
            bool UseTableUpsert = false;
            TDuration RestartInterval = TDuration::Max();
            bool ReadWithoutCommit = false;
            bool ReadWithoutConsumer = false;
            size_t CommitPeriodMs = 15'000;
            size_t CommitMessages = 1'000'000;
            std::optional<size_t> MaxMemoryUsageBytes = 15_MB;
            size_t PartitionMaxInflightBytes = 0; // zero means no limit
        };

        class TTransactionSupport;

        class TTopicWorkloadReader {
        public:
            static void RetryableReaderLoop(const TTopicWorkloadReaderParams& params);

        private:
            static void ReaderLoop(const TTopicWorkloadReaderParams& params, TInstant endTime);

            static std::vector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(NYdb::NTopic::IReadSession& readSession,
                                                                                  const TTopicWorkloadReaderParams& params,
                                                                                  std::optional<TTransactionSupport>& txSupport);

            static void TryCommitTx(const TTopicWorkloadReaderParams& params,
                                    std::optional<TTransactionSupport>& txSupport,
                                    TInstant& commitTime,
                                    TVector<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>& stopPartitionSessionEvents);
            static void TryCommitTableChanges(const TTopicWorkloadReaderParams& params,
                                              std::optional<TTransactionSupport>& txSupport);
            static void GracefullShutdown(TVector<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>& stopPartitionSessionEvents);
        };
    }
}
