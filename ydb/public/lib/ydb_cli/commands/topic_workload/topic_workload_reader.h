#pragma once

#include "topic_workload_defines.h"
#include "topic_workload_stats_collector.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

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
            bool UseTableSelect = true;
            bool UseTableUpsert = true;
            bool ReadWithoutConsumer = false;
            size_t CommitPeriod = 15;
            size_t CommitMessages = 1'000'000;
        };

        class TTransactionSupport;

        class TTopicWorkloadReader {
        public:
            static void RetryableReaderLoop(TTopicWorkloadReaderParams& params);

        private:
            static void ReaderLoop(TTopicWorkloadReaderParams& params, TInstant endTime);

            static TVector<NYdb::NTopic::TReadSessionEvent::TEvent> GetEvents(NYdb::NTopic::IReadSession& readSession,
                                                                              TTopicWorkloadReaderParams& params,
                                                                              std::optional<TTransactionSupport>& txSupport);

            static void TryCommitTx(TTopicWorkloadReaderParams& params,
                                    std::optional<TTransactionSupport>& txSupport,
                                    TInstant& commitTime,
                                    TVector<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>& stopPartitionSessionEvents);
            static void TryCommitTableChanges(TTopicWorkloadReaderParams& params,
                                              std::optional<TTransactionSupport>& txSupport);
            static void GracefullShutdown(TVector<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>& stopPartitionSessionEvents);
        };
    }
}
