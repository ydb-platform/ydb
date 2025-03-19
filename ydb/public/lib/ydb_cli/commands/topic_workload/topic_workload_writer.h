#pragma once

#include "topic_workload_defines.h"
#include "topic_workload_stats_collector.h"
#include "topic_workload_reader_transaction_support.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/logger/log.h>
#include <util/generic/string.h>

namespace NYdb {
    namespace NConsoleClient {

        struct TTopicWorkloadWriterParams {
            size_t TotalSec;
            size_t WarmupSec;
            const NYdb::TDriver& Driver;
            std::shared_ptr<TLog> Log;
            std::shared_ptr<TTopicWorkloadStatsCollector> StatsCollector;
            std::shared_ptr<std::atomic<bool>> ErrorFlag;
            std::shared_ptr<std::atomic_uint> StartedCount;
            const std::vector<TString>& GeneratedMessages;
            TString Database;
            TString TopicName;
            size_t BytesPerSec;
            size_t MessageSize;
            ui32 ProducerThreadCount;
            ui32 WriterIdx;
            ui32 PartitionCount;
            ui32 PartitionSeed;
            bool Direct;
            ui32 Codec = 0;
            bool UseTransactions = false;
            bool UseAutoPartitioning = false;
            bool UseTableSelect = false;
            bool UseTableUpsert = false;
            size_t CommitIntervalMs = 15'000;
            size_t CommitMessages = 1'000'000;
            bool UseCpuTimestamp = false;
        };

        class TTopicWorkloadWriterProducer;
        class TTopicWorkloadWriterWorker {
        public:
            static const size_t GENERATED_MESSAGES_COUNT = 32;
            static void RetryableWriterLoop(TTopicWorkloadWriterParams& params);
            static void WriterLoop(TTopicWorkloadWriterParams& params, TInstant endTime);
            static std::vector<TString> GenerateMessages(size_t messageSize);
        private:
            TTopicWorkloadWriterWorker(TTopicWorkloadWriterParams&& params);
            ~TTopicWorkloadWriterWorker();

            void Close();
            void CloseProducers();

            std::shared_ptr<TTopicWorkloadWriterProducer> CreateProducer(ui64 partitionId);

            void WaitTillNextMessageExpectedCreateTimeAndContinuationToken(std::shared_ptr<TTopicWorkloadWriterProducer> producer);

            void Process(TInstant endTime);

            TInstant GetExpectedCurrMessageCreationTimestamp() const;

            TInstant GetCreateTimestampForNextMessage();

            void TryCommitTx(TInstant& commitTime);
            void TryCommitTableChanges();

            size_t InflightMessagesSize();
            bool InflightMessagesEmpty();

            TTopicWorkloadWriterParams Params;
            ui64 BytesWritten = 0;
            std::optional<TTransactionSupport> TxSupport;
            TInstant StartTimestamp;

            std::vector<std::shared_ptr<TTopicWorkloadWriterProducer>> Producers;
            ui64 PartitionToWriteId = 0;

            std::shared_ptr<std::atomic<bool>> Closed;
            std::shared_ptr<TTopicWorkloadStatsCollector> StatsCollector;

            bool WaitForCommitTx = false;
        };
    }
}
