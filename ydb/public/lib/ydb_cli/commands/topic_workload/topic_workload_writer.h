#pragma once

#include "topic_workload_defines.h"
#include "topic_workload_stats_collector.h"
#include "topic_workload_reader_transaction_support.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

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
            size_t ByteRate;
            size_t MessageSize;
            ui32 ProducerThreadCount;
            ui32 WriterIdx;
            TString ProducerId;
            ui32 PartitionId;
            bool Direct;
            ui32 Codec = 0;
            bool UseTransactions = false;
            bool UseAutoPartitioning = false;
            bool UseTableSelect = false;
            bool UseTableUpsert = false;
            size_t CommitPeriod = 15;
            size_t CommitMessages = 1'000'000;
        };

        class TTopicWorkloadWriterWorker {
        public:
            static void RetryableWriterLoop(TTopicWorkloadWriterParams& params);
            static void WriterLoop(TTopicWorkloadWriterParams& params, TInstant endTime);
            static std::vector<TString> GenerateMessages(size_t messageSize);
        private:
            TTopicWorkloadWriterWorker(TTopicWorkloadWriterParams&& params);
            ~TTopicWorkloadWriterWorker();

            void Close();

            void Process(TInstant endTime);

            void CreateWorker();

            bool ProcessAckEvent(const NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event);

            bool ProcessReadyToAcceptEvent(NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent& event);
            bool ProcessSessionClosedEvent(const NYdb::NTopic::TSessionClosedEvent& event);

            bool ProcessEvent(NYdb::NTopic::TWriteSessionEvent::TEvent& event);

            bool WaitForInitSeqNo();

            TString GetGeneratedMessage() const;

            TInstant GetCreateTimestamp() const;

            void TryCommitTx(TTopicWorkloadWriterParams& params,
                             TInstant& commitTime);
            void TryCommitTableChanges(TTopicWorkloadWriterParams& params);

            TTopicWorkloadWriterParams Params;
            ui64 MessageId = 0;
            ui64 AckedMessageId = 0;
            ui64 BytesWritten = 0;
            std::optional<TTransactionSupport> TxSupport;
            std::shared_ptr<NYdb::NTopic::IWriteSession> WriteSession;
            TInstant StartTimestamp;

            TMaybe<NTopic::TContinuationToken> ContinuationToken;

            std::shared_ptr<std::atomic<bool>> Closed;
            std::shared_ptr<TTopicWorkloadStatsCollector> StatsCollector;

            // SeqNo - CreateTime
            THashMap<ui64, TInstant> InflightMessages;
            bool WaitForCommitTx = false;
        };
    }
}
