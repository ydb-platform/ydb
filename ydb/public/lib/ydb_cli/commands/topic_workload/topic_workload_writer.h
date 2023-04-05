#pragma once

#include "topic_workload_defines.h"
#include "topic_workload_stats_collector.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <library/cpp/logger/log.h>
#include <util/generic/string.h>

namespace NYdb {
    namespace NConsoleClient {

        struct TTopicWorkloadWriterParams {
            size_t Seconds;
            NYdb::TDriver* Driver;
            std::shared_ptr<TLog> Log;
            std::shared_ptr<TTopicWorkloadStatsCollector> StatsCollector;
            std::shared_ptr<std::atomic<bool>> ErrorFlag;
            std::shared_ptr<std::atomic_uint> StartedCount;

            size_t ByteRate;
            ui32 ProducerThreadCount;
            TString MessageGroupId;
            size_t MessageSize;
            ui32 Codec = 0;
        };

        class TTopicWorkloadWriterWorker {
        public:
            TTopicWorkloadWriterWorker(TTopicWorkloadWriterParams&& params);
            ~TTopicWorkloadWriterWorker();

            void Close();

            TDuration Process();

            void CreateWorker();

            void CreateTopicWorker();

            static void WriterLoop(TTopicWorkloadWriterParams&& params);

        private:
            bool ProcessAckEvent(const NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event);

            bool ProcessReadyToAcceptEvent(NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent& event);
            bool ProcessSessionClosedEvent(const NYdb::NTopic::TSessionClosedEvent& event);

            bool ProcessEvent(NYdb::NTopic::TWriteSessionEvent::TEvent& event);

            TString GetGeneratedMessage() const;
            void GenerateMessages();



            TTopicWorkloadWriterParams Params;
            ui64 MessageId = 0;
            ui64 AckedMessageId = 0;
            ui64 BytesWritten = 0;
            TString MessageGroupId;
            std::shared_ptr<NYdb::NTopic::IWriteSession> WriteSession;
            TInstant StartTimestamp;

            std::vector<TString> GeneratedMessages;

            NThreading::TFuture<ui64> InitSeqNo;
            bool InitSeqNoProcessed = true;
            TMaybe<NTopic::TContinuationToken> ContinuationToken;

            std::shared_ptr<std::atomic<bool>> Closed;
            std::shared_ptr<TTopicWorkloadStatsCollector> StatsCollector;

            struct TInflightMessage {
                size_t MessageSize;
                TInstant MessageTime;
            };
            THashMap<ui64, TInflightMessage> InflightMessages;
        };
    }
}