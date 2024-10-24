#pragma once

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include "topic_workload_writer.h"

#include <library/cpp/logger/log.h>
#include <util/generic/string.h>

namespace NYdb {
    namespace NConsoleClient {
        class TTopicWorkloadWriterProducer {
        public:
            TTopicWorkloadWriterProducer(
                    const TTopicWorkloadWriterParams& params,
                    std::shared_ptr<NYdb::NConsoleClient::TTopicWorkloadStatsCollector> statsCollector,
                    const TString &ProducerId,
                    const ui64 PartitionId
            );
            TTopicWorkloadWriterProducer(TTopicWorkloadWriterProducer&& other) = default;

            ~TTopicWorkloadWriterProducer();

            void Close();

            bool WaitForInitSeqNo();

            void WaitForContinuationToken(const TDuration &timeout);

            void Send(const TInstant &createTimestamp,
                      std::optional<NYdb::NTable::TTransaction> transaction);

            std::shared_ptr<NYdb::NTopic::IWriteSession> WriteSession;
            ui64 MessageId = 0;
            const TString ProducerId;
            const ui64 PartitionId;
            TMaybe<NTopic::TContinuationToken> ContinuationToken = {};
            THashMap<ui64, TInstant> InflightMessagesCreateTs;
        private:

            TString GetGeneratedMessage() const;

            void HandleAckEvent(NYdb::NTopic::TWriteSessionEvent::TAcksEvent &event);

            void HandleSessionClosed(const NYdb::NTopic::TSessionClosedEvent &event);
            NYdb::NConsoleClient::TTopicWorkloadWriterParams Params;
            std::shared_ptr<NYdb::NConsoleClient::TTopicWorkloadStatsCollector> StatsCollector;
        };
    }
}
