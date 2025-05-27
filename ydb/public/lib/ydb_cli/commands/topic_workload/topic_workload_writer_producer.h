#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include "topic_workload_writer.h"

#include <library/cpp/logger/log.h>
#include <library/cpp/unified_agent_client/clock.h>
#include <util/generic/string.h>

namespace NYdb::NConsoleClient {
    class TTopicWorkloadWriterProducer {
    public:
        TTopicWorkloadWriterProducer(
                const TTopicWorkloadWriterParams& params,
                std::shared_ptr<NYdb::NConsoleClient::TTopicWorkloadStatsCollector> statsCollector,
                const TString& producerId,
                const ui64 partitionId,
                const NUnifiedAgent::TClock& clock
                );
        void Close();
        
        void SetWriteSession(std::shared_ptr<NYdb::NTopic::IWriteSession> writeSession);
        
        bool WaitForInitSeqNo();

        void WaitForContinuationToken(const TDuration& timeout);

        void Send(const TInstant& createTimestamp,
                    std::optional<NYdb::NTable::TTransaction> transaction);

        bool ContinuationTokenDefined();

        ui64 GetCurrentMessageId();

        ui64 GetPartitionId();

        size_t InflightMessagesCnt();

        void HandleAckEvent(NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event);

        void HandleSessionClosed(const NYdb::NTopic::TSessionClosedEvent& event);
    private:
        TString GetGeneratedMessage() const;

        std::shared_ptr<NYdb::NTopic::IWriteSession> WriteSession_;
        ui64 MessageId_ = 0;
        const TString ProducerId_;
        const ui64 PartitionId_;
        TMaybe<NTopic::TContinuationToken> ContinuationToken_ = {};
        THashMap<ui64, TInstant> InflightMessagesCreateTs_;
        NYdb::NConsoleClient::TTopicWorkloadWriterParams Params_;
        std::shared_ptr<NYdb::NConsoleClient::TTopicWorkloadStatsCollector> StatsCollector_;
        const NUnifiedAgent::TClock Clock_;
    };
} // namespace NYdb::NConsoleClient
