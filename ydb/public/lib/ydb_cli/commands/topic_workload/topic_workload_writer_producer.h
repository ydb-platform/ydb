#pragma once

#include "topic_workload_writer_producer_common.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include "topic_workload_writer.h"

#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>
#include <library/cpp/logger/log.h>
#include <library/cpp/unified_agent_client/clock.h>
#include <util/generic/string.h>

namespace NYdb::NConsoleClient {
    class TTopicWorkloadWriterProducer : public TTopicWorkloadWriterProducerCommon {
    public:
        TTopicWorkloadWriterProducer(
                const TTopicWorkloadWriterParams& params,
                std::shared_ptr<NYdb::NConsoleClient::TTopicWorkloadStatsCollector> statsCollector,
                const TString& producerId,
                const ui64 partitionId,
                const NUnifiedAgent::TClock& clock
                );
        void Close() override;

        void SetWriteSession(std::shared_ptr<NYdb::NTopic::IWriteSession> writeSession);

        bool WaitForInitSeqNo() override;

        void WaitForContinuationToken(const TDuration& timeout) override;

        void Send(const TInstant& createTimestamp,
                    std::optional<NYdb::NTable::TTransaction> transaction) override;

        bool ContinuationTokenDefined() const override;

        ui64 GetCurrentMessageId() const override;

        ui64 GetPartitionId() const override;

        size_t InflightMessagesCnt() const override;

        void HandleAckEvent(NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event) override;

        void HandleSessionClosed(const NYdb::NTopic::TSessionClosedEvent& event) override;
    private:
        TString GetGeneratedMessage() const;
        NTopic::TWriteMessage::TMessageMeta GenerateMessageMeta() const;

        std::shared_ptr<NYdb::NTopic::IWriteSession> WriteSession_;
        ui64 MessageId_ = 0;
        const TString ProducerId_;
        const ui64 PartitionId_;
        TMaybe<NTopic::TContinuationToken> ContinuationToken_ = {};
        TConcurrentHashMap<ui64, TInstant> InflightMessagesCreateTs_;
        std::atomic<ui64> InflightMessagesCount_{};
        NYdb::NConsoleClient::TTopicWorkloadWriterParams Params_;
        std::shared_ptr<NYdb::NConsoleClient::TTopicWorkloadStatsCollector> StatsCollector_;
        const NUnifiedAgent::TClock Clock_;
    };
} // namespace NYdb::NConsoleClient
