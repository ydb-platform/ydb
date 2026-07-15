#pragma once

#include "topic_workload_writer_producer_common.h"

#include "topic_workload_keyed_writer.h"

#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>
#include <library/cpp/logger/log.h>
#include <library/cpp/unified_agent_client/clock.h>

#include <util/generic/string.h>

namespace NYdb::NConsoleClient {

class TTopicWorkloadKeyedWriterProducer : public TTopicWorkloadWriterProducerCommon {
public:
    TTopicWorkloadKeyedWriterProducer(
        const TTopicWorkloadKeyedWriterParams& params,
        std::shared_ptr<TTopicWorkloadStatsCollector> statsCollector,
        const TString& producerId,
        const std::string& sessionId,
        const NUnifiedAgent::TClock& clock
    );

    void Close() override;

    void SetProducer(std::shared_ptr<NYdb::NTopic::IProducer> producer);

    void Send(const TInstant& createTimestamp,
              std::optional<NYdb::NTable::TTransaction> transaction) override;

    ui64 GetCurrentMessageId() const override;

    size_t InflightMessagesCnt() const override;

    void HandleAckEvent(NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event) override;
    void HandleSessionClosed(const NYdb::NTopic::TSessionClosedEvent& event) override;

    void WaitForContinuationToken(const TDuration&) override;

private:
    std::string GetKey() const;

    std::shared_ptr<NYdb::NTopic::IProducer> Producer_;
    ui64 MessageId_ = 0;
    ui64 AckedMessageId_ = 0;
    const TString ProducerId_;
    const std::string SessionId_;
    std::mutex Lock_;
    TConcurrentHashMap<ui64, TInstant> InflightMessagesCreateTs_;
    std::atomic<ui64> InflightMessagesCount_{};

    NYdb::NConsoleClient::TTopicWorkloadKeyedWriterParams Params_;
    std::shared_ptr<NYdb::NConsoleClient::TTopicWorkloadStatsCollector> StatsCollector_;
    const NUnifiedAgent::TClock Clock_;
    std::string KeyPrefix_;
    ui64 KeyId_ = 0;
};

} // namespace NYdb::NConsoleClient
