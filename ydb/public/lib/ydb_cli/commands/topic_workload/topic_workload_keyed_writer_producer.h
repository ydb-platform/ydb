#pragma once

#include "topic_workload_keyed_writer.h"

#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>
#include <library/cpp/logger/log.h>
#include <library/cpp/unified_agent_client/clock.h>

#include <util/generic/string.h>
#include <queue>

namespace NYdb::NConsoleClient {

class TTopicWorkloadKeyedWriterProducer {
public:
    TTopicWorkloadKeyedWriterProducer(
        const TTopicWorkloadKeyedWriterParams& params,
        std::shared_ptr<TTopicWorkloadStatsCollector> statsCollector,
        const TString& producerId,
        const NUnifiedAgent::TClock& clock
    );

    void Close();

    void SetWriteSession(std::shared_ptr<NYdb::NTopic::IKeyedWriteSession> writeSession);

    void WaitForContinuationToken(const TDuration& timeout);

    void Send(const TInstant& createTimestamp,
              std::optional<NYdb::NTable::TTransaction> transaction);

    bool HasContinuationTokens();

    NYdb::NTopic::TContinuationToken GetContinuationToken();

    ui64 GetCurrentMessageId() const;

    size_t InflightMessagesCnt() const;

    void HandleAckEvent(NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event);
    void HandleSessionClosed(const NYdb::NTopic::TSessionClosedEvent& event);
    void HandleReadyToAcceptEvent(NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent& event);

private:
    TString GetGeneratedMessage() const;

    std::string GenerateKeyValue() const;
    NYdb::NTopic::TWriteMessage::TMessageMeta GenerateMessageMeta(const std::string& key) const;

private:
    std::shared_ptr<NYdb::NTopic::IKeyedWriteSession> WriteSession_;
    ui64 MessageId_ = 0;
    ui64 AckedMessageId_ = 0;
    const TString ProducerId_;
    std::mutex Lock_;
    std::queue<NYdb::NTopic::TContinuationToken> ContinuationTokens_;
    TConcurrentHashMap<ui64, TInstant> InflightMessagesCreateTs_;
    std::atomic<ui64> InflightMessagesCount_{};

    NYdb::NConsoleClient::TTopicWorkloadWriterParams Params_;
    std::shared_ptr<NYdb::NConsoleClient::TTopicWorkloadStatsCollector> StatsCollector_;
    const NUnifiedAgent::TClock Clock_;
};

} // namespace NYdb::NConsoleClient
