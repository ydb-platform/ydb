#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

namespace NYdb::NConsoleClient {

class TTopicWorkloadWriterProducerCommon {
public:
    virtual ~TTopicWorkloadWriterProducerCommon() = default;

    virtual void Close() = 0;

    virtual void Send(const TInstant& createTimestamp,
                      std::optional<NYdb::NTable::TTransaction> transaction) = 0;

    virtual ui64 GetCurrentMessageId() const = 0;

    virtual size_t InflightMessagesCnt() const = 0;

    virtual void HandleAckEvent(NYdb::NTopic::TWriteSessionEvent::TAcksEvent& event) = 0;

    virtual void HandleSessionClosed(const NYdb::NTopic::TSessionClosedEvent& event) = 0;

    virtual bool WaitForInitSeqNo() {
        return true;
    }

    virtual void WaitForContinuationToken(const TDuration&) {
    }

    virtual bool ContinuationTokenDefined() const {
        return true;
    }

    virtual ui64 GetPartitionId() const {
        return 0;
    }
};

} // namespace NYdb::NConsoleClient
