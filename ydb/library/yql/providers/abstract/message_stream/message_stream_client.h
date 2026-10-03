#pragma once

#include "message_stream_session.h"

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <memory>
#include <optional>
#include <vector>

namespace NFq {

// Backend-neutral message stream: YDB and Logbroker topics, YT queues, Kafka topics.

template <class TValue>
struct TMessageStreamResult {
    EMessageStreamStatus Status = EMessageStreamStatus::Success;
    NYql::TIssues Issues;
    TValue Value;

    bool IsSuccess() const {
        return Status == EMessageStreamStatus::Success;
    }
};

struct TMessageStreamConsumer {
    TString Name;
};

struct TMessageStreamTopicDescription {
    ui64 PartitionsCount = 0;
    std::vector<TMessageStreamConsumer> Consumers;
};

struct TMessageStreamConsumerPartition {
    ui64 PartitionId = 0;
    std::optional<ui64> StartOffset;
    std::optional<ui64> CommittedOffset;
    std::optional<ui64> EndOffset;
    std::optional<TInstant> LastWriteTime;
    std::optional<i64> Generation;
};

struct TMessageStreamConsumerDescription {
    std::vector<TMessageStreamConsumerPartition> Partitions;
};

struct TMessageStreamPartitionDescription {
    ui64 PartitionId = 0;
    // Unset when the describe response did not include partition statistics.
    // Zero means the partition is empty, not that statistics are missing.
    std::optional<ui64> StartOffset;
    std::optional<ui64> EndOffset;
};

struct TMessageStreamDescribeConsumerSettings {
    bool IncludeStats = false;
    bool IncludeLocation = false;
};

// Data plane: read sessions and offset commit. Writes stay on the federated topic client.
class IMessageStreamDataClient {
public:
    virtual ~IMessageStreamDataClient() = default;

    virtual std::shared_ptr<IMessageStreamReadSession> CreateReadSession(const TMessageStreamReadSettings& settings) = 0;
    virtual NThreading::TFuture<TMessageStreamResult<TMessageStreamOffset>> CommitOffset(
        const TString& stream, ui64 partitionId, const TString& consumer, ui64 offset) = 0;
};

class IMessageStreamClient : public IMessageStreamDataClient {
public:
    virtual NThreading::TFuture<TMessageStreamResult<TMessageStreamTopicDescription>> DescribeStream(const TString& stream) = 0;
    virtual NThreading::TFuture<TMessageStreamResult<TMessageStreamConsumerDescription>> DescribeConsumer(
        const TString& stream, const TString& consumer, const TMessageStreamDescribeConsumerSettings& settings = {}) = 0;
    virtual NThreading::TFuture<TMessageStreamResult<TMessageStreamPartitionDescription>> DescribePartition(
        const TString& stream, ui64 partitionId) = 0;
};

} // namespace NFq
