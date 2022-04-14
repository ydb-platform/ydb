#pragma once

#include "types.h"
#include "responses.h"

#include <library/cpp/threading/future/future.h>

#include <util/generic/map.h>
#include <util/generic/vector.h>

namespace NPersQueue {

struct TPartition {
    TString Topic;
    ui32 PartitionId = 0;

    bool operator <(const TPartition& other) const {
        return std::tie(Topic, PartitionId) < std::tie(other.Topic, other.PartitionId);
    }

    explicit TPartition(const TString& topic, const ui32 partitionId)
        : Topic(topic)
        , PartitionId(partitionId)
    {}
};

struct TProcessedMessage {
    TString Topic;
    TString Data;
    TString SourceIdPrefix;
    ui32 Group = 0;
};

struct TProcessedData {
    TVector<TProcessedMessage> Messages; // maybe grouped by topic?
};

struct TOriginMessage {
    TReadResponse::TData::TMessage Message;
    NThreading::TPromise<TProcessedData> Processed;
};

struct TOriginData {
    TMap<TPartition, TVector<TOriginMessage>> Messages;
};


class IProcessor {
public:
    virtual ~IProcessor() = default;

    // Returns data and promise for each message.
    // Client MUST process each message individually in a deterministic way.
    // Each original message CAN be transformed into several messages and written to several topics/sourceids.
    // But for any given original message all pairs (topic, sourceIdPrefix) of resulting messages MUST be unique.
    // Client MUST finish processing by signaling corresponding promise.
    // Otherwise, exactly-once guarantees cannot be hold.
    virtual NThreading::TFuture<TOriginData> GetNextData() noexcept = 0;
};

}
