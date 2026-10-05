#pragma once

#include "message_stream_defs.h"
#include "message_stream_session.h"

#include <library/cpp/threading/future/core/future.h>

namespace NFq {

// One instance is permanently bound to one stream in one backend/cluster.
// The factory selects and validates its nonempty path/name. Several instances
// may share a backend connection, but no method can retarget an existing client.
// Data plane: read sessions and offset commit. Writes stay on the federated topic client.
// All backends: sufficient for nontransactional progress; no external transaction/fencing token.
// YDB: topic SDK client. QYT: queue/consumer RPC adapter. Kafka: consumer/offset
// client adapter. Connection credentials, cluster selection and client sharing
// belong to the implementation; this object is not one consumer assignment.
class IMessageStreamDataClient {
public:
    // All backends: polymorphic lifetime only; use the session API for explicit shutdown.
    virtual ~IMessageStreamDataClient() = default;

    // Immutable stream identity within this client's backend/cluster.
    // YDB: topic path. QYT: queue path. Kafka: topic name. Not globally unique.
    // The returned reference remains valid for the lifetime of this client.
    virtual const TString& GetStream() const = 0;

    // YDB: wrap the SDK read session and partition controls.
    // QYT: adapt pull_queue[_consumer] into sessions; define row encoding and ownership.
    // Kafka: adapt poll and assignment/rebalance events; define timestamp/isolation
    // policy. QYT and Kafka require adapter-defined partition confirmation semantics.
    // Unsupported settings must throw TMessageStreamException(Unsupported) or
    // produce SessionClosed(Unsupported) before any partition/data events. Call
    // settings.Validate() before starting backend work.
    virtual std::shared_ptr<IMessageStreamReadSession> CreateReadSession(const TMessageStreamReadSessionSettings& settings) = 0;
    // All backends: offset is the next unprocessed position (exclusive), unlike a record ID.
    // YDB: out-of-session CommitOffset for the named topic consumer.
    // QYT: advance_queue_consumer; this signature lacks old_offset CAS and transaction
    // context, so it cannot expose atomic commit with application table updates.
    // Kafka: ordinary commits need suitable group/session ownership; administrative
    // alterConsumerGroupOffsets requires an empty group. Arbitrary live-group rewind
    // is not portable. Success follows backend acknowledgement of the stored
    // position. Value.NextOffset is that exclusive position; it is not a record
    // offset. Failures return a non-success status (transport failures may throw).
    // This operation can rewind where supported; it is not a session acknowledgement.
    virtual NThreading::TFuture<TMessageStreamResult<TMessageStreamConsumerPosition>> CommitPosition(
        TMessageStreamPartitionId partitionId, const TString& consumer, ui64 nextOffset) = 0;
};

// All backends: data and metadata refer to the same bound stream. Combines their access, but cannot provision queues/topics or consumers.
// YDB: topic data/control-plane APIs. QYT: queue pulls plus table/queue metadata.
// Kafka: consumer plus Admin/metadata APIs; no requirement to share one native
// client object between them. See backend documentation linked in message_stream_defs.h.
class IMessageStreamClient : public IMessageStreamDataClient {
public:
    // YDB: DescribeTopic. QYT: resolve queue path/cluster.
    // Kafka: describeTopics plus optional group discovery;
    // Consumers is unset when an authoritative registry cannot be obtained.
    virtual NThreading::TFuture<TMessageStreamResult<TMessageStreamDescription>> DescribeStream() = 0;
    // YDB: DescribeConsumer with statistics and generation (via SDK IncludeLocation).
    // QYT: combine consumer rows with queue bounds. Kafka: listConsumerGroupOffsets
    // plus topic/boundary metadata. Missing consumer: NotFound. An existing
    // consumer with no saved progress has nullopt CommittedOffset; do not turn
    // missing progress into NotFound or a stored zero.
    virtual NThreading::TFuture<TMessageStreamResult<TMessageStreamConsumerDescription>> DescribeConsumer(
        const TString& consumer, const TMessageStreamDescribeConsumerSettings& settings = {}) = 0;
    // All backends: fetch retained bounds under the same visibility policy as reading.
    // YDB: DescribePartition with IncludeStats.
    // QYT Queue Agent attributes are introspection snapshots, not a high-load API.
    // Kafka needs an isolation policy configured outside this signature.
    virtual NThreading::TFuture<TMessageStreamResult<TMessageStreamPartitionDescription>> DescribePartition(
        TMessageStreamPartitionId partitionId) = 0;
};

} // namespace NFq
