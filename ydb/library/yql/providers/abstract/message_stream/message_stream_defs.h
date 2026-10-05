#pragma once

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/datetime/base.h>
#include <util/system/types.h>

#include <compare>
#include <cstddef>
#include <limits>
#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>

namespace NFq {

// Backend-neutral message stream: YDB and Logbroker topics, YT queues, Kafka topics.
// A record is one stream entry with its payload, ID and metadata. A read event
// is a session notification: it carries records or reports a lifecycle change.
// Applicability: basic reads and offset storage fit all three backends; consumer
// discovery, timestamps, generations and transactional commits are not equivalent.
// Backend adapters must document unsupported capabilities and consistency guarantees.
// YDB below also denotes the existing Logbroker topic adapter; QYT means YT queues.
// QYT/Kafka mappings describe adapter requirements, not implemented capabilities.
// References for the backend mappings below:
// https://ydb.tech/docs/en/reference/ydb-sdk/topic
// https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/queues
// https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
// https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/admin/Admin.html

// Backend-independent operation outcomes. Adapters map native errors to these
// categories and preserve backend-specific details in Issues.
// YDB: normalized SDK status. QYT: normalized RPC/table/queue error.
// Kafka: normalized client/broker error. There is no one-to-one correspondence;
// BadSession/SessionExpired/SessionBusy may describe adapter state rather than
// a native QYT or Kafka status. Unsupported denotes a missing adapter capability.
enum class EMessageStreamStatus {
    Unsupported,
    Success,
    NotFound,
    Unauthorized,
    InvalidArgument,
    Unavailable,
    InternalError,
    Unknown,
    SchemeError,
    PreconditionFailed,
    Aborted,
    Overloaded,
    Timeout,
    Cancelled,
    Undetermined,
    External,
    BadSession,
    GenericError,
    AlreadyExists,
    SessionExpired,
    SessionBusy,
};

// Synchronous validation/unsupported-operation errors use this exception. Remote
// operation failures use TMessageStreamResult or a session closure event.
// YDB/QYT/Kafka: a common exception at this API boundary, not a native SDK
// exception; GetStatus() returns the normalized category for portable handling.
class TMessageStreamException : public yexception {
public:
    explicit TMessageStreamException(EMessageStreamStatus status)
        : Status(status)
    {}

    EMessageStreamStatus GetStatus() const {
        return Status;
    }

private:
    EMessageStreamStatus Status;
};

// Identifies a partition within one stream; conversion to backend IDs is explicit.
// The backend/cluster and stream identity are supplied separately. Offset identifies
// a record position within this partition. Zero is a valid ID; use std::optional
// for an unspecified partition. Do not assume IDs form [0, PartitionsCount).
//
// YDB / the current PQ adapter: topic partition ID returned by SDK GetPartitionId().
// With auto-partitioning, a split makes the parent inactive and creates children.
// https://ydb.tech/docs/en/concepts/datamodel/topic#partitioning
//
// QYT: partition_index, equal to tablet_index of the ordered dynamic table
// representing the queue. The consumer table stores partition_index as uint64.
// https://ytsaurus.tech/docs/en/user-guide/dynamic-tables/queues
//
// Kafka: partition number in TopicPartition(topic, partition). The Java API uses
// int; adapters must check the range before converting Value to a backend ID.
// https://kafka.apache.org/41/javadoc/org/apache/kafka/common/TopicPartition.html
struct TMessageStreamPartitionId {
    ui64 Value = 0;

    auto operator<=>(const TMessageStreamPartitionId&) const = default;
};

// Identifies one record within a stream by its partition and record offset.
// YDB: message partition/offset. QYT: tablet_index/row_index. Kafka: partition/offset.
// Identity is scoped to the supplied stream and its lifetime; it is not globally
// unique across clusters or stream recreation. Retention/compaction can leave gaps.
struct TMessageStreamRecordId {
    TMessageStreamPartitionId PartitionId;
    // Offset of this record within its partition.
    ui64 Offset = 0;
};

// An exclusive consumer position, distinct from a delivered record's offset.
// YDB: committed consumer offset. QYT: consumer-table offset (next row_index).
// Kafka: committed group offset (next position to consume, not last processed).
// NextOffset can be an end position or point into a gap; it need not name a record.
struct TMessageStreamConsumerPosition {
    TMessageStreamPartitionId PartitionId;
    ui64 NextOffset = 0;
};

// YDB: message metadata entry. QYT: mapped row attribute. Kafka: header.
// Preserve order and duplicate names; absent Value differs from empty bytes.
struct TMessageStreamAttribute {
    TString Name;
    std::optional<TString> Value;
};

// YDB: SDK message. QYT: one queue row encoded by the adapter.
// Kafka: consumer record. Optional metadata is absent when no equivalent exists;
// adapters must not invent producer metadata from reader/session identity.
// https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/ConsumerRecord.html
struct TMessageStreamRecord {
    // nullopt is a null payload (e.g. a Kafka tombstone), distinct from empty bytes.
    // On DecompressionError the payload is unavailable; inspect that error first.
    // Row-based adapters must explicitly configure/document their row encoding.
    std::optional<TString> Data;
    // Kafka: nullable record key, preserving empty versus absent. YDB has no
    // separate key in the read-message API; QYT needs an explicit schema mapping.
    // Do not substitute MessageGroupId or partition ID for a missing key.
    std::optional<TString> Key;
    TMessageStreamRecordId Id;
    // Producer-supplied creation time. Never synthesize it from receive time.
    // YDB: SDK CreateTime. QYT: only an explicitly mapped producer-time column.
    // Kafka: timestamp with CreateTime type; not LogAppendTime.
    std::optional<TInstant> CreateTime;
    // Backend-assigned append/commit time. Never substitute producer or receive
    // time. Kafka CreateTime belongs only in CreateTime; LogAppendTime belongs
    // here. YDB: SDK WriteTime. QYT may supply row commit time when available
    // in the queue schema.
    std::optional<TInstant> WriteTime;
    // YDB: SDK MessageGroupId and SeqNo. QYT producer sessions/sequence numbers
    // are not automatically exposed as row metadata. Kafka ConsumerRecord has
    // no equivalent producer identity/sequence fields. Leave absent unless an
    // explicit record schema/header mapping supplies equivalent values.
    std::optional<TString> MessageGroupId;
    std::optional<ui64> SeqNo;
    // YDB: message metadata fields. QYT: explicitly mapped row attributes.
    // Kafka: headers, including null values, duplicates and original order.
    std::vector<TMessageStreamAttribute> Attributes;
    // YDB: SDK per-message decompression failure. QYT/Kafka clients may report
    // decode failures at batch/request level instead; do not fabricate a record
    // ID to fit such failures here. Other failures use the session error channel.
    std::optional<TString> DecompressionError;
};

// YDB: exponential SDK read-session retry policy. QYT/Kafka: adapter policy;
// native RPC/poll retry settings are not field-for-field equivalents. Define the
// retry scope and avoid multiplying native retries by an independent outer loop.
// Retries alone do not provide ownership fencing or exactly-once processing.
struct TMessageStreamRetrySettings {
    // Initial normal/long-retry delays and backoff ceiling for that policy.
    TDuration MinDelay = TDuration::MilliSeconds(500);
    TDuration MinLongRetryDelay = TDuration::Seconds(5);
    TDuration MaxDelay = TDuration::Seconds(20);
    // Attempt/time budgets and exponential multiplier within the retry scope.
    ui32 MaxRetries = 100;
    TDuration MaxTime = TDuration::Seconds(60);
    double ScaleFactor = 2.0;
    // Opt in to retrying authentication errors; adapters must not reinterpret
    // this as permission to retry every authorization failure indefinitely.
    bool RetryAuthenticationErrors = false;
};

// Fallback when there is no stored consumer position or retention has removed it.
// YDB/QYT/Kafka adapters must implement the chosen policy or reject it before
// delivering partition/data events. An explicit position beyond the current end
// is not a retention reset: preserve it (for bounded scans/future reads), or
// reject it explicitly if the backend cannot support this operation.
enum class EMessageStreamOffsetResetPolicy {
    Error,
    Earliest,
    Latest,
};

// YDB: topic SDK read settings. QYT: queue pulls plus adapter session state.
// Kafka: consumer configuration, assignment and polling. Validation is common;
// accepting the structure does not imply every backend supports every option.
// Parameters for one CreateReadSession call on an already stream-bound client.
// Use a consumer for resumable processing, explicit partitions for manual
// assignment, or no consumer for independent scans/replay. ConfirmStart can
// override the initial offset per assignment. The client owns stream identity;
// these settings neither select another stream nor configure the connection.
struct TMessageStreamReadSessionSettings {
    // nullopt means reading without a consumer; an empty name is invalid.
    // YDB: named topic consumer. QYT: consumer table. Kafka: group.id.
    // Without a consumer there is no named progress to commit through this session.
    std::optional<TString> Consumer;
    // Nonempty: explicitly assigned partitions. Empty: backend-managed assignment,
    // which requires a consumer. There is no second source of partition IDs.
    // YDB: SDK partition selection/assignment. Kafka: assign versus subscribe.
    // QYT pulls select a partition; managed assignment needs an additional
    // coordinator in the adapter or must be rejected as Unsupported.
    std::vector<TMessageStreamPartitionId> PartitionIds;
    // Earliest resumes at the first retained position when progress is missing
    // or expired. Latest selects the visible end instead; Error refuses a reset.
    // A valid saved position wins. ConfirmStart's explicit offset overrides the
    // saved position; reset policy applies if retention has removed that offset.
    // ReadFromWriteTime remains an additional lower bound/filter after selection.
    // The YDB SDK adapter currently supports Earliest only; Error/Latest need
    // additional retained-bound information and are explicitly Unsupported.
    EMessageStreamOffsetResetPolicy OffsetResetPolicy = EMessageStreamOffsetResetPolicy::Earliest;
    // Seek by backend WriteTime, combined with the consumer/ConfirmStart position.
    // Index/batch granularity may return earlier records; the caller applies the
    // final inclusive WriteTime >= ReadFromWriteTime filter. Must not skip matching
    // retained records at/after the requested offset. This never uses producer
    // time. Reject with Unsupported if backend WriteTime seeking is unavailable.
    // YDB: SDK timestamp seek. QYT: needs a suitable timestamp column/index
    // and seek implementation. Kafka offsetsForTimes is suitable only when its
    // timestamp semantics and lookup guarantees satisfy this WriteTime contract.
    std::optional<TInstant> ReadFromWriteTime;
    // Reject creation with Unsupported unless every delivered record can carry
    // its actual backend WriteTime. ReadFromWriteTime implicitly requires this guarantee.
    // YDB exposes WriteTime. QYT requires row commit timestamps. Kafka requires
    // usable LogAppendTime for every returned record; CreateTime is insufficient.
    bool RequireWriteTime = false;
    // YDB: SDK auto-partitioning support flag. QYT tablet changes and Kafka
    // partition additions/rebalances are not YDB split/merge semantics; adapters
    // must document their handling instead of implying native flag equivalence.
    bool AutoPartitioningSupport = true;
    // YDB: SDK memory setting; zero keeps its default. QYT/Kafka: adapter buffer
    // budget, with documented accounting and backend-default behavior for zero;
    // native fetch limits alone do not bound all client/decoded memory.
    ui64 MaxMemoryUsageBytes = 0;
    // YDB: SDK trace ID. QYT/Kafka: diagnostic correlation supplied to the adapter;
    // not a consumer name, Kafka transactional.id or QYT producer session ID.
    TString TraceId;
    // All three: absent selects adapter defaults; see retry scope above.
    std::optional<TMessageStreamRetrySettings> Retry;

    // Every adapter validates before starting a session. A valid request may still
    // be Unsupported by a backend (for example managed assignment in a QYT adapter).
    void Validate() const {
        if ((Consumer && Consumer->empty()) || (!Consumer && PartitionIds.empty())) {
            ythrow TMessageStreamException(EMessageStreamStatus::InvalidArgument)
                << "Consumer name must be nonempty; reading without a consumer requires explicit partitions";
        }
        std::unordered_set<ui64> ids;
        for (const auto id : PartitionIds) {
            if (!ids.insert(id.Value).second) {
                ythrow TMessageStreamException(EMessageStreamStatus::InvalidArgument)
                    << "Duplicate partition ID: " << id.Value;
            }
        }
    }
};

// YDB: limits on SDK event retrieval. QYT/Kafka: limits on adapter output,
// not direct equivalents of pull max_row_count/max_data_weight or max.poll.records.
// An event can contain many records; lifecycle events count toward MaxEventsCount.
struct TMessageStreamGetEventsSettings {
    // Wait for initial readiness only; an empty result is still possible. Actor
    // adapters may reject Block=true with TMessageStreamException(Unsupported).
    bool Block = false;
    // Hard event count limit; zero returns immediately without consuming events.
    std::optional<size_t> MaxEventsCount;
    // Soft byte budget: an indivisible backend batch can exceed it. Accounting
    // includes decoded payload/metadata and can vary between adapters. Zero does
    // not consume events. With a positive budget an oversized first event must
    // be returned (possibly as a single backend batch) to allow progress.
    size_t MaxByteSize = std::numeric_limits<size_t>::max();
};

// Applicable to all three backends; this envelope has no per-partition error channel.
// Adapters must not silently turn partial failures into a complete successful result.
template <class TValue>
struct TMessageStreamResult {
    // All backends: normalize backend errors; the enum cannot preserve every native category.
    EMessageStreamStatus Status = EMessageStreamStatus::Unknown;
    // All backends: retain native error codes/context in diagnostics where normalization loses them.
    NYql::TIssues Issues;
    // All backends: inspect only on success; optional fields can still be unavailable.
    TValue Value{};

    static TMessageStreamResult Success(TValue value, NYql::TIssues issues = {}) {
        return {.Status = EMessageStreamStatus::Success, .Issues = std::move(issues), .Value = std::move(value)};
    }

    static TMessageStreamResult Failure(EMessageStreamStatus status, NYql::TIssues issues = {}) {
        if (status == EMessageStreamStatus::Success) {
            ythrow TMessageStreamException(EMessageStreamStatus::InvalidArgument) << "Failure requires a non-success status";
        }
        return {.Status = status, .Issues = std::move(issues)};
    }

    // All backends: operation success, not a guarantee that all optional metadata is present.
    bool IsSuccess() const {
        return Status == EMessageStreamStatus::Success;
    }
};

// Logical offset owner in all three backends, not an individual reader process.
struct TMessageStreamConsumer {
    // YDB: topic consumer name. QYT: consumer table path, potentially cluster-qualified.
    // Kafka: group.id.
    TString Name;
};

// YDB: topic partition metadata, including sealed parents. QYT: queue tablet.
// Kafka: topic partition. Active describes writability in the stream topology,
// not reader assignment or temporary broker/tablet availability; QYT/Kafka
// adapters must not turn transient unavailability into permanent inactivity.
struct TMessageStreamPartitionInfo {
    TMessageStreamPartitionId PartitionId;
    // False for a sealed partition retained for reading (e.g. a YDB split parent).
    bool Active = true;
};

// YDB: topic description. QYT: queue metadata. Kafka: topic metadata.
struct TMessageStreamDescription {
    // Complete list of readable partitions, including inactive retained ones.
    // Use actual IDs; their values need not form a contiguous range.
    std::vector<TMessageStreamPartitionInfo> Partitions;
    // A present vector is a complete registry; an empty vector means none exist.
    // nullopt means enumeration is unavailable/unsupported/incomplete. Adapters
    // must not return partial lists as complete. YDB: configured topic consumers;
    // QYT: complete queue registrations, not an inventory of all possible readers.
    // Kafka has no authoritative topic-owned registry.
    // Validate a specific consumer with DescribeConsumer, not absence in this list.
    std::optional<std::vector<TMessageStreamConsumer>> Consumers;
};

// Per-stream consumer progress; metadata and offsets need not form an atomic snapshot.
struct TMessageStreamConsumerPartition {
    // YDB: partition ID. QYT: tablet_index. Kafka: nonnegative partition number.
    // Range-check conversions to backend-specific integer types.
    TMessageStreamPartitionId PartitionId;
    // YDB: partition statistics start offset. QYT: lower_row_index after trimming.
    // Kafka: beginning offset; compaction may
    // leave gaps, so this is a lower bound, not necessarily an existing record.
    std::optional<ui64> StartOffset;
    // All backends: next unprocessed position, not the last processed record's offset.
    // Missing progress must remain distinguishable from a stored zero.
    std::optional<ui64> CommittedOffset;
    // Exclusive upper bound: YDB partition statistics end offset; QYT upper_row_index;
    // Kafka high watermark or last stable
    // offset for read_committed. Adapter isolation must match the read session.
    std::optional<ui64> EndOffset;
    // YDB: partition statistics last write time. QYT: last_row_commit_time where available.
    // Kafka: no direct equivalent in topic
    // metadata; record timestamps may be producer times. Leave unset without a
    // documented equivalent; a maximum record timestamp is not the last write time.
    std::optional<TInstant> LastWriteTime;
    // YDB partition generation has no portable equivalent. Neither QYT tablet
    // relocation nor Kafka leader/group epochs guarantee the same restart semantics.
    // Leave unset unless the adapter explicitly defines an equivalent generation.
    std::optional<i64> Generation;
};

// YDB: DescribeConsumer result. QYT: consumer state for one queue.
// Kafka: group offsets filtered to one topic.
struct TMessageStreamConsumerDescription {
    // All backends: distinguish missing commits from missing partitions. Kafka group
    // assignments alone do not enumerate the stream. Completeness requires an adapter policy.
    std::vector<TMessageStreamConsumerPartition> Partitions;
};

// Retained/readable bounds for one partition in all three backends, independent of commits.
struct TMessageStreamPartitionDescription {
    // YDB: partition ID. QYT: tablet_index. Kafka: nonnegative partition number.
    // Range-check conversions to backend-specific integer types.
    TMessageStreamPartitionId PartitionId;
    // Unset when the describe response did not include partition statistics.
    // Zero is a valid offset; it does not by itself imply an empty partition.
    // All backends: same StartOffset/EndOffset semantics as TMessageStreamConsumerPartition;
    // missing bounds are not evidence of an empty partition.
    std::optional<ui64> StartOffset;
    std::optional<ui64> EndOffset;
};

// Metadata requests, not a portable capability guarantee across backends.
struct TMessageStreamDescribeConsumerSettings {
    // YDB: DescribeConsumer IncludeStats. QYT: may require separate reads.
    // Kafka: combine group offsets and partition bounds.
    // Unsupported optional statistics remain unset; snapshots may differ in time.
    bool IncludeStats = false;
    // Request Generation when available. YDB obtains it through IncludeLocation;
    // placement itself is not returned. QYT/Kafka adapters leave Generation unset
    // unless they define an equivalent (see TMessageStreamConsumerPartition).
    bool IncludeGeneration = false;
};

} // namespace NFq
