#pragma once

#include <istream>
#include <optional>
#include <ostream>

#include <ydb/core/raw_socket/sock_impl.h>
#include <ydb/library/yql/public/decimal/yql_wide_int.h>

#include <util/generic/buffer.h>
#include <util/generic/strbuf.h>
#include <util/system/types.h>

namespace NKafka {

/*
 * There are four versions of each field:
 * - present version   - field serialized and deserialized for this version of protocol.
 * - nullable version - field can be null for this versions of protocol. Default field isn't nullable.
 * - flexible version - field write as map item tag->value (or tagged version)
 * - message flexaible version - version of message support tags
 *
 * Fields may be of type:
 * - bool    - fixed length=1
 * - int8    - fixed length=1
 * - int16   - fixed length=2
 * - uint16  - fixed length=2
 * - int32   - fixed length=4
 * - uint32  - fixed length=4
 * - int64   - fixed length=8
 * - uuid    - fixed length=16
 * - float64 - fixed length=8
 * - string  - can be nullable
 * - bytes   - can be nullable
 * - records - can be nullable
 * - struct
 * - array   - can be nullable
 */

class TKafkaRecordBatch;

using TKafkaBool = ui8;
using TKafkaInt8 = i8;
using TKafkaInt16 = i16;
using TKafkaUint16 = ui16;
using TKafkaInt32 = i32;
using TKafkaUint32 = ui32;
using TKafkaInt64 = i64;
using TKafkaUuid = NYql::TWide<ui64>;
using TKafkaFloat64 = double;
using TKafkaRawString = TString;
using TKafkaString = std::optional<TKafkaRawString>;
using TKafkaRawBytes = TArrayRef<const char>;
using TKafkaBytes = std::optional<TKafkaRawBytes>;
using TKafkaRecords = std::optional<TKafkaRecordBatch>;

using TKafkaVersion = i16;

class TKafkaVersions {
public:
    constexpr TKafkaVersions(TKafkaVersion min, TKafkaVersion max)
        : Min(min)
        , Max(max) {
    }

    TKafkaVersion Min;
    TKafkaVersion Max;
};

static constexpr TKafkaVersions VersionsNever(0, -1);
static constexpr TKafkaVersions VersionsAlways(0, Max<TKafkaVersion>());

using TWritableBuf = NKikimr::NRawSocket::TBufferedWriter;

namespace NPrivate {

struct TKafkaBoolDesc {
    static constexpr bool Default = true;
    static constexpr bool Nullable = false;
    static constexpr bool FixedLength = true;
};

struct TKafkaIntDesc {
    static constexpr bool Default = true;
    static constexpr bool Nullable = false;
    static constexpr bool FixedLength = true;
};

struct TKafkaVarintDesc {
    static constexpr bool Default = true;
    static constexpr bool Nullable = false;
    static constexpr bool FixedLength = false;
};

struct TKafkaUnsignedVarintDesc {
    static constexpr bool Default = true;
    static constexpr bool Nullable = true;
    static constexpr bool FixedLength = false;

    inline static bool IsNull(const TKafkaInt64 value) { return -1 == value; };
};

struct TKafkaUuidDesc {
    static constexpr bool Default = true;
    static constexpr bool Nullable = false;
    static constexpr bool FixedLength = true;
};

struct TKafkaFloat64Desc {
    static constexpr bool Default = true;
    static constexpr bool Nullable = false;
    static constexpr bool FixedLength = true;
};

struct TKafkaStringDesc {
    static constexpr bool Default = true;
    static constexpr bool Nullable = true;
    static constexpr bool FixedLength = false;

    inline static bool IsNull(const TKafkaString& value) { return !value; };
};

struct TKafkaStructDesc {
    static constexpr bool Default = false;
    static constexpr bool Nullable = false;
    static constexpr bool FixedLength = false;
};

struct TKafkaBytesDesc {
    static constexpr bool Default = false;
    static constexpr bool Nullable = true;
    static constexpr bool FixedLength = false;

    inline static bool IsNull(const TKafkaBytes& value) { return !value; };
};

struct TKafkaRecordsDesc {
    static constexpr bool Default = false;
    static constexpr bool Nullable = false;
    static constexpr bool FixedLength = false;
};

struct TKafkaArrayDesc {
    static constexpr bool Default = false;
    static constexpr bool Nullable = true;
    static constexpr bool FixedLength = false;

    template<typename T>
    inline static bool IsNull(const std::vector<T>& value) { return value.empty(); };
};

enum ESizeFormat {
    Default = 0,
    Varint = 1
};

} // namespace NPrivate


// see https://kafka.apache.org/11/protocol.html#protocol_error_codes
enum EKafkaErrors {
    
    UNKNOWN_SERVER_ERROR = -1,             // The server experienced an unexpected error when processing the request.
    NONE_ERROR = 0,
    OFFSET_OUT_OF_RANGE,                   // The requested offset is not within the range of offsets maintained by the server.,
    CORRUPT_MESSAGE,                       // This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.
    UNKNOWN_TOPIC_OR_PARTITION,            // This server does not host this topic-partition.
    INVALID_FETCH_SIZE,                    // The requested fetch size is invalid.
    LEADER_NOT_AVAILABLE,                  // There is no leader for this topic-partition as we are in the middle of a leadership election.
    NOT_LEADER_OR_FOLLOWER,                // For requests intended only for the leader, this error indicates that the broker is not the current leader. 
                                           // For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.
    REQUEST_TIMED_OUT,                     // The request timed out.
    BROKER_NOT_AVAILABLE,                  // The broker is not available.
    REPLICA_NOT_AVAILABLE,                 // The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests
                                           // intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.
    MESSAGE_TOO_LARGE,                     // The request included a message larger than the max message size the server will accept.
    STALE_CONTROLLER_EPOCH,                // The controller moved to another broker.
    OFFSET_METADATA_TOO_LARGE,             // The metadata field of the offset request was too large.
    NETWORK_EXCEPTION,                     // The server disconnected before a response was received.
    COORDINATOR_LOAD_IN_PROGRESS,          // The coordinator is loading and hence can't process requests.
    COORDINATOR_NOT_AVAILABLE,             // The coordinator is not available.
    NOT_COORDINATOR,                       // This is not the correct coordinator.
    INVALID_TOPIC_EXCEPTION,               // The request attempted to perform an operation on an invalid topic.
    RECORD_LIST_TOO_LARGE,                 // The request included message batch larger than the configured segment size on the server.
    NOT_ENOUGH_REPLICAS,                   // Messages are rejected since there are fewer in-sync replicas than required.
    NOT_ENOUGH_REPLICAS_AFTER_APPEND,      // Messages are written to the log, but to fewer in-sync replicas than required.
    INVALID_REQUIRED_ACKS,                 // Produce request specified an invalid value for required acks.
    ILLEGAL_GENERATION,                    // Specified group generation id is not valid.
    INCONSISTENT_GROUP_PROTOCOL,           // The group member's supported protocols are incompatible with those of existing members
                                           // or first group member tried to join with empty protocol type or empty protocol list.
    INVALID_GROUP_ID,                      // The configured groupId is invalid.
    UNKNOWN_MEMBER_ID,                     // The coordinator is not aware of this member.
    INVALID_SESSION_TIMEOUT,               // The session timeout is not within the range allowed by the broker
                                           // (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).
    REBALANCE_IN_PROGRESS,                 // The group is rebalancing, so a rejoin is needed.
    INVALID_COMMIT_OFFSET_SIZE,            // The committing offset data size is not valid.
    TOPIC_AUTHORIZATION_FAILED,            // Topic authorization failed.
    GROUP_AUTHORIZATION_FAILED,            // Group authorization failed.
    CLUSTER_AUTHORIZATION_FAILED,          // Cluster authorization failed.
    INVALID_TIMESTAMP,                     // The timestamp of the message is out of acceptable range.
    UNSUPPORTED_SASL_MECHANISM,            // The broker does not support the requested SASL mechanism.
    ILLEGAL_SASL_STATE,                    // Request is not valid given the current SASL state.
    UNSUPPORTED_VERSION,                   // The version of API is not supported.
    TOPIC_ALREADY_EXISTS,                  // Topic with this name already exists.
    INVALID_PARTITIONS,                    // Number of partitions is below 1.
    INVALID_REPLICATION_FACTOR,            // Replication factor is below 1 or larger than the number of available brokers.
    INVALID_REPLICA_ASSIGNMENT,            // Replica assignment is invalid.
    INVALID_CONFIG,                        // Configuration is invalid.
    NOT_CONTROLLER,                        // This is not the correct controller for this cluster.
    INVALID_REQUEST,                       // This most likely occurs because of a request being malformed by the
                                           // client library or the message was sent to an incompatible broker. See the broker logs
                                           // for more details.
    UNSUPPORTED_FOR_MESSAGE_FORMAT,        // The message format version on the broker does not support the request.
    POLICY_VIOLATION,                      // Request parameters do not satisfy the configured policy.
    OUT_OF_ORDER_SEQUENCE_NUMBER,          // The broker received an out of order sequence number.
    DUPLICATE_SEQUENCE_NUMBER,             // The broker received a duplicate sequence number.
    INVALID_PRODUCER_EPOCH,                // Producer attempted to produce with an old epoch.
    INVALID_TXN_STATE,                     // The producer attempted a transactional operation in an invalid state.
    INVALID_PRODUCER_ID_MAPPING,           // The producer attempted to use a producer id which is not currently assigned to
                                           // its transactional id.
    INVALID_TRANSACTION_TIMEOUT,           // The transaction timeout is larger than the maximum value allowed by
                                           // the broker (as configured by transaction.max.timeout.ms).
    CONCURRENT_TRANSACTIONS,               // The producer attempted to update a transaction
                                           // while another concurrent operation on the same transaction was ongoing.
    TRANSACTION_COORDINATOR_FENCED,        // Indicates that the transaction coordinator sending a WriteTxnMarker
                                           // is no longer the current coordinator for a given producer.
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED, // Transactional Id authorization failed.
    SECURITY_DISABLED,                     // Security features are disabled.
    OPERATION_NOT_ATTEMPTED,               // The broker did not attempt to execute this operation. This may happen for
                                           // batched RPCs where some operations in the batch failed, causing the broker to respond without
                                           // trying the rest.
    KAFKA_STORAGE_ERROR,                   // Disk error when trying to access log file on the disk.
    LOG_DIR_NOT_FOUND,                     // The user-specified log directory is not found in the broker config.
    SASL_AUTHENTICATION_FAILED,            // SASL Authentication failed.
    UNKNOWN_PRODUCER_ID,                   // This exception is raised by the broker if it could not locate the producer metadata
                                           // associated with the producerId in question. This could happen if, for instance, the producer's records
                                           // were deleted because their retention time had elapsed. Once the last records of the producerId are
                                           // removed, the producer's metadata is removed from the broker, and future appends by the producer will
                                           // return this exception.
    REASSIGNMENT_IN_PROGRESS,              // A partition reassignment is in progress.
    DELEGATION_TOKEN_AUTH_DISABLED,        // Delegation Token feature is not enabled.
    DELEGATION_TOKEN_NOT_FOUND,            // Delegation Token is not found on server.
    DELEGATION_TOKEN_OWNER_MISMATCH,       // Specified Principal is not valid Owner/Renewer.
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,  // Delegation Token requests are not allowed on PLAINTEXT/1-way SSL
                                           // channels and on delegation token authenticated channels.
    DELEGATION_TOKEN_AUTHORIZATION_FAILED, // Delegation Token authorization failed.
    DELEGATION_TOKEN_EXPIRED,              // Delegation Token is expired.
    INVALID_PRINCIPAL_TYPE,                // Supplied principalType is not supported.
    NON_EMPTY_GROUP,                       // The group is not empty.
    GROUP_ID_NOT_FOUND,                    // The group id does not exist.
    FETCH_SESSION_ID_NOT_FOUND,            // The fetch session ID was not found.
    INVALID_FETCH_SESSION_EPOCH,           // The fetch session epoch is invalid.
    LISTENER_NOT_FOUND,                    // There is no listener on the leader broker that matches the listener on which
                                           // metadata request was processed.
    TOPIC_DELETION_DISABLED,               // Topic deletion is disabled.
    FENCED_LEADER_EPOCH,                   // The leader epoch in the request is older than the epoch on the broker.
    UNKNOWN_LEADER_EPOCH,                  // The leader epoch in the request is newer than the epoch on the broker.
    UNSUPPORTED_COMPRESSION_TYPE,          // The requesting client does not support the compression type of given partition.
    STALE_BROKER_EPOCH,                    // Broker epoch has changed.
    OFFSET_NOT_AVAILABLE,                  // The leader high watermark has not caught up from a recent leader
                                           // election so the offsets cannot be guaranteed to be monotonically increasing.
    MEMBER_ID_REQUIRED,                    // The group member needs to have a valid member id before actually entering a consumer group.
    PREFERRED_LEADER_NOT_AVAILABLE,        // The preferred leader was not available.
    GROUP_MAX_SIZE_REACHED,                // The consumer group has reached its max size.
    FENCED_INSTANCE_ID,                    // The broker rejected this static consumer since
                                           // another consumer with the same group.instance.id has registered with a different member.id.
    ELIGIBLE_LEADERS_NOT_AVAILABLE,        // Eligible topic partition leaders are not available.
    ELECTION_NOT_NEEDED,                   // Leader election not needed for topic partition.
    NO_REASSIGNMENT_IN_PROGRESS,           // No partition reassignment is in progress.
    GROUP_SUBSCRIBED_TO_TOPIC,             // Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.
    INVALID_RECORD,                        // This record has failed the validation on broker and hence will be rejected.
    UNSTABLE_OFFSET_COMMIT,                // There are unstable offsets that need to be cleared.
    THROTTLING_QUOTA_EXCEEDED,             // The throttling quota has been exceeded.
    PRODUCER_FENCED,                       // There is a newer producer with the same transactionalId
                                           // which fences the current one.
    RESOURCE_NOT_FOUND,                    // A request illegally referred to a resource that does not exist.
    DUPLICATE_RESOURCE,                    // A request illegally referred to the same resource twice.
    UNACCEPTABLE_CREDENTIAL,               // Requested credential would not meet criteria for acceptability.
    INCONSISTENT_VOTER_SET,                // Indicates that the either the sender or recipient of a
                                           // voter-only request is not one of the expected voters
    INVALID_UPDATE_VERSION,                // The given update version was invalid.
    FEATURE_UPDATE_FAILED,                 // Unable to update finalized features due to an unexpected server error.
    PRINCIPAL_DESERIALIZATION_FAILURE,     // Request principal deserialization failed during forwarding.
                                           // This indicates an internal error on the broker cluster security setup.
    SNAPSHOT_NOT_FOUND,                    // Requested snapshot was not found
    POSITION_OUT_OF_RANGE,                 // Requested position is not greater than or equal to zero, and less than the size of the snapshot.
    UNKNOWN_TOPIC_ID,                      // This server does not host this topic ID.
    DUPLICATE_BROKER_REGISTRATION,         // This broker ID is already in use.
    BROKER_ID_NOT_REGISTERED,              // The given broker ID was not registered.
    INCONSISTENT_TOPIC_ID,                 // The log's topic ID did not match the topic ID in the request
    INCONSISTENT_CLUSTER_ID,               // The clusterId in the request does not match that found on the server
    TRANSACTIONAL_ID_NOT_FOUND,            // The transactionalId could not be found
    FETCH_SESSION_TOPIC_ID_ERROR,          // The fetch session encountered inconsistent topic ID usage
    INELIGIBLE_REPLICA,                    // The new ISR contains at least one ineligible replica.
    NEW_LEADER_ELECTED                     // The AlterPartition request successfully updated the partition state but the leader has changed.
};

template <typename T>
void NormalizeNumber(T& value) {
#ifndef WORDS_BIGENDIAN
    char* b = (char*)&value;
    char* e = b + sizeof(T) - 1;
    while (b < e) {
        std::swap(*b, *e);
        ++b;
        --e;
    }
#endif
}

class TKafkaWritable {
public:
    TKafkaWritable(TWritableBuf& buffer)
        : Buffer(buffer){};

    template <typename T>
    TKafkaWritable& operator<<(const T val) {
        NormalizeNumber(val);
        write((const char*)&val, sizeof(T));
        return *this;
    };

    TKafkaWritable& operator<<(const TKafkaUuid& val);
    TKafkaWritable& operator<<(const TKafkaRawBytes& val);
    TKafkaWritable& operator<<(const TKafkaRawString& val);

    void writeUnsignedVarint(TKafkaUint32 val);
    void writeVarint(TKafkaInt32 val);
    void write(const char* val, size_t length);

private:
    TWritableBuf& Buffer;
};

class TKafkaReadable {
public:
    TKafkaReadable(const TBuffer& is)
        : Is(is)
        , Position(0) {
    }

    template <typename T>
    TKafkaReadable& operator>>(T& val) {
        char* v = (char*)&val;
        read(v, sizeof(T));
        NormalizeNumber(val);
        return *this;
    };

    TKafkaReadable& operator>>(TKafkaUuid& val);

    void read(char* val, size_t length);
    char get();
    ui32 readUnsignedVarint();
    i32 readVarint();
    TArrayRef<const char> Bytes(size_t length);

    void skip(size_t length);

private:
    void checkEof(size_t length);

    const TBuffer& Is;
    size_t Position;
};

struct TReadDemand {
    constexpr TReadDemand()
        : Buffer(nullptr)
        , Length(0) {
    }

    constexpr TReadDemand(char* buffer, size_t length)
        : Buffer(buffer)
        , Length(length) {
    }

    constexpr TReadDemand(size_t length)
        : Buffer(nullptr)
        , Length(length) {
    }

    char* GetBuffer() const {
        return Buffer;
    }
    size_t GetLength() const {
        return Length;
    }
    explicit operator bool() const {
        return 0 < Length;
    }
    bool Skip() const {
        return nullptr == Buffer;
    }

    char* Buffer;
    size_t Length;
};

static constexpr TReadDemand NoDemand;

class TMessage {
public:
    virtual ~TMessage() = default;

    virtual i32 Size(TKafkaVersion version) const = 0;
    virtual void Read(TKafkaReadable& readable, TKafkaVersion version) = 0;
    virtual void Write(TKafkaWritable& writable, TKafkaVersion version) const = 0;

    bool operator==(const TMessage& other) const = default;
};

class TApiMessage: public TMessage {
public:
    using TPtr = std::shared_ptr<TApiMessage>;

    ~TApiMessage() = default;

    virtual i16 ApiKey() const = 0;
};

std::unique_ptr<TApiMessage> CreateRequest(i16 apiKey);
std::unique_ptr<TApiMessage> CreateResponse(i16 apiKey);

i16 RequestHeaderVersion(i16 apiKey, TKafkaVersion version);
i16 ResponseHeaderVersion(i16 apiKey, TKafkaVersion version);

} // namespace NKafka
