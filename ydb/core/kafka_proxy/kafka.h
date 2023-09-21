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
    
    UNKNOWN_SERVER_ERROR                   = -1, // The server experienced an unexpected error when processing the request.
    NONE_ERROR                             =  0,
    OFFSET_OUT_OF_RANGE                    =  1, // The requested offset is not within the range of offsets maintained by the server.,
    CORRUPT_MESSAGE                        =  2, // This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.
    UNKNOWN_TOPIC_OR_PARTITION             =  3, // This server does not host this topic-partition.
    INVALID_FETCH_SIZE                     =  4, // The requested fetch size is invalid.
    LEADER_NOT_AVAILABLE                   =  5, // There is no leader for this topic-partition as we are in the middle of a leadership election.
    NOT_LEADER_OR_FOLLOWER                 =  6, // For requests intended only for the leader, this error indicates that the broker is not the current leader. 
                                                 // For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.
    REQUEST_TIMED_OUT                      =  7, // The request timed out.
    BROKER_NOT_AVAILABLE                   =  8, // The broker is not available.
    REPLICA_NOT_AVAILABLE                  =  9, // The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests
                                                 // intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.
    MESSAGE_TOO_LARGE                      = 10, // The request included a message larger than the max message size the server will accept.
    STALE_CONTROLLER_EPOCH                 = 11, // The controller moved to another broker.
    OFFSET_METADATA_TOO_LARGE              = 12, // The metadata field of the offset request was too large.
    NETWORK_EXCEPTION                      = 13, // The server disconnected before a response was received.
    COORDINATOR_LOAD_IN_PROGRESS           = 14, // The coordinator is loading and hence can't process requests.
    COORDINATOR_NOT_AVAILABLE              = 15, // The coordinator is not available.
    NOT_COORDINATOR                        = 16, // This is not the correct coordinator.
    INVALID_TOPIC_EXCEPTION                = 17, // The request attempted to perform an operation on an invalid topic.
    RECORD_LIST_TOO_LARGE                  = 18, // The request included message batch larger than the configured segment size on the server.
    NOT_ENOUGH_REPLICAS                    = 19, // Messages are rejected since there are fewer in-sync replicas than required.
    NOT_ENOUGH_REPLICAS_AFTER_APPEND       = 20, // Messages are written to the log, but to fewer in-sync replicas than required.
    INVALID_REQUIRED_ACKS                  = 21, // Produce request specified an invalid value for required acks.
    ILLEGAL_GENERATION                     = 22, // Specified group generation id is not valid.
    INCONSISTENT_GROUP_PROTOCOL            = 23, // The group member's supported protocols are incompatible with those of existing members
                                                 // or first group member tried to join with empty protocol type or empty protocol list.
    INVALID_GROUP_ID                       = 24, // The configured groupId is invalid.
    UNKNOWN_MEMBER_ID                      = 25, // The coordinator is not aware of this member.
    INVALID_SESSION_TIMEOUT                = 26, // The session timeout is not within the range allowed by the broker
                                                 // (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).
    REBALANCE_IN_PROGRESS                  = 27, // The group is rebalancing, so a rejoin is needed.
    INVALID_COMMIT_OFFSET_SIZE             = 28, // The committing offset data size is not valid.
    TOPIC_AUTHORIZATION_FAILED             = 29, // Topic authorization failed.
    GROUP_AUTHORIZATION_FAILED             = 30, // Group authorization failed.
    CLUSTER_AUTHORIZATION_FAILED           = 31, // Cluster authorization failed.
    INVALID_TIMESTAMP                      = 32, // The timestamp of the message is out of acceptable range.
    UNSUPPORTED_SASL_MECHANISM             = 33, // The broker does not support the requested SASL mechanism.
    ILLEGAL_SASL_STATE                     = 34, // Request is not valid given the current SASL state.
    UNSUPPORTED_VERSION                    = 35, // The version of API is not supported.
    TOPIC_ALREADY_EXISTS                   = 36, // Topic with this name already exists.
    INVALID_PARTITIONS                     = 37, // Number of partitions is below 1.
    INVALID_REPLICATION_FACTOR             = 38, // Replication factor is below 1 or larger than the number of available brokers.
    INVALID_REPLICA_ASSIGNMENT             = 39, // Replica assignment is invalid.
    INVALID_CONFIG                         = 40, // Configuration is invalid.
    NOT_CONTROLLER                         = 41, // This is not the correct controller for this cluster.
    INVALID_REQUEST                        = 42, // This most likely occurs because of a request being malformed by the
                                                 // client library or the message was sent to an incompatible broker. See the broker logs
                                                 // for more details.
    UNSUPPORTED_FOR_MESSAGE_FORMAT         = 43, // The message format version on the broker does not support the request.
    POLICY_VIOLATION                       = 44, // Request parameters do not satisfy the configured policy.
    OUT_OF_ORDER_SEQUENCE_NUMBER           = 45, // The broker received an out of order sequence number.
    DUPLICATE_SEQUENCE_NUMBER              = 46, // The broker received a duplicate sequence number.
    INVALID_PRODUCER_EPOCH                 = 47, // Producer attempted to produce with an old epoch.
    INVALID_TXN_STATE                      = 48, // The producer attempted a transactional operation in an invalid state.
    INVALID_PRODUCER_ID_MAPPING            = 49, // The producer attempted to use a producer id which is not currently assigned to
                                                 // its transactional id.
    INVALID_TRANSACTION_TIMEOUT            = 50, // The transaction timeout is larger than the maximum value allowed by
                                                 // the broker (as configured by transaction.max.timeout.ms).
    CONCURRENT_TRANSACTIONS                = 51, // The producer attempted to update a transaction
                                                 // while another concurrent operation on the same transaction was ongoing.
    TRANSACTION_COORDINATOR_FENCED         = 52, // Indicates that the transaction coordinator sending a WriteTxnMarker
                                                 // is no longer the current coordinator for a given producer.
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED  = 53, // Transactional Id authorization failed.
    SECURITY_DISABLED                      = 54, // Security features are disabled.
    OPERATION_NOT_ATTEMPTED                = 55, // The broker did not attempt to execute this operation. This may happen for
                                                 // batched RPCs where some operations in the batch failed, causing the broker to respond without
                                                 // trying the rest.
    KAFKA_STORAGE_ERROR                    = 56, // Disk error when trying to access log file on the disk.
    LOG_DIR_NOT_FOUND                      = 57, // The user-specified log directory is not found in the broker config.
    SASL_AUTHENTICATION_FAILED             = 58, // SASL Authentication failed.
    UNKNOWN_PRODUCER_ID                    = 59, // This exception is raised by the broker if it could not locate the producer metadata
                                                 // associated with the producerId in question. This could happen if, for instance, the producer's records
                                                 // were deleted because their retention time had elapsed. Once the last records of the producerId are
                                                 // removed, the producer's metadata is removed from the broker, and future appends by the producer will
                                                 // return this exception.
    REASSIGNMENT_IN_PROGRESS               = 60, // A partition reassignment is in progress.
    DELEGATION_TOKEN_AUTH_DISABLED         = 61, // Delegation Token feature is not enabled.
    DELEGATION_TOKEN_NOT_FOUND             = 62, // Delegation Token is not found on server.
    DELEGATION_TOKEN_OWNER_MISMATCH        = 63, // Specified Principal is not valid Owner/Renewer.
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED   = 64, // Delegation Token requests are not allowed on PLAINTEXT/1-way SSL
                                                 // channels and on delegation token authenticated channels.
    DELEGATION_TOKEN_AUTHORIZATION_FAILED  = 65, // Delegation Token authorization failed.
    DELEGATION_TOKEN_EXPIRED               = 66, // Delegation Token is expired.
    INVALID_PRINCIPAL_TYPE                 = 67, // Supplied principalType is not supported.
    NON_EMPTY_GROUP                        = 68, // The group is not empty.
    GROUP_ID_NOT_FOUND                     = 69, // The group id does not exist.
    FETCH_SESSION_ID_NOT_FOUND             = 70, // The fetch session ID was not found.
    INVALID_FETCH_SESSION_EPOCH            = 71, // The fetch session epoch is invalid.
    LISTENER_NOT_FOUND                     = 72, // There is no listener on the leader broker that matches the listener on which
                                                 // metadata request was processed.
    TOPIC_DELETION_DISABLED                = 73, // Topic deletion is disabled.
    FENCED_LEADER_EPOCH                    = 74, // The leader epoch in the request is older than the epoch on the broker.
    UNKNOWN_LEADER_EPOCH                   = 75, // The leader epoch in the request is newer than the epoch on the broker.
    UNSUPPORTED_COMPRESSION_TYPE           = 76, // The requesting client does not support the compression type of given partition.
    STALE_BROKER_EPOCH                     = 77, // Broker epoch has changed.
    OFFSET_NOT_AVAILABLE                   = 78, // The leader high watermark has not caught up from a recent leader
                                                 // election so the offsets cannot be guaranteed to be monotonically increasing.
    MEMBER_ID_REQUIRED                     = 79, // The group member needs to have a valid member id before actually entering a consumer group.
    PREFERRED_LEADER_NOT_AVAILABLE         = 80, // The preferred leader was not available.
    GROUP_MAX_SIZE_REACHED                 = 81, // The consumer group has reached its max size.
    FENCED_INSTANCE_ID                     = 82, // The broker rejected this static consumer since
                                                 // another consumer with the same group.instance.id has registered with a different member.id.
    ELIGIBLE_LEADERS_NOT_AVAILABLE         = 83, // Eligible topic partition leaders are not available.
    ELECTION_NOT_NEEDED                    = 84, // Leader election not needed for topic partition.
    NO_REASSIGNMENT_IN_PROGRESS            = 85, // No partition reassignment is in progress.
    GROUP_SUBSCRIBED_TO_TOPIC              = 86, // Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.
    INVALID_RECORD                         = 87, // This record has failed the validation on broker and hence will be rejected.
    UNSTABLE_OFFSET_COMMIT                 = 88, // There are unstable offsets that need to be cleared.
    THROTTLING_QUOTA_EXCEEDED              = 89, // The throttling quota has been exceeded.
    PRODUCER_FENCED                        = 90, // There is a newer producer with the same transactionalId
                                                 // which fences the current one.
    RESOURCE_NOT_FOUND                     = 91, // A request illegally referred to a resource that does not exist.
    DUPLICATE_RESOURCE                     = 92, // A request illegally referred to the same resource twice.
    UNACCEPTABLE_CREDENTIAL                = 93, // Requested credential would not meet criteria for acceptability.
    INCONSISTENT_VOTER_SET                 = 94, // Indicates that the either the sender or recipient of a
                                                 // voter-only request is not one of the expected voters
    INVALID_UPDATE_VERSION                 = 95, // The given update version was invalid.
    FEATURE_UPDATE_FAILED                  = 96, // Unable to update finalized features due to an unexpected server error.
    PRINCIPAL_DESERIALIZATION_FAILURE      = 97, // Request principal deserialization failed during forwarding.
                                                 // This indicates an internal error on the broker cluster security setup.
    SNAPSHOT_NOT_FOUND                     = 98, // Requested snapshot was not found
    POSITION_OUT_OF_RANGE                  = 99, // Requested position is not greater than or equal to zero, and less than the size of the snapshot.
    UNKNOWN_TOPIC_ID                       = 100, // This server does not host this topic ID.
    DUPLICATE_BROKER_REGISTRATION          = 101, // This broker ID is already in use.
    BROKER_ID_NOT_REGISTERED               = 102, // The given broker ID was not registered.
    INCONSISTENT_TOPIC_ID                  = 103, // The log's topic ID did not match the topic ID in the request
    INCONSISTENT_CLUSTER_ID                = 104, // The clusterId in the request does not match that found on the server
    TRANSACTIONAL_ID_NOT_FOUND             = 105, // The transactionalId could not be found
    FETCH_SESSION_TOPIC_ID_ERROR           = 106, // The fetch session encountered inconsistent topic ID usage
    INELIGIBLE_REPLICA                     = 107, // The new ISR contains at least one ineligible replica.
    NEW_LEADER_ELECTED                     = 108  // The AlterPartition request successfully updated the partition state but the leader has changed.
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

template<class T, typename S = std::make_signed_t<T>, typename U = std::make_unsigned_t<T>>
U AsUnsigned(S value) {
    static constexpr ui8 Shift = (sizeof(T) << 3) - 1;
    return (value << 1) ^ (value >> Shift);
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

    template<class T, typename U = std::make_unsigned_t<T>>
    void writeUnsignedVarint(T v) {
        static constexpr U Mask = Max<U>() - 0x7F;

        U value = v;
        while ((value & Mask) != 0L) {
            ui8 b = (ui8) ((value & 0x7f) | 0x80);
            write((const char*)&b, sizeof(b));
            value >>= 7;
        }
        ui8 b = (ui8) value;
        write((const char*)&b, sizeof(b));        
    }

    template<std::signed_integral T, typename U = std::make_unsigned_t<T>>
    void writeVarint(T value) {
        writeUnsignedVarint<U>(AsUnsigned<T>(value));
    }

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

    template<std::unsigned_integral U>
    U readUnsignedVarint() {
        static constexpr size_t MaxLength = (sizeof(U) * 8 - 1) / 7 * 7;

        U value = 0;
        size_t i = 0;
        U b;
        while (((b = static_cast<ui8>(get())) & 0x80) != 0) {
            if (i >= MaxLength) {
                ythrow yexception() << "illegal varint length";
            }
            value |= (b & 0x7f) << i;
            i += 7;
        }

        value |= b << i;
        return value;
    }

    template<std::signed_integral S, typename U = std::make_unsigned_t<S>>
    S readVarint() {
        U v = readUnsignedVarint<U>();
        return (v >> 1) ^ -static_cast<S>(v & 1);        
    }

    TArrayRef<const char> Bytes(size_t length);

    // returns a character from the specified position. The current position does not change.
    char take(size_t shift);

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
