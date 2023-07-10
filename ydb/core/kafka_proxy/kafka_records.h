#pragma once

#include "kafka.h"

namespace NKafka {

enum ECompressionType {
    NONE = 0,
    GZIP = 1,
    SNAPPY = 2,
    LZ4 = 3,
    ZSTD = 4
};

enum ETimestampType {
    CREATE_TIME = 0,
    LOG_APPEND_TIME = 1
};

class TKafkaHeader: public TMessage  {
public:
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };

    TKafkaHeader();
    ~TKafkaHeader() = default;

    struct KeyMeta {
        using Type = TKafkaBytes;
        using TypeDesc = NPrivate::TKafkaBytesDesc;
            
        static constexpr const char* Name = "key";
        static constexpr const char* About = "";
        static const Type Default; // = {""};
        static constexpr NPrivate::ESizeFormat SizeFormat = NPrivate::ESizeFormat::Varint;

        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    KeyMeta::Type Key;

    struct ValueMeta {
        using Type = TKafkaBytes;
        using TypeDesc = NPrivate::TKafkaBytesDesc;
            
        static constexpr const char* Name = "value";
        static constexpr const char* About = "";
        static constexpr NPrivate::ESizeFormat SizeFormat = NPrivate::ESizeFormat::Varint;

        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    ValueMeta::Type Value;

    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
    bool operator==(const TKafkaHeader& other) const = default;
};



class TKafkaRecord: public TMessage {
public:
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };

    TKafkaRecord();
    ~TKafkaRecord() = default;

    struct LengthMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaVarintDesc;
        
        static constexpr const char* Name = "length";
        static constexpr const char* About = "";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    LengthMeta::Type Length;

    struct AttributesMeta {
        using Type = TKafkaInt8;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "attributes";
        static constexpr const char* About = "";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    AttributesMeta::Type Attributes;

    struct TimestampDeltaMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaVarintDesc;
        
        static constexpr const char* Name = "timestampDelta";
        static constexpr const char* About = "";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    TimestampDeltaMeta::Type TimestampDelta;

    struct OffsetDeltaMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaVarintDesc;
        
        static constexpr const char* Name = "offsetDelta";
        static constexpr const char* About = "";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    OffsetDeltaMeta::Type OffsetDelta;

    struct KeyMeta {
        using Type = TKafkaBytes;
        using TypeDesc = NPrivate::TKafkaBytesDesc;
            
        static constexpr const char* Name = "key";
        static constexpr const char* About = "";
        static const Type Default; // = {""};
        static constexpr NPrivate::ESizeFormat SizeFormat = NPrivate::ESizeFormat::Varint;
            
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    KeyMeta::Type Key;

    struct ValueMeta {
        using Type = TKafkaBytes;
        using TypeDesc = NPrivate::TKafkaBytesDesc;

        static constexpr const char* Name = "value";
        static constexpr const char* About = "";
        static constexpr NPrivate::ESizeFormat SizeFormat = NPrivate::ESizeFormat::Varint;

        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    ValueMeta::Type Value;

    struct HeadersMeta {
        using ItemType = TKafkaHeader;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TKafkaHeader>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;

        static constexpr const char* Name = "headers";
        static constexpr const char* About = "";
        static constexpr NPrivate::ESizeFormat SizeFormat = NPrivate::ESizeFormat::Varint;

        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    HeadersMeta::Type Headers;

    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
    bool operator==(const TKafkaRecord& other) const = default;
};



class TKafkaRecordBatch: public TMessage {
public:
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };

    TKafkaRecordBatch();
    ~TKafkaRecordBatch() = default;

    struct BaseOffsetMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "baseOffset";
        static constexpr const char* About = "";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    BaseOffsetMeta::Type BaseOffset;

    struct BatchLengthMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "batchLength";
        static constexpr const char* About = "";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    BatchLengthMeta::Type BatchLength;

    struct PartitionLeaderEpochMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "partitionLeaderEpoch";
        static constexpr const char* About = "";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    PartitionLeaderEpochMeta::Type PartitionLeaderEpoch;

    struct MagicMeta {
        using Type = TKafkaInt8;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "magic";
        static constexpr const char* About = "current magic value is 2";
        static constexpr Type Default = 2;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    MagicMeta::Type Magic;

    struct CrcMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "crc";
        static constexpr const char* About = "The CRC covers the data from the attributes to the end of the batch (i.e. all the bytes that follow the CRC)";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    CrcMeta::Type Crc;

    struct AttributesMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "attributes";
        static constexpr const char* About = "";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    AttributesMeta::Type Attributes;

    struct LastOffsetDeltaMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "lastOffsetDelta";
        static constexpr const char* About = "";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    LastOffsetDeltaMeta::Type LastOffsetDelta;

    struct BaseTimestampMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "baseTimestamp";
        static constexpr const char* About = "";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    BaseTimestampMeta::Type BaseTimestamp;

    struct MaxTimestampMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "maxTimestamp";
        static constexpr const char* About = "";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    MaxTimestampMeta::Type MaxTimestamp;

    struct ProducerIdMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "producerId";
        static constexpr const char* About = "";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    ProducerIdMeta::Type ProducerId;

    struct ProducerEpochMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "producerEpoch";
        static constexpr const char* About = "";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    ProducerEpochMeta::Type ProducerEpoch;

    struct BaseSequenceMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "baseSequence";
        static constexpr const char* About = "";
        static constexpr Type Default = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    BaseSequenceMeta::Type BaseSequence;

    struct RecordsMeta {
        using ItemType = TKafkaRecord;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TKafkaRecord>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "records";
        static constexpr const char* About = "";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    RecordsMeta::Type Records;

    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
    bool operator==(const TKafkaRecordBatch& other) const = default;

    ECompressionType CompressionType();
    ETimestampType TimestampType();
    bool Transactional();
    bool ControlBatch();
    bool HasDeleteHorizonMs();
};

} // namespace NKafka
