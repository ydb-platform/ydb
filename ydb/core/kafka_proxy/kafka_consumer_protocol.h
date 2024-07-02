#pragma once

#include <ydb/core/protos/grpc_pq_old.pb.h>

#include "kafka.h"

namespace NKafka {

class TConsumerProtocolSubscription : public TMessage {
public:
    typedef std::shared_ptr<TConsumerProtocolSubscription> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    
    TConsumerProtocolSubscription();
    ~TConsumerProtocolSubscription() = default;
    
    struct TopicPartition : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {1, 3};
            static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
        };
        
        TopicPartition();
        ~TopicPartition() = default;
        
        struct TopicMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "topic";
            static constexpr const char* About = "";
            static const Type Default; // = {""};

            static constexpr TKafkaVersions PresentVersions = {1, 3};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
        };
        TopicMeta::Type Topic;
        
        struct PartitionsMeta {
            using ItemType = TKafkaInt32;
            using ItemTypeDesc = NPrivate::TKafkaIntDesc;
            using Type = std::vector<TKafkaInt32>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitions";
            static constexpr const char* About = "";

            static constexpr TKafkaVersions PresentVersions = {1, 3};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
        };
        PartitionsMeta::Type Partitions;

        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TopicPartition& other) const = default;
    };
    
    struct TopicsMeta {
        using ItemType = TKafkaString;
        using ItemTypeDesc = NPrivate::TKafkaStringDesc;
        using Type = std::vector<TKafkaString>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    TopicsMeta::Type Topics;
    
    struct UserDataMeta {
        using Type = TKafkaBytes;
        using TypeDesc = NPrivate::TKafkaBytesDesc;
        
        static constexpr const char* Name = "userData";
        static constexpr const char* About = "";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    UserDataMeta::Type UserData;

    struct OwnedPartitionsMeta {
        using ItemType = TopicPartition;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TopicPartition>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "ownedPartitions";
        static constexpr const char* About = "";
        static const Type Default; // = {};

        static constexpr TKafkaVersions PresentVersions = {1, 3};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    OwnedPartitionsMeta::Type OwnedPartitions;

    struct GenerationIdMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "generationId";
        static constexpr const char* About = "";
        static const Type Default; // = -1;
        
        static constexpr TKafkaVersions PresentVersions = {2, 3};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    GenerationIdMeta::Type GenerationId;

    struct RackIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "rackId";
        static constexpr const char* About = "";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = {3, 3};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = {3, 3};
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    RackIdMeta::Type RackId;
    
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TConsumerProtocolSubscription& other) const = default;
};


class TConsumerProtocolAssignment : public TMessage {
public:
    typedef std::shared_ptr<TConsumerProtocolAssignment> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    
    TConsumerProtocolAssignment();
    ~TConsumerProtocolAssignment() = default;
    
    struct TopicPartition : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 3};
            static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
        };
        
        TopicPartition();
        ~TopicPartition() = default;
        
        struct TopicMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "topic";
            static constexpr const char* About = "";
            static const Type Default; // = {""};

            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
        };
        TopicMeta::Type Topic;
        
        struct PartitionsMeta {
            using ItemType = TKafkaInt32;
            using ItemTypeDesc = NPrivate::TKafkaIntDesc;
            using Type = std::vector<TKafkaInt32>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitions";
            static constexpr const char* About = "";

            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
        };
        PartitionsMeta::Type Partitions;

        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TopicPartition& other) const = default;
    };
    
    struct AssignedPartitionsMeta {
        using ItemType = TopicPartition;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TopicPartition>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "assignedPartitions";
        static constexpr const char* About = "";

        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    AssignedPartitionsMeta::Type AssignedPartitions;
    
    struct UserDataMeta {
        using Type = TKafkaBytes;
        using TypeDesc = NPrivate::TKafkaBytesDesc;
        
        static constexpr const char* Name = "userData";
        static constexpr const char* About = "";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    UserDataMeta::Type UserData;
    
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TConsumerProtocolAssignment& other) const = default;
};

} // namespace NKafka
