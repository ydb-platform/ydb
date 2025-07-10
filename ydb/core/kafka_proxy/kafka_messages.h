
// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.
// For generate it use kikimr/tools/kafka/generate.sh

#pragma once

#include "kafka_messages_int.h"

namespace NKafka {

enum EListenerType {
    ZK_BROKER,
    BROKER,
    CONTROLLER,
};

enum EApiKey {
    HEADER = -1, // [] 
    PRODUCE = 0, // [ZK_BROKER, BROKER] 
    FETCH = 1, // [ZK_BROKER, BROKER, CONTROLLER] 
    LIST_OFFSETS = 2, // [ZK_BROKER, BROKER] 
    METADATA = 3, // [ZK_BROKER, BROKER] 
    OFFSET_COMMIT = 8, // [ZK_BROKER, BROKER] 
    OFFSET_FETCH = 9, // [ZK_BROKER, BROKER] 
    FIND_COORDINATOR = 10, // [ZK_BROKER, BROKER] 
    JOIN_GROUP = 11, // [ZK_BROKER, BROKER] 
    HEARTBEAT = 12, // [ZK_BROKER, BROKER] 
    LEAVE_GROUP = 13, // [ZK_BROKER, BROKER] 
    SYNC_GROUP = 14, // [ZK_BROKER, BROKER] 
    DESCRIBE_GROUPS = 15, // [ZK_BROKER, BROKER] 
    LIST_GROUPS = 16, // [ZK_BROKER, BROKER] 
    SASL_HANDSHAKE = 17, // [ZK_BROKER, BROKER, CONTROLLER] 
    API_VERSIONS = 18, // [ZK_BROKER, BROKER, CONTROLLER] 
    CREATE_TOPICS = 19, // [ZK_BROKER, BROKER, CONTROLLER] 
    INIT_PRODUCER_ID = 22, // [ZK_BROKER, BROKER] 
    ADD_PARTITIONS_TO_TXN = 24, // [ZK_BROKER, BROKER] 
    ADD_OFFSETS_TO_TXN = 25, // [ZK_BROKER, BROKER] 
    END_TXN = 26, // [ZK_BROKER, BROKER] 
    TXN_OFFSET_COMMIT = 28, // [ZK_BROKER, BROKER] 
    DESCRIBE_CONFIGS = 32, // [ZK_BROKER, BROKER] 
    ALTER_CONFIGS = 33, // [ZK_BROKER, BROKER, CONTROLLER] 
    SASL_AUTHENTICATE = 36, // [ZK_BROKER, BROKER, CONTROLLER] 
    CREATE_PARTITIONS = 37, // [ZK_BROKER, BROKER, CONTROLLER] 
};

extern const std::unordered_map<EApiKey, TString> EApiKeyNames;




class TRequestHeaderData : public TApiMessage {
public:
    typedef std::shared_ptr<TRequestHeaderData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 2};
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    
    TRequestHeaderData();
    ~TRequestHeaderData() = default;
    
    struct RequestApiKeyMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "requestApiKey";
        static constexpr const char* About = "The API key of this request.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    RequestApiKeyMeta::Type RequestApiKey;
    
    struct RequestApiVersionMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "requestApiVersion";
        static constexpr const char* About = "The API version of this request.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    RequestApiVersionMeta::Type RequestApiVersion;
    
    struct CorrelationIdMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "correlationId";
        static constexpr const char* About = "The correlation ID of this request.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    CorrelationIdMeta::Type CorrelationId;
    
    struct ClientIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "clientId";
        static constexpr const char* About = "The client ID string.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    ClientIdMeta::Type ClientId;
    
    i16 ApiKey() const override { return HEADER; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TRequestHeaderData& other) const = default;
};


class TResponseHeaderData : public TApiMessage {
public:
    typedef std::shared_ptr<TResponseHeaderData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 1};
        static constexpr TKafkaVersions FlexibleVersions = {1, Max<TKafkaVersion>()};
    };
    
    TResponseHeaderData();
    ~TResponseHeaderData() = default;
    
    struct CorrelationIdMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "correlationId";
        static constexpr const char* About = "The correlation ID of this response.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {1, Max<TKafkaVersion>()};
    };
    CorrelationIdMeta::Type CorrelationId;
    
    i16 ApiKey() const override { return HEADER; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TResponseHeaderData& other) const = default;
};


class TProduceRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TProduceRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 9};
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    
    TProduceRequestData();
    ~TProduceRequestData() = default;
    
    class TTopicProduceData : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 9};
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        
        TTopicProduceData();
        ~TTopicProduceData() = default;
        
        class TPartitionProduceData : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 9};
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            
            TPartitionProduceData();
            ~TPartitionProduceData() = default;
            
            struct IndexMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "index";
                static constexpr const char* About = "The partition index.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            IndexMeta::Type Index;
            
            struct RecordsMeta {
                using Type = TKafkaRecords;
                using TypeDesc = NPrivate::TKafkaRecordsDesc;
                
                static constexpr const char* Name = "records";
                static constexpr const char* About = "The record data to be produced.";
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            RecordsMeta::Type Records;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TPartitionProduceData& other) const = default;
        };
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct PartitionDataMeta {
            using ItemType = TPartitionProduceData;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TPartitionProduceData>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitionData";
            static constexpr const char* About = "Each partition to produce to.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        PartitionDataMeta::Type PartitionData;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TTopicProduceData& other) const = default;
    };
    
    struct TransactionalIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "transactionalId";
        static constexpr const char* About = "The transactional ID, or null if the producer is not transactional.";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    TransactionalIdMeta::Type TransactionalId;
    
    struct AcksMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "acks";
        static constexpr const char* About = "The number of acknowledgments the producer requires the leader to have received before considering a request complete. Allowed values: 0 for no acknowledgments, 1 for only the leader and -1 for the full ISR.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    AcksMeta::Type Acks;
    
    struct TimeoutMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "timeoutMs";
        static constexpr const char* About = "The timeout to await a response in milliseconds.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    TimeoutMsMeta::Type TimeoutMs;
    
    struct TopicDataMeta {
        using ItemType = TTopicProduceData;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TTopicProduceData>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topicData";
        static constexpr const char* About = "Each topic to produce to.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    TopicDataMeta::Type TopicData;
    
    i16 ApiKey() const override { return PRODUCE; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TProduceRequestData& other) const = default;
};


class TProduceResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TProduceResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 9};
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    
    TProduceResponseData();
    ~TProduceResponseData() = default;
    
    class TTopicProduceResponse : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 9};
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        
        TTopicProduceResponse();
        ~TTopicProduceResponse() = default;
        
        class TPartitionProduceResponse : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 9};
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            
            TPartitionProduceResponse();
            ~TPartitionProduceResponse() = default;
            
            class TBatchIndexAndErrorMessage : public TMessage {
            public:
                struct MessageMeta {
                    static constexpr TKafkaVersions PresentVersions = {8, 9};
                    static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
                };
                
                TBatchIndexAndErrorMessage();
                ~TBatchIndexAndErrorMessage() = default;
                
                struct BatchIndexMeta {
                    using Type = TKafkaInt32;
                    using TypeDesc = NPrivate::TKafkaIntDesc;
                    
                    static constexpr const char* Name = "batchIndex";
                    static constexpr const char* About = "The batch index of the record that cause the batch to be dropped";
                    static const Type Default; // = 0;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
                };
                BatchIndexMeta::Type BatchIndex;
                
                struct BatchIndexErrorMessageMeta {
                    using Type = TKafkaString;
                    using TypeDesc = NPrivate::TKafkaStringDesc;
                    
                    static constexpr const char* Name = "batchIndexErrorMessage";
                    static constexpr const char* About = "The error message of the record that caused the batch to be dropped";
                    static const Type Default; // = std::nullopt;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                    static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
                };
                BatchIndexErrorMessageMeta::Type BatchIndexErrorMessage;
                
                i32 Size(TKafkaVersion version) const override;
                void Read(TKafkaReadable& readable, TKafkaVersion version) override;
                void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
                
                bool operator==(const TBatchIndexAndErrorMessage& other) const = default;
            };
            
            struct IndexMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "index";
                static constexpr const char* About = "The partition index.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            IndexMeta::Type Index;
            
            struct ErrorCodeMeta {
                using Type = TKafkaInt16;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "errorCode";
                static constexpr const char* About = "The error code, or 0 if there was no error.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            ErrorCodeMeta::Type ErrorCode;
            
            struct BaseOffsetMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "baseOffset";
                static constexpr const char* About = "The base offset.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            BaseOffsetMeta::Type BaseOffset;
            
            struct LogAppendTimeMsMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "logAppendTimeMs";
                static constexpr const char* About = "The timestamp returned by broker after appending the messages. If CreateTime is used for the topic, the timestamp will be -1.  If LogAppendTime is used for the topic, the timestamp will be the broker local time when the messages are appended.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {2, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            LogAppendTimeMsMeta::Type LogAppendTimeMs;
            
            struct LogStartOffsetMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "logStartOffset";
                static constexpr const char* About = "The log start offset.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            LogStartOffsetMeta::Type LogStartOffset;
            
            struct RecordErrorsMeta {
                using ItemType = TBatchIndexAndErrorMessage;
                using ItemTypeDesc = NPrivate::TKafkaStructDesc;
                using Type = std::vector<TBatchIndexAndErrorMessage>;
                using TypeDesc = NPrivate::TKafkaArrayDesc;
                
                static constexpr const char* Name = "recordErrors";
                static constexpr const char* About = "The batch indices of records that caused the batch to be dropped";
                
                static constexpr TKafkaVersions PresentVersions = {8, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            RecordErrorsMeta::Type RecordErrors;
            
            struct ErrorMessageMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "errorMessage";
                static constexpr const char* About = "The global error message summarizing the common root cause of the records that caused the batch to be dropped";
                static const Type Default; // = std::nullopt;
                
                static constexpr TKafkaVersions PresentVersions = {8, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            ErrorMessageMeta::Type ErrorMessage;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TPartitionProduceResponse& other) const = default;
        };
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct PartitionResponsesMeta {
            using ItemType = TPartitionProduceResponse;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TPartitionProduceResponse>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitionResponses";
            static constexpr const char* About = "Each partition that we produced to within the topic.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        PartitionResponsesMeta::Type PartitionResponses;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TTopicProduceResponse& other) const = default;
    };
    
    struct ResponsesMeta {
        using ItemType = TTopicProduceResponse;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TTopicProduceResponse>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "responses";
        static constexpr const char* About = "Each produce response";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    ResponsesMeta::Type Responses;
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    i16 ApiKey() const override { return PRODUCE; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TProduceResponseData& other) const = default;
};


class TFetchRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TFetchRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 13};
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    
    TFetchRequestData();
    ~TFetchRequestData() = default;
    
    class TFetchTopic : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 13};
            static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
        };
        
        TFetchTopic();
        ~TFetchTopic() = default;
        
        class TFetchPartition : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 13};
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            
            TFetchPartition();
            ~TFetchPartition() = default;
            
            struct PartitionMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "partition";
                static constexpr const char* About = "The partition index.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            PartitionMeta::Type Partition;
            
            struct CurrentLeaderEpochMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "currentLeaderEpoch";
                static constexpr const char* About = "The current leader epoch of the partition.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {9, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            CurrentLeaderEpochMeta::Type CurrentLeaderEpoch;
            
            struct FetchOffsetMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "fetchOffset";
                static constexpr const char* About = "The message offset.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            FetchOffsetMeta::Type FetchOffset;
            
            struct LastFetchedEpochMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "lastFetchedEpoch";
                static constexpr const char* About = "The epoch of the last fetched record or -1 if there is none";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {12, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            LastFetchedEpochMeta::Type LastFetchedEpoch;
            
            struct LogStartOffsetMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "logStartOffset";
                static constexpr const char* About = "The earliest available offset of the follower replica.  The field is only used when the request is sent by the follower.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            LogStartOffsetMeta::Type LogStartOffset;
            
            struct PartitionMaxBytesMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "partitionMaxBytes";
                static constexpr const char* About = "The maximum bytes to fetch from this partition.  See KIP-74 for cases where this limit may not be honored.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            PartitionMaxBytesMeta::Type PartitionMaxBytes;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TFetchPartition& other) const = default;
        };
        
        struct TopicMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "topic";
            static constexpr const char* About = "The name of the topic to fetch.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = {0, 12};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
        };
        TopicMeta::Type Topic;
        
        struct TopicIdMeta {
            using Type = TKafkaUuid;
            using TypeDesc = NPrivate::TKafkaUuidDesc;
            
            static constexpr const char* Name = "topicId";
            static constexpr const char* About = "The unique topic ID";
            static const Type Default; // = TKafkaUuid(0, 0);
            
            static constexpr TKafkaVersions PresentVersions = {13, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        TopicIdMeta::Type TopicId;
        
        struct PartitionsMeta {
            using ItemType = TFetchPartition;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TFetchPartition>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitions";
            static constexpr const char* About = "The partitions to fetch.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
        };
        PartitionsMeta::Type Partitions;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TFetchTopic& other) const = default;
    };
    
    class TForgottenTopic : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {7, 13};
            static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
        };
        
        TForgottenTopic();
        ~TForgottenTopic() = default;
        
        struct TopicMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "topic";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = {0, 12};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
        };
        TopicMeta::Type Topic;
        
        struct TopicIdMeta {
            using Type = TKafkaUuid;
            using TypeDesc = NPrivate::TKafkaUuidDesc;
            
            static constexpr const char* Name = "topicId";
            static constexpr const char* About = "The unique topic ID";
            static const Type Default; // = TKafkaUuid(0, 0);
            
            static constexpr TKafkaVersions PresentVersions = {13, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        TopicIdMeta::Type TopicId;
        
        struct PartitionsMeta {
            using ItemType = TKafkaInt32;
            using ItemTypeDesc = NPrivate::TKafkaIntDesc;
            using Type = std::vector<TKafkaInt32>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitions";
            static constexpr const char* About = "The partitions indexes to forget.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
        };
        PartitionsMeta::Type Partitions;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TForgottenTopic& other) const = default;
    };
    
    struct ClusterIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "clusterId";
        static constexpr const char* About = "The clusterId if known. This is used to validate metadata fetches prior to broker registration.";
        static constexpr const TKafkaInt32 Tag = 0;
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = {12, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsAlways;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    ClusterIdMeta::Type ClusterId;
    
    struct ReplicaIdMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "replicaId";
        static constexpr const char* About = "The broker ID of the follower, of -1 if this request is from a consumer.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    ReplicaIdMeta::Type ReplicaId;
    
    struct MaxWaitMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "maxWaitMs";
        static constexpr const char* About = "The maximum time in milliseconds to wait for the response.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    MaxWaitMsMeta::Type MaxWaitMs;
    
    struct MinBytesMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "minBytes";
        static constexpr const char* About = "The minimum bytes to accumulate in the response.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    MinBytesMeta::Type MinBytes;
    
    struct MaxBytesMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "maxBytes";
        static constexpr const char* About = "The maximum bytes to fetch.  See KIP-74 for cases where this limit may not be honored.";
        static const Type Default; // = 0x7fffffff;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    MaxBytesMeta::Type MaxBytes;
    
    struct IsolationLevelMeta {
        using Type = TKafkaInt8;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "isolationLevel";
        static constexpr const char* About = "This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {4, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    IsolationLevelMeta::Type IsolationLevel;
    
    struct SessionIdMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "sessionId";
        static constexpr const char* About = "The fetch session ID.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {7, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    SessionIdMeta::Type SessionId;
    
    struct SessionEpochMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "sessionEpoch";
        static constexpr const char* About = "The fetch session epoch, which is used for ordering requests in a session.";
        static const Type Default; // = -1;
        
        static constexpr TKafkaVersions PresentVersions = {7, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    SessionEpochMeta::Type SessionEpoch;
    
    struct TopicsMeta {
        using ItemType = TFetchTopic;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TFetchTopic>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "The topics to fetch.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    struct ForgottenTopicsDataMeta {
        using ItemType = TForgottenTopic;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TForgottenTopic>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "forgottenTopicsData";
        static constexpr const char* About = "In an incremental fetch request, the partitions to remove.";
        
        static constexpr TKafkaVersions PresentVersions = {7, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    ForgottenTopicsDataMeta::Type ForgottenTopicsData;
    
    struct RackIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "rackId";
        static constexpr const char* About = "Rack ID of the consumer making this request";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = {11, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    RackIdMeta::Type RackId;
    
    i16 ApiKey() const override { return FETCH; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TFetchRequestData& other) const = default;
};


class TFetchResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TFetchResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 13};
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    
    TFetchResponseData();
    ~TFetchResponseData() = default;
    
    class TFetchableTopicResponse : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 13};
            static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
        };
        
        TFetchableTopicResponse();
        ~TFetchableTopicResponse() = default;
        
        class TPartitionData : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 13};
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            
            TPartitionData();
            ~TPartitionData() = default;
            
            class TEpochEndOffset : public TMessage {
            public:
                struct MessageMeta {
                    static constexpr TKafkaVersions PresentVersions = {12, 13};
                    static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
                };
                
                TEpochEndOffset();
                ~TEpochEndOffset() = default;
                
                struct EpochMeta {
                    using Type = TKafkaInt32;
                    using TypeDesc = NPrivate::TKafkaIntDesc;
                    
                    static constexpr const char* Name = "epoch";
                    static constexpr const char* About = "";
                    static const Type Default; // = -1;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
                };
                EpochMeta::Type Epoch;
                
                struct EndOffsetMeta {
                    using Type = TKafkaInt64;
                    using TypeDesc = NPrivate::TKafkaIntDesc;
                    
                    static constexpr const char* Name = "endOffset";
                    static constexpr const char* About = "";
                    static const Type Default; // = -1;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
                };
                EndOffsetMeta::Type EndOffset;
                
                i32 Size(TKafkaVersion version) const override;
                void Read(TKafkaReadable& readable, TKafkaVersion version) override;
                void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
                
                bool operator==(const TEpochEndOffset& other) const = default;
            };
            
            class TLeaderIdAndEpoch : public TMessage {
            public:
                struct MessageMeta {
                    static constexpr TKafkaVersions PresentVersions = {12, 13};
                    static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
                };
                
                TLeaderIdAndEpoch();
                ~TLeaderIdAndEpoch() = default;
                
                struct LeaderIdMeta {
                    using Type = TKafkaInt32;
                    using TypeDesc = NPrivate::TKafkaIntDesc;
                    
                    static constexpr const char* Name = "leaderId";
                    static constexpr const char* About = "The ID of the current leader or -1 if the leader is unknown.";
                    static const Type Default; // = -1;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
                };
                LeaderIdMeta::Type LeaderId;
                
                struct LeaderEpochMeta {
                    using Type = TKafkaInt32;
                    using TypeDesc = NPrivate::TKafkaIntDesc;
                    
                    static constexpr const char* Name = "leaderEpoch";
                    static constexpr const char* About = "The latest known leader epoch";
                    static const Type Default; // = -1;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
                };
                LeaderEpochMeta::Type LeaderEpoch;
                
                i32 Size(TKafkaVersion version) const override;
                void Read(TKafkaReadable& readable, TKafkaVersion version) override;
                void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
                
                bool operator==(const TLeaderIdAndEpoch& other) const = default;
            };
            
            class TSnapshotId : public TMessage {
            public:
                struct MessageMeta {
                    static constexpr TKafkaVersions PresentVersions = {12, 13};
                    static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
                };
                
                TSnapshotId();
                ~TSnapshotId() = default;
                
                struct EndOffsetMeta {
                    using Type = TKafkaInt64;
                    using TypeDesc = NPrivate::TKafkaIntDesc;
                    
                    static constexpr const char* Name = "endOffset";
                    static constexpr const char* About = "";
                    static const Type Default; // = -1;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
                };
                EndOffsetMeta::Type EndOffset;
                
                struct EpochMeta {
                    using Type = TKafkaInt32;
                    using TypeDesc = NPrivate::TKafkaIntDesc;
                    
                    static constexpr const char* Name = "epoch";
                    static constexpr const char* About = "";
                    static const Type Default; // = -1;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
                };
                EpochMeta::Type Epoch;
                
                i32 Size(TKafkaVersion version) const override;
                void Read(TKafkaReadable& readable, TKafkaVersion version) override;
                void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
                
                bool operator==(const TSnapshotId& other) const = default;
            };
            
            class TAbortedTransaction : public TMessage {
            public:
                struct MessageMeta {
                    static constexpr TKafkaVersions PresentVersions = {4, 13};
                    static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
                };
                
                TAbortedTransaction();
                ~TAbortedTransaction() = default;
                
                struct ProducerIdMeta {
                    using Type = TKafkaInt64;
                    using TypeDesc = NPrivate::TKafkaIntDesc;
                    
                    static constexpr const char* Name = "producerId";
                    static constexpr const char* About = "The producer id associated with the aborted transaction.";
                    static const Type Default; // = 0;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
                };
                ProducerIdMeta::Type ProducerId;
                
                struct FirstOffsetMeta {
                    using Type = TKafkaInt64;
                    using TypeDesc = NPrivate::TKafkaIntDesc;
                    
                    static constexpr const char* Name = "firstOffset";
                    static constexpr const char* About = "The first offset in the aborted transaction.";
                    static const Type Default; // = 0;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
                };
                FirstOffsetMeta::Type FirstOffset;
                
                i32 Size(TKafkaVersion version) const override;
                void Read(TKafkaReadable& readable, TKafkaVersion version) override;
                void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
                
                bool operator==(const TAbortedTransaction& other) const = default;
            };
            
            struct PartitionIndexMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "partitionIndex";
                static constexpr const char* About = "The partition index.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            PartitionIndexMeta::Type PartitionIndex;
            
            struct ErrorCodeMeta {
                using Type = TKafkaInt16;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "errorCode";
                static constexpr const char* About = "The error code, or 0 if there was no fetch error.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            ErrorCodeMeta::Type ErrorCode;
            
            struct HighWatermarkMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "highWatermark";
                static constexpr const char* About = "The current high water mark.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            HighWatermarkMeta::Type HighWatermark;
            
            struct LastStableOffsetMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "lastStableOffset";
                static constexpr const char* About = "The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED)";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {4, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            LastStableOffsetMeta::Type LastStableOffset;
            
            struct LogStartOffsetMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "logStartOffset";
                static constexpr const char* About = "The current log start offset.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            LogStartOffsetMeta::Type LogStartOffset;
            
            struct DivergingEpochMeta {
                using Type = TEpochEndOffset;
                using TypeDesc = NPrivate::TKafkaStructDesc;
                
                static constexpr const char* Name = "divergingEpoch";
                static constexpr const char* About = "In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge";
                static constexpr const TKafkaInt32 Tag = 0;
                
                static constexpr TKafkaVersions PresentVersions = {12, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsAlways;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            DivergingEpochMeta::Type DivergingEpoch;
            
            struct CurrentLeaderMeta {
                using Type = TLeaderIdAndEpoch;
                using TypeDesc = NPrivate::TKafkaStructDesc;
                
                static constexpr const char* Name = "currentLeader";
                static constexpr const char* About = "";
                static constexpr const TKafkaInt32 Tag = 1;
                
                static constexpr TKafkaVersions PresentVersions = {12, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsAlways;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            CurrentLeaderMeta::Type CurrentLeader;
            
            struct SnapshotIdMeta {
                using Type = TSnapshotId;
                using TypeDesc = NPrivate::TKafkaStructDesc;
                
                static constexpr const char* Name = "snapshotId";
                static constexpr const char* About = "In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.";
                static constexpr const TKafkaInt32 Tag = 2;
                
                static constexpr TKafkaVersions PresentVersions = {12, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsAlways;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            SnapshotIdMeta::Type SnapshotId;
            
            struct AbortedTransactionsMeta {
                using ItemType = TAbortedTransaction;
                using ItemTypeDesc = NPrivate::TKafkaStructDesc;
                using Type = std::vector<TAbortedTransaction>;
                using TypeDesc = NPrivate::TKafkaArrayDesc;
                
                static constexpr const char* Name = "abortedTransactions";
                static constexpr const char* About = "The aborted transactions.";
                
                static constexpr TKafkaVersions PresentVersions = {4, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            AbortedTransactionsMeta::Type AbortedTransactions;
            
            struct PreferredReadReplicaMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "preferredReadReplica";
                static constexpr const char* About = "The preferred read replica for the consumer to use on its next fetch request";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {11, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            PreferredReadReplicaMeta::Type PreferredReadReplica;
            
            struct RecordsMeta {
                using Type = TKafkaRecords;
                using TypeDesc = NPrivate::TKafkaRecordsDesc;
                
                static constexpr const char* Name = "records";
                static constexpr const char* About = "The record data.";
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
            };
            RecordsMeta::Type Records;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TPartitionData& other) const = default;
        };
        
        struct TopicMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "topic";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = {0, 12};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
        };
        TopicMeta::Type Topic;
        
        struct TopicIdMeta {
            using Type = TKafkaUuid;
            using TypeDesc = NPrivate::TKafkaUuidDesc;
            
            static constexpr const char* Name = "topicId";
            static constexpr const char* About = "The unique topic ID";
            static const Type Default; // = TKafkaUuid(0, 0);
            
            static constexpr TKafkaVersions PresentVersions = {13, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        TopicIdMeta::Type TopicId;
        
        struct PartitionsMeta {
            using ItemType = TPartitionData;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TPartitionData>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitions";
            static constexpr const char* About = "The topic partitions.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
        };
        PartitionsMeta::Type Partitions;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TFetchableTopicResponse& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct ErrorCodeMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "errorCode";
        static constexpr const char* About = "The top level response error code.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {7, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    ErrorCodeMeta::Type ErrorCode;
    
    struct SessionIdMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "sessionId";
        static constexpr const char* About = "The fetch session ID, or 0 if this is not part of a fetch session.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {7, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    SessionIdMeta::Type SessionId;
    
    struct ResponsesMeta {
        using ItemType = TFetchableTopicResponse;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TFetchableTopicResponse>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "responses";
        static constexpr const char* About = "The response topics.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {12, Max<TKafkaVersion>()};
    };
    ResponsesMeta::Type Responses;
    
    i16 ApiKey() const override { return FETCH; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TFetchResponseData& other) const = default;
};


class TListOffsetsRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TListOffsetsRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 7};
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    
    TListOffsetsRequestData();
    ~TListOffsetsRequestData() = default;
    
    class TListOffsetsTopic : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 7};
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        
        TListOffsetsTopic();
        ~TListOffsetsTopic() = default;
        
        class TListOffsetsPartition : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 7};
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            
            TListOffsetsPartition();
            ~TListOffsetsPartition() = default;
            
            struct PartitionIndexMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "partitionIndex";
                static constexpr const char* About = "The partition index.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            PartitionIndexMeta::Type PartitionIndex;
            
            struct CurrentLeaderEpochMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "currentLeaderEpoch";
                static constexpr const char* About = "The current leader epoch.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {4, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            CurrentLeaderEpochMeta::Type CurrentLeaderEpoch;
            
            struct TimestampMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "timestamp";
                static constexpr const char* About = "The current timestamp.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            TimestampMeta::Type Timestamp;
            
            struct MaxNumOffsetsMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "maxNumOffsets";
                static constexpr const char* About = "The maximum number of offsets to report.";
                static const Type Default; // = 1;
                
                static constexpr TKafkaVersions PresentVersions = {0, 0};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            MaxNumOffsetsMeta::Type MaxNumOffsets;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TListOffsetsPartition& other) const = default;
        };
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct PartitionsMeta {
            using ItemType = TListOffsetsPartition;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TListOffsetsPartition>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitions";
            static constexpr const char* About = "Each partition in the request.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        PartitionsMeta::Type Partitions;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TListOffsetsTopic& other) const = default;
    };
    
    struct ReplicaIdMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "replicaId";
        static constexpr const char* About = "The broker ID of the requestor, or -1 if this request is being made by a normal consumer.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    ReplicaIdMeta::Type ReplicaId;
    
    struct IsolationLevelMeta {
        using Type = TKafkaInt8;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "isolationLevel";
        static constexpr const char* About = "This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {2, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    IsolationLevelMeta::Type IsolationLevel;
    
    struct TopicsMeta {
        using ItemType = TListOffsetsTopic;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TListOffsetsTopic>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "Each topic in the request.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    i16 ApiKey() const override { return LIST_OFFSETS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TListOffsetsRequestData& other) const = default;
};


class TListOffsetsResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TListOffsetsResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 7};
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    
    TListOffsetsResponseData();
    ~TListOffsetsResponseData() = default;
    
    class TListOffsetsTopicResponse : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 7};
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        
        TListOffsetsTopicResponse();
        ~TListOffsetsTopicResponse() = default;
        
        class TListOffsetsPartitionResponse : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 7};
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            
            TListOffsetsPartitionResponse();
            ~TListOffsetsPartitionResponse() = default;
            
            struct PartitionIndexMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "partitionIndex";
                static constexpr const char* About = "The partition index.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            PartitionIndexMeta::Type PartitionIndex;
            
            struct ErrorCodeMeta {
                using Type = TKafkaInt16;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "errorCode";
                static constexpr const char* About = "The partition error code, or 0 if there was no error.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            ErrorCodeMeta::Type ErrorCode;
            
            struct OldStyleOffsetsMeta {
                using ItemType = TKafkaInt64;
                using ItemTypeDesc = NPrivate::TKafkaIntDesc;
                using Type = std::vector<TKafkaInt64>;
                using TypeDesc = NPrivate::TKafkaArrayDesc;
                
                static constexpr const char* Name = "oldStyleOffsets";
                static constexpr const char* About = "The result offsets.";
                
                static constexpr TKafkaVersions PresentVersions = {0, 0};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            OldStyleOffsetsMeta::Type OldStyleOffsets;
            
            struct TimestampMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "timestamp";
                static constexpr const char* About = "The timestamp associated with the returned offset.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            TimestampMeta::Type Timestamp;
            
            struct OffsetMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "offset";
                static constexpr const char* About = "The returned offset.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            OffsetMeta::Type Offset;
            
            struct LeaderEpochMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "leaderEpoch";
                static constexpr const char* About = "";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {4, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            LeaderEpochMeta::Type LeaderEpoch;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TListOffsetsPartitionResponse& other) const = default;
        };
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct PartitionsMeta {
            using ItemType = TListOffsetsPartitionResponse;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TListOffsetsPartitionResponse>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitions";
            static constexpr const char* About = "Each partition in the response.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        PartitionsMeta::Type Partitions;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TListOffsetsTopicResponse& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {2, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct TopicsMeta {
        using ItemType = TListOffsetsTopicResponse;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TListOffsetsTopicResponse>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "Each topic in the response.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    i16 ApiKey() const override { return LIST_OFFSETS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TListOffsetsResponseData& other) const = default;
};


class TMetadataRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TMetadataRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 12};
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    
    TMetadataRequestData();
    ~TMetadataRequestData() = default;
    
    class TMetadataRequestTopic : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 12};
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        
        TMetadataRequestTopic();
        ~TMetadataRequestTopic() = default;
        
        struct TopicIdMeta {
            using Type = TKafkaUuid;
            using TypeDesc = NPrivate::TKafkaUuidDesc;
            
            static constexpr const char* Name = "topicId";
            static constexpr const char* About = "The topic id.";
            static const Type Default; // = TKafkaUuid(0, 0);
            
            static constexpr TKafkaVersions PresentVersions = {10, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        TopicIdMeta::Type TopicId;
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = {10, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TMetadataRequestTopic& other) const = default;
    };
    
    struct TopicsMeta {
        using ItemType = TMetadataRequestTopic;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TMetadataRequestTopic>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "The topics to fetch metadata for.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    struct AllowAutoTopicCreationMeta {
        using Type = TKafkaBool;
        using TypeDesc = NPrivate::TKafkaBoolDesc;
        
        static constexpr const char* Name = "allowAutoTopicCreation";
        static constexpr const char* About = "If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so.";
        static const Type Default; // = true;
        
        static constexpr TKafkaVersions PresentVersions = {4, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    AllowAutoTopicCreationMeta::Type AllowAutoTopicCreation;
    
    struct IncludeClusterAuthorizedOperationsMeta {
        using Type = TKafkaBool;
        using TypeDesc = NPrivate::TKafkaBoolDesc;
        
        static constexpr const char* Name = "includeClusterAuthorizedOperations";
        static constexpr const char* About = "Whether to include cluster authorized operations.";
        static const Type Default; // = false;
        
        static constexpr TKafkaVersions PresentVersions = {8, 10};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    IncludeClusterAuthorizedOperationsMeta::Type IncludeClusterAuthorizedOperations;
    
    struct IncludeTopicAuthorizedOperationsMeta {
        using Type = TKafkaBool;
        using TypeDesc = NPrivate::TKafkaBoolDesc;
        
        static constexpr const char* Name = "includeTopicAuthorizedOperations";
        static constexpr const char* About = "Whether to include topic authorized operations.";
        static const Type Default; // = false;
        
        static constexpr TKafkaVersions PresentVersions = {8, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    IncludeTopicAuthorizedOperationsMeta::Type IncludeTopicAuthorizedOperations;
    
    i16 ApiKey() const override { return METADATA; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TMetadataRequestData& other) const = default;
};


class TMetadataResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TMetadataResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 12};
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    
    TMetadataResponseData();
    ~TMetadataResponseData() = default;
    
    class TMetadataResponseBroker : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 12};
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        
        TMetadataResponseBroker();
        ~TMetadataResponseBroker() = default;
        
        struct NodeIdMeta {
            using Type = TKafkaInt32;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "nodeId";
            static constexpr const char* About = "The broker ID.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        NodeIdMeta::Type NodeId;
        
        struct HostMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "host";
            static constexpr const char* About = "The broker hostname.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        HostMeta::Type Host;
        
        struct PortMeta {
            using Type = TKafkaInt32;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "port";
            static constexpr const char* About = "The broker port.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        PortMeta::Type Port;
        
        struct RackMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "rack";
            static constexpr const char* About = "The rack of the broker, or null if it has not been assigned to a rack.";
            static const Type Default; // = std::nullopt;
            
            static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsAlways;
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        RackMeta::Type Rack;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TMetadataResponseBroker& other) const = default;
    };
    
    class TMetadataResponseTopic : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 12};
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        
        TMetadataResponseTopic();
        ~TMetadataResponseTopic() = default;
        
        class TMetadataResponsePartition : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 12};
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            
            TMetadataResponsePartition();
            ~TMetadataResponsePartition() = default;
            
            struct ErrorCodeMeta {
                using Type = TKafkaInt16;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "errorCode";
                static constexpr const char* About = "The partition error, or 0 if there was no error.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            ErrorCodeMeta::Type ErrorCode;
            
            struct PartitionIndexMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "partitionIndex";
                static constexpr const char* About = "The partition index.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            PartitionIndexMeta::Type PartitionIndex;
            
            struct LeaderIdMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "leaderId";
                static constexpr const char* About = "The ID of the leader broker.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            LeaderIdMeta::Type LeaderId;
            
            struct LeaderEpochMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "leaderEpoch";
                static constexpr const char* About = "The leader epoch of this partition.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {7, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            LeaderEpochMeta::Type LeaderEpoch;
            
            struct ReplicaNodesMeta {
                using ItemType = TKafkaInt32;
                using ItemTypeDesc = NPrivate::TKafkaIntDesc;
                using Type = std::vector<TKafkaInt32>;
                using TypeDesc = NPrivate::TKafkaArrayDesc;
                
                static constexpr const char* Name = "replicaNodes";
                static constexpr const char* About = "The set of all nodes that host this partition.";
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            ReplicaNodesMeta::Type ReplicaNodes;
            
            struct IsrNodesMeta {
                using ItemType = TKafkaInt32;
                using ItemTypeDesc = NPrivate::TKafkaIntDesc;
                using Type = std::vector<TKafkaInt32>;
                using TypeDesc = NPrivate::TKafkaArrayDesc;
                
                static constexpr const char* Name = "isrNodes";
                static constexpr const char* About = "The set of nodes that are in sync with the leader for this partition.";
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            IsrNodesMeta::Type IsrNodes;
            
            struct OfflineReplicasMeta {
                using ItemType = TKafkaInt32;
                using ItemTypeDesc = NPrivate::TKafkaIntDesc;
                using Type = std::vector<TKafkaInt32>;
                using TypeDesc = NPrivate::TKafkaArrayDesc;
                
                static constexpr const char* Name = "offlineReplicas";
                static constexpr const char* About = "The set of offline replicas of this partition.";
                
                static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
            };
            OfflineReplicasMeta::Type OfflineReplicas;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TMetadataResponsePartition& other) const = default;
        };
        
        struct ErrorCodeMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "errorCode";
            static constexpr const char* About = "The topic error, or 0 if there was no error.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        ErrorCodeMeta::Type ErrorCode;
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = {12, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct TopicIdMeta {
            using Type = TKafkaUuid;
            using TypeDesc = NPrivate::TKafkaUuidDesc;
            
            static constexpr const char* Name = "topicId";
            static constexpr const char* About = "The topic id.";
            static const Type Default; // = TKafkaUuid(0, 0);
            
            static constexpr TKafkaVersions PresentVersions = {10, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        TopicIdMeta::Type TopicId;
        
        struct IsInternalMeta {
            using Type = TKafkaBool;
            using TypeDesc = NPrivate::TKafkaBoolDesc;
            
            static constexpr const char* Name = "isInternal";
            static constexpr const char* About = "True if the topic is internal.";
            static const Type Default; // = false;
            
            static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        IsInternalMeta::Type IsInternal;
        
        struct PartitionsMeta {
            using ItemType = TMetadataResponsePartition;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TMetadataResponsePartition>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitions";
            static constexpr const char* About = "Each partition in the topic.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        PartitionsMeta::Type Partitions;
        
        struct TopicAuthorizedOperationsMeta {
            using Type = TKafkaInt32;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "topicAuthorizedOperations";
            static constexpr const char* About = "32-bit bitfield to represent authorized operations for this topic.";
            static const Type Default; // = -2147483648;
            
            static constexpr TKafkaVersions PresentVersions = {8, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
        };
        TopicAuthorizedOperationsMeta::Type TopicAuthorizedOperations;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TMetadataResponseTopic& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct BrokersMeta {
        using ItemType = TMetadataResponseBroker;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TMetadataResponseBroker>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "brokers";
        static constexpr const char* About = "Each broker in the response.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    BrokersMeta::Type Brokers;
    
    struct ClusterIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "clusterId";
        static constexpr const char* About = "The cluster ID that responding broker belongs to.";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = {2, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    ClusterIdMeta::Type ClusterId;
    
    struct ControllerIdMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "controllerId";
        static constexpr const char* About = "The ID of the controller broker.";
        static const Type Default; // = -1;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    ControllerIdMeta::Type ControllerId;
    
    struct TopicsMeta {
        using ItemType = TMetadataResponseTopic;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TMetadataResponseTopic>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "Each topic in the response.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    struct ClusterAuthorizedOperationsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "clusterAuthorizedOperations";
        static constexpr const char* About = "32-bit bitfield to represent authorized operations for this cluster.";
        static const Type Default; // = -2147483648;
        
        static constexpr TKafkaVersions PresentVersions = {8, 10};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {9, Max<TKafkaVersion>()};
    };
    ClusterAuthorizedOperationsMeta::Type ClusterAuthorizedOperations;
    
    i16 ApiKey() const override { return METADATA; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TMetadataResponseData& other) const = default;
};


class TOffsetCommitRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TOffsetCommitRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 8};
        static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
    };
    
    TOffsetCommitRequestData();
    ~TOffsetCommitRequestData() = default;
    
    class TOffsetCommitRequestTopic : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 8};
            static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
        };
        
        TOffsetCommitRequestTopic();
        ~TOffsetCommitRequestTopic() = default;
        
        class TOffsetCommitRequestPartition : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 8};
                static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
            };
            
            TOffsetCommitRequestPartition();
            ~TOffsetCommitRequestPartition() = default;
            
            struct PartitionIndexMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "partitionIndex";
                static constexpr const char* About = "The partition index.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
            };
            PartitionIndexMeta::Type PartitionIndex;
            
            struct CommittedOffsetMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "committedOffset";
                static constexpr const char* About = "The message offset to be committed.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
            };
            CommittedOffsetMeta::Type CommittedOffset;
            
            struct CommittedLeaderEpochMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "committedLeaderEpoch";
                static constexpr const char* About = "The leader epoch of this partition.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {6, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
            };
            CommittedLeaderEpochMeta::Type CommittedLeaderEpoch;
            
            struct CommitTimestampMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "commitTimestamp";
                static constexpr const char* About = "The timestamp of the commit.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {1, 1};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
            };
            CommitTimestampMeta::Type CommitTimestamp;
            
            struct CommittedMetadataMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "committedMetadata";
                static constexpr const char* About = "Any associated metadata the client wants to keep.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
            };
            CommittedMetadataMeta::Type CommittedMetadata;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TOffsetCommitRequestPartition& other) const = default;
        };
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct PartitionsMeta {
            using ItemType = TOffsetCommitRequestPartition;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TOffsetCommitRequestPartition>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitions";
            static constexpr const char* About = "Each partition to commit offsets for.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
        };
        PartitionsMeta::Type Partitions;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TOffsetCommitRequestTopic& other) const = default;
    };
    
    struct GroupIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "groupId";
        static constexpr const char* About = "The unique group identifier.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
    };
    GroupIdMeta::Type GroupId;
    
    struct GenerationIdMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "generationId";
        static constexpr const char* About = "The generation of the group.";
        static const Type Default; // = -1;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
    };
    GenerationIdMeta::Type GenerationId;
    
    struct MemberIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "memberId";
        static constexpr const char* About = "The member ID assigned by the group coordinator.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
    };
    MemberIdMeta::Type MemberId;
    
    struct GroupInstanceIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "groupInstanceId";
        static constexpr const char* About = "The unique identifier of the consumer instance provided by end user.";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = {7, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
    };
    GroupInstanceIdMeta::Type GroupInstanceId;
    
    struct RetentionTimeMsMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "retentionTimeMs";
        static constexpr const char* About = "The time period in ms to retain the offset.";
        static const Type Default; // = -1;
        
        static constexpr TKafkaVersions PresentVersions = {2, 4};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
    };
    RetentionTimeMsMeta::Type RetentionTimeMs;
    
    struct TopicsMeta {
        using ItemType = TOffsetCommitRequestTopic;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TOffsetCommitRequestTopic>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "The topics to commit offsets for.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    i16 ApiKey() const override { return OFFSET_COMMIT; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TOffsetCommitRequestData& other) const = default;
};


class TOffsetCommitResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TOffsetCommitResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 8};
        static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
    };
    
    TOffsetCommitResponseData();
    ~TOffsetCommitResponseData() = default;
    
    class TOffsetCommitResponseTopic : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 8};
            static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
        };
        
        TOffsetCommitResponseTopic();
        ~TOffsetCommitResponseTopic() = default;
        
        class TOffsetCommitResponsePartition : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 8};
                static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
            };
            
            TOffsetCommitResponsePartition();
            ~TOffsetCommitResponsePartition() = default;
            
            struct PartitionIndexMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "partitionIndex";
                static constexpr const char* About = "The partition index.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
            };
            PartitionIndexMeta::Type PartitionIndex;
            
            struct ErrorCodeMeta {
                using Type = TKafkaInt16;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "errorCode";
                static constexpr const char* About = "The error code, or 0 if there was no error.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
            };
            ErrorCodeMeta::Type ErrorCode;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TOffsetCommitResponsePartition& other) const = default;
        };
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct PartitionsMeta {
            using ItemType = TOffsetCommitResponsePartition;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TOffsetCommitResponsePartition>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitions";
            static constexpr const char* About = "The responses for each partition in the topic.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
        };
        PartitionsMeta::Type Partitions;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TOffsetCommitResponseTopic& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct TopicsMeta {
        using ItemType = TOffsetCommitResponseTopic;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TOffsetCommitResponseTopic>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "The responses for each topic.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {8, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    i16 ApiKey() const override { return OFFSET_COMMIT; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TOffsetCommitResponseData& other) const = default;
};


class TOffsetFetchRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TOffsetFetchRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 8};
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    
    TOffsetFetchRequestData();
    ~TOffsetFetchRequestData() = default;
    
    class TOffsetFetchRequestTopic : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 7};
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        
        TOffsetFetchRequestTopic();
        ~TOffsetFetchRequestTopic() = default;
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct PartitionIndexesMeta {
            using ItemType = TKafkaInt32;
            using ItemTypeDesc = NPrivate::TKafkaIntDesc;
            using Type = std::vector<TKafkaInt32>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitionIndexes";
            static constexpr const char* About = "The partition indexes we would like to fetch offsets for.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        PartitionIndexesMeta::Type PartitionIndexes;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TOffsetFetchRequestTopic& other) const = default;
    };
    
    class TOffsetFetchRequestGroup : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {8, 8};
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        
        TOffsetFetchRequestGroup();
        ~TOffsetFetchRequestGroup() = default;
        
        class TOffsetFetchRequestTopics : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {8, 8};
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            
            TOffsetFetchRequestTopics();
            ~TOffsetFetchRequestTopics() = default;
            
            struct NameMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "name";
                static constexpr const char* About = "The topic name.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            NameMeta::Type Name;
            
            struct PartitionIndexesMeta {
                using ItemType = TKafkaInt32;
                using ItemTypeDesc = NPrivate::TKafkaIntDesc;
                using Type = std::vector<TKafkaInt32>;
                using TypeDesc = NPrivate::TKafkaArrayDesc;
                
                static constexpr const char* Name = "partitionIndexes";
                static constexpr const char* About = "The partition indexes we would like to fetch offsets for.";
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            PartitionIndexesMeta::Type PartitionIndexes;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TOffsetFetchRequestTopics& other) const = default;
        };
        
        struct GroupIdMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "groupId";
            static constexpr const char* About = "The group ID.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        GroupIdMeta::Type GroupId;
        
        struct TopicsMeta {
            using ItemType = TOffsetFetchRequestTopics;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TOffsetFetchRequestTopics>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "topics";
            static constexpr const char* About = "Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsAlways;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        TopicsMeta::Type Topics;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TOffsetFetchRequestGroup& other) const = default;
    };
    
    struct GroupIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "groupId";
        static constexpr const char* About = "The group to fetch offsets for.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = {0, 7};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    GroupIdMeta::Type GroupId;
    
    struct TopicsMeta {
        using ItemType = TOffsetFetchRequestTopic;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TOffsetFetchRequestTopic>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "Each topic we would like to fetch offsets for, or null to fetch offsets for all topics.";
        
        static constexpr TKafkaVersions PresentVersions = {0, 7};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = {2, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    struct GroupsMeta {
        using ItemType = TOffsetFetchRequestGroup;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TOffsetFetchRequestGroup>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "groups";
        static constexpr const char* About = "Each group we would like to fetch offsets for";
        
        static constexpr TKafkaVersions PresentVersions = {8, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    GroupsMeta::Type Groups;
    
    struct RequireStableMeta {
        using Type = TKafkaBool;
        using TypeDesc = NPrivate::TKafkaBoolDesc;
        
        static constexpr const char* Name = "requireStable";
        static constexpr const char* About = "Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions.";
        static const Type Default; // = false;
        
        static constexpr TKafkaVersions PresentVersions = {7, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    RequireStableMeta::Type RequireStable;
    
    i16 ApiKey() const override { return OFFSET_FETCH; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TOffsetFetchRequestData& other) const = default;
};


class TOffsetFetchResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TOffsetFetchResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 8};
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    
    TOffsetFetchResponseData();
    ~TOffsetFetchResponseData() = default;
    
    class TOffsetFetchResponseTopic : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 7};
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        
        TOffsetFetchResponseTopic();
        ~TOffsetFetchResponseTopic() = default;
        
        class TOffsetFetchResponsePartition : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 7};
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            
            TOffsetFetchResponsePartition();
            ~TOffsetFetchResponsePartition() = default;
            
            struct PartitionIndexMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "partitionIndex";
                static constexpr const char* About = "The partition index.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            PartitionIndexMeta::Type PartitionIndex;
            
            struct CommittedOffsetMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "committedOffset";
                static constexpr const char* About = "The committed message offset.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            CommittedOffsetMeta::Type CommittedOffset;
            
            struct CommittedLeaderEpochMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "committedLeaderEpoch";
                static constexpr const char* About = "The leader epoch.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            CommittedLeaderEpochMeta::Type CommittedLeaderEpoch;
            
            struct MetadataMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "metadata";
                static constexpr const char* About = "The partition metadata.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            MetadataMeta::Type Metadata;
            
            struct ErrorCodeMeta {
                using Type = TKafkaInt16;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "errorCode";
                static constexpr const char* About = "The error code, or 0 if there was no error.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
            };
            ErrorCodeMeta::Type ErrorCode;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TOffsetFetchResponsePartition& other) const = default;
        };
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct PartitionsMeta {
            using ItemType = TOffsetFetchResponsePartition;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TOffsetFetchResponsePartition>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitions";
            static constexpr const char* About = "The responses per partition";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        PartitionsMeta::Type Partitions;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TOffsetFetchResponseTopic& other) const = default;
    };
    
    class TOffsetFetchResponseGroup : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {8, 8};
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        
        TOffsetFetchResponseGroup();
        ~TOffsetFetchResponseGroup() = default;
        
        class TOffsetFetchResponseTopics : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {8, 8};
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            
            TOffsetFetchResponseTopics();
            ~TOffsetFetchResponseTopics() = default;
            
            class TOffsetFetchResponsePartitions : public TMessage {
            public:
                struct MessageMeta {
                    static constexpr TKafkaVersions PresentVersions = {8, 8};
                    static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
                };
                
                TOffsetFetchResponsePartitions();
                ~TOffsetFetchResponsePartitions() = default;
                
                struct PartitionIndexMeta {
                    using Type = TKafkaInt32;
                    using TypeDesc = NPrivate::TKafkaIntDesc;
                    
                    static constexpr const char* Name = "partitionIndex";
                    static constexpr const char* About = "The partition index.";
                    static const Type Default; // = 0;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
                };
                PartitionIndexMeta::Type PartitionIndex;
                
                struct CommittedOffsetMeta {
                    using Type = TKafkaInt64;
                    using TypeDesc = NPrivate::TKafkaIntDesc;
                    
                    static constexpr const char* Name = "committedOffset";
                    static constexpr const char* About = "The committed message offset.";
                    static const Type Default; // = 0;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
                };
                CommittedOffsetMeta::Type CommittedOffset;
                
                struct CommittedLeaderEpochMeta {
                    using Type = TKafkaInt32;
                    using TypeDesc = NPrivate::TKafkaIntDesc;
                    
                    static constexpr const char* Name = "committedLeaderEpoch";
                    static constexpr const char* About = "The leader epoch.";
                    static const Type Default; // = -1;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
                };
                CommittedLeaderEpochMeta::Type CommittedLeaderEpoch;
                
                struct MetadataMeta {
                    using Type = TKafkaString;
                    using TypeDesc = NPrivate::TKafkaStringDesc;
                    
                    static constexpr const char* Name = "metadata";
                    static constexpr const char* About = "The partition metadata.";
                    static const Type Default; // = {""};
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                    static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
                };
                MetadataMeta::Type Metadata;
                
                struct ErrorCodeMeta {
                    using Type = TKafkaInt16;
                    using TypeDesc = NPrivate::TKafkaIntDesc;
                    
                    static constexpr const char* Name = "errorCode";
                    static constexpr const char* About = "The partition-level error code, or 0 if there was no error.";
                    static const Type Default; // = 0;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
                };
                ErrorCodeMeta::Type ErrorCode;
                
                i32 Size(TKafkaVersion version) const override;
                void Read(TKafkaReadable& readable, TKafkaVersion version) override;
                void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
                
                bool operator==(const TOffsetFetchResponsePartitions& other) const = default;
            };
            
            struct NameMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "name";
                static constexpr const char* About = "The topic name.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            NameMeta::Type Name;
            
            struct PartitionsMeta {
                using ItemType = TOffsetFetchResponsePartitions;
                using ItemTypeDesc = NPrivate::TKafkaStructDesc;
                using Type = std::vector<TOffsetFetchResponsePartitions>;
                using TypeDesc = NPrivate::TKafkaArrayDesc;
                
                static constexpr const char* Name = "partitions";
                static constexpr const char* About = "The responses per partition";
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            PartitionsMeta::Type Partitions;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TOffsetFetchResponseTopics& other) const = default;
        };
        
        struct GroupIdMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "groupId";
            static constexpr const char* About = "The group ID.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        GroupIdMeta::Type GroupId;
        
        struct TopicsMeta {
            using ItemType = TOffsetFetchResponseTopics;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TOffsetFetchResponseTopics>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "topics";
            static constexpr const char* About = "The responses per topic.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        TopicsMeta::Type Topics;
        
        struct ErrorCodeMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "errorCode";
            static constexpr const char* About = "The group-level error code, or 0 if there was no error.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        ErrorCodeMeta::Type ErrorCode;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TOffsetFetchResponseGroup& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct TopicsMeta {
        using ItemType = TOffsetFetchResponseTopic;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TOffsetFetchResponseTopic>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "The responses per topic.";
        
        static constexpr TKafkaVersions PresentVersions = {0, 7};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    struct ErrorCodeMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "errorCode";
        static constexpr const char* About = "The top-level error code, or 0 if there was no error.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {2, 7};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    ErrorCodeMeta::Type ErrorCode;
    
    struct GroupsMeta {
        using ItemType = TOffsetFetchResponseGroup;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TOffsetFetchResponseGroup>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "groups";
        static constexpr const char* About = "The responses per group id.";
        
        static constexpr TKafkaVersions PresentVersions = {8, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    GroupsMeta::Type Groups;
    
    i16 ApiKey() const override { return OFFSET_FETCH; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TOffsetFetchResponseData& other) const = default;
};


class TFindCoordinatorRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TFindCoordinatorRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 4};
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    
    TFindCoordinatorRequestData();
    ~TFindCoordinatorRequestData() = default;
    
    struct KeyMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "key";
        static constexpr const char* About = "The coordinator key.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    KeyMeta::Type Key;
    
    struct KeyTypeMeta {
        using Type = TKafkaInt8;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "keyType";
        static constexpr const char* About = "The coordinator key type. (Group, transaction, etc.)";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    KeyTypeMeta::Type KeyType;
    
    struct CoordinatorKeysMeta {
        using ItemType = TKafkaString;
        using ItemTypeDesc = NPrivate::TKafkaStringDesc;
        using Type = std::vector<TKafkaString>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "coordinatorKeys";
        static constexpr const char* About = "The coordinator keys.";
        
        static constexpr TKafkaVersions PresentVersions = {4, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    CoordinatorKeysMeta::Type CoordinatorKeys;
    
    i16 ApiKey() const override { return FIND_COORDINATOR; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TFindCoordinatorRequestData& other) const = default;
};


class TFindCoordinatorResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TFindCoordinatorResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 4};
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    
    TFindCoordinatorResponseData();
    ~TFindCoordinatorResponseData() = default;
    
    class TCoordinator : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {4, 4};
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        
        TCoordinator();
        ~TCoordinator() = default;
        
        struct KeyMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "key";
            static constexpr const char* About = "The coordinator key.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        KeyMeta::Type Key;
        
        struct NodeIdMeta {
            using Type = TKafkaInt32;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "nodeId";
            static constexpr const char* About = "The node id.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        NodeIdMeta::Type NodeId;
        
        struct HostMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "host";
            static constexpr const char* About = "The host name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        HostMeta::Type Host;
        
        struct PortMeta {
            using Type = TKafkaInt32;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "port";
            static constexpr const char* About = "The port.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        PortMeta::Type Port;
        
        struct ErrorCodeMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "errorCode";
            static constexpr const char* About = "The error code, or 0 if there was no error.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        ErrorCodeMeta::Type ErrorCode;
        
        struct ErrorMessageMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "errorMessage";
            static constexpr const char* About = "The error message, or null if there was no error.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsAlways;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        ErrorMessageMeta::Type ErrorMessage;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TCoordinator& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct ErrorCodeMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "errorCode";
        static constexpr const char* About = "The error code, or 0 if there was no error.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ErrorCodeMeta::Type ErrorCode;
    
    struct ErrorMessageMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "errorMessage";
        static constexpr const char* About = "The error message, or null if there was no error.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = {1, 3};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ErrorMessageMeta::Type ErrorMessage;
    
    struct NodeIdMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "nodeId";
        static constexpr const char* About = "The node id.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    NodeIdMeta::Type NodeId;
    
    struct HostMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "host";
        static constexpr const char* About = "The host name.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    HostMeta::Type Host;
    
    struct PortMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "port";
        static constexpr const char* About = "The port.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    PortMeta::Type Port;
    
    struct CoordinatorsMeta {
        using ItemType = TCoordinator;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TCoordinator>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "coordinators";
        static constexpr const char* About = "Each coordinator result in the response";
        
        static constexpr TKafkaVersions PresentVersions = {4, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    CoordinatorsMeta::Type Coordinators;
    
    i16 ApiKey() const override { return FIND_COORDINATOR; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TFindCoordinatorResponseData& other) const = default;
};


class TJoinGroupRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TJoinGroupRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 9};
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    
    TJoinGroupRequestData();
    ~TJoinGroupRequestData() = default;
    
    class TJoinGroupRequestProtocol : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 9};
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        
        TJoinGroupRequestProtocol();
        ~TJoinGroupRequestProtocol() = default;
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The protocol name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct MetadataMeta {
            using Type = TKafkaBytes;
            using TypeDesc = NPrivate::TKafkaBytesDesc;
            
            static constexpr const char* Name = "metadata";
            static constexpr const char* About = "The protocol metadata.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        MetadataMeta::Type Metadata;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TJoinGroupRequestProtocol& other) const = default;
    };
    
    struct GroupIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "groupId";
        static constexpr const char* About = "The group identifier.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    GroupIdMeta::Type GroupId;
    
    struct SessionTimeoutMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "sessionTimeoutMs";
        static constexpr const char* About = "The coordinator considers the consumer dead if it receives no heartbeat after this timeout in milliseconds.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    SessionTimeoutMsMeta::Type SessionTimeoutMs;
    
    struct RebalanceTimeoutMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "rebalanceTimeoutMs";
        static constexpr const char* About = "The maximum time in milliseconds that the coordinator will wait for each member to rejoin when rebalancing the group.";
        static const Type Default; // = -1;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    RebalanceTimeoutMsMeta::Type RebalanceTimeoutMs;
    
    struct MemberIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "memberId";
        static constexpr const char* About = "The member id assigned by the group coordinator.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    MemberIdMeta::Type MemberId;
    
    struct GroupInstanceIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "groupInstanceId";
        static constexpr const char* About = "The unique identifier of the consumer instance provided by end user.";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    GroupInstanceIdMeta::Type GroupInstanceId;
    
    struct ProtocolTypeMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "protocolType";
        static constexpr const char* About = "The unique name the for class of protocols implemented by the group we want to join.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    ProtocolTypeMeta::Type ProtocolType;
    
    struct ProtocolsMeta {
        using ItemType = TJoinGroupRequestProtocol;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TJoinGroupRequestProtocol>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "protocols";
        static constexpr const char* About = "The list of protocols that the member supports.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    ProtocolsMeta::Type Protocols;
    
    struct ReasonMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "reason";
        static constexpr const char* About = "The reason why the member (re-)joins the group.";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = {8, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    ReasonMeta::Type Reason;
    
    i16 ApiKey() const override { return JOIN_GROUP; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TJoinGroupRequestData& other) const = default;
};


class TJoinGroupResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TJoinGroupResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 9};
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    
    TJoinGroupResponseData();
    ~TJoinGroupResponseData() = default;
    
    class TJoinGroupResponseMember : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 9};
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        
        TJoinGroupResponseMember();
        ~TJoinGroupResponseMember() = default;
        
        struct MemberIdMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "memberId";
            static constexpr const char* About = "The group member ID.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        MemberIdMeta::Type MemberId;
        
        struct GroupInstanceIdMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "groupInstanceId";
            static constexpr const char* About = "The unique identifier of the consumer instance provided by end user.";
            static const Type Default; // = std::nullopt;
            
            static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsAlways;
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        GroupInstanceIdMeta::Type GroupInstanceId;
        
        struct MetadataMeta {
            using Type = TKafkaBytes;
            using TypeDesc = NPrivate::TKafkaBytesDesc;
            
            static constexpr const char* Name = "metadata";
            static constexpr const char* About = "The group member metadata.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
        };
        MetadataMeta::Type Metadata;
        
        TString MetaStr;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TJoinGroupResponseMember& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {2, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct ErrorCodeMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "errorCode";
        static constexpr const char* About = "The error code, or 0 if there was no error.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    ErrorCodeMeta::Type ErrorCode;
    
    struct GenerationIdMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "generationId";
        static constexpr const char* About = "The generation ID of the group.";
        static const Type Default; // = -1;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    GenerationIdMeta::Type GenerationId;
    
    struct ProtocolTypeMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "protocolType";
        static constexpr const char* About = "The group protocol name.";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = {7, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    ProtocolTypeMeta::Type ProtocolType;
    
    struct ProtocolNameMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "protocolName";
        static constexpr const char* About = "The group protocol selected by the coordinator.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = {7, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    ProtocolNameMeta::Type ProtocolName;
    
    struct LeaderMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "leader";
        static constexpr const char* About = "The leader of the group.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    LeaderMeta::Type Leader;
    
    struct SkipAssignmentMeta {
        using Type = TKafkaBool;
        using TypeDesc = NPrivate::TKafkaBoolDesc;
        
        static constexpr const char* Name = "skipAssignment";
        static constexpr const char* About = "True if the leader must skip running the assignment.";
        static const Type Default; // = false;
        
        static constexpr TKafkaVersions PresentVersions = {9, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    SkipAssignmentMeta::Type SkipAssignment;
    
    struct MemberIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "memberId";
        static constexpr const char* About = "The member ID assigned by the group coordinator.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    MemberIdMeta::Type MemberId;
    
    struct MembersMeta {
        using ItemType = TJoinGroupResponseMember;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TJoinGroupResponseMember>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "members";
        static constexpr const char* About = "";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {6, Max<TKafkaVersion>()};
    };
    MembersMeta::Type Members;
    
    i16 ApiKey() const override { return JOIN_GROUP; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TJoinGroupResponseData& other) const = default;
};


class THeartbeatRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<THeartbeatRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 4};
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    
    THeartbeatRequestData();
    ~THeartbeatRequestData() = default;
    
    struct GroupIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "groupId";
        static constexpr const char* About = "The group id.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    GroupIdMeta::Type GroupId;
    
    struct GenerationIdMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "generationId";
        static constexpr const char* About = "The generation of the group.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    GenerationIdMeta::Type GenerationId;
    
    struct MemberIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "memberId";
        static constexpr const char* About = "The member ID.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    MemberIdMeta::Type MemberId;
    
    struct GroupInstanceIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "groupInstanceId";
        static constexpr const char* About = "The unique identifier of the consumer instance provided by end user.";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    GroupInstanceIdMeta::Type GroupInstanceId;
    
    i16 ApiKey() const override { return HEARTBEAT; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const THeartbeatRequestData& other) const = default;
};


class THeartbeatResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<THeartbeatResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 4};
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    
    THeartbeatResponseData();
    ~THeartbeatResponseData() = default;
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct ErrorCodeMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "errorCode";
        static constexpr const char* About = "The error code, or 0 if there was no error.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    ErrorCodeMeta::Type ErrorCode;
    
    i16 ApiKey() const override { return HEARTBEAT; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const THeartbeatResponseData& other) const = default;
};


class TLeaveGroupRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TLeaveGroupRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 5};
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    
    TLeaveGroupRequestData();
    ~TLeaveGroupRequestData() = default;
    
    class TMemberIdentity : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {3, 5};
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        
        TMemberIdentity();
        ~TMemberIdentity() = default;
        
        struct MemberIdMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "memberId";
            static constexpr const char* About = "The member ID to remove from the group.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        MemberIdMeta::Type MemberId;
        
        struct GroupInstanceIdMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "groupInstanceId";
            static constexpr const char* About = "The group instance ID to remove from the group.";
            static const Type Default; // = std::nullopt;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsAlways;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        GroupInstanceIdMeta::Type GroupInstanceId;
        
        struct ReasonMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "reason";
            static constexpr const char* About = "The reason why the member left the group.";
            static const Type Default; // = std::nullopt;
            
            static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsAlways;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        ReasonMeta::Type Reason;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TMemberIdentity& other) const = default;
    };
    
    struct GroupIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "groupId";
        static constexpr const char* About = "The ID of the group to leave.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    GroupIdMeta::Type GroupId;
    
    struct MemberIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "memberId";
        static constexpr const char* About = "The member ID to remove from the group.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = {0, 2};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    MemberIdMeta::Type MemberId;
    
    struct MembersMeta {
        using ItemType = TMemberIdentity;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TMemberIdentity>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "members";
        static constexpr const char* About = "List of leaving member identities.";
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    MembersMeta::Type Members;
    
    i16 ApiKey() const override { return LEAVE_GROUP; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TLeaveGroupRequestData& other) const = default;
};


class TLeaveGroupResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TLeaveGroupResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 5};
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    
    TLeaveGroupResponseData();
    ~TLeaveGroupResponseData() = default;
    
    class TMemberResponse : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {3, 5};
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        
        TMemberResponse();
        ~TMemberResponse() = default;
        
        struct MemberIdMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "memberId";
            static constexpr const char* About = "The member ID to remove from the group.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        MemberIdMeta::Type MemberId;
        
        struct GroupInstanceIdMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "groupInstanceId";
            static constexpr const char* About = "The group instance ID to remove from the group.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsAlways;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        GroupInstanceIdMeta::Type GroupInstanceId;
        
        struct ErrorCodeMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "errorCode";
            static constexpr const char* About = "The error code, or 0 if there was no error.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        ErrorCodeMeta::Type ErrorCode;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TMemberResponse& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct ErrorCodeMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "errorCode";
        static constexpr const char* About = "The error code, or 0 if there was no error.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    ErrorCodeMeta::Type ErrorCode;
    
    struct MembersMeta {
        using ItemType = TMemberResponse;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TMemberResponse>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "members";
        static constexpr const char* About = "List of leaving member responses.";
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    MembersMeta::Type Members;
    
    i16 ApiKey() const override { return LEAVE_GROUP; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TLeaveGroupResponseData& other) const = default;
};


class TSyncGroupRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TSyncGroupRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 5};
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    
    TSyncGroupRequestData();
    ~TSyncGroupRequestData() = default;
    
    class TSyncGroupRequestAssignment : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 5};
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        
        TSyncGroupRequestAssignment();
        ~TSyncGroupRequestAssignment() = default;
        
        struct MemberIdMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "memberId";
            static constexpr const char* About = "The ID of the member to assign.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        MemberIdMeta::Type MemberId;
        
        struct AssignmentMeta {
            using Type = TKafkaBytes;
            using TypeDesc = NPrivate::TKafkaBytesDesc;
            
            static constexpr const char* Name = "assignment";
            static constexpr const char* About = "The member assignment.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        AssignmentMeta::Type Assignment;
        
        TString AssignmentStr;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TSyncGroupRequestAssignment& other) const = default;
    };
    
    struct GroupIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "groupId";
        static constexpr const char* About = "The unique group identifier.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    GroupIdMeta::Type GroupId;
    
    struct GenerationIdMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "generationId";
        static constexpr const char* About = "The generation of the group.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    GenerationIdMeta::Type GenerationId;
    
    struct MemberIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "memberId";
        static constexpr const char* About = "The member ID assigned by the group.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    MemberIdMeta::Type MemberId;
    
    struct GroupInstanceIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "groupInstanceId";
        static constexpr const char* About = "The unique identifier of the consumer instance provided by end user.";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    GroupInstanceIdMeta::Type GroupInstanceId;
    
    struct ProtocolTypeMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "protocolType";
        static constexpr const char* About = "The group protocol type.";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    ProtocolTypeMeta::Type ProtocolType;
    
    struct ProtocolNameMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "protocolName";
        static constexpr const char* About = "The group protocol name.";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    ProtocolNameMeta::Type ProtocolName;
    
    struct AssignmentsMeta {
        using ItemType = TSyncGroupRequestAssignment;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TSyncGroupRequestAssignment>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "assignments";
        static constexpr const char* About = "Each assignment.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    AssignmentsMeta::Type Assignments;
    
    i16 ApiKey() const override { return SYNC_GROUP; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TSyncGroupRequestData& other) const = default;
};


class TSyncGroupResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TSyncGroupResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 5};
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    
    TSyncGroupResponseData();
    ~TSyncGroupResponseData() = default;
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct ErrorCodeMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "errorCode";
        static constexpr const char* About = "The error code, or 0 if there was no error.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    ErrorCodeMeta::Type ErrorCode;
    
    struct ProtocolTypeMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "protocolType";
        static constexpr const char* About = "The group protocol type.";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    ProtocolTypeMeta::Type ProtocolType;
    
    struct ProtocolNameMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "protocolName";
        static constexpr const char* About = "The group protocol name.";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    ProtocolNameMeta::Type ProtocolName;
    
    struct AssignmentMeta {
        using Type = TKafkaBytes;
        using TypeDesc = NPrivate::TKafkaBytesDesc;
        
        static constexpr const char* Name = "assignment";
        static constexpr const char* About = "The member assignment.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    AssignmentMeta::Type Assignment;
    
    TString AssignmentStr;
    
    i16 ApiKey() const override { return SYNC_GROUP; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TSyncGroupResponseData& other) const = default;
};


class TDescribeGroupsRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TDescribeGroupsRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 5};
        static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
    };
    
    TDescribeGroupsRequestData();
    ~TDescribeGroupsRequestData() = default;
    
    struct GroupsMeta {
        using ItemType = TKafkaString;
        using ItemTypeDesc = NPrivate::TKafkaStringDesc;
        using Type = std::vector<TKafkaString>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "groups";
        static constexpr const char* About = "The names of the groups to describe";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
    };
    GroupsMeta::Type Groups;
    
    struct IncludeAuthorizedOperationsMeta {
        using Type = TKafkaBool;
        using TypeDesc = NPrivate::TKafkaBoolDesc;
        
        static constexpr const char* Name = "includeAuthorizedOperations";
        static constexpr const char* About = "Whether to include authorized operations.";
        static const Type Default; // = false;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
    };
    IncludeAuthorizedOperationsMeta::Type IncludeAuthorizedOperations;
    
    i16 ApiKey() const override { return DESCRIBE_GROUPS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TDescribeGroupsRequestData& other) const = default;
};


class TDescribeGroupsResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TDescribeGroupsResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 5};
        static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
    };
    
    TDescribeGroupsResponseData();
    ~TDescribeGroupsResponseData() = default;
    
    class TDescribedGroup : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 5};
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        
        TDescribedGroup();
        ~TDescribedGroup() = default;
        
        class TDescribedGroupMember : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 5};
                static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
            };
            
            TDescribedGroupMember();
            ~TDescribedGroupMember() = default;
            
            struct MemberIdMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "memberId";
                static constexpr const char* About = "The member ID assigned by the group coordinator.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
            };
            MemberIdMeta::Type MemberId;
            
            struct GroupInstanceIdMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "groupInstanceId";
                static constexpr const char* About = "The unique identifier of the consumer instance provided by end user.";
                static const Type Default; // = std::nullopt;
                
                static constexpr TKafkaVersions PresentVersions = {4, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
            };
            GroupInstanceIdMeta::Type GroupInstanceId;
            
            struct ClientIdMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "clientId";
                static constexpr const char* About = "The client ID used in the member's latest join group request.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
            };
            ClientIdMeta::Type ClientId;
            
            struct ClientHostMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "clientHost";
                static constexpr const char* About = "The client host.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
            };
            ClientHostMeta::Type ClientHost;
            
            struct MemberMetadataMeta {
                using Type = TKafkaBytes;
                using TypeDesc = NPrivate::TKafkaBytesDesc;
                
                static constexpr const char* Name = "memberMetadata";
                static constexpr const char* About = "The metadata corresponding to the current group protocol in use.";
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
            };
            MemberMetadataMeta::Type MemberMetadata;
            
            struct MemberAssignmentMeta {
                using Type = TKafkaBytes;
                using TypeDesc = NPrivate::TKafkaBytesDesc;
                
                static constexpr const char* Name = "memberAssignment";
                static constexpr const char* About = "The current assignment provided by the group leader.";
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
            };
            MemberAssignmentMeta::Type MemberAssignment;
            
            TString MemberAssignmentStr;
            
            TString MemberMetadataStr;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TDescribedGroupMember& other) const = default;
        };
        
        struct ErrorCodeMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "errorCode";
            static constexpr const char* About = "The describe error, or 0 if there was no error.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        ErrorCodeMeta::Type ErrorCode;
        
        struct GroupIdMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "groupId";
            static constexpr const char* About = "The group ID string.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        GroupIdMeta::Type GroupId;
        
        struct GroupStateMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "groupState";
            static constexpr const char* About = "The group state string, or the empty string.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        GroupStateMeta::Type GroupState;
        
        struct ProtocolTypeMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "protocolType";
            static constexpr const char* About = "The group protocol type, or the empty string.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        ProtocolTypeMeta::Type ProtocolType;
        
        struct ProtocolDataMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "protocolData";
            static constexpr const char* About = "The group protocol data, or the empty string.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        ProtocolDataMeta::Type ProtocolData;
        
        struct MembersMeta {
            using ItemType = TDescribedGroupMember;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TDescribedGroupMember>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "members";
            static constexpr const char* About = "The group members.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        MembersMeta::Type Members;
        
        struct AuthorizedOperationsMeta {
            using Type = TKafkaInt32;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "authorizedOperations";
            static constexpr const char* About = "32-bit bitfield to represent authorized operations for this group.";
            static const Type Default; // = -2147483648;
            
            static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        AuthorizedOperationsMeta::Type AuthorizedOperations;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TDescribedGroup& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct GroupsMeta {
        using ItemType = TDescribedGroup;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TDescribedGroup>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "groups";
        static constexpr const char* About = "Each described group.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
    };
    GroupsMeta::Type Groups;
    
    i16 ApiKey() const override { return DESCRIBE_GROUPS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TDescribeGroupsResponseData& other) const = default;
};


class TListGroupsRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TListGroupsRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 4};
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    
    TListGroupsRequestData();
    ~TListGroupsRequestData() = default;
    
    struct StatesFilterMeta {
        using ItemType = TKafkaString;
        using ItemTypeDesc = NPrivate::TKafkaStringDesc;
        using Type = std::vector<TKafkaString>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "statesFilter";
        static constexpr const char* About = "The states of the groups we want to list. If empty all groups are returned with their state.";
        
        static constexpr TKafkaVersions PresentVersions = {4, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    StatesFilterMeta::Type StatesFilter;
    
    i16 ApiKey() const override { return LIST_GROUPS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TListGroupsRequestData& other) const = default;
};


class TListGroupsResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TListGroupsResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 4};
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    
    TListGroupsResponseData();
    ~TListGroupsResponseData() = default;
    
    class TListedGroup : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 4};
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        
        TListedGroup();
        ~TListedGroup() = default;
        
        struct GroupIdMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "groupId";
            static constexpr const char* About = "The group ID.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        GroupIdMeta::Type GroupId;
        
        struct ProtocolTypeMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "protocolType";
            static constexpr const char* About = "The group protocol type.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        ProtocolTypeMeta::Type ProtocolType;
        
        struct GroupStateMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "groupState";
            static constexpr const char* About = "The group state name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = {4, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        GroupStateMeta::Type GroupState;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TListedGroup& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct ErrorCodeMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "errorCode";
        static constexpr const char* About = "The error code, or 0 if there was no error.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ErrorCodeMeta::Type ErrorCode;
    
    struct GroupsMeta {
        using ItemType = TListedGroup;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TListedGroup>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "groups";
        static constexpr const char* About = "Each group in the response.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    GroupsMeta::Type Groups;
    
    i16 ApiKey() const override { return LIST_GROUPS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TListGroupsResponseData& other) const = default;
};


class TSaslHandshakeRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TSaslHandshakeRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 1};
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    
    TSaslHandshakeRequestData();
    ~TSaslHandshakeRequestData() = default;
    
    struct MechanismMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "mechanism";
        static constexpr const char* About = "The SASL mechanism chosen by the client.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    MechanismMeta::Type Mechanism;
    
    i16 ApiKey() const override { return SASL_HANDSHAKE; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TSaslHandshakeRequestData& other) const = default;
};


class TSaslHandshakeResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TSaslHandshakeResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 1};
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    
    TSaslHandshakeResponseData();
    ~TSaslHandshakeResponseData() = default;
    
    struct ErrorCodeMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "errorCode";
        static constexpr const char* About = "The error code, or 0 if there was no error.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    ErrorCodeMeta::Type ErrorCode;
    
    struct MechanismsMeta {
        using ItemType = TKafkaString;
        using ItemTypeDesc = NPrivate::TKafkaStringDesc;
        using Type = std::vector<TKafkaString>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "mechanisms";
        static constexpr const char* About = "The mechanisms enabled in the server.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsNever;
    };
    MechanismsMeta::Type Mechanisms;
    
    i16 ApiKey() const override { return SASL_HANDSHAKE; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TSaslHandshakeResponseData& other) const = default;
};


class TApiVersionsRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TApiVersionsRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    
    TApiVersionsRequestData();
    ~TApiVersionsRequestData() = default;
    
    struct ClientSoftwareNameMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "clientSoftwareName";
        static constexpr const char* About = "The name of the client.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    ClientSoftwareNameMeta::Type ClientSoftwareName;
    
    struct ClientSoftwareVersionMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "clientSoftwareVersion";
        static constexpr const char* About = "The version of the client.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    ClientSoftwareVersionMeta::Type ClientSoftwareVersion;
    
    i16 ApiKey() const override { return API_VERSIONS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TApiVersionsRequestData& other) const = default;
};


class TApiVersionsResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TApiVersionsResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    
    TApiVersionsResponseData();
    ~TApiVersionsResponseData() = default;
    
    class TApiVersion : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 3};
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        
        TApiVersion();
        ~TApiVersion() = default;
        
        struct ApiKeyMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "apiKey";
            static constexpr const char* About = "The API index.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        ApiKeyMeta::Type ApiKey;
        
        struct MinVersionMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "minVersion";
            static constexpr const char* About = "The minimum supported version, inclusive.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        MinVersionMeta::Type MinVersion;
        
        struct MaxVersionMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "maxVersion";
            static constexpr const char* About = "The maximum supported version, inclusive.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        MaxVersionMeta::Type MaxVersion;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TApiVersion& other) const = default;
    };
    
    class TSupportedFeatureKey : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {3, 3};
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        
        TSupportedFeatureKey();
        ~TSupportedFeatureKey() = default;
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The name of the feature.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        NameMeta::Type Name;
        
        struct MinVersionMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "minVersion";
            static constexpr const char* About = "The minimum supported version for the feature.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        MinVersionMeta::Type MinVersion;
        
        struct MaxVersionMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "maxVersion";
            static constexpr const char* About = "The maximum supported version for the feature.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        MaxVersionMeta::Type MaxVersion;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TSupportedFeatureKey& other) const = default;
    };
    
    class TFinalizedFeatureKey : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {3, 3};
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        
        TFinalizedFeatureKey();
        ~TFinalizedFeatureKey() = default;
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The name of the feature.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        NameMeta::Type Name;
        
        struct MaxVersionLevelMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "maxVersionLevel";
            static constexpr const char* About = "The cluster-wide finalized max version level for the feature.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        MaxVersionLevelMeta::Type MaxVersionLevel;
        
        struct MinVersionLevelMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "minVersionLevel";
            static constexpr const char* About = "The cluster-wide finalized min version level for the feature.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        MinVersionLevelMeta::Type MinVersionLevel;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TFinalizedFeatureKey& other) const = default;
    };
    
    struct ErrorCodeMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "errorCode";
        static constexpr const char* About = "The top-level error code.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ErrorCodeMeta::Type ErrorCode;
    
    struct ApiKeysMeta {
        using ItemType = TApiVersion;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TApiVersion>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "apiKeys";
        static constexpr const char* About = "The APIs supported by the broker.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ApiKeysMeta::Type ApiKeys;
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct SupportedFeaturesMeta {
        using ItemType = TSupportedFeatureKey;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TSupportedFeatureKey>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "supportedFeatures";
        static constexpr const char* About = "Features supported by the broker.";
        static constexpr const TKafkaInt32 Tag = 0;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsAlways;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    SupportedFeaturesMeta::Type SupportedFeatures;
    
    struct FinalizedFeaturesEpochMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "finalizedFeaturesEpoch";
        static constexpr const char* About = "The monotonically increasing epoch for the finalized features information. Valid values are >= 0. A value of -1 is special and represents unknown epoch.";
        static constexpr const TKafkaInt32 Tag = 1;
        static const Type Default; // = -1;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsAlways;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    FinalizedFeaturesEpochMeta::Type FinalizedFeaturesEpoch;
    
    struct FinalizedFeaturesMeta {
        using ItemType = TFinalizedFeatureKey;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TFinalizedFeatureKey>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "finalizedFeatures";
        static constexpr const char* About = "List of cluster-wide finalized features. The information is valid only if FinalizedFeaturesEpoch >= 0.";
        static constexpr const TKafkaInt32 Tag = 2;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsAlways;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    FinalizedFeaturesMeta::Type FinalizedFeatures;
    
    struct ZkMigrationReadyMeta {
        using Type = TKafkaBool;
        using TypeDesc = NPrivate::TKafkaBoolDesc;
        
        static constexpr const char* Name = "zkMigrationReady";
        static constexpr const char* About = "Set by a KRaft controller if the required configurations for ZK migration are present";
        static constexpr const TKafkaInt32 Tag = 3;
        static const Type Default; // = false;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsAlways;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    ZkMigrationReadyMeta::Type ZkMigrationReady;
    
    i16 ApiKey() const override { return API_VERSIONS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TApiVersionsResponseData& other) const = default;
};


class TCreateTopicsRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TCreateTopicsRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 7};
        static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
    };
    
    TCreateTopicsRequestData();
    ~TCreateTopicsRequestData() = default;
    
    class TCreatableTopic : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 7};
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        
        TCreatableTopic();
        ~TCreatableTopic() = default;
        
        class TCreatableReplicaAssignment : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 7};
                static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
            };
            
            TCreatableReplicaAssignment();
            ~TCreatableReplicaAssignment() = default;
            
            struct PartitionIndexMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "partitionIndex";
                static constexpr const char* About = "The partition index.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
            };
            PartitionIndexMeta::Type PartitionIndex;
            
            struct BrokerIdsMeta {
                using ItemType = TKafkaInt32;
                using ItemTypeDesc = NPrivate::TKafkaIntDesc;
                using Type = std::vector<TKafkaInt32>;
                using TypeDesc = NPrivate::TKafkaArrayDesc;
                
                static constexpr const char* Name = "brokerIds";
                static constexpr const char* About = "The brokers to place the partition on.";
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
            };
            BrokerIdsMeta::Type BrokerIds;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TCreatableReplicaAssignment& other) const = default;
        };
        
        class TCreateableTopicConfig : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 7};
                static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
            };
            
            TCreateableTopicConfig();
            ~TCreateableTopicConfig() = default;
            
            struct NameMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "name";
                static constexpr const char* About = "The configuration name.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
            };
            NameMeta::Type Name;
            
            struct ValueMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "value";
                static constexpr const char* About = "The configuration value.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
            };
            ValueMeta::Type Value;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TCreateableTopicConfig& other) const = default;
        };
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct NumPartitionsMeta {
            using Type = TKafkaInt32;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "numPartitions";
            static constexpr const char* About = "The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        NumPartitionsMeta::Type NumPartitions;
        
        struct ReplicationFactorMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "replicationFactor";
            static constexpr const char* About = "The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        ReplicationFactorMeta::Type ReplicationFactor;
        
        struct AssignmentsMeta {
            using ItemType = TCreatableReplicaAssignment;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TCreatableReplicaAssignment>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "assignments";
            static constexpr const char* About = "The manual partition assignment, or the empty array if we are using automatic assignment.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        AssignmentsMeta::Type Assignments;
        
        struct ConfigsMeta {
            using ItemType = TCreateableTopicConfig;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TCreateableTopicConfig>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "configs";
            static constexpr const char* About = "The custom topic configurations to set.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        ConfigsMeta::Type Configs;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TCreatableTopic& other) const = default;
    };
    
    struct TopicsMeta {
        using ItemType = TCreatableTopic;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TCreatableTopic>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "The topics to create.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    struct TimeoutMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "timeoutMs";
        static constexpr const char* About = "How long to wait in milliseconds before timing out the request.";
        static const Type Default; // = 60000;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
    };
    TimeoutMsMeta::Type TimeoutMs;
    
    struct ValidateOnlyMeta {
        using Type = TKafkaBool;
        using TypeDesc = NPrivate::TKafkaBoolDesc;
        
        static constexpr const char* Name = "validateOnly";
        static constexpr const char* About = "If true, check that the topics can be created as specified, but don't create anything.";
        static const Type Default; // = false;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
    };
    ValidateOnlyMeta::Type ValidateOnly;
    
    i16 ApiKey() const override { return CREATE_TOPICS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TCreateTopicsRequestData& other) const = default;
};


class TCreateTopicsResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TCreateTopicsResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 7};
        static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
    };
    
    TCreateTopicsResponseData();
    ~TCreateTopicsResponseData() = default;
    
    class TCreatableTopicResult : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 7};
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        
        TCreatableTopicResult();
        ~TCreatableTopicResult() = default;
        
        class TCreatableTopicConfigs : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {5, 7};
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            
            TCreatableTopicConfigs();
            ~TCreatableTopicConfigs() = default;
            
            struct NameMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "name";
                static constexpr const char* About = "The configuration name.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            NameMeta::Type Name;
            
            struct ValueMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "value";
                static constexpr const char* About = "The configuration value.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            ValueMeta::Type Value;
            
            struct ReadOnlyMeta {
                using Type = TKafkaBool;
                using TypeDesc = NPrivate::TKafkaBoolDesc;
                
                static constexpr const char* Name = "readOnly";
                static constexpr const char* About = "True if the configuration is read-only.";
                static const Type Default; // = false;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            ReadOnlyMeta::Type ReadOnly;
            
            struct ConfigSourceMeta {
                using Type = TKafkaInt8;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "configSource";
                static constexpr const char* About = "The configuration source.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            ConfigSourceMeta::Type ConfigSource;
            
            struct IsSensitiveMeta {
                using Type = TKafkaBool;
                using TypeDesc = NPrivate::TKafkaBoolDesc;
                
                static constexpr const char* Name = "isSensitive";
                static constexpr const char* About = "True if this configuration is sensitive.";
                static const Type Default; // = false;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
            };
            IsSensitiveMeta::Type IsSensitive;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TCreatableTopicConfigs& other) const = default;
        };
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct TopicIdMeta {
            using Type = TKafkaUuid;
            using TypeDesc = NPrivate::TKafkaUuidDesc;
            
            static constexpr const char* Name = "topicId";
            static constexpr const char* About = "The unique topic ID";
            static const Type Default; // = TKafkaUuid(0, 0);
            
            static constexpr TKafkaVersions PresentVersions = {7, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        TopicIdMeta::Type TopicId;
        
        struct ErrorCodeMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "errorCode";
            static constexpr const char* About = "The error code, or 0 if there was no error.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        ErrorCodeMeta::Type ErrorCode;
        
        struct ErrorMessageMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "errorMessage";
            static constexpr const char* About = "The error message, or null if there was no error.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsAlways;
            static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
        };
        ErrorMessageMeta::Type ErrorMessage;
        
        struct TopicConfigErrorCodeMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "topicConfigErrorCode";
            static constexpr const char* About = "Optional topic config error returned if configs are not returned in the response.";
            static constexpr const TKafkaInt32 Tag = 0;
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsAlways;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        TopicConfigErrorCodeMeta::Type TopicConfigErrorCode;
        
        struct NumPartitionsMeta {
            using Type = TKafkaInt32;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "numPartitions";
            static constexpr const char* About = "Number of partitions of the topic.";
            static const Type Default; // = -1;
            
            static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        NumPartitionsMeta::Type NumPartitions;
        
        struct ReplicationFactorMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "replicationFactor";
            static constexpr const char* About = "Replication factor of the topic.";
            static const Type Default; // = -1;
            
            static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        ReplicationFactorMeta::Type ReplicationFactor;
        
        struct ConfigsMeta {
            using ItemType = TCreatableTopicConfigs;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TCreatableTopicConfigs>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "configs";
            static constexpr const char* About = "Configuration of the topic.";
            
            static constexpr TKafkaVersions PresentVersions = {5, Max<TKafkaVersion>()};
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsAlways;
            static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
        };
        ConfigsMeta::Type Configs;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TCreatableTopicResult& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {2, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct TopicsMeta {
        using ItemType = TCreatableTopicResult;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TCreatableTopicResult>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "Results for each topic we tried to create.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {5, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    i16 ApiKey() const override { return CREATE_TOPICS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TCreateTopicsResponseData& other) const = default;
};


class TInitProducerIdRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TInitProducerIdRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 4};
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    
    TInitProducerIdRequestData();
    ~TInitProducerIdRequestData() = default;
    
    struct TransactionalIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "transactionalId";
        static constexpr const char* About = "The transactional id, or null if the producer is not transactional.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    TransactionalIdMeta::Type TransactionalId;
    
    struct TransactionTimeoutMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "transactionTimeoutMs";
        static constexpr const char* About = "The time in ms to wait before aborting idle transactions sent by this producer. This is only relevant if a TransactionalId has been defined.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    TransactionTimeoutMsMeta::Type TransactionTimeoutMs;
    
    struct ProducerIdMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "producerId";
        static constexpr const char* About = "The producer id. This is used to disambiguate requests if a transactional id is reused following its expiration.";
        static const Type Default; // = -1;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    ProducerIdMeta::Type ProducerId;
    
    struct ProducerEpochMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "producerEpoch";
        static constexpr const char* About = "The producer's current epoch. This will be checked against the producer epoch on the broker, and the request will return an error if they do not match.";
        static const Type Default; // = -1;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    ProducerEpochMeta::Type ProducerEpoch;
    
    i16 ApiKey() const override { return INIT_PRODUCER_ID; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TInitProducerIdRequestData& other) const = default;
};


class TInitProducerIdResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TInitProducerIdResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 4};
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    
    TInitProducerIdResponseData();
    ~TInitProducerIdResponseData() = default;
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct ErrorCodeMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "errorCode";
        static constexpr const char* About = "The error code, or 0 if there was no error.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    ErrorCodeMeta::Type ErrorCode;
    
    struct ProducerIdMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "producerId";
        static constexpr const char* About = "The current producer id.";
        static const Type Default; // = -1;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    ProducerIdMeta::Type ProducerId;
    
    struct ProducerEpochMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "producerEpoch";
        static constexpr const char* About = "The current epoch associated with the producer id.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    ProducerEpochMeta::Type ProducerEpoch;
    
    i16 ApiKey() const override { return INIT_PRODUCER_ID; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TInitProducerIdResponseData& other) const = default;
};


class TAddPartitionsToTxnRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TAddPartitionsToTxnRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    
    TAddPartitionsToTxnRequestData();
    ~TAddPartitionsToTxnRequestData() = default;
    
    class TAddPartitionsToTxnTopic : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 3};
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        
        TAddPartitionsToTxnTopic();
        ~TAddPartitionsToTxnTopic() = default;
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The name of the topic.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct PartitionsMeta {
            using ItemType = TKafkaInt32;
            using ItemTypeDesc = NPrivate::TKafkaIntDesc;
            using Type = std::vector<TKafkaInt32>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitions";
            static constexpr const char* About = "The partition indexes to add to the transaction";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        PartitionsMeta::Type Partitions;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TAddPartitionsToTxnTopic& other) const = default;
    };
    
    struct TransactionalIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "transactionalId";
        static constexpr const char* About = "The transactional id corresponding to the transaction.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    TransactionalIdMeta::Type TransactionalId;
    
    struct ProducerIdMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "producerId";
        static constexpr const char* About = "Current producer id in use by the transactional id.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ProducerIdMeta::Type ProducerId;
    
    struct ProducerEpochMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "producerEpoch";
        static constexpr const char* About = "Current epoch associated with the producer id.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ProducerEpochMeta::Type ProducerEpoch;
    
    struct TopicsMeta {
        using ItemType = TAddPartitionsToTxnTopic;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TAddPartitionsToTxnTopic>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "The partitions to add to the transaction.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    i16 ApiKey() const override { return ADD_PARTITIONS_TO_TXN; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TAddPartitionsToTxnRequestData& other) const = default;
};


class TAddPartitionsToTxnResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TAddPartitionsToTxnResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    
    TAddPartitionsToTxnResponseData();
    ~TAddPartitionsToTxnResponseData() = default;
    
    class TAddPartitionsToTxnTopicResult : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 3};
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        
        TAddPartitionsToTxnTopicResult();
        ~TAddPartitionsToTxnTopicResult() = default;
        
        class TAddPartitionsToTxnPartitionResult : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 3};
                static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
            };
            
            TAddPartitionsToTxnPartitionResult();
            ~TAddPartitionsToTxnPartitionResult() = default;
            
            struct PartitionIndexMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "partitionIndex";
                static constexpr const char* About = "The partition indexes.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
            };
            PartitionIndexMeta::Type PartitionIndex;
            
            struct ErrorCodeMeta {
                using Type = TKafkaInt16;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "errorCode";
                static constexpr const char* About = "The response error code.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
            };
            ErrorCodeMeta::Type ErrorCode;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TAddPartitionsToTxnPartitionResult& other) const = default;
        };
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct ResultsMeta {
            using ItemType = TAddPartitionsToTxnPartitionResult;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TAddPartitionsToTxnPartitionResult>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "results";
            static constexpr const char* About = "The results for each partition";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        ResultsMeta::Type Results;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TAddPartitionsToTxnTopicResult& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct ResultsMeta {
        using ItemType = TAddPartitionsToTxnTopicResult;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TAddPartitionsToTxnTopicResult>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "results";
        static constexpr const char* About = "The results for each topic.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ResultsMeta::Type Results;
    
    i16 ApiKey() const override { return ADD_PARTITIONS_TO_TXN; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TAddPartitionsToTxnResponseData& other) const = default;
};


class TAddOffsetsToTxnRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TAddOffsetsToTxnRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    
    TAddOffsetsToTxnRequestData();
    ~TAddOffsetsToTxnRequestData() = default;
    
    struct TransactionalIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "transactionalId";
        static constexpr const char* About = "The transactional id corresponding to the transaction.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    TransactionalIdMeta::Type TransactionalId;
    
    struct ProducerIdMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "producerId";
        static constexpr const char* About = "Current producer id in use by the transactional id.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ProducerIdMeta::Type ProducerId;
    
    struct ProducerEpochMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "producerEpoch";
        static constexpr const char* About = "Current epoch associated with the producer id.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ProducerEpochMeta::Type ProducerEpoch;
    
    struct GroupIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "groupId";
        static constexpr const char* About = "The unique group identifier.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    GroupIdMeta::Type GroupId;
    
    i16 ApiKey() const override { return ADD_OFFSETS_TO_TXN; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TAddOffsetsToTxnRequestData& other) const = default;
};


class TAddOffsetsToTxnResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TAddOffsetsToTxnResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    
    TAddOffsetsToTxnResponseData();
    ~TAddOffsetsToTxnResponseData() = default;
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct ErrorCodeMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "errorCode";
        static constexpr const char* About = "The response error code, or 0 if there was no error.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ErrorCodeMeta::Type ErrorCode;
    
    i16 ApiKey() const override { return ADD_OFFSETS_TO_TXN; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TAddOffsetsToTxnResponseData& other) const = default;
};


class TEndTxnRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TEndTxnRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    
    TEndTxnRequestData();
    ~TEndTxnRequestData() = default;
    
    struct TransactionalIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "transactionalId";
        static constexpr const char* About = "The ID of the transaction to end.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    TransactionalIdMeta::Type TransactionalId;
    
    struct ProducerIdMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "producerId";
        static constexpr const char* About = "The producer ID.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ProducerIdMeta::Type ProducerId;
    
    struct ProducerEpochMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "producerEpoch";
        static constexpr const char* About = "The current epoch associated with the producer.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ProducerEpochMeta::Type ProducerEpoch;
    
    struct CommittedMeta {
        using Type = TKafkaBool;
        using TypeDesc = NPrivate::TKafkaBoolDesc;
        
        static constexpr const char* Name = "committed";
        static constexpr const char* About = "True if the transaction was committed, false if it was aborted.";
        static const Type Default; // = false;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    CommittedMeta::Type Committed;
    
    i16 ApiKey() const override { return END_TXN; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TEndTxnRequestData& other) const = default;
};


class TEndTxnResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TEndTxnResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    
    TEndTxnResponseData();
    ~TEndTxnResponseData() = default;
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct ErrorCodeMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "errorCode";
        static constexpr const char* About = "The error code, or 0 if there was no error.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ErrorCodeMeta::Type ErrorCode;
    
    i16 ApiKey() const override { return END_TXN; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TEndTxnResponseData& other) const = default;
};


class TTxnOffsetCommitRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TTxnOffsetCommitRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    
    TTxnOffsetCommitRequestData();
    ~TTxnOffsetCommitRequestData() = default;
    
    class TTxnOffsetCommitRequestTopic : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 3};
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        
        TTxnOffsetCommitRequestTopic();
        ~TTxnOffsetCommitRequestTopic() = default;
        
        class TTxnOffsetCommitRequestPartition : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 3};
                static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
            };
            
            TTxnOffsetCommitRequestPartition();
            ~TTxnOffsetCommitRequestPartition() = default;
            
            struct PartitionIndexMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "partitionIndex";
                static constexpr const char* About = "The index of the partition within the topic.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
            };
            PartitionIndexMeta::Type PartitionIndex;
            
            struct CommittedOffsetMeta {
                using Type = TKafkaInt64;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "committedOffset";
                static constexpr const char* About = "The message offset to be committed.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
            };
            CommittedOffsetMeta::Type CommittedOffset;
            
            struct CommittedLeaderEpochMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "committedLeaderEpoch";
                static constexpr const char* About = "The leader epoch of the last consumed record.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {2, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
            };
            CommittedLeaderEpochMeta::Type CommittedLeaderEpoch;
            
            struct CommittedMetadataMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "committedMetadata";
                static constexpr const char* About = "Any associated metadata the client wants to keep.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
            };
            CommittedMetadataMeta::Type CommittedMetadata;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TTxnOffsetCommitRequestPartition& other) const = default;
        };
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct PartitionsMeta {
            using ItemType = TTxnOffsetCommitRequestPartition;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TTxnOffsetCommitRequestPartition>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitions";
            static constexpr const char* About = "The partitions inside the topic that we want to committ offsets for.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        PartitionsMeta::Type Partitions;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TTxnOffsetCommitRequestTopic& other) const = default;
    };
    
    struct TransactionalIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "transactionalId";
        static constexpr const char* About = "The ID of the transaction.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    TransactionalIdMeta::Type TransactionalId;
    
    struct GroupIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "groupId";
        static constexpr const char* About = "The ID of the group.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    GroupIdMeta::Type GroupId;
    
    struct ProducerIdMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "producerId";
        static constexpr const char* About = "The current producer ID in use by the transactional ID.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ProducerIdMeta::Type ProducerId;
    
    struct ProducerEpochMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "producerEpoch";
        static constexpr const char* About = "The current epoch associated with the producer ID.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ProducerEpochMeta::Type ProducerEpoch;
    
    struct GenerationIdMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "generationId";
        static constexpr const char* About = "The generation of the consumer.";
        static const Type Default; // = -1;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    GenerationIdMeta::Type GenerationId;
    
    struct MemberIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "memberId";
        static constexpr const char* About = "The member ID assigned by the group coordinator.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    MemberIdMeta::Type MemberId;
    
    struct GroupInstanceIdMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "groupInstanceId";
        static constexpr const char* About = "The unique identifier of the consumer instance provided by end user.";
        static const Type Default; // = std::nullopt;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = VersionsAlways;
    };
    GroupInstanceIdMeta::Type GroupInstanceId;
    
    struct TopicsMeta {
        using ItemType = TTxnOffsetCommitRequestTopic;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TTxnOffsetCommitRequestTopic>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "Each topic that we want to commit offsets for.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    i16 ApiKey() const override { return TXN_OFFSET_COMMIT; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TTxnOffsetCommitRequestData& other) const = default;
};


class TTxnOffsetCommitResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TTxnOffsetCommitResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    
    TTxnOffsetCommitResponseData();
    ~TTxnOffsetCommitResponseData() = default;
    
    class TTxnOffsetCommitResponseTopic : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 3};
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        
        TTxnOffsetCommitResponseTopic();
        ~TTxnOffsetCommitResponseTopic() = default;
        
        class TTxnOffsetCommitResponsePartition : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 3};
                static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
            };
            
            TTxnOffsetCommitResponsePartition();
            ~TTxnOffsetCommitResponsePartition() = default;
            
            struct PartitionIndexMeta {
                using Type = TKafkaInt32;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "partitionIndex";
                static constexpr const char* About = "The partition index.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
            };
            PartitionIndexMeta::Type PartitionIndex;
            
            struct ErrorCodeMeta {
                using Type = TKafkaInt16;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "errorCode";
                static constexpr const char* About = "The error code, or 0 if there was no error.";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
            };
            ErrorCodeMeta::Type ErrorCode;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TTxnOffsetCommitResponsePartition& other) const = default;
        };
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct PartitionsMeta {
            using ItemType = TTxnOffsetCommitResponsePartition;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TTxnOffsetCommitResponsePartition>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "partitions";
            static constexpr const char* About = "The responses for each partition in the topic.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
        };
        PartitionsMeta::Type Partitions;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TTxnOffsetCommitResponseTopic& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct TopicsMeta {
        using ItemType = TTxnOffsetCommitResponseTopic;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TTxnOffsetCommitResponseTopic>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "The responses for each topic.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {3, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    i16 ApiKey() const override { return TXN_OFFSET_COMMIT; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TTxnOffsetCommitResponseData& other) const = default;
};


class TDescribeConfigsRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TDescribeConfigsRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 4};
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    
    TDescribeConfigsRequestData();
    ~TDescribeConfigsRequestData() = default;
    
    class TDescribeConfigsResource : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 4};
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        
        TDescribeConfigsResource();
        ~TDescribeConfigsResource() = default;
        
        struct ResourceTypeMeta {
            using Type = TKafkaInt8;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "resourceType";
            static constexpr const char* About = "The resource type.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        ResourceTypeMeta::Type ResourceType;
        
        struct ResourceNameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "resourceName";
            static constexpr const char* About = "The resource name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        ResourceNameMeta::Type ResourceName;
        
        struct ConfigurationKeysMeta {
            using ItemType = TKafkaString;
            using ItemTypeDesc = NPrivate::TKafkaStringDesc;
            using Type = std::vector<TKafkaString>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "configurationKeys";
            static constexpr const char* About = "The configuration keys to list, or null to list all configuration keys.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsAlways;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        ConfigurationKeysMeta::Type ConfigurationKeys;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TDescribeConfigsResource& other) const = default;
    };
    
    struct ResourcesMeta {
        using ItemType = TDescribeConfigsResource;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TDescribeConfigsResource>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "resources";
        static constexpr const char* About = "The resources whose configurations we want to describe.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    ResourcesMeta::Type Resources;
    
    struct IncludeSynonymsMeta {
        using Type = TKafkaBool;
        using TypeDesc = NPrivate::TKafkaBoolDesc;
        
        static constexpr const char* Name = "includeSynonyms";
        static constexpr const char* About = "True if we should include all synonyms.";
        static const Type Default; // = false;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    IncludeSynonymsMeta::Type IncludeSynonyms;
    
    struct IncludeDocumentationMeta {
        using Type = TKafkaBool;
        using TypeDesc = NPrivate::TKafkaBoolDesc;
        
        static constexpr const char* Name = "includeDocumentation";
        static constexpr const char* About = "True if we should include configuration documentation.";
        static const Type Default; // = false;
        
        static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    IncludeDocumentationMeta::Type IncludeDocumentation;
    
    i16 ApiKey() const override { return DESCRIBE_CONFIGS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TDescribeConfigsRequestData& other) const = default;
};


class TDescribeConfigsResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TDescribeConfigsResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 4};
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    
    TDescribeConfigsResponseData();
    ~TDescribeConfigsResponseData() = default;
    
    class TDescribeConfigsResult : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 4};
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        
        TDescribeConfigsResult();
        ~TDescribeConfigsResult() = default;
        
        class TDescribeConfigsResourceResult : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 4};
                static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
            };
            
            TDescribeConfigsResourceResult();
            ~TDescribeConfigsResourceResult() = default;
            
            class TDescribeConfigsSynonym : public TMessage {
            public:
                struct MessageMeta {
                    static constexpr TKafkaVersions PresentVersions = {1, 4};
                    static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
                };
                
                TDescribeConfigsSynonym();
                ~TDescribeConfigsSynonym() = default;
                
                struct NameMeta {
                    using Type = TKafkaString;
                    using TypeDesc = NPrivate::TKafkaStringDesc;
                    
                    static constexpr const char* Name = "name";
                    static constexpr const char* About = "The synonym name.";
                    static const Type Default; // = {""};
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
                };
                NameMeta::Type Name;
                
                struct ValueMeta {
                    using Type = TKafkaString;
                    using TypeDesc = NPrivate::TKafkaStringDesc;
                    
                    static constexpr const char* Name = "value";
                    static constexpr const char* About = "The synonym value.";
                    static const Type Default; // = {""};
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                    static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
                };
                ValueMeta::Type Value;
                
                struct SourceMeta {
                    using Type = TKafkaInt8;
                    using TypeDesc = NPrivate::TKafkaIntDesc;
                    
                    static constexpr const char* Name = "source";
                    static constexpr const char* About = "The synonym source.";
                    static const Type Default; // = 0;
                    
                    static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                    static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                    static constexpr TKafkaVersions NullableVersions = VersionsNever;
                    static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
                };
                SourceMeta::Type Source;
                
                i32 Size(TKafkaVersion version) const override;
                void Read(TKafkaReadable& readable, TKafkaVersion version) override;
                void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
                
                bool operator==(const TDescribeConfigsSynonym& other) const = default;
            };
            
            struct NameMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "name";
                static constexpr const char* About = "The configuration name.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
            };
            NameMeta::Type Name;
            
            struct ValueMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "value";
                static constexpr const char* About = "The configuration value.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
            };
            ValueMeta::Type Value;
            
            struct ReadOnlyMeta {
                using Type = TKafkaBool;
                using TypeDesc = NPrivate::TKafkaBoolDesc;
                
                static constexpr const char* Name = "readOnly";
                static constexpr const char* About = "True if the configuration is read-only.";
                static const Type Default; // = false;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
            };
            ReadOnlyMeta::Type ReadOnly;
            
            struct IsDefaultMeta {
                using Type = TKafkaBool;
                using TypeDesc = NPrivate::TKafkaBoolDesc;
                
                static constexpr const char* Name = "isDefault";
                static constexpr const char* About = "True if the configuration is not set.";
                static const Type Default; // = false;
                
                static constexpr TKafkaVersions PresentVersions = {0, 0};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
            };
            IsDefaultMeta::Type IsDefault;
            
            struct ConfigSourceMeta {
                using Type = TKafkaInt8;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "configSource";
                static constexpr const char* About = "The configuration source.";
                static const Type Default; // = -1;
                
                static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
            };
            ConfigSourceMeta::Type ConfigSource;
            
            struct IsSensitiveMeta {
                using Type = TKafkaBool;
                using TypeDesc = NPrivate::TKafkaBoolDesc;
                
                static constexpr const char* Name = "isSensitive";
                static constexpr const char* About = "True if this configuration is sensitive.";
                static const Type Default; // = false;
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
            };
            IsSensitiveMeta::Type IsSensitive;
            
            struct SynonymsMeta {
                using ItemType = TDescribeConfigsSynonym;
                using ItemTypeDesc = NPrivate::TKafkaStructDesc;
                using Type = std::vector<TDescribeConfigsSynonym>;
                using TypeDesc = NPrivate::TKafkaArrayDesc;
                
                static constexpr const char* Name = "synonyms";
                static constexpr const char* About = "The synonyms for this configuration key.";
                
                static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
            };
            SynonymsMeta::Type Synonyms;
            
            struct ConfigTypeMeta {
                using Type = TKafkaInt8;
                using TypeDesc = NPrivate::TKafkaIntDesc;
                
                static constexpr const char* Name = "configType";
                static constexpr const char* About = "The configuration data type. Type can be one of the following values - BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD";
                static const Type Default; // = 0;
                
                static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
            };
            ConfigTypeMeta::Type ConfigType;
            
            struct DocumentationMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "documentation";
                static constexpr const char* About = "The configuration documentation.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = {3, Max<TKafkaVersion>()};
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
            };
            DocumentationMeta::Type Documentation;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TDescribeConfigsResourceResult& other) const = default;
        };
        
        struct ErrorCodeMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "errorCode";
            static constexpr const char* About = "The error code, or 0 if we were able to successfully describe the configurations.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        ErrorCodeMeta::Type ErrorCode;
        
        struct ErrorMessageMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "errorMessage";
            static constexpr const char* About = "The error message, or null if we were able to successfully describe the configurations.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsAlways;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        ErrorMessageMeta::Type ErrorMessage;
        
        struct ResourceTypeMeta {
            using Type = TKafkaInt8;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "resourceType";
            static constexpr const char* About = "The resource type.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        ResourceTypeMeta::Type ResourceType;
        
        struct ResourceNameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "resourceName";
            static constexpr const char* About = "The resource name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        ResourceNameMeta::Type ResourceName;
        
        struct ConfigsMeta {
            using ItemType = TDescribeConfigsResourceResult;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TDescribeConfigsResourceResult>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "configs";
            static constexpr const char* About = "Each listed configuration.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
        };
        ConfigsMeta::Type Configs;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TDescribeConfigsResult& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct ResultsMeta {
        using ItemType = TDescribeConfigsResult;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TDescribeConfigsResult>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "results";
        static constexpr const char* About = "The results for each resource.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {4, Max<TKafkaVersion>()};
    };
    ResultsMeta::Type Results;
    
    i16 ApiKey() const override { return DESCRIBE_CONFIGS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TDescribeConfigsResponseData& other) const = default;
};


class TAlterConfigsRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TAlterConfigsRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 2};
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    
    TAlterConfigsRequestData();
    ~TAlterConfigsRequestData() = default;
    
    class TAlterConfigsResource : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 2};
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        
        TAlterConfigsResource();
        ~TAlterConfigsResource() = default;
        
        class TAlterableConfig : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 2};
                static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
            };
            
            TAlterableConfig();
            ~TAlterableConfig() = default;
            
            struct NameMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "name";
                static constexpr const char* About = "The configuration key name.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
            };
            NameMeta::Type Name;
            
            struct ValueMeta {
                using Type = TKafkaString;
                using TypeDesc = NPrivate::TKafkaStringDesc;
                
                static constexpr const char* Name = "value";
                static constexpr const char* About = "The value to set for the configuration key.";
                static const Type Default; // = {""};
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsAlways;
                static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
            };
            ValueMeta::Type Value;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TAlterableConfig& other) const = default;
        };
        
        struct ResourceTypeMeta {
            using Type = TKafkaInt8;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "resourceType";
            static constexpr const char* About = "The resource type.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        ResourceTypeMeta::Type ResourceType;
        
        struct ResourceNameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "resourceName";
            static constexpr const char* About = "The resource name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        ResourceNameMeta::Type ResourceName;
        
        struct ConfigsMeta {
            using ItemType = TAlterableConfig;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TAlterableConfig>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "configs";
            static constexpr const char* About = "The configurations.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        ConfigsMeta::Type Configs;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TAlterConfigsResource& other) const = default;
    };
    
    struct ResourcesMeta {
        using ItemType = TAlterConfigsResource;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TAlterConfigsResource>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "resources";
        static constexpr const char* About = "The updates for each resource.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    ResourcesMeta::Type Resources;
    
    struct ValidateOnlyMeta {
        using Type = TKafkaBool;
        using TypeDesc = NPrivate::TKafkaBoolDesc;
        
        static constexpr const char* Name = "validateOnly";
        static constexpr const char* About = "True if we should validate the request, but not change the configurations.";
        static const Type Default; // = false;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    ValidateOnlyMeta::Type ValidateOnly;
    
    i16 ApiKey() const override { return ALTER_CONFIGS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TAlterConfigsRequestData& other) const = default;
};


class TAlterConfigsResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TAlterConfigsResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 2};
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    
    TAlterConfigsResponseData();
    ~TAlterConfigsResponseData() = default;
    
    class TAlterConfigsResourceResponse : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 2};
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        
        TAlterConfigsResourceResponse();
        ~TAlterConfigsResourceResponse() = default;
        
        struct ErrorCodeMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "errorCode";
            static constexpr const char* About = "The resource error code.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        ErrorCodeMeta::Type ErrorCode;
        
        struct ErrorMessageMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "errorMessage";
            static constexpr const char* About = "The resource error message, or null if there was no error.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsAlways;
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        ErrorMessageMeta::Type ErrorMessage;
        
        struct ResourceTypeMeta {
            using Type = TKafkaInt8;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "resourceType";
            static constexpr const char* About = "The resource type.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        ResourceTypeMeta::Type ResourceType;
        
        struct ResourceNameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "resourceName";
            static constexpr const char* About = "The resource name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        ResourceNameMeta::Type ResourceName;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TAlterConfigsResourceResponse& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct ResponsesMeta {
        using ItemType = TAlterConfigsResourceResponse;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TAlterConfigsResourceResponse>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "responses";
        static constexpr const char* About = "The responses for each resource.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    ResponsesMeta::Type Responses;
    
    i16 ApiKey() const override { return ALTER_CONFIGS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TAlterConfigsResponseData& other) const = default;
};


class TSaslAuthenticateRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TSaslAuthenticateRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 2};
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    
    TSaslAuthenticateRequestData();
    ~TSaslAuthenticateRequestData() = default;
    
    struct AuthBytesMeta {
        using Type = TKafkaBytes;
        using TypeDesc = NPrivate::TKafkaBytesDesc;
        
        static constexpr const char* Name = "authBytes";
        static constexpr const char* About = "The SASL authentication bytes from the client, as defined by the SASL mechanism.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    AuthBytesMeta::Type AuthBytes;
    
    i16 ApiKey() const override { return SASL_AUTHENTICATE; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TSaslAuthenticateRequestData& other) const = default;
};


class TSaslAuthenticateResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TSaslAuthenticateResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 2};
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    
    TSaslAuthenticateResponseData();
    ~TSaslAuthenticateResponseData() = default;
    
    struct ErrorCodeMeta {
        using Type = TKafkaInt16;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "errorCode";
        static constexpr const char* About = "The error code, or 0 if there was no error.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    ErrorCodeMeta::Type ErrorCode;
    
    struct ErrorMessageMeta {
        using Type = TKafkaString;
        using TypeDesc = NPrivate::TKafkaStringDesc;
        
        static constexpr const char* Name = "errorMessage";
        static constexpr const char* About = "The error message, or null if there was no error.";
        static const Type Default; // = {""};
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsAlways;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    ErrorMessageMeta::Type ErrorMessage;
    
    struct AuthBytesMeta {
        using Type = TKafkaBytes;
        using TypeDesc = NPrivate::TKafkaBytesDesc;
        
        static constexpr const char* Name = "authBytes";
        static constexpr const char* About = "The SASL authentication bytes from the server, as defined by the SASL mechanism.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    AuthBytesMeta::Type AuthBytes;
    
    struct SessionLifetimeMsMeta {
        using Type = TKafkaInt64;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "sessionLifetimeMs";
        static constexpr const char* About = "The SASL authentication bytes from the server, as defined by the SASL mechanism.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = {1, Max<TKafkaVersion>()};
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    SessionLifetimeMsMeta::Type SessionLifetimeMs;
    
    i16 ApiKey() const override { return SASL_AUTHENTICATE; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TSaslAuthenticateResponseData& other) const = default;
};


class TCreatePartitionsRequestData : public TApiMessage {
public:
    typedef std::shared_ptr<TCreatePartitionsRequestData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    
    TCreatePartitionsRequestData();
    ~TCreatePartitionsRequestData() = default;
    
    class TCreatePartitionsTopic : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 3};
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        
        TCreatePartitionsTopic();
        ~TCreatePartitionsTopic() = default;
        
        class TCreatePartitionsAssignment : public TMessage {
        public:
            struct MessageMeta {
                static constexpr TKafkaVersions PresentVersions = {0, 3};
                static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
            };
            
            TCreatePartitionsAssignment();
            ~TCreatePartitionsAssignment() = default;
            
            struct BrokerIdsMeta {
                using ItemType = TKafkaInt32;
                using ItemTypeDesc = NPrivate::TKafkaIntDesc;
                using Type = std::vector<TKafkaInt32>;
                using TypeDesc = NPrivate::TKafkaArrayDesc;
                
                static constexpr const char* Name = "brokerIds";
                static constexpr const char* About = "The assigned broker IDs.";
                
                static constexpr TKafkaVersions PresentVersions = VersionsAlways;
                static constexpr TKafkaVersions TaggedVersions = VersionsNever;
                static constexpr TKafkaVersions NullableVersions = VersionsNever;
                static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
            };
            BrokerIdsMeta::Type BrokerIds;
            
            i32 Size(TKafkaVersion version) const override;
            void Read(TKafkaReadable& readable, TKafkaVersion version) override;
            void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
            
            bool operator==(const TCreatePartitionsAssignment& other) const = default;
        };
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct CountMeta {
            using Type = TKafkaInt32;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "count";
            static constexpr const char* About = "The new partition count.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        CountMeta::Type Count;
        
        struct AssignmentsMeta {
            using ItemType = TCreatePartitionsAssignment;
            using ItemTypeDesc = NPrivate::TKafkaStructDesc;
            using Type = std::vector<TCreatePartitionsAssignment>;
            using TypeDesc = NPrivate::TKafkaArrayDesc;
            
            static constexpr const char* Name = "assignments";
            static constexpr const char* About = "The new partition assignments.";
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsAlways;
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        AssignmentsMeta::Type Assignments;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TCreatePartitionsTopic& other) const = default;
    };
    
    struct TopicsMeta {
        using ItemType = TCreatePartitionsTopic;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TCreatePartitionsTopic>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "topics";
        static constexpr const char* About = "Each topic that we want to create new partitions inside.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    TopicsMeta::Type Topics;
    
    struct TimeoutMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "timeoutMs";
        static constexpr const char* About = "The time in ms to wait for the partitions to be created.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    TimeoutMsMeta::Type TimeoutMs;
    
    struct ValidateOnlyMeta {
        using Type = TKafkaBool;
        using TypeDesc = NPrivate::TKafkaBoolDesc;
        
        static constexpr const char* Name = "validateOnly";
        static constexpr const char* About = "If true, then validate the request, but don't actually increase the number of partitions.";
        static const Type Default; // = false;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    ValidateOnlyMeta::Type ValidateOnly;
    
    i16 ApiKey() const override { return CREATE_PARTITIONS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TCreatePartitionsRequestData& other) const = default;
};


class TCreatePartitionsResponseData : public TApiMessage {
public:
    typedef std::shared_ptr<TCreatePartitionsResponseData> TPtr;
    
    struct MessageMeta {
        static constexpr TKafkaVersions PresentVersions = {0, 3};
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    
    TCreatePartitionsResponseData();
    ~TCreatePartitionsResponseData() = default;
    
    class TCreatePartitionsTopicResult : public TMessage {
    public:
        struct MessageMeta {
            static constexpr TKafkaVersions PresentVersions = {0, 3};
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        
        TCreatePartitionsTopicResult();
        ~TCreatePartitionsTopicResult() = default;
        
        struct NameMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "name";
            static constexpr const char* About = "The topic name.";
            static const Type Default; // = {""};
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        NameMeta::Type Name;
        
        struct ErrorCodeMeta {
            using Type = TKafkaInt16;
            using TypeDesc = NPrivate::TKafkaIntDesc;
            
            static constexpr const char* Name = "errorCode";
            static constexpr const char* About = "The result error, or zero if there was no error.";
            static const Type Default; // = 0;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsNever;
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        ErrorCodeMeta::Type ErrorCode;
        
        struct ErrorMessageMeta {
            using Type = TKafkaString;
            using TypeDesc = NPrivate::TKafkaStringDesc;
            
            static constexpr const char* Name = "errorMessage";
            static constexpr const char* About = "The result message, or null if there was no error.";
            static const Type Default; // = std::nullopt;
            
            static constexpr TKafkaVersions PresentVersions = VersionsAlways;
            static constexpr TKafkaVersions TaggedVersions = VersionsNever;
            static constexpr TKafkaVersions NullableVersions = VersionsAlways;
            static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
        };
        ErrorMessageMeta::Type ErrorMessage;
        
        i32 Size(TKafkaVersion version) const override;
        void Read(TKafkaReadable& readable, TKafkaVersion version) override;
        void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
        
        bool operator==(const TCreatePartitionsTopicResult& other) const = default;
    };
    
    struct ThrottleTimeMsMeta {
        using Type = TKafkaInt32;
        using TypeDesc = NPrivate::TKafkaIntDesc;
        
        static constexpr const char* Name = "throttleTimeMs";
        static constexpr const char* About = "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.";
        static const Type Default; // = 0;
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    ThrottleTimeMsMeta::Type ThrottleTimeMs;
    
    struct ResultsMeta {
        using ItemType = TCreatePartitionsTopicResult;
        using ItemTypeDesc = NPrivate::TKafkaStructDesc;
        using Type = std::vector<TCreatePartitionsTopicResult>;
        using TypeDesc = NPrivate::TKafkaArrayDesc;
        
        static constexpr const char* Name = "results";
        static constexpr const char* About = "The partition creation results for each topic.";
        
        static constexpr TKafkaVersions PresentVersions = VersionsAlways;
        static constexpr TKafkaVersions TaggedVersions = VersionsNever;
        static constexpr TKafkaVersions NullableVersions = VersionsNever;
        static constexpr TKafkaVersions FlexibleVersions = {2, Max<TKafkaVersion>()};
    };
    ResultsMeta::Type Results;
    
    i16 ApiKey() const override { return CREATE_PARTITIONS; };
    i32 Size(TKafkaVersion version) const override;
    void Read(TKafkaReadable& readable, TKafkaVersion version) override;
    void Write(TKafkaWritable& writable, TKafkaVersion version) const override;
    
    bool operator==(const TCreatePartitionsResponseData& other) const = default;
};

} // namespace NKafka 
