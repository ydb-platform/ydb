
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
    METADATA = 3, // [ZK_BROKER, BROKER] 
    API_VERSIONS = 18, // [ZK_BROKER, BROKER, CONTROLLER] 
    INIT_PRODUCER_ID = 22, // [ZK_BROKER, BROKER] 
};






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

} // namespace NKafka 
