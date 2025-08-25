
// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.
// For generate it use kikimr/tools/kafka/generate.sh

#include "kafka_messages.h"

namespace NKafka {

const std::unordered_map<EApiKey, TString> EApiKeyNames = {
    {EApiKey::HEADER, "HEADER"},
    {EApiKey::PRODUCE, "PRODUCE"},
    {EApiKey::FETCH, "FETCH"},
    {EApiKey::LIST_OFFSETS, "LIST_OFFSETS"},
    {EApiKey::METADATA, "METADATA"},
    {EApiKey::OFFSET_COMMIT, "OFFSET_COMMIT"},
    {EApiKey::OFFSET_FETCH, "OFFSET_FETCH"},
    {EApiKey::FIND_COORDINATOR, "FIND_COORDINATOR"},
    {EApiKey::JOIN_GROUP, "JOIN_GROUP"},
    {EApiKey::HEARTBEAT, "HEARTBEAT"},
    {EApiKey::LEAVE_GROUP, "LEAVE_GROUP"},
    {EApiKey::SYNC_GROUP, "SYNC_GROUP"},
    {EApiKey::DESCRIBE_GROUPS, "DESCRIBE_GROUPS"},
    {EApiKey::LIST_GROUPS, "LIST_GROUPS"},
    {EApiKey::SASL_HANDSHAKE, "SASL_HANDSHAKE"},
    {EApiKey::API_VERSIONS, "API_VERSIONS"},
    {EApiKey::CREATE_TOPICS, "CREATE_TOPICS"},
    {EApiKey::INIT_PRODUCER_ID, "INIT_PRODUCER_ID"},
    {EApiKey::ADD_PARTITIONS_TO_TXN, "ADD_PARTITIONS_TO_TXN"},
    {EApiKey::ADD_OFFSETS_TO_TXN, "ADD_OFFSETS_TO_TXN"},
    {EApiKey::END_TXN, "END_TXN"},
    {EApiKey::TXN_OFFSET_COMMIT, "TXN_OFFSET_COMMIT"},
    {EApiKey::DESCRIBE_CONFIGS, "DESCRIBE_CONFIGS"},
    {EApiKey::ALTER_CONFIGS, "ALTER_CONFIGS"},
    {EApiKey::SASL_AUTHENTICATE, "SASL_AUTHENTICATE"},
    {EApiKey::CREATE_PARTITIONS, "CREATE_PARTITIONS"},
};


std::unique_ptr<TApiMessage> CreateRequest(i16 apiKey) {
    switch (apiKey) {
        case PRODUCE:
            return std::make_unique<TProduceRequestData>();
        case FETCH:
            return std::make_unique<TFetchRequestData>();
        case LIST_OFFSETS:
            return std::make_unique<TListOffsetsRequestData>();
        case METADATA:
            return std::make_unique<TMetadataRequestData>();
        case OFFSET_COMMIT:
            return std::make_unique<TOffsetCommitRequestData>();
        case OFFSET_FETCH:
            return std::make_unique<TOffsetFetchRequestData>();
        case FIND_COORDINATOR:
            return std::make_unique<TFindCoordinatorRequestData>();
        case JOIN_GROUP:
            return std::make_unique<TJoinGroupRequestData>();
        case HEARTBEAT:
            return std::make_unique<THeartbeatRequestData>();
        case LEAVE_GROUP:
            return std::make_unique<TLeaveGroupRequestData>();
        case SYNC_GROUP:
            return std::make_unique<TSyncGroupRequestData>();
        case DESCRIBE_GROUPS:
            return std::make_unique<TDescribeGroupsRequestData>();
        case LIST_GROUPS:
            return std::make_unique<TListGroupsRequestData>();
        case SASL_HANDSHAKE:
            return std::make_unique<TSaslHandshakeRequestData>();
        case API_VERSIONS:
            return std::make_unique<TApiVersionsRequestData>();
        case CREATE_TOPICS:
            return std::make_unique<TCreateTopicsRequestData>();
        case INIT_PRODUCER_ID:
            return std::make_unique<TInitProducerIdRequestData>();
        case ADD_PARTITIONS_TO_TXN:
            return std::make_unique<TAddPartitionsToTxnRequestData>();
        case ADD_OFFSETS_TO_TXN:
            return std::make_unique<TAddOffsetsToTxnRequestData>();
        case END_TXN:
            return std::make_unique<TEndTxnRequestData>();
        case TXN_OFFSET_COMMIT:
            return std::make_unique<TTxnOffsetCommitRequestData>();
        case DESCRIBE_CONFIGS:
            return std::make_unique<TDescribeConfigsRequestData>();
        case ALTER_CONFIGS:
            return std::make_unique<TAlterConfigsRequestData>();
        case SASL_AUTHENTICATE:
            return std::make_unique<TSaslAuthenticateRequestData>();
        case CREATE_PARTITIONS:
            return std::make_unique<TCreatePartitionsRequestData>();
        default:
            ythrow yexception() << "Unsupported request API key " <<  apiKey;
    }
}

std::unique_ptr<TApiMessage> CreateResponse(i16 apiKey) {
    switch (apiKey) {
        case PRODUCE:
            return std::make_unique<TProduceResponseData>();
        case FETCH:
            return std::make_unique<TFetchResponseData>();
        case LIST_OFFSETS:
            return std::make_unique<TListOffsetsResponseData>();
        case METADATA:
            return std::make_unique<TMetadataResponseData>();
        case OFFSET_COMMIT:
            return std::make_unique<TOffsetCommitResponseData>();
        case OFFSET_FETCH:
            return std::make_unique<TOffsetFetchResponseData>();
        case FIND_COORDINATOR:
            return std::make_unique<TFindCoordinatorResponseData>();
        case JOIN_GROUP:
            return std::make_unique<TJoinGroupResponseData>();
        case HEARTBEAT:
            return std::make_unique<THeartbeatResponseData>();
        case LEAVE_GROUP:
            return std::make_unique<TLeaveGroupResponseData>();
        case SYNC_GROUP:
            return std::make_unique<TSyncGroupResponseData>();
        case DESCRIBE_GROUPS:
            return std::make_unique<TDescribeGroupsResponseData>();
        case LIST_GROUPS:
            return std::make_unique<TListGroupsResponseData>();
        case SASL_HANDSHAKE:
            return std::make_unique<TSaslHandshakeResponseData>();
        case API_VERSIONS:
            return std::make_unique<TApiVersionsResponseData>();
        case CREATE_TOPICS:
            return std::make_unique<TCreateTopicsResponseData>();
        case INIT_PRODUCER_ID:
            return std::make_unique<TInitProducerIdResponseData>();
        case ADD_PARTITIONS_TO_TXN:
            return std::make_unique<TAddPartitionsToTxnResponseData>();
        case ADD_OFFSETS_TO_TXN:
            return std::make_unique<TAddOffsetsToTxnResponseData>();
        case END_TXN:
            return std::make_unique<TEndTxnResponseData>();
        case TXN_OFFSET_COMMIT:
            return std::make_unique<TTxnOffsetCommitResponseData>();
        case DESCRIBE_CONFIGS:
            return std::make_unique<TDescribeConfigsResponseData>();
        case ALTER_CONFIGS:
            return std::make_unique<TAlterConfigsResponseData>();
        case SASL_AUTHENTICATE:
            return std::make_unique<TSaslAuthenticateResponseData>();
        case CREATE_PARTITIONS:
            return std::make_unique<TCreatePartitionsResponseData>();
        default:
            ythrow yexception() << "Unsupported response API key " <<  apiKey;
    }
}

TKafkaVersion RequestHeaderVersion(i16 apiKey, TKafkaVersion _version) {
    switch (apiKey) {
        case PRODUCE:
            if (_version >= 9) {
                return 2;
            } else {
                return 1;
            }
        case FETCH:
            if (_version >= 12) {
                return 2;
            } else {
                return 1;
            }
        case LIST_OFFSETS:
            if (_version >= 6) {
                return 2;
            } else {
                return 1;
            }
        case METADATA:
            if (_version >= 9) {
                return 2;
            } else {
                return 1;
            }
        case OFFSET_COMMIT:
            if (_version >= 8) {
                return 2;
            } else {
                return 1;
            }
        case OFFSET_FETCH:
            if (_version >= 6) {
                return 2;
            } else {
                return 1;
            }
        case FIND_COORDINATOR:
            if (_version >= 3) {
                return 2;
            } else {
                return 1;
            }
        case JOIN_GROUP:
            if (_version >= 6) {
                return 2;
            } else {
                return 1;
            }
        case HEARTBEAT:
            if (_version >= 4) {
                return 2;
            } else {
                return 1;
            }
        case LEAVE_GROUP:
            if (_version >= 4) {
                return 2;
            } else {
                return 1;
            }
        case SYNC_GROUP:
            if (_version >= 4) {
                return 2;
            } else {
                return 1;
            }
        case DESCRIBE_GROUPS:
            if (_version >= 5) {
                return 2;
            } else {
                return 1;
            }
        case LIST_GROUPS:
            if (_version >= 3) {
                return 2;
            } else {
                return 1;
            }
        case SASL_HANDSHAKE:
            return 1;
        case API_VERSIONS:
            if (_version >= 3) {
                return 2;
            } else {
                return 1;
            }
        case CREATE_TOPICS:
            if (_version >= 5) {
                return 2;
            } else {
                return 1;
            }
        case INIT_PRODUCER_ID:
            if (_version >= 2) {
                return 2;
            } else {
                return 1;
            }
        case ADD_PARTITIONS_TO_TXN:
            if (_version >= 3) {
                return 2;
            } else {
                return 1;
            }
        case ADD_OFFSETS_TO_TXN:
            if (_version >= 3) {
                return 2;
            } else {
                return 1;
            }
        case END_TXN:
            if (_version >= 3) {
                return 2;
            } else {
                return 1;
            }
        case TXN_OFFSET_COMMIT:
            if (_version >= 3) {
                return 2;
            } else {
                return 1;
            }
        case DESCRIBE_CONFIGS:
            if (_version >= 4) {
                return 2;
            } else {
                return 1;
            }
        case ALTER_CONFIGS:
            if (_version >= 2) {
                return 2;
            } else {
                return 1;
            }
        case SASL_AUTHENTICATE:
            if (_version >= 2) {
                return 2;
            } else {
                return 1;
            }
        case CREATE_PARTITIONS:
            if (_version >= 2) {
                return 2;
            } else {
                return 1;
            }
        default:
            ythrow yexception() << "Unsupported API key " << apiKey;
            break;
    }
}

TKafkaVersion ResponseHeaderVersion(i16 apiKey, TKafkaVersion _version) {
    switch (apiKey) {
        case PRODUCE:
            if (_version >= 9) {
                return 1;
            } else {
                return 0;
            }
        case FETCH:
            if (_version >= 12) {
                return 1;
            } else {
                return 0;
            }
        case LIST_OFFSETS:
            if (_version >= 6) {
                return 1;
            } else {
                return 0;
            }
        case METADATA:
            if (_version >= 9) {
                return 1;
            } else {
                return 0;
            }
        case OFFSET_COMMIT:
            if (_version >= 8) {
                return 1;
            } else {
                return 0;
            }
        case OFFSET_FETCH:
            if (_version >= 6) {
                return 1;
            } else {
                return 0;
            }
        case FIND_COORDINATOR:
            if (_version >= 3) {
                return 1;
            } else {
                return 0;
            }
        case JOIN_GROUP:
            if (_version >= 6) {
                return 1;
            } else {
                return 0;
            }
        case HEARTBEAT:
            if (_version >= 4) {
                return 1;
            } else {
                return 0;
            }
        case LEAVE_GROUP:
            if (_version >= 4) {
                return 1;
            } else {
                return 0;
            }
        case SYNC_GROUP:
            if (_version >= 4) {
                return 1;
            } else {
                return 0;
            }
        case DESCRIBE_GROUPS:
            if (_version >= 5) {
                return 1;
            } else {
                return 0;
            }
        case LIST_GROUPS:
            if (_version >= 3) {
                return 1;
            } else {
                return 0;
            }
        case SASL_HANDSHAKE:
            return 0;
        case API_VERSIONS:
            // ApiVersionsResponse always includes a v0 header.
            // See KIP-511 for details.
            return 0;
        case CREATE_TOPICS:
            if (_version >= 5) {
                return 1;
            } else {
                return 0;
            }
        case INIT_PRODUCER_ID:
            if (_version >= 2) {
                return 1;
            } else {
                return 0;
            }
        case ADD_PARTITIONS_TO_TXN:
            if (_version >= 3) {
                return 1;
            } else {
                return 0;
            }
        case ADD_OFFSETS_TO_TXN:
            if (_version >= 3) {
                return 1;
            } else {
                return 0;
            }
        case END_TXN:
            if (_version >= 3) {
                return 1;
            } else {
                return 0;
            }
        case TXN_OFFSET_COMMIT:
            if (_version >= 3) {
                return 1;
            } else {
                return 0;
            }
        case DESCRIBE_CONFIGS:
            if (_version >= 4) {
                return 1;
            } else {
                return 0;
            }
        case ALTER_CONFIGS:
            if (_version >= 2) {
                return 1;
            } else {
                return 0;
            }
        case SASL_AUTHENTICATE:
            if (_version >= 2) {
                return 1;
            } else {
                return 0;
            }
        case CREATE_PARTITIONS:
            if (_version >= 2) {
                return 1;
            } else {
                return 0;
            }
        default:
            ythrow yexception() << "Unsupported API key " << apiKey;
            break;
    }
}





//
// TRequestHeaderData
//
const TRequestHeaderData::RequestApiKeyMeta::Type TRequestHeaderData::RequestApiKeyMeta::Default = 0;
const TRequestHeaderData::RequestApiVersionMeta::Type TRequestHeaderData::RequestApiVersionMeta::Default = 0;
const TRequestHeaderData::CorrelationIdMeta::Type TRequestHeaderData::CorrelationIdMeta::Default = 0;
const TRequestHeaderData::ClientIdMeta::Type TRequestHeaderData::ClientIdMeta::Default = {""};

TRequestHeaderData::TRequestHeaderData() 
        : RequestApiKey(RequestApiKeyMeta::Default)
        , RequestApiVersion(RequestApiVersionMeta::Default)
        , CorrelationId(CorrelationIdMeta::Default)
        , ClientId(ClientIdMeta::Default)
{}

void TRequestHeaderData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TRequestHeaderData";
    }
    NPrivate::Read<RequestApiKeyMeta>(_readable, _version, RequestApiKey);
    NPrivate::Read<RequestApiVersionMeta>(_readable, _version, RequestApiVersion);
    NPrivate::Read<CorrelationIdMeta>(_readable, _version, CorrelationId);
    NPrivate::Read<ClientIdMeta>(_readable, _version, ClientId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TRequestHeaderData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TRequestHeaderData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<RequestApiKeyMeta>(_collector, _writable, _version, RequestApiKey);
    NPrivate::Write<RequestApiVersionMeta>(_collector, _writable, _version, RequestApiVersion);
    NPrivate::Write<CorrelationIdMeta>(_collector, _writable, _version, CorrelationId);
    NPrivate::Write<ClientIdMeta>(_collector, _writable, _version, ClientId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TRequestHeaderData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<RequestApiKeyMeta>(_collector, _version, RequestApiKey);
    NPrivate::Size<RequestApiVersionMeta>(_collector, _version, RequestApiVersion);
    NPrivate::Size<CorrelationIdMeta>(_collector, _version, CorrelationId);
    NPrivate::Size<ClientIdMeta>(_collector, _version, ClientId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TResponseHeaderData
//
const TResponseHeaderData::CorrelationIdMeta::Type TResponseHeaderData::CorrelationIdMeta::Default = 0;

TResponseHeaderData::TResponseHeaderData() 
        : CorrelationId(CorrelationIdMeta::Default)
{}

void TResponseHeaderData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TResponseHeaderData";
    }
    NPrivate::Read<CorrelationIdMeta>(_readable, _version, CorrelationId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TResponseHeaderData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TResponseHeaderData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<CorrelationIdMeta>(_collector, _writable, _version, CorrelationId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TResponseHeaderData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<CorrelationIdMeta>(_collector, _version, CorrelationId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TProduceRequestData
//
const TProduceRequestData::TransactionalIdMeta::Type TProduceRequestData::TransactionalIdMeta::Default = std::nullopt;
const TProduceRequestData::AcksMeta::Type TProduceRequestData::AcksMeta::Default = 0;
const TProduceRequestData::TimeoutMsMeta::Type TProduceRequestData::TimeoutMsMeta::Default = 0;

TProduceRequestData::TProduceRequestData() 
        : TransactionalId(TransactionalIdMeta::Default)
        , Acks(AcksMeta::Default)
        , TimeoutMs(TimeoutMsMeta::Default)
{}

void TProduceRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceRequestData";
    }
    NPrivate::Read<TransactionalIdMeta>(_readable, _version, TransactionalId);
    NPrivate::Read<AcksMeta>(_readable, _version, Acks);
    NPrivate::Read<TimeoutMsMeta>(_readable, _version, TimeoutMs);
    NPrivate::Read<TopicDataMeta>(_readable, _version, TopicData);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TProduceRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TProduceRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TransactionalIdMeta>(_collector, _writable, _version, TransactionalId);
    NPrivate::Write<AcksMeta>(_collector, _writable, _version, Acks);
    NPrivate::Write<TimeoutMsMeta>(_collector, _writable, _version, TimeoutMs);
    NPrivate::Write<TopicDataMeta>(_collector, _writable, _version, TopicData);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TransactionalIdMeta>(_collector, _version, TransactionalId);
    NPrivate::Size<AcksMeta>(_collector, _version, Acks);
    NPrivate::Size<TimeoutMsMeta>(_collector, _version, TimeoutMs);
    NPrivate::Size<TopicDataMeta>(_collector, _version, TopicData);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TProduceRequestData::TTopicProduceData
//
const TProduceRequestData::TTopicProduceData::NameMeta::Type TProduceRequestData::TTopicProduceData::NameMeta::Default = {""};

TProduceRequestData::TTopicProduceData::TTopicProduceData() 
        : Name(NameMeta::Default)
{}

void TProduceRequestData::TTopicProduceData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceRequestData::TTopicProduceData";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionDataMeta>(_readable, _version, PartitionData);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TProduceRequestData::TTopicProduceData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TProduceRequestData::TTopicProduceData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionDataMeta>(_collector, _writable, _version, PartitionData);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceRequestData::TTopicProduceData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionDataMeta>(_collector, _version, PartitionData);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TProduceRequestData::TTopicProduceData::TPartitionProduceData
//
const TProduceRequestData::TTopicProduceData::TPartitionProduceData::IndexMeta::Type TProduceRequestData::TTopicProduceData::TPartitionProduceData::IndexMeta::Default = 0;

TProduceRequestData::TTopicProduceData::TPartitionProduceData::TPartitionProduceData() 
        : Index(IndexMeta::Default)
{}

void TProduceRequestData::TTopicProduceData::TPartitionProduceData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceRequestData::TTopicProduceData::TPartitionProduceData";
    }
    NPrivate::Read<IndexMeta>(_readable, _version, Index);
    NPrivate::Read<RecordsMeta>(_readable, _version, Records);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TProduceRequestData::TTopicProduceData::TPartitionProduceData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TProduceRequestData::TTopicProduceData::TPartitionProduceData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<IndexMeta>(_collector, _writable, _version, Index);
    NPrivate::Write<RecordsMeta>(_collector, _writable, _version, Records);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceRequestData::TTopicProduceData::TPartitionProduceData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<IndexMeta>(_collector, _version, Index);
    NPrivate::Size<RecordsMeta>(_collector, _version, Records);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TProduceResponseData
//
const TProduceResponseData::ThrottleTimeMsMeta::Type TProduceResponseData::ThrottleTimeMsMeta::Default = 0;

TProduceResponseData::TProduceResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
{}

void TProduceResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceResponseData";
    }
    NPrivate::Read<ResponsesMeta>(_readable, _version, Responses);
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TProduceResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TProduceResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ResponsesMeta>(_collector, _writable, _version, Responses);
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ResponsesMeta>(_collector, _version, Responses);
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TProduceResponseData::TTopicProduceResponse
//
const TProduceResponseData::TTopicProduceResponse::NameMeta::Type TProduceResponseData::TTopicProduceResponse::NameMeta::Default = {""};

TProduceResponseData::TTopicProduceResponse::TTopicProduceResponse() 
        : Name(NameMeta::Default)
{}

void TProduceResponseData::TTopicProduceResponse::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceResponseData::TTopicProduceResponse";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionResponsesMeta>(_readable, _version, PartitionResponses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TProduceResponseData::TTopicProduceResponse::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TProduceResponseData::TTopicProduceResponse";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionResponsesMeta>(_collector, _writable, _version, PartitionResponses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceResponseData::TTopicProduceResponse::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionResponsesMeta>(_collector, _version, PartitionResponses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse
//
const TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::IndexMeta::Type TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::IndexMeta::Default = 0;
const TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::ErrorCodeMeta::Type TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::ErrorCodeMeta::Default = 0;
const TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::BaseOffsetMeta::Type TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::BaseOffsetMeta::Default = 0;
const TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::LogAppendTimeMsMeta::Type TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::LogAppendTimeMsMeta::Default = -1;
const TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::LogStartOffsetMeta::Type TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::LogStartOffsetMeta::Default = -1;
const TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::ErrorMessageMeta::Type TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::ErrorMessageMeta::Default = std::nullopt;

TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TPartitionProduceResponse() 
        : Index(IndexMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , BaseOffset(BaseOffsetMeta::Default)
        , LogAppendTimeMs(LogAppendTimeMsMeta::Default)
        , LogStartOffset(LogStartOffsetMeta::Default)
        , ErrorMessage(ErrorMessageMeta::Default)
{}

void TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse";
    }
    NPrivate::Read<IndexMeta>(_readable, _version, Index);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<BaseOffsetMeta>(_readable, _version, BaseOffset);
    NPrivate::Read<LogAppendTimeMsMeta>(_readable, _version, LogAppendTimeMs);
    NPrivate::Read<LogStartOffsetMeta>(_readable, _version, LogStartOffset);
    NPrivate::Read<RecordErrorsMeta>(_readable, _version, RecordErrors);
    NPrivate::Read<ErrorMessageMeta>(_readable, _version, ErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<IndexMeta>(_collector, _writable, _version, Index);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<BaseOffsetMeta>(_collector, _writable, _version, BaseOffset);
    NPrivate::Write<LogAppendTimeMsMeta>(_collector, _writable, _version, LogAppendTimeMs);
    NPrivate::Write<LogStartOffsetMeta>(_collector, _writable, _version, LogStartOffset);
    NPrivate::Write<RecordErrorsMeta>(_collector, _writable, _version, RecordErrors);
    NPrivate::Write<ErrorMessageMeta>(_collector, _writable, _version, ErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<IndexMeta>(_collector, _version, Index);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<BaseOffsetMeta>(_collector, _version, BaseOffset);
    NPrivate::Size<LogAppendTimeMsMeta>(_collector, _version, LogAppendTimeMs);
    NPrivate::Size<LogStartOffsetMeta>(_collector, _version, LogStartOffset);
    NPrivate::Size<RecordErrorsMeta>(_collector, _version, RecordErrors);
    NPrivate::Size<ErrorMessageMeta>(_collector, _version, ErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage
//
const TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::BatchIndexMeta::Type TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::BatchIndexMeta::Default = 0;
const TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::BatchIndexErrorMessageMeta::Type TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::BatchIndexErrorMessageMeta::Default = std::nullopt;

TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::TBatchIndexAndErrorMessage() 
        : BatchIndex(BatchIndexMeta::Default)
        , BatchIndexErrorMessage(BatchIndexErrorMessageMeta::Default)
{}

void TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage";
    }
    NPrivate::Read<BatchIndexMeta>(_readable, _version, BatchIndex);
    NPrivate::Read<BatchIndexErrorMessageMeta>(_readable, _version, BatchIndexErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<BatchIndexMeta>(_collector, _writable, _version, BatchIndex);
    NPrivate::Write<BatchIndexErrorMessageMeta>(_collector, _writable, _version, BatchIndexErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<BatchIndexMeta>(_collector, _version, BatchIndex);
    NPrivate::Size<BatchIndexErrorMessageMeta>(_collector, _version, BatchIndexErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TFetchRequestData
//
const TFetchRequestData::ClusterIdMeta::Type TFetchRequestData::ClusterIdMeta::Default = std::nullopt;
const TFetchRequestData::ReplicaIdMeta::Type TFetchRequestData::ReplicaIdMeta::Default = 0;
const TFetchRequestData::MaxWaitMsMeta::Type TFetchRequestData::MaxWaitMsMeta::Default = 0;
const TFetchRequestData::MinBytesMeta::Type TFetchRequestData::MinBytesMeta::Default = 0;
const TFetchRequestData::MaxBytesMeta::Type TFetchRequestData::MaxBytesMeta::Default = 0x7fffffff;
const TFetchRequestData::IsolationLevelMeta::Type TFetchRequestData::IsolationLevelMeta::Default = 0;
const TFetchRequestData::SessionIdMeta::Type TFetchRequestData::SessionIdMeta::Default = 0;
const TFetchRequestData::SessionEpochMeta::Type TFetchRequestData::SessionEpochMeta::Default = -1;
const TFetchRequestData::RackIdMeta::Type TFetchRequestData::RackIdMeta::Default = {""};

TFetchRequestData::TFetchRequestData() 
        : ClusterId(ClusterIdMeta::Default)
        , ReplicaId(ReplicaIdMeta::Default)
        , MaxWaitMs(MaxWaitMsMeta::Default)
        , MinBytes(MinBytesMeta::Default)
        , MaxBytes(MaxBytesMeta::Default)
        , IsolationLevel(IsolationLevelMeta::Default)
        , SessionId(SessionIdMeta::Default)
        , SessionEpoch(SessionEpochMeta::Default)
        , RackId(RackIdMeta::Default)
{}

void TFetchRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchRequestData";
    }
    NPrivate::Read<ClusterIdMeta>(_readable, _version, ClusterId);
    NPrivate::Read<ReplicaIdMeta>(_readable, _version, ReplicaId);
    NPrivate::Read<MaxWaitMsMeta>(_readable, _version, MaxWaitMs);
    NPrivate::Read<MinBytesMeta>(_readable, _version, MinBytes);
    NPrivate::Read<MaxBytesMeta>(_readable, _version, MaxBytes);
    NPrivate::Read<IsolationLevelMeta>(_readable, _version, IsolationLevel);
    NPrivate::Read<SessionIdMeta>(_readable, _version, SessionId);
    NPrivate::Read<SessionEpochMeta>(_readable, _version, SessionEpoch);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    NPrivate::Read<ForgottenTopicsDataMeta>(_readable, _version, ForgottenTopicsData);
    NPrivate::Read<RackIdMeta>(_readable, _version, RackId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                case ClusterIdMeta::Tag:
                    NPrivate::ReadTag<ClusterIdMeta>(_readable, _version, ClusterId);
                    break;
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ClusterIdMeta>(_collector, _writable, _version, ClusterId);
    NPrivate::Write<ReplicaIdMeta>(_collector, _writable, _version, ReplicaId);
    NPrivate::Write<MaxWaitMsMeta>(_collector, _writable, _version, MaxWaitMs);
    NPrivate::Write<MinBytesMeta>(_collector, _writable, _version, MinBytes);
    NPrivate::Write<MaxBytesMeta>(_collector, _writable, _version, MaxBytes);
    NPrivate::Write<IsolationLevelMeta>(_collector, _writable, _version, IsolationLevel);
    NPrivate::Write<SessionIdMeta>(_collector, _writable, _version, SessionId);
    NPrivate::Write<SessionEpochMeta>(_collector, _writable, _version, SessionEpoch);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    NPrivate::Write<ForgottenTopicsDataMeta>(_collector, _writable, _version, ForgottenTopicsData);
    NPrivate::Write<RackIdMeta>(_collector, _writable, _version, RackId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
        NPrivate::WriteTag<ClusterIdMeta>(_writable, _version, ClusterId);
    }
}

i32 TFetchRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ClusterIdMeta>(_collector, _version, ClusterId);
    NPrivate::Size<ReplicaIdMeta>(_collector, _version, ReplicaId);
    NPrivate::Size<MaxWaitMsMeta>(_collector, _version, MaxWaitMs);
    NPrivate::Size<MinBytesMeta>(_collector, _version, MinBytes);
    NPrivate::Size<MaxBytesMeta>(_collector, _version, MaxBytes);
    NPrivate::Size<IsolationLevelMeta>(_collector, _version, IsolationLevel);
    NPrivate::Size<SessionIdMeta>(_collector, _version, SessionId);
    NPrivate::Size<SessionEpochMeta>(_collector, _version, SessionEpoch);
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    NPrivate::Size<ForgottenTopicsDataMeta>(_collector, _version, ForgottenTopicsData);
    NPrivate::Size<RackIdMeta>(_collector, _version, RackId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TFetchRequestData::TFetchTopic
//
const TFetchRequestData::TFetchTopic::TopicMeta::Type TFetchRequestData::TFetchTopic::TopicMeta::Default = {""};
const TFetchRequestData::TFetchTopic::TopicIdMeta::Type TFetchRequestData::TFetchTopic::TopicIdMeta::Default = TKafkaUuid(0, 0);

TFetchRequestData::TFetchTopic::TFetchTopic() 
        : Topic(TopicMeta::Default)
        , TopicId(TopicIdMeta::Default)
{}

void TFetchRequestData::TFetchTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchRequestData::TFetchTopic";
    }
    NPrivate::Read<TopicMeta>(_readable, _version, Topic);
    NPrivate::Read<TopicIdMeta>(_readable, _version, TopicId);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchRequestData::TFetchTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchRequestData::TFetchTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicMeta>(_collector, _writable, _version, Topic);
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, TopicId);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchRequestData::TFetchTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicMeta>(_collector, _version, Topic);
    NPrivate::Size<TopicIdMeta>(_collector, _version, TopicId);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TFetchRequestData::TFetchTopic::TFetchPartition
//
const TFetchRequestData::TFetchTopic::TFetchPartition::PartitionMeta::Type TFetchRequestData::TFetchTopic::TFetchPartition::PartitionMeta::Default = 0;
const TFetchRequestData::TFetchTopic::TFetchPartition::CurrentLeaderEpochMeta::Type TFetchRequestData::TFetchTopic::TFetchPartition::CurrentLeaderEpochMeta::Default = -1;
const TFetchRequestData::TFetchTopic::TFetchPartition::FetchOffsetMeta::Type TFetchRequestData::TFetchTopic::TFetchPartition::FetchOffsetMeta::Default = 0;
const TFetchRequestData::TFetchTopic::TFetchPartition::LastFetchedEpochMeta::Type TFetchRequestData::TFetchTopic::TFetchPartition::LastFetchedEpochMeta::Default = -1;
const TFetchRequestData::TFetchTopic::TFetchPartition::LogStartOffsetMeta::Type TFetchRequestData::TFetchTopic::TFetchPartition::LogStartOffsetMeta::Default = -1;
const TFetchRequestData::TFetchTopic::TFetchPartition::PartitionMaxBytesMeta::Type TFetchRequestData::TFetchTopic::TFetchPartition::PartitionMaxBytesMeta::Default = 0;

TFetchRequestData::TFetchTopic::TFetchPartition::TFetchPartition() 
        : Partition(PartitionMeta::Default)
        , CurrentLeaderEpoch(CurrentLeaderEpochMeta::Default)
        , FetchOffset(FetchOffsetMeta::Default)
        , LastFetchedEpoch(LastFetchedEpochMeta::Default)
        , LogStartOffset(LogStartOffsetMeta::Default)
        , PartitionMaxBytes(PartitionMaxBytesMeta::Default)
{}

void TFetchRequestData::TFetchTopic::TFetchPartition::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchRequestData::TFetchTopic::TFetchPartition";
    }
    NPrivate::Read<PartitionMeta>(_readable, _version, Partition);
    NPrivate::Read<CurrentLeaderEpochMeta>(_readable, _version, CurrentLeaderEpoch);
    NPrivate::Read<FetchOffsetMeta>(_readable, _version, FetchOffset);
    NPrivate::Read<LastFetchedEpochMeta>(_readable, _version, LastFetchedEpoch);
    NPrivate::Read<LogStartOffsetMeta>(_readable, _version, LogStartOffset);
    NPrivate::Read<PartitionMaxBytesMeta>(_readable, _version, PartitionMaxBytes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchRequestData::TFetchTopic::TFetchPartition::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchRequestData::TFetchTopic::TFetchPartition";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<PartitionMeta>(_collector, _writable, _version, Partition);
    NPrivate::Write<CurrentLeaderEpochMeta>(_collector, _writable, _version, CurrentLeaderEpoch);
    NPrivate::Write<FetchOffsetMeta>(_collector, _writable, _version, FetchOffset);
    NPrivate::Write<LastFetchedEpochMeta>(_collector, _writable, _version, LastFetchedEpoch);
    NPrivate::Write<LogStartOffsetMeta>(_collector, _writable, _version, LogStartOffset);
    NPrivate::Write<PartitionMaxBytesMeta>(_collector, _writable, _version, PartitionMaxBytes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchRequestData::TFetchTopic::TFetchPartition::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<PartitionMeta>(_collector, _version, Partition);
    NPrivate::Size<CurrentLeaderEpochMeta>(_collector, _version, CurrentLeaderEpoch);
    NPrivate::Size<FetchOffsetMeta>(_collector, _version, FetchOffset);
    NPrivate::Size<LastFetchedEpochMeta>(_collector, _version, LastFetchedEpoch);
    NPrivate::Size<LogStartOffsetMeta>(_collector, _version, LogStartOffset);
    NPrivate::Size<PartitionMaxBytesMeta>(_collector, _version, PartitionMaxBytes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TFetchRequestData::TForgottenTopic
//
const TFetchRequestData::TForgottenTopic::TopicMeta::Type TFetchRequestData::TForgottenTopic::TopicMeta::Default = {""};
const TFetchRequestData::TForgottenTopic::TopicIdMeta::Type TFetchRequestData::TForgottenTopic::TopicIdMeta::Default = TKafkaUuid(0, 0);

TFetchRequestData::TForgottenTopic::TForgottenTopic() 
        : Topic(TopicMeta::Default)
        , TopicId(TopicIdMeta::Default)
{}

void TFetchRequestData::TForgottenTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchRequestData::TForgottenTopic";
    }
    NPrivate::Read<TopicMeta>(_readable, _version, Topic);
    NPrivate::Read<TopicIdMeta>(_readable, _version, TopicId);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchRequestData::TForgottenTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchRequestData::TForgottenTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicMeta>(_collector, _writable, _version, Topic);
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, TopicId);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchRequestData::TForgottenTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicMeta>(_collector, _version, Topic);
    NPrivate::Size<TopicIdMeta>(_collector, _version, TopicId);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TFetchResponseData
//
const TFetchResponseData::ThrottleTimeMsMeta::Type TFetchResponseData::ThrottleTimeMsMeta::Default = 0;
const TFetchResponseData::ErrorCodeMeta::Type TFetchResponseData::ErrorCodeMeta::Default = 0;
const TFetchResponseData::SessionIdMeta::Type TFetchResponseData::SessionIdMeta::Default = 0;

TFetchResponseData::TFetchResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , SessionId(SessionIdMeta::Default)
{}

void TFetchResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<SessionIdMeta>(_readable, _version, SessionId);
    NPrivate::Read<ResponsesMeta>(_readable, _version, Responses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<SessionIdMeta>(_collector, _writable, _version, SessionId);
    NPrivate::Write<ResponsesMeta>(_collector, _writable, _version, Responses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<SessionIdMeta>(_collector, _version, SessionId);
    NPrivate::Size<ResponsesMeta>(_collector, _version, Responses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TFetchResponseData::TFetchableTopicResponse
//
const TFetchResponseData::TFetchableTopicResponse::TopicMeta::Type TFetchResponseData::TFetchableTopicResponse::TopicMeta::Default = {""};
const TFetchResponseData::TFetchableTopicResponse::TopicIdMeta::Type TFetchResponseData::TFetchableTopicResponse::TopicIdMeta::Default = TKafkaUuid(0, 0);

TFetchResponseData::TFetchableTopicResponse::TFetchableTopicResponse() 
        : Topic(TopicMeta::Default)
        , TopicId(TopicIdMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse";
    }
    NPrivate::Read<TopicMeta>(_readable, _version, Topic);
    NPrivate::Read<TopicIdMeta>(_readable, _version, TopicId);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchResponseData::TFetchableTopicResponse::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchResponseData::TFetchableTopicResponse";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicMeta>(_collector, _writable, _version, Topic);
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, TopicId);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicMeta>(_collector, _version, Topic);
    NPrivate::Size<TopicIdMeta>(_collector, _version, TopicId);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TFetchResponseData::TFetchableTopicResponse::TPartitionData
//
const TFetchResponseData::TFetchableTopicResponse::TPartitionData::PartitionIndexMeta::Type TFetchResponseData::TFetchableTopicResponse::TPartitionData::PartitionIndexMeta::Default = 0;
const TFetchResponseData::TFetchableTopicResponse::TPartitionData::ErrorCodeMeta::Type TFetchResponseData::TFetchableTopicResponse::TPartitionData::ErrorCodeMeta::Default = 0;
const TFetchResponseData::TFetchableTopicResponse::TPartitionData::HighWatermarkMeta::Type TFetchResponseData::TFetchableTopicResponse::TPartitionData::HighWatermarkMeta::Default = 0;
const TFetchResponseData::TFetchableTopicResponse::TPartitionData::LastStableOffsetMeta::Type TFetchResponseData::TFetchableTopicResponse::TPartitionData::LastStableOffsetMeta::Default = -1;
const TFetchResponseData::TFetchableTopicResponse::TPartitionData::LogStartOffsetMeta::Type TFetchResponseData::TFetchableTopicResponse::TPartitionData::LogStartOffsetMeta::Default = -1;
const TFetchResponseData::TFetchableTopicResponse::TPartitionData::PreferredReadReplicaMeta::Type TFetchResponseData::TFetchableTopicResponse::TPartitionData::PreferredReadReplicaMeta::Default = -1;

TFetchResponseData::TFetchableTopicResponse::TPartitionData::TPartitionData() 
        : PartitionIndex(PartitionIndexMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , HighWatermark(HighWatermarkMeta::Default)
        , LastStableOffset(LastStableOffsetMeta::Default)
        , LogStartOffset(LogStartOffsetMeta::Default)
        , PreferredReadReplica(PreferredReadReplicaMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData";
    }
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, PartitionIndex);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<HighWatermarkMeta>(_readable, _version, HighWatermark);
    NPrivate::Read<LastStableOffsetMeta>(_readable, _version, LastStableOffset);
    NPrivate::Read<LogStartOffsetMeta>(_readable, _version, LogStartOffset);
    NPrivate::Read<DivergingEpochMeta>(_readable, _version, DivergingEpoch);
    NPrivate::Read<CurrentLeaderMeta>(_readable, _version, CurrentLeader);
    NPrivate::Read<SnapshotIdMeta>(_readable, _version, SnapshotId);
    NPrivate::Read<AbortedTransactionsMeta>(_readable, _version, AbortedTransactions);
    NPrivate::Read<PreferredReadReplicaMeta>(_readable, _version, PreferredReadReplica);
    NPrivate::Read<RecordsMeta>(_readable, _version, Records);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                case DivergingEpochMeta::Tag:
                    NPrivate::ReadTag<DivergingEpochMeta>(_readable, _version, DivergingEpoch);
                    break;
                case CurrentLeaderMeta::Tag:
                    NPrivate::ReadTag<CurrentLeaderMeta>(_readable, _version, CurrentLeader);
                    break;
                case SnapshotIdMeta::Tag:
                    NPrivate::ReadTag<SnapshotIdMeta>(_readable, _version, SnapshotId);
                    break;
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, PartitionIndex);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<HighWatermarkMeta>(_collector, _writable, _version, HighWatermark);
    NPrivate::Write<LastStableOffsetMeta>(_collector, _writable, _version, LastStableOffset);
    NPrivate::Write<LogStartOffsetMeta>(_collector, _writable, _version, LogStartOffset);
    NPrivate::Write<DivergingEpochMeta>(_collector, _writable, _version, DivergingEpoch);
    NPrivate::Write<CurrentLeaderMeta>(_collector, _writable, _version, CurrentLeader);
    NPrivate::Write<SnapshotIdMeta>(_collector, _writable, _version, SnapshotId);
    NPrivate::Write<AbortedTransactionsMeta>(_collector, _writable, _version, AbortedTransactions);
    NPrivate::Write<PreferredReadReplicaMeta>(_collector, _writable, _version, PreferredReadReplica);
    NPrivate::Write<RecordsMeta>(_collector, _writable, _version, Records);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
        NPrivate::WriteTag<DivergingEpochMeta>(_writable, _version, DivergingEpoch);
        NPrivate::WriteTag<CurrentLeaderMeta>(_writable, _version, CurrentLeader);
        NPrivate::WriteTag<SnapshotIdMeta>(_writable, _version, SnapshotId);
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::TPartitionData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<PartitionIndexMeta>(_collector, _version, PartitionIndex);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<HighWatermarkMeta>(_collector, _version, HighWatermark);
    NPrivate::Size<LastStableOffsetMeta>(_collector, _version, LastStableOffset);
    NPrivate::Size<LogStartOffsetMeta>(_collector, _version, LogStartOffset);
    NPrivate::Size<DivergingEpochMeta>(_collector, _version, DivergingEpoch);
    NPrivate::Size<CurrentLeaderMeta>(_collector, _version, CurrentLeader);
    NPrivate::Size<SnapshotIdMeta>(_collector, _version, SnapshotId);
    NPrivate::Size<AbortedTransactionsMeta>(_collector, _version, AbortedTransactions);
    NPrivate::Size<PreferredReadReplicaMeta>(_collector, _version, PreferredReadReplica);
    NPrivate::Size<RecordsMeta>(_collector, _version, Records);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset
//
const TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::EpochMeta::Type TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::EpochMeta::Default = -1;
const TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::EndOffsetMeta::Type TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::EndOffsetMeta::Default = -1;

TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::TEpochEndOffset() 
        : Epoch(EpochMeta::Default)
        , EndOffset(EndOffsetMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset";
    }
    NPrivate::Read<EpochMeta>(_readable, _version, Epoch);
    NPrivate::Read<EndOffsetMeta>(_readable, _version, EndOffset);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<EpochMeta>(_collector, _writable, _version, Epoch);
    NPrivate::Write<EndOffsetMeta>(_collector, _writable, _version, EndOffset);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<EpochMeta>(_collector, _version, Epoch);
    NPrivate::Size<EndOffsetMeta>(_collector, _version, EndOffset);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch
//
const TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::LeaderIdMeta::Type TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::LeaderIdMeta::Default = -1;
const TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::LeaderEpochMeta::Type TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::LeaderEpochMeta::Default = -1;

TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::TLeaderIdAndEpoch() 
        : LeaderId(LeaderIdMeta::Default)
        , LeaderEpoch(LeaderEpochMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch";
    }
    NPrivate::Read<LeaderIdMeta>(_readable, _version, LeaderId);
    NPrivate::Read<LeaderEpochMeta>(_readable, _version, LeaderEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<LeaderIdMeta>(_collector, _writable, _version, LeaderId);
    NPrivate::Write<LeaderEpochMeta>(_collector, _writable, _version, LeaderEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<LeaderIdMeta>(_collector, _version, LeaderId);
    NPrivate::Size<LeaderEpochMeta>(_collector, _version, LeaderEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId
//
const TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::EndOffsetMeta::Type TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::EndOffsetMeta::Default = -1;
const TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::EpochMeta::Type TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::EpochMeta::Default = -1;

TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::TSnapshotId() 
        : EndOffset(EndOffsetMeta::Default)
        , Epoch(EpochMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId";
    }
    NPrivate::Read<EndOffsetMeta>(_readable, _version, EndOffset);
    NPrivate::Read<EpochMeta>(_readable, _version, Epoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<EndOffsetMeta>(_collector, _writable, _version, EndOffset);
    NPrivate::Write<EpochMeta>(_collector, _writable, _version, Epoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<EndOffsetMeta>(_collector, _version, EndOffset);
    NPrivate::Size<EpochMeta>(_collector, _version, Epoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction
//
const TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::ProducerIdMeta::Type TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::ProducerIdMeta::Default = 0;
const TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::FirstOffsetMeta::Type TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::FirstOffsetMeta::Default = 0;

TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::TAbortedTransaction() 
        : ProducerId(ProducerIdMeta::Default)
        , FirstOffset(FirstOffsetMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction";
    }
    NPrivate::Read<ProducerIdMeta>(_readable, _version, ProducerId);
    NPrivate::Read<FirstOffsetMeta>(_readable, _version, FirstOffset);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ProducerIdMeta>(_collector, _writable, _version, ProducerId);
    NPrivate::Write<FirstOffsetMeta>(_collector, _writable, _version, FirstOffset);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ProducerIdMeta>(_collector, _version, ProducerId);
    NPrivate::Size<FirstOffsetMeta>(_collector, _version, FirstOffset);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TListOffsetsRequestData
//
const TListOffsetsRequestData::ReplicaIdMeta::Type TListOffsetsRequestData::ReplicaIdMeta::Default = 0;
const TListOffsetsRequestData::IsolationLevelMeta::Type TListOffsetsRequestData::IsolationLevelMeta::Default = 0;

TListOffsetsRequestData::TListOffsetsRequestData() 
        : ReplicaId(ReplicaIdMeta::Default)
        , IsolationLevel(IsolationLevelMeta::Default)
{}

void TListOffsetsRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TListOffsetsRequestData";
    }
    NPrivate::Read<ReplicaIdMeta>(_readable, _version, ReplicaId);
    NPrivate::Read<IsolationLevelMeta>(_readable, _version, IsolationLevel);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TListOffsetsRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TListOffsetsRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ReplicaIdMeta>(_collector, _writable, _version, ReplicaId);
    NPrivate::Write<IsolationLevelMeta>(_collector, _writable, _version, IsolationLevel);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TListOffsetsRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ReplicaIdMeta>(_collector, _version, ReplicaId);
    NPrivate::Size<IsolationLevelMeta>(_collector, _version, IsolationLevel);
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TListOffsetsRequestData::TListOffsetsTopic
//
const TListOffsetsRequestData::TListOffsetsTopic::NameMeta::Type TListOffsetsRequestData::TListOffsetsTopic::NameMeta::Default = {""};

TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsTopic() 
        : Name(NameMeta::Default)
{}

void TListOffsetsRequestData::TListOffsetsTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TListOffsetsRequestData::TListOffsetsTopic";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TListOffsetsRequestData::TListOffsetsTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TListOffsetsRequestData::TListOffsetsTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TListOffsetsRequestData::TListOffsetsTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition
//
const TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition::PartitionIndexMeta::Type TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition::PartitionIndexMeta::Default = 0;
const TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition::CurrentLeaderEpochMeta::Type TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition::CurrentLeaderEpochMeta::Default = -1;
const TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition::TimestampMeta::Type TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition::TimestampMeta::Default = 0;
const TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition::MaxNumOffsetsMeta::Type TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition::MaxNumOffsetsMeta::Default = 1;

TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition::TListOffsetsPartition() 
        : PartitionIndex(PartitionIndexMeta::Default)
        , CurrentLeaderEpoch(CurrentLeaderEpochMeta::Default)
        , Timestamp(TimestampMeta::Default)
        , MaxNumOffsets(MaxNumOffsetsMeta::Default)
{}

void TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition";
    }
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, PartitionIndex);
    NPrivate::Read<CurrentLeaderEpochMeta>(_readable, _version, CurrentLeaderEpoch);
    NPrivate::Read<TimestampMeta>(_readable, _version, Timestamp);
    NPrivate::Read<MaxNumOffsetsMeta>(_readable, _version, MaxNumOffsets);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, PartitionIndex);
    NPrivate::Write<CurrentLeaderEpochMeta>(_collector, _writable, _version, CurrentLeaderEpoch);
    NPrivate::Write<TimestampMeta>(_collector, _writable, _version, Timestamp);
    NPrivate::Write<MaxNumOffsetsMeta>(_collector, _writable, _version, MaxNumOffsets);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TListOffsetsRequestData::TListOffsetsTopic::TListOffsetsPartition::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<PartitionIndexMeta>(_collector, _version, PartitionIndex);
    NPrivate::Size<CurrentLeaderEpochMeta>(_collector, _version, CurrentLeaderEpoch);
    NPrivate::Size<TimestampMeta>(_collector, _version, Timestamp);
    NPrivate::Size<MaxNumOffsetsMeta>(_collector, _version, MaxNumOffsets);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TListOffsetsResponseData
//
const TListOffsetsResponseData::ThrottleTimeMsMeta::Type TListOffsetsResponseData::ThrottleTimeMsMeta::Default = 0;

TListOffsetsResponseData::TListOffsetsResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
{}

void TListOffsetsResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TListOffsetsResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TListOffsetsResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TListOffsetsResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TListOffsetsResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TListOffsetsResponseData::TListOffsetsTopicResponse
//
const TListOffsetsResponseData::TListOffsetsTopicResponse::NameMeta::Type TListOffsetsResponseData::TListOffsetsTopicResponse::NameMeta::Default = {""};

TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsTopicResponse() 
        : Name(NameMeta::Default)
{}

void TListOffsetsResponseData::TListOffsetsTopicResponse::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TListOffsetsResponseData::TListOffsetsTopicResponse";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TListOffsetsResponseData::TListOffsetsTopicResponse::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TListOffsetsResponseData::TListOffsetsTopicResponse";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TListOffsetsResponseData::TListOffsetsTopicResponse::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse
//
const TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse::PartitionIndexMeta::Type TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse::PartitionIndexMeta::Default = 0;
const TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse::ErrorCodeMeta::Type TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse::ErrorCodeMeta::Default = 0;
const TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse::TimestampMeta::Type TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse::TimestampMeta::Default = -1;
const TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse::OffsetMeta::Type TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse::OffsetMeta::Default = -1;
const TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse::LeaderEpochMeta::Type TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse::LeaderEpochMeta::Default = -1;

TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse::TListOffsetsPartitionResponse() 
        : PartitionIndex(PartitionIndexMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , Timestamp(TimestampMeta::Default)
        , Offset(OffsetMeta::Default)
        , LeaderEpoch(LeaderEpochMeta::Default)
{}

void TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse";
    }
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, PartitionIndex);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<OldStyleOffsetsMeta>(_readable, _version, OldStyleOffsets);
    NPrivate::Read<TimestampMeta>(_readable, _version, Timestamp);
    NPrivate::Read<OffsetMeta>(_readable, _version, Offset);
    NPrivate::Read<LeaderEpochMeta>(_readable, _version, LeaderEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, PartitionIndex);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<OldStyleOffsetsMeta>(_collector, _writable, _version, OldStyleOffsets);
    NPrivate::Write<TimestampMeta>(_collector, _writable, _version, Timestamp);
    NPrivate::Write<OffsetMeta>(_collector, _writable, _version, Offset);
    NPrivate::Write<LeaderEpochMeta>(_collector, _writable, _version, LeaderEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TListOffsetsResponseData::TListOffsetsTopicResponse::TListOffsetsPartitionResponse::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<PartitionIndexMeta>(_collector, _version, PartitionIndex);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<OldStyleOffsetsMeta>(_collector, _version, OldStyleOffsets);
    NPrivate::Size<TimestampMeta>(_collector, _version, Timestamp);
    NPrivate::Size<OffsetMeta>(_collector, _version, Offset);
    NPrivate::Size<LeaderEpochMeta>(_collector, _version, LeaderEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TMetadataRequestData
//
const TMetadataRequestData::AllowAutoTopicCreationMeta::Type TMetadataRequestData::AllowAutoTopicCreationMeta::Default = true;
const TMetadataRequestData::IncludeClusterAuthorizedOperationsMeta::Type TMetadataRequestData::IncludeClusterAuthorizedOperationsMeta::Default = false;
const TMetadataRequestData::IncludeTopicAuthorizedOperationsMeta::Type TMetadataRequestData::IncludeTopicAuthorizedOperationsMeta::Default = false;

TMetadataRequestData::TMetadataRequestData() 
        : AllowAutoTopicCreation(AllowAutoTopicCreationMeta::Default)
        , IncludeClusterAuthorizedOperations(IncludeClusterAuthorizedOperationsMeta::Default)
        , IncludeTopicAuthorizedOperations(IncludeTopicAuthorizedOperationsMeta::Default)
{}

void TMetadataRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataRequestData";
    }
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    NPrivate::Read<AllowAutoTopicCreationMeta>(_readable, _version, AllowAutoTopicCreation);
    NPrivate::Read<IncludeClusterAuthorizedOperationsMeta>(_readable, _version, IncludeClusterAuthorizedOperations);
    NPrivate::Read<IncludeTopicAuthorizedOperationsMeta>(_readable, _version, IncludeTopicAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TMetadataRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TMetadataRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    NPrivate::Write<AllowAutoTopicCreationMeta>(_collector, _writable, _version, AllowAutoTopicCreation);
    NPrivate::Write<IncludeClusterAuthorizedOperationsMeta>(_collector, _writable, _version, IncludeClusterAuthorizedOperations);
    NPrivate::Write<IncludeTopicAuthorizedOperationsMeta>(_collector, _writable, _version, IncludeTopicAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    NPrivate::Size<AllowAutoTopicCreationMeta>(_collector, _version, AllowAutoTopicCreation);
    NPrivate::Size<IncludeClusterAuthorizedOperationsMeta>(_collector, _version, IncludeClusterAuthorizedOperations);
    NPrivate::Size<IncludeTopicAuthorizedOperationsMeta>(_collector, _version, IncludeTopicAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TMetadataRequestData::TMetadataRequestTopic
//
const TMetadataRequestData::TMetadataRequestTopic::TopicIdMeta::Type TMetadataRequestData::TMetadataRequestTopic::TopicIdMeta::Default = TKafkaUuid(0, 0);
const TMetadataRequestData::TMetadataRequestTopic::NameMeta::Type TMetadataRequestData::TMetadataRequestTopic::NameMeta::Default = {""};

TMetadataRequestData::TMetadataRequestTopic::TMetadataRequestTopic() 
        : TopicId(TopicIdMeta::Default)
        , Name(NameMeta::Default)
{}

void TMetadataRequestData::TMetadataRequestTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataRequestData::TMetadataRequestTopic";
    }
    NPrivate::Read<TopicIdMeta>(_readable, _version, TopicId);
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TMetadataRequestData::TMetadataRequestTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TMetadataRequestData::TMetadataRequestTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, TopicId);
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataRequestData::TMetadataRequestTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicIdMeta>(_collector, _version, TopicId);
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TMetadataResponseData
//
const TMetadataResponseData::ThrottleTimeMsMeta::Type TMetadataResponseData::ThrottleTimeMsMeta::Default = 0;
const TMetadataResponseData::ClusterIdMeta::Type TMetadataResponseData::ClusterIdMeta::Default = std::nullopt;
const TMetadataResponseData::ControllerIdMeta::Type TMetadataResponseData::ControllerIdMeta::Default = -1;
const TMetadataResponseData::ClusterAuthorizedOperationsMeta::Type TMetadataResponseData::ClusterAuthorizedOperationsMeta::Default = -2147483648;

TMetadataResponseData::TMetadataResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ClusterId(ClusterIdMeta::Default)
        , ControllerId(ControllerIdMeta::Default)
        , ClusterAuthorizedOperations(ClusterAuthorizedOperationsMeta::Default)
{}

void TMetadataResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<BrokersMeta>(_readable, _version, Brokers);
    NPrivate::Read<ClusterIdMeta>(_readable, _version, ClusterId);
    NPrivate::Read<ControllerIdMeta>(_readable, _version, ControllerId);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    NPrivate::Read<ClusterAuthorizedOperationsMeta>(_readable, _version, ClusterAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TMetadataResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TMetadataResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<BrokersMeta>(_collector, _writable, _version, Brokers);
    NPrivate::Write<ClusterIdMeta>(_collector, _writable, _version, ClusterId);
    NPrivate::Write<ControllerIdMeta>(_collector, _writable, _version, ControllerId);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    NPrivate::Write<ClusterAuthorizedOperationsMeta>(_collector, _writable, _version, ClusterAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<BrokersMeta>(_collector, _version, Brokers);
    NPrivate::Size<ClusterIdMeta>(_collector, _version, ClusterId);
    NPrivate::Size<ControllerIdMeta>(_collector, _version, ControllerId);
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    NPrivate::Size<ClusterAuthorizedOperationsMeta>(_collector, _version, ClusterAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TMetadataResponseData::TMetadataResponseBroker
//
const TMetadataResponseData::TMetadataResponseBroker::NodeIdMeta::Type TMetadataResponseData::TMetadataResponseBroker::NodeIdMeta::Default = 0;
const TMetadataResponseData::TMetadataResponseBroker::HostMeta::Type TMetadataResponseData::TMetadataResponseBroker::HostMeta::Default = {""};
const TMetadataResponseData::TMetadataResponseBroker::PortMeta::Type TMetadataResponseData::TMetadataResponseBroker::PortMeta::Default = 0;
const TMetadataResponseData::TMetadataResponseBroker::RackMeta::Type TMetadataResponseData::TMetadataResponseBroker::RackMeta::Default = std::nullopt;

TMetadataResponseData::TMetadataResponseBroker::TMetadataResponseBroker() 
        : NodeId(NodeIdMeta::Default)
        , Host(HostMeta::Default)
        , Port(PortMeta::Default)
        , Rack(RackMeta::Default)
{}

void TMetadataResponseData::TMetadataResponseBroker::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataResponseData::TMetadataResponseBroker";
    }
    NPrivate::Read<NodeIdMeta>(_readable, _version, NodeId);
    NPrivate::Read<HostMeta>(_readable, _version, Host);
    NPrivate::Read<PortMeta>(_readable, _version, Port);
    NPrivate::Read<RackMeta>(_readable, _version, Rack);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TMetadataResponseData::TMetadataResponseBroker::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TMetadataResponseData::TMetadataResponseBroker";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NodeIdMeta>(_collector, _writable, _version, NodeId);
    NPrivate::Write<HostMeta>(_collector, _writable, _version, Host);
    NPrivate::Write<PortMeta>(_collector, _writable, _version, Port);
    NPrivate::Write<RackMeta>(_collector, _writable, _version, Rack);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataResponseData::TMetadataResponseBroker::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NodeIdMeta>(_collector, _version, NodeId);
    NPrivate::Size<HostMeta>(_collector, _version, Host);
    NPrivate::Size<PortMeta>(_collector, _version, Port);
    NPrivate::Size<RackMeta>(_collector, _version, Rack);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TMetadataResponseData::TMetadataResponseTopic
//
const TMetadataResponseData::TMetadataResponseTopic::ErrorCodeMeta::Type TMetadataResponseData::TMetadataResponseTopic::ErrorCodeMeta::Default = 0;
const TMetadataResponseData::TMetadataResponseTopic::NameMeta::Type TMetadataResponseData::TMetadataResponseTopic::NameMeta::Default = {""};
const TMetadataResponseData::TMetadataResponseTopic::TopicIdMeta::Type TMetadataResponseData::TMetadataResponseTopic::TopicIdMeta::Default = TKafkaUuid(0, 0);
const TMetadataResponseData::TMetadataResponseTopic::IsInternalMeta::Type TMetadataResponseData::TMetadataResponseTopic::IsInternalMeta::Default = false;
const TMetadataResponseData::TMetadataResponseTopic::TopicAuthorizedOperationsMeta::Type TMetadataResponseData::TMetadataResponseTopic::TopicAuthorizedOperationsMeta::Default = -2147483648;

TMetadataResponseData::TMetadataResponseTopic::TMetadataResponseTopic() 
        : ErrorCode(ErrorCodeMeta::Default)
        , Name(NameMeta::Default)
        , TopicId(TopicIdMeta::Default)
        , IsInternal(IsInternalMeta::Default)
        , TopicAuthorizedOperations(TopicAuthorizedOperationsMeta::Default)
{}

void TMetadataResponseData::TMetadataResponseTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataResponseData::TMetadataResponseTopic";
    }
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<TopicIdMeta>(_readable, _version, TopicId);
    NPrivate::Read<IsInternalMeta>(_readable, _version, IsInternal);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    NPrivate::Read<TopicAuthorizedOperationsMeta>(_readable, _version, TopicAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TMetadataResponseData::TMetadataResponseTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TMetadataResponseData::TMetadataResponseTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, TopicId);
    NPrivate::Write<IsInternalMeta>(_collector, _writable, _version, IsInternal);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    NPrivate::Write<TopicAuthorizedOperationsMeta>(_collector, _writable, _version, TopicAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataResponseData::TMetadataResponseTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<TopicIdMeta>(_collector, _version, TopicId);
    NPrivate::Size<IsInternalMeta>(_collector, _version, IsInternal);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    NPrivate::Size<TopicAuthorizedOperationsMeta>(_collector, _version, TopicAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition
//
const TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::ErrorCodeMeta::Type TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::ErrorCodeMeta::Default = 0;
const TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::PartitionIndexMeta::Type TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::PartitionIndexMeta::Default = 0;
const TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::LeaderIdMeta::Type TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::LeaderIdMeta::Default = 0;
const TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::LeaderEpochMeta::Type TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::LeaderEpochMeta::Default = -1;

TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::TMetadataResponsePartition() 
        : ErrorCode(ErrorCodeMeta::Default)
        , PartitionIndex(PartitionIndexMeta::Default)
        , LeaderId(LeaderIdMeta::Default)
        , LeaderEpoch(LeaderEpochMeta::Default)
{}

void TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition";
    }
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, PartitionIndex);
    NPrivate::Read<LeaderIdMeta>(_readable, _version, LeaderId);
    NPrivate::Read<LeaderEpochMeta>(_readable, _version, LeaderEpoch);
    NPrivate::Read<ReplicaNodesMeta>(_readable, _version, ReplicaNodes);
    NPrivate::Read<IsrNodesMeta>(_readable, _version, IsrNodes);
    NPrivate::Read<OfflineReplicasMeta>(_readable, _version, OfflineReplicas);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, PartitionIndex);
    NPrivate::Write<LeaderIdMeta>(_collector, _writable, _version, LeaderId);
    NPrivate::Write<LeaderEpochMeta>(_collector, _writable, _version, LeaderEpoch);
    NPrivate::Write<ReplicaNodesMeta>(_collector, _writable, _version, ReplicaNodes);
    NPrivate::Write<IsrNodesMeta>(_collector, _writable, _version, IsrNodes);
    NPrivate::Write<OfflineReplicasMeta>(_collector, _writable, _version, OfflineReplicas);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<PartitionIndexMeta>(_collector, _version, PartitionIndex);
    NPrivate::Size<LeaderIdMeta>(_collector, _version, LeaderId);
    NPrivate::Size<LeaderEpochMeta>(_collector, _version, LeaderEpoch);
    NPrivate::Size<ReplicaNodesMeta>(_collector, _version, ReplicaNodes);
    NPrivate::Size<IsrNodesMeta>(_collector, _version, IsrNodes);
    NPrivate::Size<OfflineReplicasMeta>(_collector, _version, OfflineReplicas);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetCommitRequestData
//
const TOffsetCommitRequestData::GroupIdMeta::Type TOffsetCommitRequestData::GroupIdMeta::Default = {""};
const TOffsetCommitRequestData::GenerationIdMeta::Type TOffsetCommitRequestData::GenerationIdMeta::Default = -1;
const TOffsetCommitRequestData::MemberIdMeta::Type TOffsetCommitRequestData::MemberIdMeta::Default = {""};
const TOffsetCommitRequestData::GroupInstanceIdMeta::Type TOffsetCommitRequestData::GroupInstanceIdMeta::Default = std::nullopt;
const TOffsetCommitRequestData::RetentionTimeMsMeta::Type TOffsetCommitRequestData::RetentionTimeMsMeta::Default = -1;

TOffsetCommitRequestData::TOffsetCommitRequestData() 
        : GroupId(GroupIdMeta::Default)
        , GenerationId(GenerationIdMeta::Default)
        , MemberId(MemberIdMeta::Default)
        , GroupInstanceId(GroupInstanceIdMeta::Default)
        , RetentionTimeMs(RetentionTimeMsMeta::Default)
{}

void TOffsetCommitRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetCommitRequestData";
    }
    NPrivate::Read<GroupIdMeta>(_readable, _version, GroupId);
    NPrivate::Read<GenerationIdMeta>(_readable, _version, GenerationId);
    NPrivate::Read<MemberIdMeta>(_readable, _version, MemberId);
    NPrivate::Read<GroupInstanceIdMeta>(_readable, _version, GroupInstanceId);
    NPrivate::Read<RetentionTimeMsMeta>(_readable, _version, RetentionTimeMs);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetCommitRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetCommitRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<GroupIdMeta>(_collector, _writable, _version, GroupId);
    NPrivate::Write<GenerationIdMeta>(_collector, _writable, _version, GenerationId);
    NPrivate::Write<MemberIdMeta>(_collector, _writable, _version, MemberId);
    NPrivate::Write<GroupInstanceIdMeta>(_collector, _writable, _version, GroupInstanceId);
    NPrivate::Write<RetentionTimeMsMeta>(_collector, _writable, _version, RetentionTimeMs);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetCommitRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<GroupIdMeta>(_collector, _version, GroupId);
    NPrivate::Size<GenerationIdMeta>(_collector, _version, GenerationId);
    NPrivate::Size<MemberIdMeta>(_collector, _version, MemberId);
    NPrivate::Size<GroupInstanceIdMeta>(_collector, _version, GroupInstanceId);
    NPrivate::Size<RetentionTimeMsMeta>(_collector, _version, RetentionTimeMs);
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetCommitRequestData::TOffsetCommitRequestTopic
//
const TOffsetCommitRequestData::TOffsetCommitRequestTopic::NameMeta::Type TOffsetCommitRequestData::TOffsetCommitRequestTopic::NameMeta::Default = {""};

TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestTopic() 
        : Name(NameMeta::Default)
{}

void TOffsetCommitRequestData::TOffsetCommitRequestTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetCommitRequestData::TOffsetCommitRequestTopic";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetCommitRequestData::TOffsetCommitRequestTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetCommitRequestData::TOffsetCommitRequestTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetCommitRequestData::TOffsetCommitRequestTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition
//
const TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition::PartitionIndexMeta::Type TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition::PartitionIndexMeta::Default = 0;
const TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition::CommittedOffsetMeta::Type TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition::CommittedOffsetMeta::Default = 0;
const TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition::CommittedLeaderEpochMeta::Type TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition::CommittedLeaderEpochMeta::Default = -1;
const TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition::CommitTimestampMeta::Type TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition::CommitTimestampMeta::Default = -1;
const TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition::CommittedMetadataMeta::Type TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition::CommittedMetadataMeta::Default = {""};

TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition::TOffsetCommitRequestPartition() 
        : PartitionIndex(PartitionIndexMeta::Default)
        , CommittedOffset(CommittedOffsetMeta::Default)
        , CommittedLeaderEpoch(CommittedLeaderEpochMeta::Default)
        , CommitTimestamp(CommitTimestampMeta::Default)
        , CommittedMetadata(CommittedMetadataMeta::Default)
{}

void TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition";
    }
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, PartitionIndex);
    NPrivate::Read<CommittedOffsetMeta>(_readable, _version, CommittedOffset);
    NPrivate::Read<CommittedLeaderEpochMeta>(_readable, _version, CommittedLeaderEpoch);
    NPrivate::Read<CommitTimestampMeta>(_readable, _version, CommitTimestamp);
    NPrivate::Read<CommittedMetadataMeta>(_readable, _version, CommittedMetadata);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, PartitionIndex);
    NPrivate::Write<CommittedOffsetMeta>(_collector, _writable, _version, CommittedOffset);
    NPrivate::Write<CommittedLeaderEpochMeta>(_collector, _writable, _version, CommittedLeaderEpoch);
    NPrivate::Write<CommitTimestampMeta>(_collector, _writable, _version, CommitTimestamp);
    NPrivate::Write<CommittedMetadataMeta>(_collector, _writable, _version, CommittedMetadata);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetCommitRequestData::TOffsetCommitRequestTopic::TOffsetCommitRequestPartition::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<PartitionIndexMeta>(_collector, _version, PartitionIndex);
    NPrivate::Size<CommittedOffsetMeta>(_collector, _version, CommittedOffset);
    NPrivate::Size<CommittedLeaderEpochMeta>(_collector, _version, CommittedLeaderEpoch);
    NPrivate::Size<CommitTimestampMeta>(_collector, _version, CommitTimestamp);
    NPrivate::Size<CommittedMetadataMeta>(_collector, _version, CommittedMetadata);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetCommitResponseData
//
const TOffsetCommitResponseData::ThrottleTimeMsMeta::Type TOffsetCommitResponseData::ThrottleTimeMsMeta::Default = 0;

TOffsetCommitResponseData::TOffsetCommitResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
{}

void TOffsetCommitResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetCommitResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetCommitResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetCommitResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetCommitResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetCommitResponseData::TOffsetCommitResponseTopic
//
const TOffsetCommitResponseData::TOffsetCommitResponseTopic::NameMeta::Type TOffsetCommitResponseData::TOffsetCommitResponseTopic::NameMeta::Default = {""};

TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponseTopic() 
        : Name(NameMeta::Default)
{}

void TOffsetCommitResponseData::TOffsetCommitResponseTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetCommitResponseData::TOffsetCommitResponseTopic";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetCommitResponseData::TOffsetCommitResponseTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetCommitResponseData::TOffsetCommitResponseTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetCommitResponseData::TOffsetCommitResponseTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition
//
const TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition::PartitionIndexMeta::Type TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition::PartitionIndexMeta::Default = 0;
const TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition::ErrorCodeMeta::Type TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition::ErrorCodeMeta::Default = 0;

TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition::TOffsetCommitResponsePartition() 
        : PartitionIndex(PartitionIndexMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
{}

void TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition";
    }
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, PartitionIndex);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, PartitionIndex);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetCommitResponseData::TOffsetCommitResponseTopic::TOffsetCommitResponsePartition::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<PartitionIndexMeta>(_collector, _version, PartitionIndex);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetFetchRequestData
//
const TOffsetFetchRequestData::GroupIdMeta::Type TOffsetFetchRequestData::GroupIdMeta::Default = {""};
const TOffsetFetchRequestData::RequireStableMeta::Type TOffsetFetchRequestData::RequireStableMeta::Default = false;

TOffsetFetchRequestData::TOffsetFetchRequestData() 
        : GroupId(GroupIdMeta::Default)
        , RequireStable(RequireStableMeta::Default)
{}

void TOffsetFetchRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetFetchRequestData";
    }
    NPrivate::Read<GroupIdMeta>(_readable, _version, GroupId);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    NPrivate::Read<GroupsMeta>(_readable, _version, Groups);
    NPrivate::Read<RequireStableMeta>(_readable, _version, RequireStable);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetFetchRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetFetchRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<GroupIdMeta>(_collector, _writable, _version, GroupId);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    NPrivate::Write<GroupsMeta>(_collector, _writable, _version, Groups);
    NPrivate::Write<RequireStableMeta>(_collector, _writable, _version, RequireStable);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetFetchRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<GroupIdMeta>(_collector, _version, GroupId);
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    NPrivate::Size<GroupsMeta>(_collector, _version, Groups);
    NPrivate::Size<RequireStableMeta>(_collector, _version, RequireStable);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetFetchRequestData::TOffsetFetchRequestTopic
//
const TOffsetFetchRequestData::TOffsetFetchRequestTopic::NameMeta::Type TOffsetFetchRequestData::TOffsetFetchRequestTopic::NameMeta::Default = {""};

TOffsetFetchRequestData::TOffsetFetchRequestTopic::TOffsetFetchRequestTopic() 
        : Name(NameMeta::Default)
{}

void TOffsetFetchRequestData::TOffsetFetchRequestTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetFetchRequestData::TOffsetFetchRequestTopic";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionIndexesMeta>(_readable, _version, PartitionIndexes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetFetchRequestData::TOffsetFetchRequestTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetFetchRequestData::TOffsetFetchRequestTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionIndexesMeta>(_collector, _writable, _version, PartitionIndexes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetFetchRequestData::TOffsetFetchRequestTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionIndexesMeta>(_collector, _version, PartitionIndexes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetFetchRequestData::TOffsetFetchRequestGroup
//
const TOffsetFetchRequestData::TOffsetFetchRequestGroup::GroupIdMeta::Type TOffsetFetchRequestData::TOffsetFetchRequestGroup::GroupIdMeta::Default = {""};

TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestGroup() 
        : GroupId(GroupIdMeta::Default)
{}

void TOffsetFetchRequestData::TOffsetFetchRequestGroup::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetFetchRequestData::TOffsetFetchRequestGroup";
    }
    NPrivate::Read<GroupIdMeta>(_readable, _version, GroupId);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetFetchRequestData::TOffsetFetchRequestGroup::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetFetchRequestData::TOffsetFetchRequestGroup";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<GroupIdMeta>(_collector, _writable, _version, GroupId);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetFetchRequestData::TOffsetFetchRequestGroup::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<GroupIdMeta>(_collector, _version, GroupId);
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics
//
const TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics::NameMeta::Type TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics::NameMeta::Default = {""};

TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics::TOffsetFetchRequestTopics() 
        : Name(NameMeta::Default)
{}

void TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionIndexesMeta>(_readable, _version, PartitionIndexes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionIndexesMeta>(_collector, _writable, _version, PartitionIndexes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetFetchRequestData::TOffsetFetchRequestGroup::TOffsetFetchRequestTopics::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionIndexesMeta>(_collector, _version, PartitionIndexes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetFetchResponseData
//
const TOffsetFetchResponseData::ThrottleTimeMsMeta::Type TOffsetFetchResponseData::ThrottleTimeMsMeta::Default = 0;
const TOffsetFetchResponseData::ErrorCodeMeta::Type TOffsetFetchResponseData::ErrorCodeMeta::Default = 0;

TOffsetFetchResponseData::TOffsetFetchResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
{}

void TOffsetFetchResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetFetchResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<GroupsMeta>(_readable, _version, Groups);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetFetchResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetFetchResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<GroupsMeta>(_collector, _writable, _version, Groups);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetFetchResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<GroupsMeta>(_collector, _version, Groups);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetFetchResponseData::TOffsetFetchResponseTopic
//
const TOffsetFetchResponseData::TOffsetFetchResponseTopic::NameMeta::Type TOffsetFetchResponseData::TOffsetFetchResponseTopic::NameMeta::Default = {""};

TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponseTopic() 
        : Name(NameMeta::Default)
{}

void TOffsetFetchResponseData::TOffsetFetchResponseTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetFetchResponseData::TOffsetFetchResponseTopic";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetFetchResponseData::TOffsetFetchResponseTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetFetchResponseData::TOffsetFetchResponseTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetFetchResponseData::TOffsetFetchResponseTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition
//
const TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition::PartitionIndexMeta::Type TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition::PartitionIndexMeta::Default = 0;
const TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition::CommittedOffsetMeta::Type TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition::CommittedOffsetMeta::Default = 0;
const TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition::CommittedLeaderEpochMeta::Type TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition::CommittedLeaderEpochMeta::Default = -1;
const TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition::MetadataMeta::Type TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition::MetadataMeta::Default = {""};
const TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition::ErrorCodeMeta::Type TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition::ErrorCodeMeta::Default = 0;

TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition::TOffsetFetchResponsePartition() 
        : PartitionIndex(PartitionIndexMeta::Default)
        , CommittedOffset(CommittedOffsetMeta::Default)
        , CommittedLeaderEpoch(CommittedLeaderEpochMeta::Default)
        , Metadata(MetadataMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
{}

void TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition";
    }
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, PartitionIndex);
    NPrivate::Read<CommittedOffsetMeta>(_readable, _version, CommittedOffset);
    NPrivate::Read<CommittedLeaderEpochMeta>(_readable, _version, CommittedLeaderEpoch);
    NPrivate::Read<MetadataMeta>(_readable, _version, Metadata);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, PartitionIndex);
    NPrivate::Write<CommittedOffsetMeta>(_collector, _writable, _version, CommittedOffset);
    NPrivate::Write<CommittedLeaderEpochMeta>(_collector, _writable, _version, CommittedLeaderEpoch);
    NPrivate::Write<MetadataMeta>(_collector, _writable, _version, Metadata);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetFetchResponseData::TOffsetFetchResponseTopic::TOffsetFetchResponsePartition::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<PartitionIndexMeta>(_collector, _version, PartitionIndex);
    NPrivate::Size<CommittedOffsetMeta>(_collector, _version, CommittedOffset);
    NPrivate::Size<CommittedLeaderEpochMeta>(_collector, _version, CommittedLeaderEpoch);
    NPrivate::Size<MetadataMeta>(_collector, _version, Metadata);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetFetchResponseData::TOffsetFetchResponseGroup
//
const TOffsetFetchResponseData::TOffsetFetchResponseGroup::GroupIdMeta::Type TOffsetFetchResponseData::TOffsetFetchResponseGroup::GroupIdMeta::Default = {""};
const TOffsetFetchResponseData::TOffsetFetchResponseGroup::ErrorCodeMeta::Type TOffsetFetchResponseData::TOffsetFetchResponseGroup::ErrorCodeMeta::Default = 0;

TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseGroup() 
        : GroupId(GroupIdMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
{}

void TOffsetFetchResponseData::TOffsetFetchResponseGroup::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetFetchResponseData::TOffsetFetchResponseGroup";
    }
    NPrivate::Read<GroupIdMeta>(_readable, _version, GroupId);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetFetchResponseData::TOffsetFetchResponseGroup::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetFetchResponseData::TOffsetFetchResponseGroup";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<GroupIdMeta>(_collector, _writable, _version, GroupId);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetFetchResponseData::TOffsetFetchResponseGroup::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<GroupIdMeta>(_collector, _version, GroupId);
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics
//
const TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::NameMeta::Type TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::NameMeta::Default = {""};

TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponseTopics() 
        : Name(NameMeta::Default)
{}

void TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions
//
const TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions::PartitionIndexMeta::Type TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions::PartitionIndexMeta::Default = 0;
const TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions::CommittedOffsetMeta::Type TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions::CommittedOffsetMeta::Default = 0;
const TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions::CommittedLeaderEpochMeta::Type TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions::CommittedLeaderEpochMeta::Default = -1;
const TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions::MetadataMeta::Type TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions::MetadataMeta::Default = {""};
const TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions::ErrorCodeMeta::Type TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions::ErrorCodeMeta::Default = 0;

TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions::TOffsetFetchResponsePartitions() 
        : PartitionIndex(PartitionIndexMeta::Default)
        , CommittedOffset(CommittedOffsetMeta::Default)
        , CommittedLeaderEpoch(CommittedLeaderEpochMeta::Default)
        , Metadata(MetadataMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
{}

void TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions";
    }
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, PartitionIndex);
    NPrivate::Read<CommittedOffsetMeta>(_readable, _version, CommittedOffset);
    NPrivate::Read<CommittedLeaderEpochMeta>(_readable, _version, CommittedLeaderEpoch);
    NPrivate::Read<MetadataMeta>(_readable, _version, Metadata);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, PartitionIndex);
    NPrivate::Write<CommittedOffsetMeta>(_collector, _writable, _version, CommittedOffset);
    NPrivate::Write<CommittedLeaderEpochMeta>(_collector, _writable, _version, CommittedLeaderEpoch);
    NPrivate::Write<MetadataMeta>(_collector, _writable, _version, Metadata);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TOffsetFetchResponseData::TOffsetFetchResponseGroup::TOffsetFetchResponseTopics::TOffsetFetchResponsePartitions::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<PartitionIndexMeta>(_collector, _version, PartitionIndex);
    NPrivate::Size<CommittedOffsetMeta>(_collector, _version, CommittedOffset);
    NPrivate::Size<CommittedLeaderEpochMeta>(_collector, _version, CommittedLeaderEpoch);
    NPrivate::Size<MetadataMeta>(_collector, _version, Metadata);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TFindCoordinatorRequestData
//
const TFindCoordinatorRequestData::KeyMeta::Type TFindCoordinatorRequestData::KeyMeta::Default = {""};
const TFindCoordinatorRequestData::KeyTypeMeta::Type TFindCoordinatorRequestData::KeyTypeMeta::Default = 0;

TFindCoordinatorRequestData::TFindCoordinatorRequestData() 
        : Key(KeyMeta::Default)
        , KeyType(KeyTypeMeta::Default)
{}

void TFindCoordinatorRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFindCoordinatorRequestData";
    }
    NPrivate::Read<KeyMeta>(_readable, _version, Key);
    NPrivate::Read<KeyTypeMeta>(_readable, _version, KeyType);
    NPrivate::Read<CoordinatorKeysMeta>(_readable, _version, CoordinatorKeys);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFindCoordinatorRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFindCoordinatorRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<KeyMeta>(_collector, _writable, _version, Key);
    NPrivate::Write<KeyTypeMeta>(_collector, _writable, _version, KeyType);
    NPrivate::Write<CoordinatorKeysMeta>(_collector, _writable, _version, CoordinatorKeys);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFindCoordinatorRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<KeyMeta>(_collector, _version, Key);
    NPrivate::Size<KeyTypeMeta>(_collector, _version, KeyType);
    NPrivate::Size<CoordinatorKeysMeta>(_collector, _version, CoordinatorKeys);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TFindCoordinatorResponseData
//
const TFindCoordinatorResponseData::ThrottleTimeMsMeta::Type TFindCoordinatorResponseData::ThrottleTimeMsMeta::Default = 0;
const TFindCoordinatorResponseData::ErrorCodeMeta::Type TFindCoordinatorResponseData::ErrorCodeMeta::Default = 0;
const TFindCoordinatorResponseData::ErrorMessageMeta::Type TFindCoordinatorResponseData::ErrorMessageMeta::Default = {""};
const TFindCoordinatorResponseData::NodeIdMeta::Type TFindCoordinatorResponseData::NodeIdMeta::Default = 0;
const TFindCoordinatorResponseData::HostMeta::Type TFindCoordinatorResponseData::HostMeta::Default = {""};
const TFindCoordinatorResponseData::PortMeta::Type TFindCoordinatorResponseData::PortMeta::Default = 0;

TFindCoordinatorResponseData::TFindCoordinatorResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , ErrorMessage(ErrorMessageMeta::Default)
        , NodeId(NodeIdMeta::Default)
        , Host(HostMeta::Default)
        , Port(PortMeta::Default)
{}

void TFindCoordinatorResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFindCoordinatorResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<ErrorMessageMeta>(_readable, _version, ErrorMessage);
    NPrivate::Read<NodeIdMeta>(_readable, _version, NodeId);
    NPrivate::Read<HostMeta>(_readable, _version, Host);
    NPrivate::Read<PortMeta>(_readable, _version, Port);
    NPrivate::Read<CoordinatorsMeta>(_readable, _version, Coordinators);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFindCoordinatorResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFindCoordinatorResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<ErrorMessageMeta>(_collector, _writable, _version, ErrorMessage);
    NPrivate::Write<NodeIdMeta>(_collector, _writable, _version, NodeId);
    NPrivate::Write<HostMeta>(_collector, _writable, _version, Host);
    NPrivate::Write<PortMeta>(_collector, _writable, _version, Port);
    NPrivate::Write<CoordinatorsMeta>(_collector, _writable, _version, Coordinators);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFindCoordinatorResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<ErrorMessageMeta>(_collector, _version, ErrorMessage);
    NPrivate::Size<NodeIdMeta>(_collector, _version, NodeId);
    NPrivate::Size<HostMeta>(_collector, _version, Host);
    NPrivate::Size<PortMeta>(_collector, _version, Port);
    NPrivate::Size<CoordinatorsMeta>(_collector, _version, Coordinators);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TFindCoordinatorResponseData::TCoordinator
//
const TFindCoordinatorResponseData::TCoordinator::KeyMeta::Type TFindCoordinatorResponseData::TCoordinator::KeyMeta::Default = {""};
const TFindCoordinatorResponseData::TCoordinator::NodeIdMeta::Type TFindCoordinatorResponseData::TCoordinator::NodeIdMeta::Default = 0;
const TFindCoordinatorResponseData::TCoordinator::HostMeta::Type TFindCoordinatorResponseData::TCoordinator::HostMeta::Default = {""};
const TFindCoordinatorResponseData::TCoordinator::PortMeta::Type TFindCoordinatorResponseData::TCoordinator::PortMeta::Default = 0;
const TFindCoordinatorResponseData::TCoordinator::ErrorCodeMeta::Type TFindCoordinatorResponseData::TCoordinator::ErrorCodeMeta::Default = 0;
const TFindCoordinatorResponseData::TCoordinator::ErrorMessageMeta::Type TFindCoordinatorResponseData::TCoordinator::ErrorMessageMeta::Default = {""};

TFindCoordinatorResponseData::TCoordinator::TCoordinator() 
        : Key(KeyMeta::Default)
        , NodeId(NodeIdMeta::Default)
        , Host(HostMeta::Default)
        , Port(PortMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , ErrorMessage(ErrorMessageMeta::Default)
{}

void TFindCoordinatorResponseData::TCoordinator::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFindCoordinatorResponseData::TCoordinator";
    }
    NPrivate::Read<KeyMeta>(_readable, _version, Key);
    NPrivate::Read<NodeIdMeta>(_readable, _version, NodeId);
    NPrivate::Read<HostMeta>(_readable, _version, Host);
    NPrivate::Read<PortMeta>(_readable, _version, Port);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<ErrorMessageMeta>(_readable, _version, ErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFindCoordinatorResponseData::TCoordinator::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFindCoordinatorResponseData::TCoordinator";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<KeyMeta>(_collector, _writable, _version, Key);
    NPrivate::Write<NodeIdMeta>(_collector, _writable, _version, NodeId);
    NPrivate::Write<HostMeta>(_collector, _writable, _version, Host);
    NPrivate::Write<PortMeta>(_collector, _writable, _version, Port);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<ErrorMessageMeta>(_collector, _writable, _version, ErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFindCoordinatorResponseData::TCoordinator::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<KeyMeta>(_collector, _version, Key);
    NPrivate::Size<NodeIdMeta>(_collector, _version, NodeId);
    NPrivate::Size<HostMeta>(_collector, _version, Host);
    NPrivate::Size<PortMeta>(_collector, _version, Port);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<ErrorMessageMeta>(_collector, _version, ErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TJoinGroupRequestData
//
const TJoinGroupRequestData::GroupIdMeta::Type TJoinGroupRequestData::GroupIdMeta::Default = {""};
const TJoinGroupRequestData::SessionTimeoutMsMeta::Type TJoinGroupRequestData::SessionTimeoutMsMeta::Default = 0;
const TJoinGroupRequestData::RebalanceTimeoutMsMeta::Type TJoinGroupRequestData::RebalanceTimeoutMsMeta::Default = -1;
const TJoinGroupRequestData::MemberIdMeta::Type TJoinGroupRequestData::MemberIdMeta::Default = {""};
const TJoinGroupRequestData::GroupInstanceIdMeta::Type TJoinGroupRequestData::GroupInstanceIdMeta::Default = std::nullopt;
const TJoinGroupRequestData::ProtocolTypeMeta::Type TJoinGroupRequestData::ProtocolTypeMeta::Default = {""};
const TJoinGroupRequestData::ReasonMeta::Type TJoinGroupRequestData::ReasonMeta::Default = std::nullopt;

TJoinGroupRequestData::TJoinGroupRequestData() 
        : GroupId(GroupIdMeta::Default)
        , SessionTimeoutMs(SessionTimeoutMsMeta::Default)
        , RebalanceTimeoutMs(RebalanceTimeoutMsMeta::Default)
        , MemberId(MemberIdMeta::Default)
        , GroupInstanceId(GroupInstanceIdMeta::Default)
        , ProtocolType(ProtocolTypeMeta::Default)
        , Reason(ReasonMeta::Default)
{}

void TJoinGroupRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TJoinGroupRequestData";
    }
    NPrivate::Read<GroupIdMeta>(_readable, _version, GroupId);
    NPrivate::Read<SessionTimeoutMsMeta>(_readable, _version, SessionTimeoutMs);
    NPrivate::Read<RebalanceTimeoutMsMeta>(_readable, _version, RebalanceTimeoutMs);
    NPrivate::Read<MemberIdMeta>(_readable, _version, MemberId);
    NPrivate::Read<GroupInstanceIdMeta>(_readable, _version, GroupInstanceId);
    NPrivate::Read<ProtocolTypeMeta>(_readable, _version, ProtocolType);
    NPrivate::Read<ProtocolsMeta>(_readable, _version, Protocols);
    NPrivate::Read<ReasonMeta>(_readable, _version, Reason);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TJoinGroupRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TJoinGroupRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<GroupIdMeta>(_collector, _writable, _version, GroupId);
    NPrivate::Write<SessionTimeoutMsMeta>(_collector, _writable, _version, SessionTimeoutMs);
    NPrivate::Write<RebalanceTimeoutMsMeta>(_collector, _writable, _version, RebalanceTimeoutMs);
    NPrivate::Write<MemberIdMeta>(_collector, _writable, _version, MemberId);
    NPrivate::Write<GroupInstanceIdMeta>(_collector, _writable, _version, GroupInstanceId);
    NPrivate::Write<ProtocolTypeMeta>(_collector, _writable, _version, ProtocolType);
    NPrivate::Write<ProtocolsMeta>(_collector, _writable, _version, Protocols);
    NPrivate::Write<ReasonMeta>(_collector, _writable, _version, Reason);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TJoinGroupRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<GroupIdMeta>(_collector, _version, GroupId);
    NPrivate::Size<SessionTimeoutMsMeta>(_collector, _version, SessionTimeoutMs);
    NPrivate::Size<RebalanceTimeoutMsMeta>(_collector, _version, RebalanceTimeoutMs);
    NPrivate::Size<MemberIdMeta>(_collector, _version, MemberId);
    NPrivate::Size<GroupInstanceIdMeta>(_collector, _version, GroupInstanceId);
    NPrivate::Size<ProtocolTypeMeta>(_collector, _version, ProtocolType);
    NPrivate::Size<ProtocolsMeta>(_collector, _version, Protocols);
    NPrivate::Size<ReasonMeta>(_collector, _version, Reason);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TJoinGroupRequestData::TJoinGroupRequestProtocol
//
const TJoinGroupRequestData::TJoinGroupRequestProtocol::NameMeta::Type TJoinGroupRequestData::TJoinGroupRequestProtocol::NameMeta::Default = {""};

TJoinGroupRequestData::TJoinGroupRequestProtocol::TJoinGroupRequestProtocol() 
        : Name(NameMeta::Default)
{}

void TJoinGroupRequestData::TJoinGroupRequestProtocol::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TJoinGroupRequestData::TJoinGroupRequestProtocol";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<MetadataMeta>(_readable, _version, Metadata);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TJoinGroupRequestData::TJoinGroupRequestProtocol::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TJoinGroupRequestData::TJoinGroupRequestProtocol";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<MetadataMeta>(_collector, _writable, _version, Metadata);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TJoinGroupRequestData::TJoinGroupRequestProtocol::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<MetadataMeta>(_collector, _version, Metadata);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TJoinGroupResponseData
//
const TJoinGroupResponseData::ThrottleTimeMsMeta::Type TJoinGroupResponseData::ThrottleTimeMsMeta::Default = 0;
const TJoinGroupResponseData::ErrorCodeMeta::Type TJoinGroupResponseData::ErrorCodeMeta::Default = 0;
const TJoinGroupResponseData::GenerationIdMeta::Type TJoinGroupResponseData::GenerationIdMeta::Default = -1;
const TJoinGroupResponseData::ProtocolTypeMeta::Type TJoinGroupResponseData::ProtocolTypeMeta::Default = std::nullopt;
const TJoinGroupResponseData::ProtocolNameMeta::Type TJoinGroupResponseData::ProtocolNameMeta::Default = {""};
const TJoinGroupResponseData::LeaderMeta::Type TJoinGroupResponseData::LeaderMeta::Default = {""};
const TJoinGroupResponseData::SkipAssignmentMeta::Type TJoinGroupResponseData::SkipAssignmentMeta::Default = false;
const TJoinGroupResponseData::MemberIdMeta::Type TJoinGroupResponseData::MemberIdMeta::Default = {""};

TJoinGroupResponseData::TJoinGroupResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , GenerationId(GenerationIdMeta::Default)
        , ProtocolType(ProtocolTypeMeta::Default)
        , ProtocolName(ProtocolNameMeta::Default)
        , Leader(LeaderMeta::Default)
        , SkipAssignment(SkipAssignmentMeta::Default)
        , MemberId(MemberIdMeta::Default)
{}

void TJoinGroupResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TJoinGroupResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<GenerationIdMeta>(_readable, _version, GenerationId);
    NPrivate::Read<ProtocolTypeMeta>(_readable, _version, ProtocolType);
    NPrivate::Read<ProtocolNameMeta>(_readable, _version, ProtocolName);
    NPrivate::Read<LeaderMeta>(_readable, _version, Leader);
    NPrivate::Read<SkipAssignmentMeta>(_readable, _version, SkipAssignment);
    NPrivate::Read<MemberIdMeta>(_readable, _version, MemberId);
    NPrivate::Read<MembersMeta>(_readable, _version, Members);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TJoinGroupResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TJoinGroupResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<GenerationIdMeta>(_collector, _writable, _version, GenerationId);
    NPrivate::Write<ProtocolTypeMeta>(_collector, _writable, _version, ProtocolType);
    NPrivate::Write<ProtocolNameMeta>(_collector, _writable, _version, ProtocolName);
    NPrivate::Write<LeaderMeta>(_collector, _writable, _version, Leader);
    NPrivate::Write<SkipAssignmentMeta>(_collector, _writable, _version, SkipAssignment);
    NPrivate::Write<MemberIdMeta>(_collector, _writable, _version, MemberId);
    NPrivate::Write<MembersMeta>(_collector, _writable, _version, Members);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TJoinGroupResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<GenerationIdMeta>(_collector, _version, GenerationId);
    NPrivate::Size<ProtocolTypeMeta>(_collector, _version, ProtocolType);
    NPrivate::Size<ProtocolNameMeta>(_collector, _version, ProtocolName);
    NPrivate::Size<LeaderMeta>(_collector, _version, Leader);
    NPrivate::Size<SkipAssignmentMeta>(_collector, _version, SkipAssignment);
    NPrivate::Size<MemberIdMeta>(_collector, _version, MemberId);
    NPrivate::Size<MembersMeta>(_collector, _version, Members);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TJoinGroupResponseData::TJoinGroupResponseMember
//
const TJoinGroupResponseData::TJoinGroupResponseMember::MemberIdMeta::Type TJoinGroupResponseData::TJoinGroupResponseMember::MemberIdMeta::Default = {""};
const TJoinGroupResponseData::TJoinGroupResponseMember::GroupInstanceIdMeta::Type TJoinGroupResponseData::TJoinGroupResponseMember::GroupInstanceIdMeta::Default = std::nullopt;

TJoinGroupResponseData::TJoinGroupResponseMember::TJoinGroupResponseMember() 
        : MemberId(MemberIdMeta::Default)
        , GroupInstanceId(GroupInstanceIdMeta::Default)
{}

void TJoinGroupResponseData::TJoinGroupResponseMember::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TJoinGroupResponseData::TJoinGroupResponseMember";
    }
    NPrivate::Read<MemberIdMeta>(_readable, _version, MemberId);
    NPrivate::Read<GroupInstanceIdMeta>(_readable, _version, GroupInstanceId);
    NPrivate::Read<MetadataMeta>(_readable, _version, Metadata);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TJoinGroupResponseData::TJoinGroupResponseMember::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TJoinGroupResponseData::TJoinGroupResponseMember";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<MemberIdMeta>(_collector, _writable, _version, MemberId);
    NPrivate::Write<GroupInstanceIdMeta>(_collector, _writable, _version, GroupInstanceId);
    NPrivate::Write<MetadataMeta>(_collector, _writable, _version, Metadata);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TJoinGroupResponseData::TJoinGroupResponseMember::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<MemberIdMeta>(_collector, _version, MemberId);
    NPrivate::Size<GroupInstanceIdMeta>(_collector, _version, GroupInstanceId);
    NPrivate::Size<MetadataMeta>(_collector, _version, Metadata);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// THeartbeatRequestData
//
const THeartbeatRequestData::GroupIdMeta::Type THeartbeatRequestData::GroupIdMeta::Default = {""};
const THeartbeatRequestData::GenerationIdMeta::Type THeartbeatRequestData::GenerationIdMeta::Default = 0;
const THeartbeatRequestData::MemberIdMeta::Type THeartbeatRequestData::MemberIdMeta::Default = {""};
const THeartbeatRequestData::GroupInstanceIdMeta::Type THeartbeatRequestData::GroupInstanceIdMeta::Default = std::nullopt;

THeartbeatRequestData::THeartbeatRequestData() 
        : GroupId(GroupIdMeta::Default)
        , GenerationId(GenerationIdMeta::Default)
        , MemberId(MemberIdMeta::Default)
        , GroupInstanceId(GroupInstanceIdMeta::Default)
{}

void THeartbeatRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of THeartbeatRequestData";
    }
    NPrivate::Read<GroupIdMeta>(_readable, _version, GroupId);
    NPrivate::Read<GenerationIdMeta>(_readable, _version, GenerationId);
    NPrivate::Read<MemberIdMeta>(_readable, _version, MemberId);
    NPrivate::Read<GroupInstanceIdMeta>(_readable, _version, GroupInstanceId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void THeartbeatRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of THeartbeatRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<GroupIdMeta>(_collector, _writable, _version, GroupId);
    NPrivate::Write<GenerationIdMeta>(_collector, _writable, _version, GenerationId);
    NPrivate::Write<MemberIdMeta>(_collector, _writable, _version, MemberId);
    NPrivate::Write<GroupInstanceIdMeta>(_collector, _writable, _version, GroupInstanceId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 THeartbeatRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<GroupIdMeta>(_collector, _version, GroupId);
    NPrivate::Size<GenerationIdMeta>(_collector, _version, GenerationId);
    NPrivate::Size<MemberIdMeta>(_collector, _version, MemberId);
    NPrivate::Size<GroupInstanceIdMeta>(_collector, _version, GroupInstanceId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// THeartbeatResponseData
//
const THeartbeatResponseData::ThrottleTimeMsMeta::Type THeartbeatResponseData::ThrottleTimeMsMeta::Default = 0;
const THeartbeatResponseData::ErrorCodeMeta::Type THeartbeatResponseData::ErrorCodeMeta::Default = 0;

THeartbeatResponseData::THeartbeatResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
{}

void THeartbeatResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of THeartbeatResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void THeartbeatResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of THeartbeatResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 THeartbeatResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TLeaveGroupRequestData
//
const TLeaveGroupRequestData::GroupIdMeta::Type TLeaveGroupRequestData::GroupIdMeta::Default = {""};
const TLeaveGroupRequestData::MemberIdMeta::Type TLeaveGroupRequestData::MemberIdMeta::Default = {""};

TLeaveGroupRequestData::TLeaveGroupRequestData() 
        : GroupId(GroupIdMeta::Default)
        , MemberId(MemberIdMeta::Default)
{}

void TLeaveGroupRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TLeaveGroupRequestData";
    }
    NPrivate::Read<GroupIdMeta>(_readable, _version, GroupId);
    NPrivate::Read<MemberIdMeta>(_readable, _version, MemberId);
    NPrivate::Read<MembersMeta>(_readable, _version, Members);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TLeaveGroupRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TLeaveGroupRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<GroupIdMeta>(_collector, _writable, _version, GroupId);
    NPrivate::Write<MemberIdMeta>(_collector, _writable, _version, MemberId);
    NPrivate::Write<MembersMeta>(_collector, _writable, _version, Members);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TLeaveGroupRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<GroupIdMeta>(_collector, _version, GroupId);
    NPrivate::Size<MemberIdMeta>(_collector, _version, MemberId);
    NPrivate::Size<MembersMeta>(_collector, _version, Members);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TLeaveGroupRequestData::TMemberIdentity
//
const TLeaveGroupRequestData::TMemberIdentity::MemberIdMeta::Type TLeaveGroupRequestData::TMemberIdentity::MemberIdMeta::Default = {""};
const TLeaveGroupRequestData::TMemberIdentity::GroupInstanceIdMeta::Type TLeaveGroupRequestData::TMemberIdentity::GroupInstanceIdMeta::Default = std::nullopt;
const TLeaveGroupRequestData::TMemberIdentity::ReasonMeta::Type TLeaveGroupRequestData::TMemberIdentity::ReasonMeta::Default = std::nullopt;

TLeaveGroupRequestData::TMemberIdentity::TMemberIdentity() 
        : MemberId(MemberIdMeta::Default)
        , GroupInstanceId(GroupInstanceIdMeta::Default)
        , Reason(ReasonMeta::Default)
{}

void TLeaveGroupRequestData::TMemberIdentity::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TLeaveGroupRequestData::TMemberIdentity";
    }
    NPrivate::Read<MemberIdMeta>(_readable, _version, MemberId);
    NPrivate::Read<GroupInstanceIdMeta>(_readable, _version, GroupInstanceId);
    NPrivate::Read<ReasonMeta>(_readable, _version, Reason);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TLeaveGroupRequestData::TMemberIdentity::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TLeaveGroupRequestData::TMemberIdentity";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<MemberIdMeta>(_collector, _writable, _version, MemberId);
    NPrivate::Write<GroupInstanceIdMeta>(_collector, _writable, _version, GroupInstanceId);
    NPrivate::Write<ReasonMeta>(_collector, _writable, _version, Reason);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TLeaveGroupRequestData::TMemberIdentity::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<MemberIdMeta>(_collector, _version, MemberId);
    NPrivate::Size<GroupInstanceIdMeta>(_collector, _version, GroupInstanceId);
    NPrivate::Size<ReasonMeta>(_collector, _version, Reason);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TLeaveGroupResponseData
//
const TLeaveGroupResponseData::ThrottleTimeMsMeta::Type TLeaveGroupResponseData::ThrottleTimeMsMeta::Default = 0;
const TLeaveGroupResponseData::ErrorCodeMeta::Type TLeaveGroupResponseData::ErrorCodeMeta::Default = 0;

TLeaveGroupResponseData::TLeaveGroupResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
{}

void TLeaveGroupResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TLeaveGroupResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<MembersMeta>(_readable, _version, Members);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TLeaveGroupResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TLeaveGroupResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<MembersMeta>(_collector, _writable, _version, Members);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TLeaveGroupResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<MembersMeta>(_collector, _version, Members);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TLeaveGroupResponseData::TMemberResponse
//
const TLeaveGroupResponseData::TMemberResponse::MemberIdMeta::Type TLeaveGroupResponseData::TMemberResponse::MemberIdMeta::Default = {""};
const TLeaveGroupResponseData::TMemberResponse::GroupInstanceIdMeta::Type TLeaveGroupResponseData::TMemberResponse::GroupInstanceIdMeta::Default = {""};
const TLeaveGroupResponseData::TMemberResponse::ErrorCodeMeta::Type TLeaveGroupResponseData::TMemberResponse::ErrorCodeMeta::Default = 0;

TLeaveGroupResponseData::TMemberResponse::TMemberResponse() 
        : MemberId(MemberIdMeta::Default)
        , GroupInstanceId(GroupInstanceIdMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
{}

void TLeaveGroupResponseData::TMemberResponse::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TLeaveGroupResponseData::TMemberResponse";
    }
    NPrivate::Read<MemberIdMeta>(_readable, _version, MemberId);
    NPrivate::Read<GroupInstanceIdMeta>(_readable, _version, GroupInstanceId);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TLeaveGroupResponseData::TMemberResponse::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TLeaveGroupResponseData::TMemberResponse";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<MemberIdMeta>(_collector, _writable, _version, MemberId);
    NPrivate::Write<GroupInstanceIdMeta>(_collector, _writable, _version, GroupInstanceId);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TLeaveGroupResponseData::TMemberResponse::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<MemberIdMeta>(_collector, _version, MemberId);
    NPrivate::Size<GroupInstanceIdMeta>(_collector, _version, GroupInstanceId);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TSyncGroupRequestData
//
const TSyncGroupRequestData::GroupIdMeta::Type TSyncGroupRequestData::GroupIdMeta::Default = {""};
const TSyncGroupRequestData::GenerationIdMeta::Type TSyncGroupRequestData::GenerationIdMeta::Default = 0;
const TSyncGroupRequestData::MemberIdMeta::Type TSyncGroupRequestData::MemberIdMeta::Default = {""};
const TSyncGroupRequestData::GroupInstanceIdMeta::Type TSyncGroupRequestData::GroupInstanceIdMeta::Default = std::nullopt;
const TSyncGroupRequestData::ProtocolTypeMeta::Type TSyncGroupRequestData::ProtocolTypeMeta::Default = std::nullopt;
const TSyncGroupRequestData::ProtocolNameMeta::Type TSyncGroupRequestData::ProtocolNameMeta::Default = std::nullopt;

TSyncGroupRequestData::TSyncGroupRequestData() 
        : GroupId(GroupIdMeta::Default)
        , GenerationId(GenerationIdMeta::Default)
        , MemberId(MemberIdMeta::Default)
        , GroupInstanceId(GroupInstanceIdMeta::Default)
        , ProtocolType(ProtocolTypeMeta::Default)
        , ProtocolName(ProtocolNameMeta::Default)
{}

void TSyncGroupRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TSyncGroupRequestData";
    }
    NPrivate::Read<GroupIdMeta>(_readable, _version, GroupId);
    NPrivate::Read<GenerationIdMeta>(_readable, _version, GenerationId);
    NPrivate::Read<MemberIdMeta>(_readable, _version, MemberId);
    NPrivate::Read<GroupInstanceIdMeta>(_readable, _version, GroupInstanceId);
    NPrivate::Read<ProtocolTypeMeta>(_readable, _version, ProtocolType);
    NPrivate::Read<ProtocolNameMeta>(_readable, _version, ProtocolName);
    NPrivate::Read<AssignmentsMeta>(_readable, _version, Assignments);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TSyncGroupRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TSyncGroupRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<GroupIdMeta>(_collector, _writable, _version, GroupId);
    NPrivate::Write<GenerationIdMeta>(_collector, _writable, _version, GenerationId);
    NPrivate::Write<MemberIdMeta>(_collector, _writable, _version, MemberId);
    NPrivate::Write<GroupInstanceIdMeta>(_collector, _writable, _version, GroupInstanceId);
    NPrivate::Write<ProtocolTypeMeta>(_collector, _writable, _version, ProtocolType);
    NPrivate::Write<ProtocolNameMeta>(_collector, _writable, _version, ProtocolName);
    NPrivate::Write<AssignmentsMeta>(_collector, _writable, _version, Assignments);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TSyncGroupRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<GroupIdMeta>(_collector, _version, GroupId);
    NPrivate::Size<GenerationIdMeta>(_collector, _version, GenerationId);
    NPrivate::Size<MemberIdMeta>(_collector, _version, MemberId);
    NPrivate::Size<GroupInstanceIdMeta>(_collector, _version, GroupInstanceId);
    NPrivate::Size<ProtocolTypeMeta>(_collector, _version, ProtocolType);
    NPrivate::Size<ProtocolNameMeta>(_collector, _version, ProtocolName);
    NPrivate::Size<AssignmentsMeta>(_collector, _version, Assignments);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TSyncGroupRequestData::TSyncGroupRequestAssignment
//
const TSyncGroupRequestData::TSyncGroupRequestAssignment::MemberIdMeta::Type TSyncGroupRequestData::TSyncGroupRequestAssignment::MemberIdMeta::Default = {""};

TSyncGroupRequestData::TSyncGroupRequestAssignment::TSyncGroupRequestAssignment() 
        : MemberId(MemberIdMeta::Default)
{}

void TSyncGroupRequestData::TSyncGroupRequestAssignment::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TSyncGroupRequestData::TSyncGroupRequestAssignment";
    }
    NPrivate::Read<MemberIdMeta>(_readable, _version, MemberId);
    NPrivate::Read<AssignmentMeta>(_readable, _version, Assignment);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TSyncGroupRequestData::TSyncGroupRequestAssignment::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TSyncGroupRequestData::TSyncGroupRequestAssignment";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<MemberIdMeta>(_collector, _writable, _version, MemberId);
    NPrivate::Write<AssignmentMeta>(_collector, _writable, _version, Assignment);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TSyncGroupRequestData::TSyncGroupRequestAssignment::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<MemberIdMeta>(_collector, _version, MemberId);
    NPrivate::Size<AssignmentMeta>(_collector, _version, Assignment);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TSyncGroupResponseData
//
const TSyncGroupResponseData::ThrottleTimeMsMeta::Type TSyncGroupResponseData::ThrottleTimeMsMeta::Default = 0;
const TSyncGroupResponseData::ErrorCodeMeta::Type TSyncGroupResponseData::ErrorCodeMeta::Default = 0;
const TSyncGroupResponseData::ProtocolTypeMeta::Type TSyncGroupResponseData::ProtocolTypeMeta::Default = std::nullopt;
const TSyncGroupResponseData::ProtocolNameMeta::Type TSyncGroupResponseData::ProtocolNameMeta::Default = std::nullopt;

TSyncGroupResponseData::TSyncGroupResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , ProtocolType(ProtocolTypeMeta::Default)
        , ProtocolName(ProtocolNameMeta::Default)
{}

void TSyncGroupResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TSyncGroupResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<ProtocolTypeMeta>(_readable, _version, ProtocolType);
    NPrivate::Read<ProtocolNameMeta>(_readable, _version, ProtocolName);
    NPrivate::Read<AssignmentMeta>(_readable, _version, Assignment);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TSyncGroupResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TSyncGroupResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<ProtocolTypeMeta>(_collector, _writable, _version, ProtocolType);
    NPrivate::Write<ProtocolNameMeta>(_collector, _writable, _version, ProtocolName);
    NPrivate::Write<AssignmentMeta>(_collector, _writable, _version, Assignment);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TSyncGroupResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<ProtocolTypeMeta>(_collector, _version, ProtocolType);
    NPrivate::Size<ProtocolNameMeta>(_collector, _version, ProtocolName);
    NPrivate::Size<AssignmentMeta>(_collector, _version, Assignment);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TDescribeGroupsRequestData
//
const TDescribeGroupsRequestData::IncludeAuthorizedOperationsMeta::Type TDescribeGroupsRequestData::IncludeAuthorizedOperationsMeta::Default = false;

TDescribeGroupsRequestData::TDescribeGroupsRequestData() 
        : IncludeAuthorizedOperations(IncludeAuthorizedOperationsMeta::Default)
{}

void TDescribeGroupsRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TDescribeGroupsRequestData";
    }
    NPrivate::Read<GroupsMeta>(_readable, _version, Groups);
    NPrivate::Read<IncludeAuthorizedOperationsMeta>(_readable, _version, IncludeAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TDescribeGroupsRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TDescribeGroupsRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<GroupsMeta>(_collector, _writable, _version, Groups);
    NPrivate::Write<IncludeAuthorizedOperationsMeta>(_collector, _writable, _version, IncludeAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TDescribeGroupsRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<GroupsMeta>(_collector, _version, Groups);
    NPrivate::Size<IncludeAuthorizedOperationsMeta>(_collector, _version, IncludeAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TDescribeGroupsResponseData
//
const TDescribeGroupsResponseData::ThrottleTimeMsMeta::Type TDescribeGroupsResponseData::ThrottleTimeMsMeta::Default = 0;

TDescribeGroupsResponseData::TDescribeGroupsResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
{}

void TDescribeGroupsResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TDescribeGroupsResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<GroupsMeta>(_readable, _version, Groups);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TDescribeGroupsResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TDescribeGroupsResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<GroupsMeta>(_collector, _writable, _version, Groups);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TDescribeGroupsResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<GroupsMeta>(_collector, _version, Groups);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TDescribeGroupsResponseData::TDescribedGroup
//
const TDescribeGroupsResponseData::TDescribedGroup::ErrorCodeMeta::Type TDescribeGroupsResponseData::TDescribedGroup::ErrorCodeMeta::Default = 0;
const TDescribeGroupsResponseData::TDescribedGroup::GroupIdMeta::Type TDescribeGroupsResponseData::TDescribedGroup::GroupIdMeta::Default = {""};
const TDescribeGroupsResponseData::TDescribedGroup::GroupStateMeta::Type TDescribeGroupsResponseData::TDescribedGroup::GroupStateMeta::Default = {""};
const TDescribeGroupsResponseData::TDescribedGroup::ProtocolTypeMeta::Type TDescribeGroupsResponseData::TDescribedGroup::ProtocolTypeMeta::Default = {""};
const TDescribeGroupsResponseData::TDescribedGroup::ProtocolDataMeta::Type TDescribeGroupsResponseData::TDescribedGroup::ProtocolDataMeta::Default = {""};
const TDescribeGroupsResponseData::TDescribedGroup::AuthorizedOperationsMeta::Type TDescribeGroupsResponseData::TDescribedGroup::AuthorizedOperationsMeta::Default = -2147483648;

TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroup() 
        : ErrorCode(ErrorCodeMeta::Default)
        , GroupId(GroupIdMeta::Default)
        , GroupState(GroupStateMeta::Default)
        , ProtocolType(ProtocolTypeMeta::Default)
        , ProtocolData(ProtocolDataMeta::Default)
        , AuthorizedOperations(AuthorizedOperationsMeta::Default)
{}

void TDescribeGroupsResponseData::TDescribedGroup::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TDescribeGroupsResponseData::TDescribedGroup";
    }
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<GroupIdMeta>(_readable, _version, GroupId);
    NPrivate::Read<GroupStateMeta>(_readable, _version, GroupState);
    NPrivate::Read<ProtocolTypeMeta>(_readable, _version, ProtocolType);
    NPrivate::Read<ProtocolDataMeta>(_readable, _version, ProtocolData);
    NPrivate::Read<MembersMeta>(_readable, _version, Members);
    NPrivate::Read<AuthorizedOperationsMeta>(_readable, _version, AuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TDescribeGroupsResponseData::TDescribedGroup::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TDescribeGroupsResponseData::TDescribedGroup";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<GroupIdMeta>(_collector, _writable, _version, GroupId);
    NPrivate::Write<GroupStateMeta>(_collector, _writable, _version, GroupState);
    NPrivate::Write<ProtocolTypeMeta>(_collector, _writable, _version, ProtocolType);
    NPrivate::Write<ProtocolDataMeta>(_collector, _writable, _version, ProtocolData);
    NPrivate::Write<MembersMeta>(_collector, _writable, _version, Members);
    NPrivate::Write<AuthorizedOperationsMeta>(_collector, _writable, _version, AuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TDescribeGroupsResponseData::TDescribedGroup::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<GroupIdMeta>(_collector, _version, GroupId);
    NPrivate::Size<GroupStateMeta>(_collector, _version, GroupState);
    NPrivate::Size<ProtocolTypeMeta>(_collector, _version, ProtocolType);
    NPrivate::Size<ProtocolDataMeta>(_collector, _version, ProtocolData);
    NPrivate::Size<MembersMeta>(_collector, _version, Members);
    NPrivate::Size<AuthorizedOperationsMeta>(_collector, _version, AuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember
//
const TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember::MemberIdMeta::Type TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember::MemberIdMeta::Default = {""};
const TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember::GroupInstanceIdMeta::Type TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember::GroupInstanceIdMeta::Default = std::nullopt;
const TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember::ClientIdMeta::Type TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember::ClientIdMeta::Default = {""};
const TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember::ClientHostMeta::Type TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember::ClientHostMeta::Default = {""};

TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember::TDescribedGroupMember() 
        : MemberId(MemberIdMeta::Default)
        , GroupInstanceId(GroupInstanceIdMeta::Default)
        , ClientId(ClientIdMeta::Default)
        , ClientHost(ClientHostMeta::Default)
{}

void TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember";
    }
    NPrivate::Read<MemberIdMeta>(_readable, _version, MemberId);
    NPrivate::Read<GroupInstanceIdMeta>(_readable, _version, GroupInstanceId);
    NPrivate::Read<ClientIdMeta>(_readable, _version, ClientId);
    NPrivate::Read<ClientHostMeta>(_readable, _version, ClientHost);
    NPrivate::Read<MemberMetadataMeta>(_readable, _version, MemberMetadata);
    NPrivate::Read<MemberAssignmentMeta>(_readable, _version, MemberAssignment);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<MemberIdMeta>(_collector, _writable, _version, MemberId);
    NPrivate::Write<GroupInstanceIdMeta>(_collector, _writable, _version, GroupInstanceId);
    NPrivate::Write<ClientIdMeta>(_collector, _writable, _version, ClientId);
    NPrivate::Write<ClientHostMeta>(_collector, _writable, _version, ClientHost);
    NPrivate::Write<MemberMetadataMeta>(_collector, _writable, _version, MemberMetadata);
    NPrivate::Write<MemberAssignmentMeta>(_collector, _writable, _version, MemberAssignment);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TDescribeGroupsResponseData::TDescribedGroup::TDescribedGroupMember::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<MemberIdMeta>(_collector, _version, MemberId);
    NPrivate::Size<GroupInstanceIdMeta>(_collector, _version, GroupInstanceId);
    NPrivate::Size<ClientIdMeta>(_collector, _version, ClientId);
    NPrivate::Size<ClientHostMeta>(_collector, _version, ClientHost);
    NPrivate::Size<MemberMetadataMeta>(_collector, _version, MemberMetadata);
    NPrivate::Size<MemberAssignmentMeta>(_collector, _version, MemberAssignment);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TListGroupsRequestData
//

TListGroupsRequestData::TListGroupsRequestData() 
{}

void TListGroupsRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TListGroupsRequestData";
    }
    NPrivate::Read<StatesFilterMeta>(_readable, _version, StatesFilter);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TListGroupsRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TListGroupsRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<StatesFilterMeta>(_collector, _writable, _version, StatesFilter);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TListGroupsRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<StatesFilterMeta>(_collector, _version, StatesFilter);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TListGroupsResponseData
//
const TListGroupsResponseData::ThrottleTimeMsMeta::Type TListGroupsResponseData::ThrottleTimeMsMeta::Default = 0;
const TListGroupsResponseData::ErrorCodeMeta::Type TListGroupsResponseData::ErrorCodeMeta::Default = 0;

TListGroupsResponseData::TListGroupsResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
{}

void TListGroupsResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TListGroupsResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<GroupsMeta>(_readable, _version, Groups);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TListGroupsResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TListGroupsResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<GroupsMeta>(_collector, _writable, _version, Groups);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TListGroupsResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<GroupsMeta>(_collector, _version, Groups);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TListGroupsResponseData::TListedGroup
//
const TListGroupsResponseData::TListedGroup::GroupIdMeta::Type TListGroupsResponseData::TListedGroup::GroupIdMeta::Default = {""};
const TListGroupsResponseData::TListedGroup::ProtocolTypeMeta::Type TListGroupsResponseData::TListedGroup::ProtocolTypeMeta::Default = {""};
const TListGroupsResponseData::TListedGroup::GroupStateMeta::Type TListGroupsResponseData::TListedGroup::GroupStateMeta::Default = {""};

TListGroupsResponseData::TListedGroup::TListedGroup() 
        : GroupId(GroupIdMeta::Default)
        , ProtocolType(ProtocolTypeMeta::Default)
        , GroupState(GroupStateMeta::Default)
{}

void TListGroupsResponseData::TListedGroup::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TListGroupsResponseData::TListedGroup";
    }
    NPrivate::Read<GroupIdMeta>(_readable, _version, GroupId);
    NPrivate::Read<ProtocolTypeMeta>(_readable, _version, ProtocolType);
    NPrivate::Read<GroupStateMeta>(_readable, _version, GroupState);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TListGroupsResponseData::TListedGroup::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TListGroupsResponseData::TListedGroup";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<GroupIdMeta>(_collector, _writable, _version, GroupId);
    NPrivate::Write<ProtocolTypeMeta>(_collector, _writable, _version, ProtocolType);
    NPrivate::Write<GroupStateMeta>(_collector, _writable, _version, GroupState);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TListGroupsResponseData::TListedGroup::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<GroupIdMeta>(_collector, _version, GroupId);
    NPrivate::Size<ProtocolTypeMeta>(_collector, _version, ProtocolType);
    NPrivate::Size<GroupStateMeta>(_collector, _version, GroupState);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TSaslHandshakeRequestData
//
const TSaslHandshakeRequestData::MechanismMeta::Type TSaslHandshakeRequestData::MechanismMeta::Default = {""};

TSaslHandshakeRequestData::TSaslHandshakeRequestData() 
        : Mechanism(MechanismMeta::Default)
{}

void TSaslHandshakeRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TSaslHandshakeRequestData";
    }
    NPrivate::Read<MechanismMeta>(_readable, _version, Mechanism);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TSaslHandshakeRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TSaslHandshakeRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<MechanismMeta>(_collector, _writable, _version, Mechanism);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TSaslHandshakeRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<MechanismMeta>(_collector, _version, Mechanism);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TSaslHandshakeResponseData
//
const TSaslHandshakeResponseData::ErrorCodeMeta::Type TSaslHandshakeResponseData::ErrorCodeMeta::Default = 0;

TSaslHandshakeResponseData::TSaslHandshakeResponseData() 
        : ErrorCode(ErrorCodeMeta::Default)
{}

void TSaslHandshakeResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TSaslHandshakeResponseData";
    }
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<MechanismsMeta>(_readable, _version, Mechanisms);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TSaslHandshakeResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TSaslHandshakeResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<MechanismsMeta>(_collector, _writable, _version, Mechanisms);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TSaslHandshakeResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<MechanismsMeta>(_collector, _version, Mechanisms);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TApiVersionsRequestData
//
const TApiVersionsRequestData::ClientSoftwareNameMeta::Type TApiVersionsRequestData::ClientSoftwareNameMeta::Default = {""};
const TApiVersionsRequestData::ClientSoftwareVersionMeta::Type TApiVersionsRequestData::ClientSoftwareVersionMeta::Default = {""};

TApiVersionsRequestData::TApiVersionsRequestData() 
        : ClientSoftwareName(ClientSoftwareNameMeta::Default)
        , ClientSoftwareVersion(ClientSoftwareVersionMeta::Default)
{}

void TApiVersionsRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsRequestData";
    }
    NPrivate::Read<ClientSoftwareNameMeta>(_readable, _version, ClientSoftwareName);
    NPrivate::Read<ClientSoftwareVersionMeta>(_readable, _version, ClientSoftwareVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TApiVersionsRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TApiVersionsRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ClientSoftwareNameMeta>(_collector, _writable, _version, ClientSoftwareName);
    NPrivate::Write<ClientSoftwareVersionMeta>(_collector, _writable, _version, ClientSoftwareVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TApiVersionsRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ClientSoftwareNameMeta>(_collector, _version, ClientSoftwareName);
    NPrivate::Size<ClientSoftwareVersionMeta>(_collector, _version, ClientSoftwareVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TApiVersionsResponseData
//
const TApiVersionsResponseData::ErrorCodeMeta::Type TApiVersionsResponseData::ErrorCodeMeta::Default = 0;
const TApiVersionsResponseData::ThrottleTimeMsMeta::Type TApiVersionsResponseData::ThrottleTimeMsMeta::Default = 0;
const TApiVersionsResponseData::FinalizedFeaturesEpochMeta::Type TApiVersionsResponseData::FinalizedFeaturesEpochMeta::Default = -1;
const TApiVersionsResponseData::ZkMigrationReadyMeta::Type TApiVersionsResponseData::ZkMigrationReadyMeta::Default = false;

TApiVersionsResponseData::TApiVersionsResponseData() 
        : ErrorCode(ErrorCodeMeta::Default)
        , ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , FinalizedFeaturesEpoch(FinalizedFeaturesEpochMeta::Default)
        , ZkMigrationReady(ZkMigrationReadyMeta::Default)
{}

void TApiVersionsResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsResponseData";
    }
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<ApiKeysMeta>(_readable, _version, ApiKeys);
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<SupportedFeaturesMeta>(_readable, _version, SupportedFeatures);
    NPrivate::Read<FinalizedFeaturesEpochMeta>(_readable, _version, FinalizedFeaturesEpoch);
    NPrivate::Read<FinalizedFeaturesMeta>(_readable, _version, FinalizedFeatures);
    NPrivate::Read<ZkMigrationReadyMeta>(_readable, _version, ZkMigrationReady);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                case SupportedFeaturesMeta::Tag:
                    NPrivate::ReadTag<SupportedFeaturesMeta>(_readable, _version, SupportedFeatures);
                    break;
                case FinalizedFeaturesEpochMeta::Tag:
                    NPrivate::ReadTag<FinalizedFeaturesEpochMeta>(_readable, _version, FinalizedFeaturesEpoch);
                    break;
                case FinalizedFeaturesMeta::Tag:
                    NPrivate::ReadTag<FinalizedFeaturesMeta>(_readable, _version, FinalizedFeatures);
                    break;
                case ZkMigrationReadyMeta::Tag:
                    NPrivate::ReadTag<ZkMigrationReadyMeta>(_readable, _version, ZkMigrationReady);
                    break;
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TApiVersionsResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TApiVersionsResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<ApiKeysMeta>(_collector, _writable, _version, ApiKeys);
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<SupportedFeaturesMeta>(_collector, _writable, _version, SupportedFeatures);
    NPrivate::Write<FinalizedFeaturesEpochMeta>(_collector, _writable, _version, FinalizedFeaturesEpoch);
    NPrivate::Write<FinalizedFeaturesMeta>(_collector, _writable, _version, FinalizedFeatures);
    NPrivate::Write<ZkMigrationReadyMeta>(_collector, _writable, _version, ZkMigrationReady);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
        NPrivate::WriteTag<SupportedFeaturesMeta>(_writable, _version, SupportedFeatures);
        NPrivate::WriteTag<FinalizedFeaturesEpochMeta>(_writable, _version, FinalizedFeaturesEpoch);
        NPrivate::WriteTag<FinalizedFeaturesMeta>(_writable, _version, FinalizedFeatures);
        NPrivate::WriteTag<ZkMigrationReadyMeta>(_writable, _version, ZkMigrationReady);
    }
}

i32 TApiVersionsResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<ApiKeysMeta>(_collector, _version, ApiKeys);
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<SupportedFeaturesMeta>(_collector, _version, SupportedFeatures);
    NPrivate::Size<FinalizedFeaturesEpochMeta>(_collector, _version, FinalizedFeaturesEpoch);
    NPrivate::Size<FinalizedFeaturesMeta>(_collector, _version, FinalizedFeatures);
    NPrivate::Size<ZkMigrationReadyMeta>(_collector, _version, ZkMigrationReady);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TApiVersionsResponseData::TApiVersion
//
const TApiVersionsResponseData::TApiVersion::ApiKeyMeta::Type TApiVersionsResponseData::TApiVersion::ApiKeyMeta::Default = 0;
const TApiVersionsResponseData::TApiVersion::MinVersionMeta::Type TApiVersionsResponseData::TApiVersion::MinVersionMeta::Default = 0;
const TApiVersionsResponseData::TApiVersion::MaxVersionMeta::Type TApiVersionsResponseData::TApiVersion::MaxVersionMeta::Default = 0;

TApiVersionsResponseData::TApiVersion::TApiVersion() 
        : ApiKey(ApiKeyMeta::Default)
        , MinVersion(MinVersionMeta::Default)
        , MaxVersion(MaxVersionMeta::Default)
{}

void TApiVersionsResponseData::TApiVersion::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsResponseData::TApiVersion";
    }
    NPrivate::Read<ApiKeyMeta>(_readable, _version, ApiKey);
    NPrivate::Read<MinVersionMeta>(_readable, _version, MinVersion);
    NPrivate::Read<MaxVersionMeta>(_readable, _version, MaxVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TApiVersionsResponseData::TApiVersion::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TApiVersionsResponseData::TApiVersion";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ApiKeyMeta>(_collector, _writable, _version, ApiKey);
    NPrivate::Write<MinVersionMeta>(_collector, _writable, _version, MinVersion);
    NPrivate::Write<MaxVersionMeta>(_collector, _writable, _version, MaxVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TApiVersionsResponseData::TApiVersion::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ApiKeyMeta>(_collector, _version, ApiKey);
    NPrivate::Size<MinVersionMeta>(_collector, _version, MinVersion);
    NPrivate::Size<MaxVersionMeta>(_collector, _version, MaxVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TApiVersionsResponseData::TSupportedFeatureKey
//
const TApiVersionsResponseData::TSupportedFeatureKey::NameMeta::Type TApiVersionsResponseData::TSupportedFeatureKey::NameMeta::Default = {""};
const TApiVersionsResponseData::TSupportedFeatureKey::MinVersionMeta::Type TApiVersionsResponseData::TSupportedFeatureKey::MinVersionMeta::Default = 0;
const TApiVersionsResponseData::TSupportedFeatureKey::MaxVersionMeta::Type TApiVersionsResponseData::TSupportedFeatureKey::MaxVersionMeta::Default = 0;

TApiVersionsResponseData::TSupportedFeatureKey::TSupportedFeatureKey() 
        : Name(NameMeta::Default)
        , MinVersion(MinVersionMeta::Default)
        , MaxVersion(MaxVersionMeta::Default)
{}

void TApiVersionsResponseData::TSupportedFeatureKey::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsResponseData::TSupportedFeatureKey";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<MinVersionMeta>(_readable, _version, MinVersion);
    NPrivate::Read<MaxVersionMeta>(_readable, _version, MaxVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TApiVersionsResponseData::TSupportedFeatureKey::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TApiVersionsResponseData::TSupportedFeatureKey";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<MinVersionMeta>(_collector, _writable, _version, MinVersion);
    NPrivate::Write<MaxVersionMeta>(_collector, _writable, _version, MaxVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TApiVersionsResponseData::TSupportedFeatureKey::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<MinVersionMeta>(_collector, _version, MinVersion);
    NPrivate::Size<MaxVersionMeta>(_collector, _version, MaxVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TApiVersionsResponseData::TFinalizedFeatureKey
//
const TApiVersionsResponseData::TFinalizedFeatureKey::NameMeta::Type TApiVersionsResponseData::TFinalizedFeatureKey::NameMeta::Default = {""};
const TApiVersionsResponseData::TFinalizedFeatureKey::MaxVersionLevelMeta::Type TApiVersionsResponseData::TFinalizedFeatureKey::MaxVersionLevelMeta::Default = 0;
const TApiVersionsResponseData::TFinalizedFeatureKey::MinVersionLevelMeta::Type TApiVersionsResponseData::TFinalizedFeatureKey::MinVersionLevelMeta::Default = 0;

TApiVersionsResponseData::TFinalizedFeatureKey::TFinalizedFeatureKey() 
        : Name(NameMeta::Default)
        , MaxVersionLevel(MaxVersionLevelMeta::Default)
        , MinVersionLevel(MinVersionLevelMeta::Default)
{}

void TApiVersionsResponseData::TFinalizedFeatureKey::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsResponseData::TFinalizedFeatureKey";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<MaxVersionLevelMeta>(_readable, _version, MaxVersionLevel);
    NPrivate::Read<MinVersionLevelMeta>(_readable, _version, MinVersionLevel);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TApiVersionsResponseData::TFinalizedFeatureKey::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TApiVersionsResponseData::TFinalizedFeatureKey";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<MaxVersionLevelMeta>(_collector, _writable, _version, MaxVersionLevel);
    NPrivate::Write<MinVersionLevelMeta>(_collector, _writable, _version, MinVersionLevel);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TApiVersionsResponseData::TFinalizedFeatureKey::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<MaxVersionLevelMeta>(_collector, _version, MaxVersionLevel);
    NPrivate::Size<MinVersionLevelMeta>(_collector, _version, MinVersionLevel);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TCreateTopicsRequestData
//
const TCreateTopicsRequestData::TimeoutMsMeta::Type TCreateTopicsRequestData::TimeoutMsMeta::Default = 60000;
const TCreateTopicsRequestData::ValidateOnlyMeta::Type TCreateTopicsRequestData::ValidateOnlyMeta::Default = false;

TCreateTopicsRequestData::TCreateTopicsRequestData() 
        : TimeoutMs(TimeoutMsMeta::Default)
        , ValidateOnly(ValidateOnlyMeta::Default)
{}

void TCreateTopicsRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TCreateTopicsRequestData";
    }
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    NPrivate::Read<TimeoutMsMeta>(_readable, _version, TimeoutMs);
    NPrivate::Read<ValidateOnlyMeta>(_readable, _version, ValidateOnly);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TCreateTopicsRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TCreateTopicsRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    NPrivate::Write<TimeoutMsMeta>(_collector, _writable, _version, TimeoutMs);
    NPrivate::Write<ValidateOnlyMeta>(_collector, _writable, _version, ValidateOnly);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TCreateTopicsRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    NPrivate::Size<TimeoutMsMeta>(_collector, _version, TimeoutMs);
    NPrivate::Size<ValidateOnlyMeta>(_collector, _version, ValidateOnly);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TCreateTopicsRequestData::TCreatableTopic
//
const TCreateTopicsRequestData::TCreatableTopic::NameMeta::Type TCreateTopicsRequestData::TCreatableTopic::NameMeta::Default = {""};
const TCreateTopicsRequestData::TCreatableTopic::NumPartitionsMeta::Type TCreateTopicsRequestData::TCreatableTopic::NumPartitionsMeta::Default = 0;
const TCreateTopicsRequestData::TCreatableTopic::ReplicationFactorMeta::Type TCreateTopicsRequestData::TCreatableTopic::ReplicationFactorMeta::Default = 0;

TCreateTopicsRequestData::TCreatableTopic::TCreatableTopic() 
        : Name(NameMeta::Default)
        , NumPartitions(NumPartitionsMeta::Default)
        , ReplicationFactor(ReplicationFactorMeta::Default)
{}

void TCreateTopicsRequestData::TCreatableTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TCreateTopicsRequestData::TCreatableTopic";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<NumPartitionsMeta>(_readable, _version, NumPartitions);
    NPrivate::Read<ReplicationFactorMeta>(_readable, _version, ReplicationFactor);
    NPrivate::Read<AssignmentsMeta>(_readable, _version, Assignments);
    NPrivate::Read<ConfigsMeta>(_readable, _version, Configs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TCreateTopicsRequestData::TCreatableTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TCreateTopicsRequestData::TCreatableTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<NumPartitionsMeta>(_collector, _writable, _version, NumPartitions);
    NPrivate::Write<ReplicationFactorMeta>(_collector, _writable, _version, ReplicationFactor);
    NPrivate::Write<AssignmentsMeta>(_collector, _writable, _version, Assignments);
    NPrivate::Write<ConfigsMeta>(_collector, _writable, _version, Configs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TCreateTopicsRequestData::TCreatableTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<NumPartitionsMeta>(_collector, _version, NumPartitions);
    NPrivate::Size<ReplicationFactorMeta>(_collector, _version, ReplicationFactor);
    NPrivate::Size<AssignmentsMeta>(_collector, _version, Assignments);
    NPrivate::Size<ConfigsMeta>(_collector, _version, Configs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TCreateTopicsRequestData::TCreatableTopic::TCreatableReplicaAssignment
//
const TCreateTopicsRequestData::TCreatableTopic::TCreatableReplicaAssignment::PartitionIndexMeta::Type TCreateTopicsRequestData::TCreatableTopic::TCreatableReplicaAssignment::PartitionIndexMeta::Default = 0;

TCreateTopicsRequestData::TCreatableTopic::TCreatableReplicaAssignment::TCreatableReplicaAssignment() 
        : PartitionIndex(PartitionIndexMeta::Default)
{}

void TCreateTopicsRequestData::TCreatableTopic::TCreatableReplicaAssignment::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TCreateTopicsRequestData::TCreatableTopic::TCreatableReplicaAssignment";
    }
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, PartitionIndex);
    NPrivate::Read<BrokerIdsMeta>(_readable, _version, BrokerIds);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TCreateTopicsRequestData::TCreatableTopic::TCreatableReplicaAssignment::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TCreateTopicsRequestData::TCreatableTopic::TCreatableReplicaAssignment";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, PartitionIndex);
    NPrivate::Write<BrokerIdsMeta>(_collector, _writable, _version, BrokerIds);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TCreateTopicsRequestData::TCreatableTopic::TCreatableReplicaAssignment::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<PartitionIndexMeta>(_collector, _version, PartitionIndex);
    NPrivate::Size<BrokerIdsMeta>(_collector, _version, BrokerIds);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig
//
const TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig::NameMeta::Type TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig::NameMeta::Default = {""};
const TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig::ValueMeta::Type TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig::ValueMeta::Default = {""};

TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig::TCreateableTopicConfig() 
        : Name(NameMeta::Default)
        , Value(ValueMeta::Default)
{}

void TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<ValueMeta>(_readable, _version, Value);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<ValueMeta>(_collector, _writable, _version, Value);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TCreateTopicsRequestData::TCreatableTopic::TCreateableTopicConfig::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<ValueMeta>(_collector, _version, Value);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TCreateTopicsResponseData
//
const TCreateTopicsResponseData::ThrottleTimeMsMeta::Type TCreateTopicsResponseData::ThrottleTimeMsMeta::Default = 0;

TCreateTopicsResponseData::TCreateTopicsResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
{}

void TCreateTopicsResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TCreateTopicsResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TCreateTopicsResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TCreateTopicsResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TCreateTopicsResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TCreateTopicsResponseData::TCreatableTopicResult
//
const TCreateTopicsResponseData::TCreatableTopicResult::NameMeta::Type TCreateTopicsResponseData::TCreatableTopicResult::NameMeta::Default = {""};
const TCreateTopicsResponseData::TCreatableTopicResult::TopicIdMeta::Type TCreateTopicsResponseData::TCreatableTopicResult::TopicIdMeta::Default = TKafkaUuid(0, 0);
const TCreateTopicsResponseData::TCreatableTopicResult::ErrorCodeMeta::Type TCreateTopicsResponseData::TCreatableTopicResult::ErrorCodeMeta::Default = 0;
const TCreateTopicsResponseData::TCreatableTopicResult::ErrorMessageMeta::Type TCreateTopicsResponseData::TCreatableTopicResult::ErrorMessageMeta::Default = {""};
const TCreateTopicsResponseData::TCreatableTopicResult::TopicConfigErrorCodeMeta::Type TCreateTopicsResponseData::TCreatableTopicResult::TopicConfigErrorCodeMeta::Default = 0;
const TCreateTopicsResponseData::TCreatableTopicResult::NumPartitionsMeta::Type TCreateTopicsResponseData::TCreatableTopicResult::NumPartitionsMeta::Default = -1;
const TCreateTopicsResponseData::TCreatableTopicResult::ReplicationFactorMeta::Type TCreateTopicsResponseData::TCreatableTopicResult::ReplicationFactorMeta::Default = -1;

TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicResult() 
        : Name(NameMeta::Default)
        , TopicId(TopicIdMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , ErrorMessage(ErrorMessageMeta::Default)
        , TopicConfigErrorCode(TopicConfigErrorCodeMeta::Default)
        , NumPartitions(NumPartitionsMeta::Default)
        , ReplicationFactor(ReplicationFactorMeta::Default)
{}

void TCreateTopicsResponseData::TCreatableTopicResult::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TCreateTopicsResponseData::TCreatableTopicResult";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<TopicIdMeta>(_readable, _version, TopicId);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<ErrorMessageMeta>(_readable, _version, ErrorMessage);
    NPrivate::Read<TopicConfigErrorCodeMeta>(_readable, _version, TopicConfigErrorCode);
    NPrivate::Read<NumPartitionsMeta>(_readable, _version, NumPartitions);
    NPrivate::Read<ReplicationFactorMeta>(_readable, _version, ReplicationFactor);
    NPrivate::Read<ConfigsMeta>(_readable, _version, Configs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                case TopicConfigErrorCodeMeta::Tag:
                    NPrivate::ReadTag<TopicConfigErrorCodeMeta>(_readable, _version, TopicConfigErrorCode);
                    break;
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TCreateTopicsResponseData::TCreatableTopicResult::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TCreateTopicsResponseData::TCreatableTopicResult";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, TopicId);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<ErrorMessageMeta>(_collector, _writable, _version, ErrorMessage);
    NPrivate::Write<TopicConfigErrorCodeMeta>(_collector, _writable, _version, TopicConfigErrorCode);
    NPrivate::Write<NumPartitionsMeta>(_collector, _writable, _version, NumPartitions);
    NPrivate::Write<ReplicationFactorMeta>(_collector, _writable, _version, ReplicationFactor);
    NPrivate::Write<ConfigsMeta>(_collector, _writable, _version, Configs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
        NPrivate::WriteTag<TopicConfigErrorCodeMeta>(_writable, _version, TopicConfigErrorCode);
    }
}

i32 TCreateTopicsResponseData::TCreatableTopicResult::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<TopicIdMeta>(_collector, _version, TopicId);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<ErrorMessageMeta>(_collector, _version, ErrorMessage);
    NPrivate::Size<TopicConfigErrorCodeMeta>(_collector, _version, TopicConfigErrorCode);
    NPrivate::Size<NumPartitionsMeta>(_collector, _version, NumPartitions);
    NPrivate::Size<ReplicationFactorMeta>(_collector, _version, ReplicationFactor);
    NPrivate::Size<ConfigsMeta>(_collector, _version, Configs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs
//
const TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs::NameMeta::Type TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs::NameMeta::Default = {""};
const TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs::ValueMeta::Type TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs::ValueMeta::Default = {""};
const TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs::ReadOnlyMeta::Type TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs::ReadOnlyMeta::Default = false;
const TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs::ConfigSourceMeta::Type TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs::ConfigSourceMeta::Default = -1;
const TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs::IsSensitiveMeta::Type TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs::IsSensitiveMeta::Default = false;

TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs::TCreatableTopicConfigs() 
        : Name(NameMeta::Default)
        , Value(ValueMeta::Default)
        , ReadOnly(ReadOnlyMeta::Default)
        , ConfigSource(ConfigSourceMeta::Default)
        , IsSensitive(IsSensitiveMeta::Default)
{}

void TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<ValueMeta>(_readable, _version, Value);
    NPrivate::Read<ReadOnlyMeta>(_readable, _version, ReadOnly);
    NPrivate::Read<ConfigSourceMeta>(_readable, _version, ConfigSource);
    NPrivate::Read<IsSensitiveMeta>(_readable, _version, IsSensitive);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<ValueMeta>(_collector, _writable, _version, Value);
    NPrivate::Write<ReadOnlyMeta>(_collector, _writable, _version, ReadOnly);
    NPrivate::Write<ConfigSourceMeta>(_collector, _writable, _version, ConfigSource);
    NPrivate::Write<IsSensitiveMeta>(_collector, _writable, _version, IsSensitive);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TCreateTopicsResponseData::TCreatableTopicResult::TCreatableTopicConfigs::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<ValueMeta>(_collector, _version, Value);
    NPrivate::Size<ReadOnlyMeta>(_collector, _version, ReadOnly);
    NPrivate::Size<ConfigSourceMeta>(_collector, _version, ConfigSource);
    NPrivate::Size<IsSensitiveMeta>(_collector, _version, IsSensitive);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TInitProducerIdRequestData
//
const TInitProducerIdRequestData::TransactionalIdMeta::Type TInitProducerIdRequestData::TransactionalIdMeta::Default = {""};
const TInitProducerIdRequestData::TransactionTimeoutMsMeta::Type TInitProducerIdRequestData::TransactionTimeoutMsMeta::Default = 0;
const TInitProducerIdRequestData::ProducerIdMeta::Type TInitProducerIdRequestData::ProducerIdMeta::Default = -1;
const TInitProducerIdRequestData::ProducerEpochMeta::Type TInitProducerIdRequestData::ProducerEpochMeta::Default = -1;

TInitProducerIdRequestData::TInitProducerIdRequestData() 
        : TransactionalId(TransactionalIdMeta::Default)
        , TransactionTimeoutMs(TransactionTimeoutMsMeta::Default)
        , ProducerId(ProducerIdMeta::Default)
        , ProducerEpoch(ProducerEpochMeta::Default)
{}

void TInitProducerIdRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TInitProducerIdRequestData";
    }
    NPrivate::Read<TransactionalIdMeta>(_readable, _version, TransactionalId);
    NPrivate::Read<TransactionTimeoutMsMeta>(_readable, _version, TransactionTimeoutMs);
    NPrivate::Read<ProducerIdMeta>(_readable, _version, ProducerId);
    NPrivate::Read<ProducerEpochMeta>(_readable, _version, ProducerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TInitProducerIdRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TInitProducerIdRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TransactionalIdMeta>(_collector, _writable, _version, TransactionalId);
    NPrivate::Write<TransactionTimeoutMsMeta>(_collector, _writable, _version, TransactionTimeoutMs);
    NPrivate::Write<ProducerIdMeta>(_collector, _writable, _version, ProducerId);
    NPrivate::Write<ProducerEpochMeta>(_collector, _writable, _version, ProducerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TInitProducerIdRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TransactionalIdMeta>(_collector, _version, TransactionalId);
    NPrivate::Size<TransactionTimeoutMsMeta>(_collector, _version, TransactionTimeoutMs);
    NPrivate::Size<ProducerIdMeta>(_collector, _version, ProducerId);
    NPrivate::Size<ProducerEpochMeta>(_collector, _version, ProducerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TInitProducerIdResponseData
//
const TInitProducerIdResponseData::ThrottleTimeMsMeta::Type TInitProducerIdResponseData::ThrottleTimeMsMeta::Default = 0;
const TInitProducerIdResponseData::ErrorCodeMeta::Type TInitProducerIdResponseData::ErrorCodeMeta::Default = 0;
const TInitProducerIdResponseData::ProducerIdMeta::Type TInitProducerIdResponseData::ProducerIdMeta::Default = -1;
const TInitProducerIdResponseData::ProducerEpochMeta::Type TInitProducerIdResponseData::ProducerEpochMeta::Default = 0;

TInitProducerIdResponseData::TInitProducerIdResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , ProducerId(ProducerIdMeta::Default)
        , ProducerEpoch(ProducerEpochMeta::Default)
{}

void TInitProducerIdResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TInitProducerIdResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<ProducerIdMeta>(_readable, _version, ProducerId);
    NPrivate::Read<ProducerEpochMeta>(_readable, _version, ProducerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TInitProducerIdResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TInitProducerIdResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<ProducerIdMeta>(_collector, _writable, _version, ProducerId);
    NPrivate::Write<ProducerEpochMeta>(_collector, _writable, _version, ProducerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TInitProducerIdResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<ProducerIdMeta>(_collector, _version, ProducerId);
    NPrivate::Size<ProducerEpochMeta>(_collector, _version, ProducerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TAddPartitionsToTxnRequestData
//
const TAddPartitionsToTxnRequestData::TransactionalIdMeta::Type TAddPartitionsToTxnRequestData::TransactionalIdMeta::Default = {""};
const TAddPartitionsToTxnRequestData::ProducerIdMeta::Type TAddPartitionsToTxnRequestData::ProducerIdMeta::Default = 0;
const TAddPartitionsToTxnRequestData::ProducerEpochMeta::Type TAddPartitionsToTxnRequestData::ProducerEpochMeta::Default = 0;

TAddPartitionsToTxnRequestData::TAddPartitionsToTxnRequestData() 
        : TransactionalId(TransactionalIdMeta::Default)
        , ProducerId(ProducerIdMeta::Default)
        , ProducerEpoch(ProducerEpochMeta::Default)
{}

void TAddPartitionsToTxnRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TAddPartitionsToTxnRequestData";
    }
    NPrivate::Read<TransactionalIdMeta>(_readable, _version, TransactionalId);
    NPrivate::Read<ProducerIdMeta>(_readable, _version, ProducerId);
    NPrivate::Read<ProducerEpochMeta>(_readable, _version, ProducerEpoch);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TAddPartitionsToTxnRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TAddPartitionsToTxnRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TransactionalIdMeta>(_collector, _writable, _version, TransactionalId);
    NPrivate::Write<ProducerIdMeta>(_collector, _writable, _version, ProducerId);
    NPrivate::Write<ProducerEpochMeta>(_collector, _writable, _version, ProducerEpoch);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TAddPartitionsToTxnRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TransactionalIdMeta>(_collector, _version, TransactionalId);
    NPrivate::Size<ProducerIdMeta>(_collector, _version, ProducerId);
    NPrivate::Size<ProducerEpochMeta>(_collector, _version, ProducerEpoch);
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TAddPartitionsToTxnRequestData::TAddPartitionsToTxnTopic
//
const TAddPartitionsToTxnRequestData::TAddPartitionsToTxnTopic::NameMeta::Type TAddPartitionsToTxnRequestData::TAddPartitionsToTxnTopic::NameMeta::Default = {""};

TAddPartitionsToTxnRequestData::TAddPartitionsToTxnTopic::TAddPartitionsToTxnTopic() 
        : Name(NameMeta::Default)
{}

void TAddPartitionsToTxnRequestData::TAddPartitionsToTxnTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TAddPartitionsToTxnRequestData::TAddPartitionsToTxnTopic";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TAddPartitionsToTxnRequestData::TAddPartitionsToTxnTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TAddPartitionsToTxnRequestData::TAddPartitionsToTxnTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TAddPartitionsToTxnRequestData::TAddPartitionsToTxnTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TAddPartitionsToTxnResponseData
//
const TAddPartitionsToTxnResponseData::ThrottleTimeMsMeta::Type TAddPartitionsToTxnResponseData::ThrottleTimeMsMeta::Default = 0;

TAddPartitionsToTxnResponseData::TAddPartitionsToTxnResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
{}

void TAddPartitionsToTxnResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TAddPartitionsToTxnResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ResultsMeta>(_readable, _version, Results);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TAddPartitionsToTxnResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TAddPartitionsToTxnResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ResultsMeta>(_collector, _writable, _version, Results);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TAddPartitionsToTxnResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ResultsMeta>(_collector, _version, Results);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult
//
const TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::NameMeta::Type TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::NameMeta::Default = {""};

TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnTopicResult() 
        : Name(NameMeta::Default)
{}

void TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<ResultsMeta>(_readable, _version, Results);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<ResultsMeta>(_collector, _writable, _version, Results);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<ResultsMeta>(_collector, _version, Results);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult
//
const TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult::PartitionIndexMeta::Type TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult::PartitionIndexMeta::Default = 0;
const TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult::ErrorCodeMeta::Type TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult::ErrorCodeMeta::Default = 0;

TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult::TAddPartitionsToTxnPartitionResult() 
        : PartitionIndex(PartitionIndexMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
{}

void TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult";
    }
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, PartitionIndex);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, PartitionIndex);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TAddPartitionsToTxnResponseData::TAddPartitionsToTxnTopicResult::TAddPartitionsToTxnPartitionResult::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<PartitionIndexMeta>(_collector, _version, PartitionIndex);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TAddOffsetsToTxnRequestData
//
const TAddOffsetsToTxnRequestData::TransactionalIdMeta::Type TAddOffsetsToTxnRequestData::TransactionalIdMeta::Default = {""};
const TAddOffsetsToTxnRequestData::ProducerIdMeta::Type TAddOffsetsToTxnRequestData::ProducerIdMeta::Default = 0;
const TAddOffsetsToTxnRequestData::ProducerEpochMeta::Type TAddOffsetsToTxnRequestData::ProducerEpochMeta::Default = 0;
const TAddOffsetsToTxnRequestData::GroupIdMeta::Type TAddOffsetsToTxnRequestData::GroupIdMeta::Default = {""};

TAddOffsetsToTxnRequestData::TAddOffsetsToTxnRequestData() 
        : TransactionalId(TransactionalIdMeta::Default)
        , ProducerId(ProducerIdMeta::Default)
        , ProducerEpoch(ProducerEpochMeta::Default)
        , GroupId(GroupIdMeta::Default)
{}

void TAddOffsetsToTxnRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TAddOffsetsToTxnRequestData";
    }
    NPrivate::Read<TransactionalIdMeta>(_readable, _version, TransactionalId);
    NPrivate::Read<ProducerIdMeta>(_readable, _version, ProducerId);
    NPrivate::Read<ProducerEpochMeta>(_readable, _version, ProducerEpoch);
    NPrivate::Read<GroupIdMeta>(_readable, _version, GroupId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TAddOffsetsToTxnRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TAddOffsetsToTxnRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TransactionalIdMeta>(_collector, _writable, _version, TransactionalId);
    NPrivate::Write<ProducerIdMeta>(_collector, _writable, _version, ProducerId);
    NPrivate::Write<ProducerEpochMeta>(_collector, _writable, _version, ProducerEpoch);
    NPrivate::Write<GroupIdMeta>(_collector, _writable, _version, GroupId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TAddOffsetsToTxnRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TransactionalIdMeta>(_collector, _version, TransactionalId);
    NPrivate::Size<ProducerIdMeta>(_collector, _version, ProducerId);
    NPrivate::Size<ProducerEpochMeta>(_collector, _version, ProducerEpoch);
    NPrivate::Size<GroupIdMeta>(_collector, _version, GroupId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TAddOffsetsToTxnResponseData
//
const TAddOffsetsToTxnResponseData::ThrottleTimeMsMeta::Type TAddOffsetsToTxnResponseData::ThrottleTimeMsMeta::Default = 0;
const TAddOffsetsToTxnResponseData::ErrorCodeMeta::Type TAddOffsetsToTxnResponseData::ErrorCodeMeta::Default = 0;

TAddOffsetsToTxnResponseData::TAddOffsetsToTxnResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
{}

void TAddOffsetsToTxnResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TAddOffsetsToTxnResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TAddOffsetsToTxnResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TAddOffsetsToTxnResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TAddOffsetsToTxnResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TEndTxnRequestData
//
const TEndTxnRequestData::TransactionalIdMeta::Type TEndTxnRequestData::TransactionalIdMeta::Default = {""};
const TEndTxnRequestData::ProducerIdMeta::Type TEndTxnRequestData::ProducerIdMeta::Default = 0;
const TEndTxnRequestData::ProducerEpochMeta::Type TEndTxnRequestData::ProducerEpochMeta::Default = 0;
const TEndTxnRequestData::CommittedMeta::Type TEndTxnRequestData::CommittedMeta::Default = false;

TEndTxnRequestData::TEndTxnRequestData() 
        : TransactionalId(TransactionalIdMeta::Default)
        , ProducerId(ProducerIdMeta::Default)
        , ProducerEpoch(ProducerEpochMeta::Default)
        , Committed(CommittedMeta::Default)
{}

void TEndTxnRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TEndTxnRequestData";
    }
    NPrivate::Read<TransactionalIdMeta>(_readable, _version, TransactionalId);
    NPrivate::Read<ProducerIdMeta>(_readable, _version, ProducerId);
    NPrivate::Read<ProducerEpochMeta>(_readable, _version, ProducerEpoch);
    NPrivate::Read<CommittedMeta>(_readable, _version, Committed);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TEndTxnRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TEndTxnRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TransactionalIdMeta>(_collector, _writable, _version, TransactionalId);
    NPrivate::Write<ProducerIdMeta>(_collector, _writable, _version, ProducerId);
    NPrivate::Write<ProducerEpochMeta>(_collector, _writable, _version, ProducerEpoch);
    NPrivate::Write<CommittedMeta>(_collector, _writable, _version, Committed);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TEndTxnRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TransactionalIdMeta>(_collector, _version, TransactionalId);
    NPrivate::Size<ProducerIdMeta>(_collector, _version, ProducerId);
    NPrivate::Size<ProducerEpochMeta>(_collector, _version, ProducerEpoch);
    NPrivate::Size<CommittedMeta>(_collector, _version, Committed);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TEndTxnResponseData
//
const TEndTxnResponseData::ThrottleTimeMsMeta::Type TEndTxnResponseData::ThrottleTimeMsMeta::Default = 0;
const TEndTxnResponseData::ErrorCodeMeta::Type TEndTxnResponseData::ErrorCodeMeta::Default = 0;

TEndTxnResponseData::TEndTxnResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
{}

void TEndTxnResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TEndTxnResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TEndTxnResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TEndTxnResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TEndTxnResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TTxnOffsetCommitRequestData
//
const TTxnOffsetCommitRequestData::TransactionalIdMeta::Type TTxnOffsetCommitRequestData::TransactionalIdMeta::Default = {""};
const TTxnOffsetCommitRequestData::GroupIdMeta::Type TTxnOffsetCommitRequestData::GroupIdMeta::Default = {""};
const TTxnOffsetCommitRequestData::ProducerIdMeta::Type TTxnOffsetCommitRequestData::ProducerIdMeta::Default = 0;
const TTxnOffsetCommitRequestData::ProducerEpochMeta::Type TTxnOffsetCommitRequestData::ProducerEpochMeta::Default = 0;
const TTxnOffsetCommitRequestData::GenerationIdMeta::Type TTxnOffsetCommitRequestData::GenerationIdMeta::Default = -1;
const TTxnOffsetCommitRequestData::MemberIdMeta::Type TTxnOffsetCommitRequestData::MemberIdMeta::Default = {""};
const TTxnOffsetCommitRequestData::GroupInstanceIdMeta::Type TTxnOffsetCommitRequestData::GroupInstanceIdMeta::Default = std::nullopt;

TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestData() 
        : TransactionalId(TransactionalIdMeta::Default)
        , GroupId(GroupIdMeta::Default)
        , ProducerId(ProducerIdMeta::Default)
        , ProducerEpoch(ProducerEpochMeta::Default)
        , GenerationId(GenerationIdMeta::Default)
        , MemberId(MemberIdMeta::Default)
        , GroupInstanceId(GroupInstanceIdMeta::Default)
{}

void TTxnOffsetCommitRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TTxnOffsetCommitRequestData";
    }
    NPrivate::Read<TransactionalIdMeta>(_readable, _version, TransactionalId);
    NPrivate::Read<GroupIdMeta>(_readable, _version, GroupId);
    NPrivate::Read<ProducerIdMeta>(_readable, _version, ProducerId);
    NPrivate::Read<ProducerEpochMeta>(_readable, _version, ProducerEpoch);
    NPrivate::Read<GenerationIdMeta>(_readable, _version, GenerationId);
    NPrivate::Read<MemberIdMeta>(_readable, _version, MemberId);
    NPrivate::Read<GroupInstanceIdMeta>(_readable, _version, GroupInstanceId);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TTxnOffsetCommitRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TTxnOffsetCommitRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TransactionalIdMeta>(_collector, _writable, _version, TransactionalId);
    NPrivate::Write<GroupIdMeta>(_collector, _writable, _version, GroupId);
    NPrivate::Write<ProducerIdMeta>(_collector, _writable, _version, ProducerId);
    NPrivate::Write<ProducerEpochMeta>(_collector, _writable, _version, ProducerEpoch);
    NPrivate::Write<GenerationIdMeta>(_collector, _writable, _version, GenerationId);
    NPrivate::Write<MemberIdMeta>(_collector, _writable, _version, MemberId);
    NPrivate::Write<GroupInstanceIdMeta>(_collector, _writable, _version, GroupInstanceId);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TTxnOffsetCommitRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TransactionalIdMeta>(_collector, _version, TransactionalId);
    NPrivate::Size<GroupIdMeta>(_collector, _version, GroupId);
    NPrivate::Size<ProducerIdMeta>(_collector, _version, ProducerId);
    NPrivate::Size<ProducerEpochMeta>(_collector, _version, ProducerEpoch);
    NPrivate::Size<GenerationIdMeta>(_collector, _version, GenerationId);
    NPrivate::Size<MemberIdMeta>(_collector, _version, MemberId);
    NPrivate::Size<GroupInstanceIdMeta>(_collector, _version, GroupInstanceId);
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic
//
const TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::NameMeta::Type TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::NameMeta::Default = {""};

TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestTopic() 
        : Name(NameMeta::Default)
{}

void TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition
//
const TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition::PartitionIndexMeta::Type TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition::PartitionIndexMeta::Default = 0;
const TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition::CommittedOffsetMeta::Type TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition::CommittedOffsetMeta::Default = 0;
const TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition::CommittedLeaderEpochMeta::Type TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition::CommittedLeaderEpochMeta::Default = -1;
const TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition::CommittedMetadataMeta::Type TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition::CommittedMetadataMeta::Default = {""};

TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition::TTxnOffsetCommitRequestPartition() 
        : PartitionIndex(PartitionIndexMeta::Default)
        , CommittedOffset(CommittedOffsetMeta::Default)
        , CommittedLeaderEpoch(CommittedLeaderEpochMeta::Default)
        , CommittedMetadata(CommittedMetadataMeta::Default)
{}

void TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition";
    }
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, PartitionIndex);
    NPrivate::Read<CommittedOffsetMeta>(_readable, _version, CommittedOffset);
    NPrivate::Read<CommittedLeaderEpochMeta>(_readable, _version, CommittedLeaderEpoch);
    NPrivate::Read<CommittedMetadataMeta>(_readable, _version, CommittedMetadata);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, PartitionIndex);
    NPrivate::Write<CommittedOffsetMeta>(_collector, _writable, _version, CommittedOffset);
    NPrivate::Write<CommittedLeaderEpochMeta>(_collector, _writable, _version, CommittedLeaderEpoch);
    NPrivate::Write<CommittedMetadataMeta>(_collector, _writable, _version, CommittedMetadata);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TTxnOffsetCommitRequestData::TTxnOffsetCommitRequestTopic::TTxnOffsetCommitRequestPartition::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<PartitionIndexMeta>(_collector, _version, PartitionIndex);
    NPrivate::Size<CommittedOffsetMeta>(_collector, _version, CommittedOffset);
    NPrivate::Size<CommittedLeaderEpochMeta>(_collector, _version, CommittedLeaderEpoch);
    NPrivate::Size<CommittedMetadataMeta>(_collector, _version, CommittedMetadata);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TTxnOffsetCommitResponseData
//
const TTxnOffsetCommitResponseData::ThrottleTimeMsMeta::Type TTxnOffsetCommitResponseData::ThrottleTimeMsMeta::Default = 0;

TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
{}

void TTxnOffsetCommitResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TTxnOffsetCommitResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TTxnOffsetCommitResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TTxnOffsetCommitResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TTxnOffsetCommitResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic
//
const TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::NameMeta::Type TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::NameMeta::Default = {""};

TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponseTopic() 
        : Name(NameMeta::Default)
{}

void TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition
//
const TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition::PartitionIndexMeta::Type TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition::PartitionIndexMeta::Default = 0;
const TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition::ErrorCodeMeta::Type TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition::ErrorCodeMeta::Default = 0;

TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition::TTxnOffsetCommitResponsePartition() 
        : PartitionIndex(PartitionIndexMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
{}

void TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition";
    }
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, PartitionIndex);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, PartitionIndex);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TTxnOffsetCommitResponseData::TTxnOffsetCommitResponseTopic::TTxnOffsetCommitResponsePartition::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<PartitionIndexMeta>(_collector, _version, PartitionIndex);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TDescribeConfigsRequestData
//
const TDescribeConfigsRequestData::IncludeSynonymsMeta::Type TDescribeConfigsRequestData::IncludeSynonymsMeta::Default = false;
const TDescribeConfigsRequestData::IncludeDocumentationMeta::Type TDescribeConfigsRequestData::IncludeDocumentationMeta::Default = false;

TDescribeConfigsRequestData::TDescribeConfigsRequestData() 
        : IncludeSynonyms(IncludeSynonymsMeta::Default)
        , IncludeDocumentation(IncludeDocumentationMeta::Default)
{}

void TDescribeConfigsRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TDescribeConfigsRequestData";
    }
    NPrivate::Read<ResourcesMeta>(_readable, _version, Resources);
    NPrivate::Read<IncludeSynonymsMeta>(_readable, _version, IncludeSynonyms);
    NPrivate::Read<IncludeDocumentationMeta>(_readable, _version, IncludeDocumentation);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TDescribeConfigsRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TDescribeConfigsRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ResourcesMeta>(_collector, _writable, _version, Resources);
    NPrivate::Write<IncludeSynonymsMeta>(_collector, _writable, _version, IncludeSynonyms);
    NPrivate::Write<IncludeDocumentationMeta>(_collector, _writable, _version, IncludeDocumentation);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TDescribeConfigsRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ResourcesMeta>(_collector, _version, Resources);
    NPrivate::Size<IncludeSynonymsMeta>(_collector, _version, IncludeSynonyms);
    NPrivate::Size<IncludeDocumentationMeta>(_collector, _version, IncludeDocumentation);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TDescribeConfigsRequestData::TDescribeConfigsResource
//
const TDescribeConfigsRequestData::TDescribeConfigsResource::ResourceTypeMeta::Type TDescribeConfigsRequestData::TDescribeConfigsResource::ResourceTypeMeta::Default = 0;
const TDescribeConfigsRequestData::TDescribeConfigsResource::ResourceNameMeta::Type TDescribeConfigsRequestData::TDescribeConfigsResource::ResourceNameMeta::Default = {""};

TDescribeConfigsRequestData::TDescribeConfigsResource::TDescribeConfigsResource() 
        : ResourceType(ResourceTypeMeta::Default)
        , ResourceName(ResourceNameMeta::Default)
{}

void TDescribeConfigsRequestData::TDescribeConfigsResource::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TDescribeConfigsRequestData::TDescribeConfigsResource";
    }
    NPrivate::Read<ResourceTypeMeta>(_readable, _version, ResourceType);
    NPrivate::Read<ResourceNameMeta>(_readable, _version, ResourceName);
    NPrivate::Read<ConfigurationKeysMeta>(_readable, _version, ConfigurationKeys);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TDescribeConfigsRequestData::TDescribeConfigsResource::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TDescribeConfigsRequestData::TDescribeConfigsResource";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ResourceTypeMeta>(_collector, _writable, _version, ResourceType);
    NPrivate::Write<ResourceNameMeta>(_collector, _writable, _version, ResourceName);
    NPrivate::Write<ConfigurationKeysMeta>(_collector, _writable, _version, ConfigurationKeys);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TDescribeConfigsRequestData::TDescribeConfigsResource::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ResourceTypeMeta>(_collector, _version, ResourceType);
    NPrivate::Size<ResourceNameMeta>(_collector, _version, ResourceName);
    NPrivate::Size<ConfigurationKeysMeta>(_collector, _version, ConfigurationKeys);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TDescribeConfigsResponseData
//
const TDescribeConfigsResponseData::ThrottleTimeMsMeta::Type TDescribeConfigsResponseData::ThrottleTimeMsMeta::Default = 0;

TDescribeConfigsResponseData::TDescribeConfigsResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
{}

void TDescribeConfigsResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TDescribeConfigsResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ResultsMeta>(_readable, _version, Results);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TDescribeConfigsResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TDescribeConfigsResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ResultsMeta>(_collector, _writable, _version, Results);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TDescribeConfigsResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ResultsMeta>(_collector, _version, Results);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TDescribeConfigsResponseData::TDescribeConfigsResult
//
const TDescribeConfigsResponseData::TDescribeConfigsResult::ErrorCodeMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::ErrorCodeMeta::Default = 0;
const TDescribeConfigsResponseData::TDescribeConfigsResult::ErrorMessageMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::ErrorMessageMeta::Default = {""};
const TDescribeConfigsResponseData::TDescribeConfigsResult::ResourceTypeMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::ResourceTypeMeta::Default = 0;
const TDescribeConfigsResponseData::TDescribeConfigsResult::ResourceNameMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::ResourceNameMeta::Default = {""};

TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResult() 
        : ErrorCode(ErrorCodeMeta::Default)
        , ErrorMessage(ErrorMessageMeta::Default)
        , ResourceType(ResourceTypeMeta::Default)
        , ResourceName(ResourceNameMeta::Default)
{}

void TDescribeConfigsResponseData::TDescribeConfigsResult::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TDescribeConfigsResponseData::TDescribeConfigsResult";
    }
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<ErrorMessageMeta>(_readable, _version, ErrorMessage);
    NPrivate::Read<ResourceTypeMeta>(_readable, _version, ResourceType);
    NPrivate::Read<ResourceNameMeta>(_readable, _version, ResourceName);
    NPrivate::Read<ConfigsMeta>(_readable, _version, Configs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TDescribeConfigsResponseData::TDescribeConfigsResult::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TDescribeConfigsResponseData::TDescribeConfigsResult";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<ErrorMessageMeta>(_collector, _writable, _version, ErrorMessage);
    NPrivate::Write<ResourceTypeMeta>(_collector, _writable, _version, ResourceType);
    NPrivate::Write<ResourceNameMeta>(_collector, _writable, _version, ResourceName);
    NPrivate::Write<ConfigsMeta>(_collector, _writable, _version, Configs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TDescribeConfigsResponseData::TDescribeConfigsResult::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<ErrorMessageMeta>(_collector, _version, ErrorMessage);
    NPrivate::Size<ResourceTypeMeta>(_collector, _version, ResourceType);
    NPrivate::Size<ResourceNameMeta>(_collector, _version, ResourceName);
    NPrivate::Size<ConfigsMeta>(_collector, _version, Configs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult
//
const TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::NameMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::NameMeta::Default = {""};
const TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::ValueMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::ValueMeta::Default = {""};
const TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::ReadOnlyMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::ReadOnlyMeta::Default = false;
const TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::IsDefaultMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::IsDefaultMeta::Default = false;
const TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::ConfigSourceMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::ConfigSourceMeta::Default = -1;
const TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::IsSensitiveMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::IsSensitiveMeta::Default = false;
const TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::ConfigTypeMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::ConfigTypeMeta::Default = 0;
const TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::DocumentationMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::DocumentationMeta::Default = {""};

TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::TDescribeConfigsResourceResult() 
        : Name(NameMeta::Default)
        , Value(ValueMeta::Default)
        , ReadOnly(ReadOnlyMeta::Default)
        , IsDefault(IsDefaultMeta::Default)
        , ConfigSource(ConfigSourceMeta::Default)
        , IsSensitive(IsSensitiveMeta::Default)
        , ConfigType(ConfigTypeMeta::Default)
        , Documentation(DocumentationMeta::Default)
{}

void TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<ValueMeta>(_readable, _version, Value);
    NPrivate::Read<ReadOnlyMeta>(_readable, _version, ReadOnly);
    NPrivate::Read<IsDefaultMeta>(_readable, _version, IsDefault);
    NPrivate::Read<ConfigSourceMeta>(_readable, _version, ConfigSource);
    NPrivate::Read<IsSensitiveMeta>(_readable, _version, IsSensitive);
    NPrivate::Read<SynonymsMeta>(_readable, _version, Synonyms);
    NPrivate::Read<ConfigTypeMeta>(_readable, _version, ConfigType);
    NPrivate::Read<DocumentationMeta>(_readable, _version, Documentation);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<ValueMeta>(_collector, _writable, _version, Value);
    NPrivate::Write<ReadOnlyMeta>(_collector, _writable, _version, ReadOnly);
    NPrivate::Write<IsDefaultMeta>(_collector, _writable, _version, IsDefault);
    NPrivate::Write<ConfigSourceMeta>(_collector, _writable, _version, ConfigSource);
    NPrivate::Write<IsSensitiveMeta>(_collector, _writable, _version, IsSensitive);
    NPrivate::Write<SynonymsMeta>(_collector, _writable, _version, Synonyms);
    NPrivate::Write<ConfigTypeMeta>(_collector, _writable, _version, ConfigType);
    NPrivate::Write<DocumentationMeta>(_collector, _writable, _version, Documentation);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<ValueMeta>(_collector, _version, Value);
    NPrivate::Size<ReadOnlyMeta>(_collector, _version, ReadOnly);
    NPrivate::Size<IsDefaultMeta>(_collector, _version, IsDefault);
    NPrivate::Size<ConfigSourceMeta>(_collector, _version, ConfigSource);
    NPrivate::Size<IsSensitiveMeta>(_collector, _version, IsSensitive);
    NPrivate::Size<SynonymsMeta>(_collector, _version, Synonyms);
    NPrivate::Size<ConfigTypeMeta>(_collector, _version, ConfigType);
    NPrivate::Size<DocumentationMeta>(_collector, _version, Documentation);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::TDescribeConfigsSynonym
//
const TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::TDescribeConfigsSynonym::NameMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::TDescribeConfigsSynonym::NameMeta::Default = {""};
const TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::TDescribeConfigsSynonym::ValueMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::TDescribeConfigsSynonym::ValueMeta::Default = {""};
const TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::TDescribeConfigsSynonym::SourceMeta::Type TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::TDescribeConfigsSynonym::SourceMeta::Default = 0;

TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::TDescribeConfigsSynonym::TDescribeConfigsSynonym() 
        : Name(NameMeta::Default)
        , Value(ValueMeta::Default)
        , Source(SourceMeta::Default)
{}

void TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::TDescribeConfigsSynonym::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::TDescribeConfigsSynonym";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<ValueMeta>(_readable, _version, Value);
    NPrivate::Read<SourceMeta>(_readable, _version, Source);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::TDescribeConfigsSynonym::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::TDescribeConfigsSynonym";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<ValueMeta>(_collector, _writable, _version, Value);
    NPrivate::Write<SourceMeta>(_collector, _writable, _version, Source);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TDescribeConfigsResponseData::TDescribeConfigsResult::TDescribeConfigsResourceResult::TDescribeConfigsSynonym::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<ValueMeta>(_collector, _version, Value);
    NPrivate::Size<SourceMeta>(_collector, _version, Source);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TAlterConfigsRequestData
//
const TAlterConfigsRequestData::ValidateOnlyMeta::Type TAlterConfigsRequestData::ValidateOnlyMeta::Default = false;

TAlterConfigsRequestData::TAlterConfigsRequestData() 
        : ValidateOnly(ValidateOnlyMeta::Default)
{}

void TAlterConfigsRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TAlterConfigsRequestData";
    }
    NPrivate::Read<ResourcesMeta>(_readable, _version, Resources);
    NPrivate::Read<ValidateOnlyMeta>(_readable, _version, ValidateOnly);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TAlterConfigsRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TAlterConfigsRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ResourcesMeta>(_collector, _writable, _version, Resources);
    NPrivate::Write<ValidateOnlyMeta>(_collector, _writable, _version, ValidateOnly);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TAlterConfigsRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ResourcesMeta>(_collector, _version, Resources);
    NPrivate::Size<ValidateOnlyMeta>(_collector, _version, ValidateOnly);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TAlterConfigsRequestData::TAlterConfigsResource
//
const TAlterConfigsRequestData::TAlterConfigsResource::ResourceTypeMeta::Type TAlterConfigsRequestData::TAlterConfigsResource::ResourceTypeMeta::Default = 0;
const TAlterConfigsRequestData::TAlterConfigsResource::ResourceNameMeta::Type TAlterConfigsRequestData::TAlterConfigsResource::ResourceNameMeta::Default = {""};

TAlterConfigsRequestData::TAlterConfigsResource::TAlterConfigsResource() 
        : ResourceType(ResourceTypeMeta::Default)
        , ResourceName(ResourceNameMeta::Default)
{}

void TAlterConfigsRequestData::TAlterConfigsResource::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TAlterConfigsRequestData::TAlterConfigsResource";
    }
    NPrivate::Read<ResourceTypeMeta>(_readable, _version, ResourceType);
    NPrivate::Read<ResourceNameMeta>(_readable, _version, ResourceName);
    NPrivate::Read<ConfigsMeta>(_readable, _version, Configs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TAlterConfigsRequestData::TAlterConfigsResource::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TAlterConfigsRequestData::TAlterConfigsResource";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ResourceTypeMeta>(_collector, _writable, _version, ResourceType);
    NPrivate::Write<ResourceNameMeta>(_collector, _writable, _version, ResourceName);
    NPrivate::Write<ConfigsMeta>(_collector, _writable, _version, Configs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TAlterConfigsRequestData::TAlterConfigsResource::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ResourceTypeMeta>(_collector, _version, ResourceType);
    NPrivate::Size<ResourceNameMeta>(_collector, _version, ResourceName);
    NPrivate::Size<ConfigsMeta>(_collector, _version, Configs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TAlterConfigsRequestData::TAlterConfigsResource::TAlterableConfig
//
const TAlterConfigsRequestData::TAlterConfigsResource::TAlterableConfig::NameMeta::Type TAlterConfigsRequestData::TAlterConfigsResource::TAlterableConfig::NameMeta::Default = {""};
const TAlterConfigsRequestData::TAlterConfigsResource::TAlterableConfig::ValueMeta::Type TAlterConfigsRequestData::TAlterConfigsResource::TAlterableConfig::ValueMeta::Default = {""};

TAlterConfigsRequestData::TAlterConfigsResource::TAlterableConfig::TAlterableConfig() 
        : Name(NameMeta::Default)
        , Value(ValueMeta::Default)
{}

void TAlterConfigsRequestData::TAlterConfigsResource::TAlterableConfig::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TAlterConfigsRequestData::TAlterConfigsResource::TAlterableConfig";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<ValueMeta>(_readable, _version, Value);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TAlterConfigsRequestData::TAlterConfigsResource::TAlterableConfig::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TAlterConfigsRequestData::TAlterConfigsResource::TAlterableConfig";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<ValueMeta>(_collector, _writable, _version, Value);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TAlterConfigsRequestData::TAlterConfigsResource::TAlterableConfig::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<ValueMeta>(_collector, _version, Value);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TAlterConfigsResponseData
//
const TAlterConfigsResponseData::ThrottleTimeMsMeta::Type TAlterConfigsResponseData::ThrottleTimeMsMeta::Default = 0;

TAlterConfigsResponseData::TAlterConfigsResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
{}

void TAlterConfigsResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TAlterConfigsResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ResponsesMeta>(_readable, _version, Responses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TAlterConfigsResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TAlterConfigsResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ResponsesMeta>(_collector, _writable, _version, Responses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TAlterConfigsResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ResponsesMeta>(_collector, _version, Responses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TAlterConfigsResponseData::TAlterConfigsResourceResponse
//
const TAlterConfigsResponseData::TAlterConfigsResourceResponse::ErrorCodeMeta::Type TAlterConfigsResponseData::TAlterConfigsResourceResponse::ErrorCodeMeta::Default = 0;
const TAlterConfigsResponseData::TAlterConfigsResourceResponse::ErrorMessageMeta::Type TAlterConfigsResponseData::TAlterConfigsResourceResponse::ErrorMessageMeta::Default = {""};
const TAlterConfigsResponseData::TAlterConfigsResourceResponse::ResourceTypeMeta::Type TAlterConfigsResponseData::TAlterConfigsResourceResponse::ResourceTypeMeta::Default = 0;
const TAlterConfigsResponseData::TAlterConfigsResourceResponse::ResourceNameMeta::Type TAlterConfigsResponseData::TAlterConfigsResourceResponse::ResourceNameMeta::Default = {""};

TAlterConfigsResponseData::TAlterConfigsResourceResponse::TAlterConfigsResourceResponse() 
        : ErrorCode(ErrorCodeMeta::Default)
        , ErrorMessage(ErrorMessageMeta::Default)
        , ResourceType(ResourceTypeMeta::Default)
        , ResourceName(ResourceNameMeta::Default)
{}

void TAlterConfigsResponseData::TAlterConfigsResourceResponse::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TAlterConfigsResponseData::TAlterConfigsResourceResponse";
    }
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<ErrorMessageMeta>(_readable, _version, ErrorMessage);
    NPrivate::Read<ResourceTypeMeta>(_readable, _version, ResourceType);
    NPrivate::Read<ResourceNameMeta>(_readable, _version, ResourceName);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TAlterConfigsResponseData::TAlterConfigsResourceResponse::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TAlterConfigsResponseData::TAlterConfigsResourceResponse";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<ErrorMessageMeta>(_collector, _writable, _version, ErrorMessage);
    NPrivate::Write<ResourceTypeMeta>(_collector, _writable, _version, ResourceType);
    NPrivate::Write<ResourceNameMeta>(_collector, _writable, _version, ResourceName);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TAlterConfigsResponseData::TAlterConfigsResourceResponse::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<ErrorMessageMeta>(_collector, _version, ErrorMessage);
    NPrivate::Size<ResourceTypeMeta>(_collector, _version, ResourceType);
    NPrivate::Size<ResourceNameMeta>(_collector, _version, ResourceName);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TSaslAuthenticateRequestData
//

TSaslAuthenticateRequestData::TSaslAuthenticateRequestData() 
{}

void TSaslAuthenticateRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TSaslAuthenticateRequestData";
    }
    NPrivate::Read<AuthBytesMeta>(_readable, _version, AuthBytes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TSaslAuthenticateRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TSaslAuthenticateRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<AuthBytesMeta>(_collector, _writable, _version, AuthBytes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TSaslAuthenticateRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<AuthBytesMeta>(_collector, _version, AuthBytes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TSaslAuthenticateResponseData
//
const TSaslAuthenticateResponseData::ErrorCodeMeta::Type TSaslAuthenticateResponseData::ErrorCodeMeta::Default = 0;
const TSaslAuthenticateResponseData::ErrorMessageMeta::Type TSaslAuthenticateResponseData::ErrorMessageMeta::Default = {""};
const TSaslAuthenticateResponseData::SessionLifetimeMsMeta::Type TSaslAuthenticateResponseData::SessionLifetimeMsMeta::Default = 0;

TSaslAuthenticateResponseData::TSaslAuthenticateResponseData() 
        : ErrorCode(ErrorCodeMeta::Default)
        , ErrorMessage(ErrorMessageMeta::Default)
        , SessionLifetimeMs(SessionLifetimeMsMeta::Default)
{}

void TSaslAuthenticateResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TSaslAuthenticateResponseData";
    }
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<ErrorMessageMeta>(_readable, _version, ErrorMessage);
    NPrivate::Read<AuthBytesMeta>(_readable, _version, AuthBytes);
    NPrivate::Read<SessionLifetimeMsMeta>(_readable, _version, SessionLifetimeMs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TSaslAuthenticateResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TSaslAuthenticateResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<ErrorMessageMeta>(_collector, _writable, _version, ErrorMessage);
    NPrivate::Write<AuthBytesMeta>(_collector, _writable, _version, AuthBytes);
    NPrivate::Write<SessionLifetimeMsMeta>(_collector, _writable, _version, SessionLifetimeMs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TSaslAuthenticateResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<ErrorMessageMeta>(_collector, _version, ErrorMessage);
    NPrivate::Size<AuthBytesMeta>(_collector, _version, AuthBytes);
    NPrivate::Size<SessionLifetimeMsMeta>(_collector, _version, SessionLifetimeMs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TCreatePartitionsRequestData
//
const TCreatePartitionsRequestData::TimeoutMsMeta::Type TCreatePartitionsRequestData::TimeoutMsMeta::Default = 0;
const TCreatePartitionsRequestData::ValidateOnlyMeta::Type TCreatePartitionsRequestData::ValidateOnlyMeta::Default = false;

TCreatePartitionsRequestData::TCreatePartitionsRequestData() 
        : TimeoutMs(TimeoutMsMeta::Default)
        , ValidateOnly(ValidateOnlyMeta::Default)
{}

void TCreatePartitionsRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TCreatePartitionsRequestData";
    }
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    NPrivate::Read<TimeoutMsMeta>(_readable, _version, TimeoutMs);
    NPrivate::Read<ValidateOnlyMeta>(_readable, _version, ValidateOnly);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TCreatePartitionsRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TCreatePartitionsRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    NPrivate::Write<TimeoutMsMeta>(_collector, _writable, _version, TimeoutMs);
    NPrivate::Write<ValidateOnlyMeta>(_collector, _writable, _version, ValidateOnly);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TCreatePartitionsRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    NPrivate::Size<TimeoutMsMeta>(_collector, _version, TimeoutMs);
    NPrivate::Size<ValidateOnlyMeta>(_collector, _version, ValidateOnly);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TCreatePartitionsRequestData::TCreatePartitionsTopic
//
const TCreatePartitionsRequestData::TCreatePartitionsTopic::NameMeta::Type TCreatePartitionsRequestData::TCreatePartitionsTopic::NameMeta::Default = {""};
const TCreatePartitionsRequestData::TCreatePartitionsTopic::CountMeta::Type TCreatePartitionsRequestData::TCreatePartitionsTopic::CountMeta::Default = 0;

TCreatePartitionsRequestData::TCreatePartitionsTopic::TCreatePartitionsTopic() 
        : Name(NameMeta::Default)
        , Count(CountMeta::Default)
{}

void TCreatePartitionsRequestData::TCreatePartitionsTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TCreatePartitionsRequestData::TCreatePartitionsTopic";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<CountMeta>(_readable, _version, Count);
    NPrivate::Read<AssignmentsMeta>(_readable, _version, Assignments);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TCreatePartitionsRequestData::TCreatePartitionsTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TCreatePartitionsRequestData::TCreatePartitionsTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<CountMeta>(_collector, _writable, _version, Count);
    NPrivate::Write<AssignmentsMeta>(_collector, _writable, _version, Assignments);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TCreatePartitionsRequestData::TCreatePartitionsTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<CountMeta>(_collector, _version, Count);
    NPrivate::Size<AssignmentsMeta>(_collector, _version, Assignments);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TCreatePartitionsRequestData::TCreatePartitionsTopic::TCreatePartitionsAssignment
//

TCreatePartitionsRequestData::TCreatePartitionsTopic::TCreatePartitionsAssignment::TCreatePartitionsAssignment() 
{}

void TCreatePartitionsRequestData::TCreatePartitionsTopic::TCreatePartitionsAssignment::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TCreatePartitionsRequestData::TCreatePartitionsTopic::TCreatePartitionsAssignment";
    }
    NPrivate::Read<BrokerIdsMeta>(_readable, _version, BrokerIds);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TCreatePartitionsRequestData::TCreatePartitionsTopic::TCreatePartitionsAssignment::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TCreatePartitionsRequestData::TCreatePartitionsTopic::TCreatePartitionsAssignment";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<BrokerIdsMeta>(_collector, _writable, _version, BrokerIds);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TCreatePartitionsRequestData::TCreatePartitionsTopic::TCreatePartitionsAssignment::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<BrokerIdsMeta>(_collector, _version, BrokerIds);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TCreatePartitionsResponseData
//
const TCreatePartitionsResponseData::ThrottleTimeMsMeta::Type TCreatePartitionsResponseData::ThrottleTimeMsMeta::Default = 0;

TCreatePartitionsResponseData::TCreatePartitionsResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
{}

void TCreatePartitionsResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TCreatePartitionsResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ResultsMeta>(_readable, _version, Results);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TCreatePartitionsResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TCreatePartitionsResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ResultsMeta>(_collector, _writable, _version, Results);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TCreatePartitionsResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ResultsMeta>(_collector, _version, Results);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TCreatePartitionsResponseData::TCreatePartitionsTopicResult
//
const TCreatePartitionsResponseData::TCreatePartitionsTopicResult::NameMeta::Type TCreatePartitionsResponseData::TCreatePartitionsTopicResult::NameMeta::Default = {""};
const TCreatePartitionsResponseData::TCreatePartitionsTopicResult::ErrorCodeMeta::Type TCreatePartitionsResponseData::TCreatePartitionsTopicResult::ErrorCodeMeta::Default = 0;
const TCreatePartitionsResponseData::TCreatePartitionsTopicResult::ErrorMessageMeta::Type TCreatePartitionsResponseData::TCreatePartitionsTopicResult::ErrorMessageMeta::Default = std::nullopt;

TCreatePartitionsResponseData::TCreatePartitionsTopicResult::TCreatePartitionsTopicResult() 
        : Name(NameMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , ErrorMessage(ErrorMessageMeta::Default)
{}

void TCreatePartitionsResponseData::TCreatePartitionsTopicResult::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TCreatePartitionsResponseData::TCreatePartitionsTopicResult";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<ErrorMessageMeta>(_readable, _version, ErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        ui32 _numTaggedFields = _readable.readUnsignedVarint<ui32>();
        for (ui32 _i = 0; _i < _numTaggedFields; ++_i) {
            ui32 _tag = _readable.readUnsignedVarint<ui32>();
            ui32 _size = _readable.readUnsignedVarint<ui32>();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TCreatePartitionsResponseData::TCreatePartitionsTopicResult::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TCreatePartitionsResponseData::TCreatePartitionsTopicResult";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<ErrorMessageMeta>(_collector, _writable, _version, ErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TCreatePartitionsResponseData::TCreatePartitionsTopicResult::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<ErrorMessageMeta>(_collector, _version, ErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}
} //namespace NKafka
