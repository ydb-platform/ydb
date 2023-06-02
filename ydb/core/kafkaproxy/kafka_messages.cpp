
// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.
// For generate it use kikimr/tools/kafka/generate.sh

#include "kafka_messages.h"

namespace NKafka {

std::unique_ptr<TApiMessage> CreateRequest(i16 apiKey) {
    switch (apiKey) {
        case PRODUCE:
            return std::make_unique<TProduceRequestData>();
        case FETCH:
            return std::make_unique<TFetchRequestData>();
        case METADATA:
            return std::make_unique<TMetadataRequestData>();
        case API_VERSIONS:
            return std::make_unique<TApiVersionsRequestData>();
        case INIT_PRODUCER_ID:
            return std::make_unique<TInitProducerIdRequestData>();
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
        case METADATA:
            return std::make_unique<TMetadataResponseData>();
        case API_VERSIONS:
            return std::make_unique<TApiVersionsResponseData>();
        case INIT_PRODUCER_ID:
            return std::make_unique<TInitProducerIdResponseData>();
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
        case METADATA:
            if (_version >= 9) {
                return 2;
            } else {
                return 1;
            }
        case API_VERSIONS:
            if (_version >= 3) {
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
        case METADATA:
            if (_version >= 9) {
                return 1;
            } else {
                return 0;
            }
        case API_VERSIONS:
            // ApiVersionsResponse always includes a v0 header.
            // See KIP-511 for details.
            return 0;
        case INIT_PRODUCER_ID:
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
        : requestApiKey(RequestApiKeyMeta::Default)
        , requestApiVersion(RequestApiVersionMeta::Default)
        , correlationId(CorrelationIdMeta::Default)
        , clientId(ClientIdMeta::Default)
{}

void TRequestHeaderData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TRequestHeaderData";
    }
    NPrivate::Read<RequestApiKeyMeta>(_readable, _version, requestApiKey);
    NPrivate::Read<RequestApiVersionMeta>(_readable, _version, requestApiVersion);
    NPrivate::Read<CorrelationIdMeta>(_readable, _version, correlationId);
    NPrivate::Read<ClientIdMeta>(_readable, _version, clientId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TRequestHeaderData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TRequestHeaderData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<RequestApiKeyMeta>(_collector, _writable, _version, requestApiKey);
    NPrivate::Write<RequestApiVersionMeta>(_collector, _writable, _version, requestApiVersion);
    NPrivate::Write<CorrelationIdMeta>(_collector, _writable, _version, correlationId);
    NPrivate::Write<ClientIdMeta>(_collector, _writable, _version, clientId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TRequestHeaderData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<RequestApiKeyMeta>(_collector, _version, requestApiKey);
    NPrivate::Size<RequestApiVersionMeta>(_collector, _version, requestApiVersion);
    NPrivate::Size<CorrelationIdMeta>(_collector, _version, correlationId);
    NPrivate::Size<ClientIdMeta>(_collector, _version, clientId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TResponseHeaderData
//
const TResponseHeaderData::CorrelationIdMeta::Type TResponseHeaderData::CorrelationIdMeta::Default = 0;

TResponseHeaderData::TResponseHeaderData() 
        : correlationId(CorrelationIdMeta::Default)
{}

void TResponseHeaderData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TResponseHeaderData";
    }
    NPrivate::Read<CorrelationIdMeta>(_readable, _version, correlationId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TResponseHeaderData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TResponseHeaderData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<CorrelationIdMeta>(_collector, _writable, _version, correlationId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TResponseHeaderData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<CorrelationIdMeta>(_collector, _version, correlationId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : transactionalId(TransactionalIdMeta::Default)
        , acks(AcksMeta::Default)
        , timeoutMs(TimeoutMsMeta::Default)
{}

void TProduceRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceRequestData";
    }
    NPrivate::Read<TransactionalIdMeta>(_readable, _version, transactionalId);
    NPrivate::Read<AcksMeta>(_readable, _version, acks);
    NPrivate::Read<TimeoutMsMeta>(_readable, _version, timeoutMs);
    NPrivate::Read<TopicDataMeta>(_readable, _version, topicData);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TProduceRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TProduceRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TransactionalIdMeta>(_collector, _writable, _version, transactionalId);
    NPrivate::Write<AcksMeta>(_collector, _writable, _version, acks);
    NPrivate::Write<TimeoutMsMeta>(_collector, _writable, _version, timeoutMs);
    NPrivate::Write<TopicDataMeta>(_collector, _writable, _version, topicData);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TransactionalIdMeta>(_collector, _version, transactionalId);
    NPrivate::Size<AcksMeta>(_collector, _version, acks);
    NPrivate::Size<TimeoutMsMeta>(_collector, _version, timeoutMs);
    NPrivate::Size<TopicDataMeta>(_collector, _version, topicData);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TProduceRequestData::TTopicProduceData
//
const TProduceRequestData::TTopicProduceData::NameMeta::Type TProduceRequestData::TTopicProduceData::NameMeta::Default = {""};

TProduceRequestData::TTopicProduceData::TTopicProduceData() 
        : name(NameMeta::Default)
{}

void TProduceRequestData::TTopicProduceData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceRequestData::TTopicProduceData";
    }
    NPrivate::Read<NameMeta>(_readable, _version, name);
    NPrivate::Read<PartitionDataMeta>(_readable, _version, partitionData);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TProduceRequestData::TTopicProduceData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TProduceRequestData::TTopicProduceData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, name);
    NPrivate::Write<PartitionDataMeta>(_collector, _writable, _version, partitionData);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceRequestData::TTopicProduceData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, name);
    NPrivate::Size<PartitionDataMeta>(_collector, _version, partitionData);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TProduceRequestData::TTopicProduceData::TPartitionProduceData
//
const TProduceRequestData::TTopicProduceData::TPartitionProduceData::IndexMeta::Type TProduceRequestData::TTopicProduceData::TPartitionProduceData::IndexMeta::Default = 0;

TProduceRequestData::TTopicProduceData::TPartitionProduceData::TPartitionProduceData() 
        : index(IndexMeta::Default)
{}

void TProduceRequestData::TTopicProduceData::TPartitionProduceData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceRequestData::TTopicProduceData::TPartitionProduceData";
    }
    NPrivate::Read<IndexMeta>(_readable, _version, index);
    NPrivate::Read<RecordsMeta>(_readable, _version, records);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TProduceRequestData::TTopicProduceData::TPartitionProduceData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TProduceRequestData::TTopicProduceData::TPartitionProduceData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<IndexMeta>(_collector, _writable, _version, index);
    NPrivate::Write<RecordsMeta>(_collector, _writable, _version, records);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceRequestData::TTopicProduceData::TPartitionProduceData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<IndexMeta>(_collector, _version, index);
    NPrivate::Size<RecordsMeta>(_collector, _version, records);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TProduceResponseData
//
const TProduceResponseData::ThrottleTimeMsMeta::Type TProduceResponseData::ThrottleTimeMsMeta::Default = 0;

TProduceResponseData::TProduceResponseData() 
        : throttleTimeMs(ThrottleTimeMsMeta::Default)
{}

void TProduceResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceResponseData";
    }
    NPrivate::Read<ResponsesMeta>(_readable, _version, responses);
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, throttleTimeMs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TProduceResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TProduceResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ResponsesMeta>(_collector, _writable, _version, responses);
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, throttleTimeMs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ResponsesMeta>(_collector, _version, responses);
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, throttleTimeMs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}


//
// TProduceResponseData::TTopicProduceResponse
//
const TProduceResponseData::TTopicProduceResponse::NameMeta::Type TProduceResponseData::TTopicProduceResponse::NameMeta::Default = {""};

TProduceResponseData::TTopicProduceResponse::TTopicProduceResponse() 
        : name(NameMeta::Default)
{}

void TProduceResponseData::TTopicProduceResponse::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceResponseData::TTopicProduceResponse";
    }
    NPrivate::Read<NameMeta>(_readable, _version, name);
    NPrivate::Read<PartitionResponsesMeta>(_readable, _version, partitionResponses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TProduceResponseData::TTopicProduceResponse::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TProduceResponseData::TTopicProduceResponse";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, name);
    NPrivate::Write<PartitionResponsesMeta>(_collector, _writable, _version, partitionResponses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceResponseData::TTopicProduceResponse::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, name);
    NPrivate::Size<PartitionResponsesMeta>(_collector, _version, partitionResponses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : index(IndexMeta::Default)
        , errorCode(ErrorCodeMeta::Default)
        , baseOffset(BaseOffsetMeta::Default)
        , logAppendTimeMs(LogAppendTimeMsMeta::Default)
        , logStartOffset(LogStartOffsetMeta::Default)
        , errorMessage(ErrorMessageMeta::Default)
{}

void TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse";
    }
    NPrivate::Read<IndexMeta>(_readable, _version, index);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, errorCode);
    NPrivate::Read<BaseOffsetMeta>(_readable, _version, baseOffset);
    NPrivate::Read<LogAppendTimeMsMeta>(_readable, _version, logAppendTimeMs);
    NPrivate::Read<LogStartOffsetMeta>(_readable, _version, logStartOffset);
    NPrivate::Read<RecordErrorsMeta>(_readable, _version, recordErrors);
    NPrivate::Read<ErrorMessageMeta>(_readable, _version, errorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<IndexMeta>(_collector, _writable, _version, index);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, errorCode);
    NPrivate::Write<BaseOffsetMeta>(_collector, _writable, _version, baseOffset);
    NPrivate::Write<LogAppendTimeMsMeta>(_collector, _writable, _version, logAppendTimeMs);
    NPrivate::Write<LogStartOffsetMeta>(_collector, _writable, _version, logStartOffset);
    NPrivate::Write<RecordErrorsMeta>(_collector, _writable, _version, recordErrors);
    NPrivate::Write<ErrorMessageMeta>(_collector, _writable, _version, errorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<IndexMeta>(_collector, _version, index);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, errorCode);
    NPrivate::Size<BaseOffsetMeta>(_collector, _version, baseOffset);
    NPrivate::Size<LogAppendTimeMsMeta>(_collector, _version, logAppendTimeMs);
    NPrivate::Size<LogStartOffsetMeta>(_collector, _version, logStartOffset);
    NPrivate::Size<RecordErrorsMeta>(_collector, _version, recordErrors);
    NPrivate::Size<ErrorMessageMeta>(_collector, _version, errorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : batchIndex(BatchIndexMeta::Default)
        , batchIndexErrorMessage(BatchIndexErrorMessageMeta::Default)
{}

void TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage";
    }
    NPrivate::Read<BatchIndexMeta>(_readable, _version, batchIndex);
    NPrivate::Read<BatchIndexErrorMessageMeta>(_readable, _version, batchIndexErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<BatchIndexMeta>(_collector, _writable, _version, batchIndex);
    NPrivate::Write<BatchIndexErrorMessageMeta>(_collector, _writable, _version, batchIndexErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<BatchIndexMeta>(_collector, _version, batchIndex);
    NPrivate::Size<BatchIndexErrorMessageMeta>(_collector, _version, batchIndexErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : clusterId(ClusterIdMeta::Default)
        , replicaId(ReplicaIdMeta::Default)
        , maxWaitMs(MaxWaitMsMeta::Default)
        , minBytes(MinBytesMeta::Default)
        , maxBytes(MaxBytesMeta::Default)
        , isolationLevel(IsolationLevelMeta::Default)
        , sessionId(SessionIdMeta::Default)
        , sessionEpoch(SessionEpochMeta::Default)
        , rackId(RackIdMeta::Default)
{}

void TFetchRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchRequestData";
    }
    NPrivate::Read<ClusterIdMeta>(_readable, _version, clusterId);
    NPrivate::Read<ReplicaIdMeta>(_readable, _version, replicaId);
    NPrivate::Read<MaxWaitMsMeta>(_readable, _version, maxWaitMs);
    NPrivate::Read<MinBytesMeta>(_readable, _version, minBytes);
    NPrivate::Read<MaxBytesMeta>(_readable, _version, maxBytes);
    NPrivate::Read<IsolationLevelMeta>(_readable, _version, isolationLevel);
    NPrivate::Read<SessionIdMeta>(_readable, _version, sessionId);
    NPrivate::Read<SessionEpochMeta>(_readable, _version, sessionEpoch);
    NPrivate::Read<TopicsMeta>(_readable, _version, topics);
    NPrivate::Read<ForgottenTopicsDataMeta>(_readable, _version, forgottenTopicsData);
    NPrivate::Read<RackIdMeta>(_readable, _version, rackId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                case ClusterIdMeta::Tag:
                    NPrivate::ReadTag<ClusterIdMeta>(_readable, _version, clusterId);
                    break;
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ClusterIdMeta>(_collector, _writable, _version, clusterId);
    NPrivate::Write<ReplicaIdMeta>(_collector, _writable, _version, replicaId);
    NPrivate::Write<MaxWaitMsMeta>(_collector, _writable, _version, maxWaitMs);
    NPrivate::Write<MinBytesMeta>(_collector, _writable, _version, minBytes);
    NPrivate::Write<MaxBytesMeta>(_collector, _writable, _version, maxBytes);
    NPrivate::Write<IsolationLevelMeta>(_collector, _writable, _version, isolationLevel);
    NPrivate::Write<SessionIdMeta>(_collector, _writable, _version, sessionId);
    NPrivate::Write<SessionEpochMeta>(_collector, _writable, _version, sessionEpoch);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, topics);
    NPrivate::Write<ForgottenTopicsDataMeta>(_collector, _writable, _version, forgottenTopicsData);
    NPrivate::Write<RackIdMeta>(_collector, _writable, _version, rackId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
        NPrivate::WriteTag<ClusterIdMeta>(_writable, _version, clusterId);
    }
}

i32 TFetchRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ClusterIdMeta>(_collector, _version, clusterId);
    NPrivate::Size<ReplicaIdMeta>(_collector, _version, replicaId);
    NPrivate::Size<MaxWaitMsMeta>(_collector, _version, maxWaitMs);
    NPrivate::Size<MinBytesMeta>(_collector, _version, minBytes);
    NPrivate::Size<MaxBytesMeta>(_collector, _version, maxBytes);
    NPrivate::Size<IsolationLevelMeta>(_collector, _version, isolationLevel);
    NPrivate::Size<SessionIdMeta>(_collector, _version, sessionId);
    NPrivate::Size<SessionEpochMeta>(_collector, _version, sessionEpoch);
    NPrivate::Size<TopicsMeta>(_collector, _version, topics);
    NPrivate::Size<ForgottenTopicsDataMeta>(_collector, _version, forgottenTopicsData);
    NPrivate::Size<RackIdMeta>(_collector, _version, rackId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : topic(TopicMeta::Default)
        , topicId(TopicIdMeta::Default)
{}

void TFetchRequestData::TFetchTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchRequestData::TFetchTopic";
    }
    NPrivate::Read<TopicMeta>(_readable, _version, topic);
    NPrivate::Read<TopicIdMeta>(_readable, _version, topicId);
    NPrivate::Read<PartitionsMeta>(_readable, _version, partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchRequestData::TFetchTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchRequestData::TFetchTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicMeta>(_collector, _writable, _version, topic);
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, topicId);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchRequestData::TFetchTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicMeta>(_collector, _version, topic);
    NPrivate::Size<TopicIdMeta>(_collector, _version, topicId);
    NPrivate::Size<PartitionsMeta>(_collector, _version, partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : partition(PartitionMeta::Default)
        , currentLeaderEpoch(CurrentLeaderEpochMeta::Default)
        , fetchOffset(FetchOffsetMeta::Default)
        , lastFetchedEpoch(LastFetchedEpochMeta::Default)
        , logStartOffset(LogStartOffsetMeta::Default)
        , partitionMaxBytes(PartitionMaxBytesMeta::Default)
{}

void TFetchRequestData::TFetchTopic::TFetchPartition::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchRequestData::TFetchTopic::TFetchPartition";
    }
    NPrivate::Read<PartitionMeta>(_readable, _version, partition);
    NPrivate::Read<CurrentLeaderEpochMeta>(_readable, _version, currentLeaderEpoch);
    NPrivate::Read<FetchOffsetMeta>(_readable, _version, fetchOffset);
    NPrivate::Read<LastFetchedEpochMeta>(_readable, _version, lastFetchedEpoch);
    NPrivate::Read<LogStartOffsetMeta>(_readable, _version, logStartOffset);
    NPrivate::Read<PartitionMaxBytesMeta>(_readable, _version, partitionMaxBytes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchRequestData::TFetchTopic::TFetchPartition::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchRequestData::TFetchTopic::TFetchPartition";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<PartitionMeta>(_collector, _writable, _version, partition);
    NPrivate::Write<CurrentLeaderEpochMeta>(_collector, _writable, _version, currentLeaderEpoch);
    NPrivate::Write<FetchOffsetMeta>(_collector, _writable, _version, fetchOffset);
    NPrivate::Write<LastFetchedEpochMeta>(_collector, _writable, _version, lastFetchedEpoch);
    NPrivate::Write<LogStartOffsetMeta>(_collector, _writable, _version, logStartOffset);
    NPrivate::Write<PartitionMaxBytesMeta>(_collector, _writable, _version, partitionMaxBytes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchRequestData::TFetchTopic::TFetchPartition::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<PartitionMeta>(_collector, _version, partition);
    NPrivate::Size<CurrentLeaderEpochMeta>(_collector, _version, currentLeaderEpoch);
    NPrivate::Size<FetchOffsetMeta>(_collector, _version, fetchOffset);
    NPrivate::Size<LastFetchedEpochMeta>(_collector, _version, lastFetchedEpoch);
    NPrivate::Size<LogStartOffsetMeta>(_collector, _version, logStartOffset);
    NPrivate::Size<PartitionMaxBytesMeta>(_collector, _version, partitionMaxBytes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : topic(TopicMeta::Default)
        , topicId(TopicIdMeta::Default)
{}

void TFetchRequestData::TForgottenTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchRequestData::TForgottenTopic";
    }
    NPrivate::Read<TopicMeta>(_readable, _version, topic);
    NPrivate::Read<TopicIdMeta>(_readable, _version, topicId);
    NPrivate::Read<PartitionsMeta>(_readable, _version, partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchRequestData::TForgottenTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchRequestData::TForgottenTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicMeta>(_collector, _writable, _version, topic);
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, topicId);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchRequestData::TForgottenTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicMeta>(_collector, _version, topic);
    NPrivate::Size<TopicIdMeta>(_collector, _version, topicId);
    NPrivate::Size<PartitionsMeta>(_collector, _version, partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : throttleTimeMs(ThrottleTimeMsMeta::Default)
        , errorCode(ErrorCodeMeta::Default)
        , sessionId(SessionIdMeta::Default)
{}

void TFetchResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, throttleTimeMs);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, errorCode);
    NPrivate::Read<SessionIdMeta>(_readable, _version, sessionId);
    NPrivate::Read<ResponsesMeta>(_readable, _version, responses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, throttleTimeMs);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, errorCode);
    NPrivate::Write<SessionIdMeta>(_collector, _writable, _version, sessionId);
    NPrivate::Write<ResponsesMeta>(_collector, _writable, _version, responses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, throttleTimeMs);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, errorCode);
    NPrivate::Size<SessionIdMeta>(_collector, _version, sessionId);
    NPrivate::Size<ResponsesMeta>(_collector, _version, responses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : topic(TopicMeta::Default)
        , topicId(TopicIdMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse";
    }
    NPrivate::Read<TopicMeta>(_readable, _version, topic);
    NPrivate::Read<TopicIdMeta>(_readable, _version, topicId);
    NPrivate::Read<PartitionsMeta>(_readable, _version, partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchResponseData::TFetchableTopicResponse::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchResponseData::TFetchableTopicResponse";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicMeta>(_collector, _writable, _version, topic);
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, topicId);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicMeta>(_collector, _version, topic);
    NPrivate::Size<TopicIdMeta>(_collector, _version, topicId);
    NPrivate::Size<PartitionsMeta>(_collector, _version, partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : partitionIndex(PartitionIndexMeta::Default)
        , errorCode(ErrorCodeMeta::Default)
        , highWatermark(HighWatermarkMeta::Default)
        , lastStableOffset(LastStableOffsetMeta::Default)
        , logStartOffset(LogStartOffsetMeta::Default)
        , preferredReadReplica(PreferredReadReplicaMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData";
    }
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, partitionIndex);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, errorCode);
    NPrivate::Read<HighWatermarkMeta>(_readable, _version, highWatermark);
    NPrivate::Read<LastStableOffsetMeta>(_readable, _version, lastStableOffset);
    NPrivate::Read<LogStartOffsetMeta>(_readable, _version, logStartOffset);
    NPrivate::Read<DivergingEpochMeta>(_readable, _version, divergingEpoch);
    NPrivate::Read<CurrentLeaderMeta>(_readable, _version, currentLeader);
    NPrivate::Read<SnapshotIdMeta>(_readable, _version, snapshotId);
    NPrivate::Read<AbortedTransactionsMeta>(_readable, _version, abortedTransactions);
    NPrivate::Read<PreferredReadReplicaMeta>(_readable, _version, preferredReadReplica);
    NPrivate::Read<RecordsMeta>(_readable, _version, records);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                case DivergingEpochMeta::Tag:
                    NPrivate::ReadTag<DivergingEpochMeta>(_readable, _version, divergingEpoch);
                    break;
                case CurrentLeaderMeta::Tag:
                    NPrivate::ReadTag<CurrentLeaderMeta>(_readable, _version, currentLeader);
                    break;
                case SnapshotIdMeta::Tag:
                    NPrivate::ReadTag<SnapshotIdMeta>(_readable, _version, snapshotId);
                    break;
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, partitionIndex);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, errorCode);
    NPrivate::Write<HighWatermarkMeta>(_collector, _writable, _version, highWatermark);
    NPrivate::Write<LastStableOffsetMeta>(_collector, _writable, _version, lastStableOffset);
    NPrivate::Write<LogStartOffsetMeta>(_collector, _writable, _version, logStartOffset);
    NPrivate::Write<DivergingEpochMeta>(_collector, _writable, _version, divergingEpoch);
    NPrivate::Write<CurrentLeaderMeta>(_collector, _writable, _version, currentLeader);
    NPrivate::Write<SnapshotIdMeta>(_collector, _writable, _version, snapshotId);
    NPrivate::Write<AbortedTransactionsMeta>(_collector, _writable, _version, abortedTransactions);
    NPrivate::Write<PreferredReadReplicaMeta>(_collector, _writable, _version, preferredReadReplica);
    NPrivate::Write<RecordsMeta>(_collector, _writable, _version, records);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
        NPrivate::WriteTag<DivergingEpochMeta>(_writable, _version, divergingEpoch);
        NPrivate::WriteTag<CurrentLeaderMeta>(_writable, _version, currentLeader);
        NPrivate::WriteTag<SnapshotIdMeta>(_writable, _version, snapshotId);
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::TPartitionData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<PartitionIndexMeta>(_collector, _version, partitionIndex);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, errorCode);
    NPrivate::Size<HighWatermarkMeta>(_collector, _version, highWatermark);
    NPrivate::Size<LastStableOffsetMeta>(_collector, _version, lastStableOffset);
    NPrivate::Size<LogStartOffsetMeta>(_collector, _version, logStartOffset);
    NPrivate::Size<DivergingEpochMeta>(_collector, _version, divergingEpoch);
    NPrivate::Size<CurrentLeaderMeta>(_collector, _version, currentLeader);
    NPrivate::Size<SnapshotIdMeta>(_collector, _version, snapshotId);
    NPrivate::Size<AbortedTransactionsMeta>(_collector, _version, abortedTransactions);
    NPrivate::Size<PreferredReadReplicaMeta>(_collector, _version, preferredReadReplica);
    NPrivate::Size<RecordsMeta>(_collector, _version, records);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : epoch(EpochMeta::Default)
        , endOffset(EndOffsetMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset";
    }
    NPrivate::Read<EpochMeta>(_readable, _version, epoch);
    NPrivate::Read<EndOffsetMeta>(_readable, _version, endOffset);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<EpochMeta>(_collector, _writable, _version, epoch);
    NPrivate::Write<EndOffsetMeta>(_collector, _writable, _version, endOffset);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<EpochMeta>(_collector, _version, epoch);
    NPrivate::Size<EndOffsetMeta>(_collector, _version, endOffset);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : leaderId(LeaderIdMeta::Default)
        , leaderEpoch(LeaderEpochMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch";
    }
    NPrivate::Read<LeaderIdMeta>(_readable, _version, leaderId);
    NPrivate::Read<LeaderEpochMeta>(_readable, _version, leaderEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<LeaderIdMeta>(_collector, _writable, _version, leaderId);
    NPrivate::Write<LeaderEpochMeta>(_collector, _writable, _version, leaderEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<LeaderIdMeta>(_collector, _version, leaderId);
    NPrivate::Size<LeaderEpochMeta>(_collector, _version, leaderEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : endOffset(EndOffsetMeta::Default)
        , epoch(EpochMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId";
    }
    NPrivate::Read<EndOffsetMeta>(_readable, _version, endOffset);
    NPrivate::Read<EpochMeta>(_readable, _version, epoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<EndOffsetMeta>(_collector, _writable, _version, endOffset);
    NPrivate::Write<EpochMeta>(_collector, _writable, _version, epoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<EndOffsetMeta>(_collector, _version, endOffset);
    NPrivate::Size<EpochMeta>(_collector, _version, epoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : producerId(ProducerIdMeta::Default)
        , firstOffset(FirstOffsetMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction";
    }
    NPrivate::Read<ProducerIdMeta>(_readable, _version, producerId);
    NPrivate::Read<FirstOffsetMeta>(_readable, _version, firstOffset);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ProducerIdMeta>(_collector, _writable, _version, producerId);
    NPrivate::Write<FirstOffsetMeta>(_collector, _writable, _version, firstOffset);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ProducerIdMeta>(_collector, _version, producerId);
    NPrivate::Size<FirstOffsetMeta>(_collector, _version, firstOffset);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : allowAutoTopicCreation(AllowAutoTopicCreationMeta::Default)
        , includeClusterAuthorizedOperations(IncludeClusterAuthorizedOperationsMeta::Default)
        , includeTopicAuthorizedOperations(IncludeTopicAuthorizedOperationsMeta::Default)
{}

void TMetadataRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataRequestData";
    }
    NPrivate::Read<TopicsMeta>(_readable, _version, topics);
    NPrivate::Read<AllowAutoTopicCreationMeta>(_readable, _version, allowAutoTopicCreation);
    NPrivate::Read<IncludeClusterAuthorizedOperationsMeta>(_readable, _version, includeClusterAuthorizedOperations);
    NPrivate::Read<IncludeTopicAuthorizedOperationsMeta>(_readable, _version, includeTopicAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TMetadataRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TMetadataRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, topics);
    NPrivate::Write<AllowAutoTopicCreationMeta>(_collector, _writable, _version, allowAutoTopicCreation);
    NPrivate::Write<IncludeClusterAuthorizedOperationsMeta>(_collector, _writable, _version, includeClusterAuthorizedOperations);
    NPrivate::Write<IncludeTopicAuthorizedOperationsMeta>(_collector, _writable, _version, includeTopicAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicsMeta>(_collector, _version, topics);
    NPrivate::Size<AllowAutoTopicCreationMeta>(_collector, _version, allowAutoTopicCreation);
    NPrivate::Size<IncludeClusterAuthorizedOperationsMeta>(_collector, _version, includeClusterAuthorizedOperations);
    NPrivate::Size<IncludeTopicAuthorizedOperationsMeta>(_collector, _version, includeTopicAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : topicId(TopicIdMeta::Default)
        , name(NameMeta::Default)
{}

void TMetadataRequestData::TMetadataRequestTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataRequestData::TMetadataRequestTopic";
    }
    NPrivate::Read<TopicIdMeta>(_readable, _version, topicId);
    NPrivate::Read<NameMeta>(_readable, _version, name);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TMetadataRequestData::TMetadataRequestTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TMetadataRequestData::TMetadataRequestTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, topicId);
    NPrivate::Write<NameMeta>(_collector, _writable, _version, name);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataRequestData::TMetadataRequestTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicIdMeta>(_collector, _version, topicId);
    NPrivate::Size<NameMeta>(_collector, _version, name);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : throttleTimeMs(ThrottleTimeMsMeta::Default)
        , clusterId(ClusterIdMeta::Default)
        , controllerId(ControllerIdMeta::Default)
        , clusterAuthorizedOperations(ClusterAuthorizedOperationsMeta::Default)
{}

void TMetadataResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, throttleTimeMs);
    NPrivate::Read<BrokersMeta>(_readable, _version, brokers);
    NPrivate::Read<ClusterIdMeta>(_readable, _version, clusterId);
    NPrivate::Read<ControllerIdMeta>(_readable, _version, controllerId);
    NPrivate::Read<TopicsMeta>(_readable, _version, topics);
    NPrivate::Read<ClusterAuthorizedOperationsMeta>(_readable, _version, clusterAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TMetadataResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TMetadataResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, throttleTimeMs);
    NPrivate::Write<BrokersMeta>(_collector, _writable, _version, brokers);
    NPrivate::Write<ClusterIdMeta>(_collector, _writable, _version, clusterId);
    NPrivate::Write<ControllerIdMeta>(_collector, _writable, _version, controllerId);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, topics);
    NPrivate::Write<ClusterAuthorizedOperationsMeta>(_collector, _writable, _version, clusterAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, throttleTimeMs);
    NPrivate::Size<BrokersMeta>(_collector, _version, brokers);
    NPrivate::Size<ClusterIdMeta>(_collector, _version, clusterId);
    NPrivate::Size<ControllerIdMeta>(_collector, _version, controllerId);
    NPrivate::Size<TopicsMeta>(_collector, _version, topics);
    NPrivate::Size<ClusterAuthorizedOperationsMeta>(_collector, _version, clusterAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : nodeId(NodeIdMeta::Default)
        , host(HostMeta::Default)
        , port(PortMeta::Default)
        , rack(RackMeta::Default)
{}

void TMetadataResponseData::TMetadataResponseBroker::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataResponseData::TMetadataResponseBroker";
    }
    NPrivate::Read<NodeIdMeta>(_readable, _version, nodeId);
    NPrivate::Read<HostMeta>(_readable, _version, host);
    NPrivate::Read<PortMeta>(_readable, _version, port);
    NPrivate::Read<RackMeta>(_readable, _version, rack);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TMetadataResponseData::TMetadataResponseBroker::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TMetadataResponseData::TMetadataResponseBroker";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NodeIdMeta>(_collector, _writable, _version, nodeId);
    NPrivate::Write<HostMeta>(_collector, _writable, _version, host);
    NPrivate::Write<PortMeta>(_collector, _writable, _version, port);
    NPrivate::Write<RackMeta>(_collector, _writable, _version, rack);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataResponseData::TMetadataResponseBroker::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NodeIdMeta>(_collector, _version, nodeId);
    NPrivate::Size<HostMeta>(_collector, _version, host);
    NPrivate::Size<PortMeta>(_collector, _version, port);
    NPrivate::Size<RackMeta>(_collector, _version, rack);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : errorCode(ErrorCodeMeta::Default)
        , name(NameMeta::Default)
        , topicId(TopicIdMeta::Default)
        , isInternal(IsInternalMeta::Default)
        , topicAuthorizedOperations(TopicAuthorizedOperationsMeta::Default)
{}

void TMetadataResponseData::TMetadataResponseTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataResponseData::TMetadataResponseTopic";
    }
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, errorCode);
    NPrivate::Read<NameMeta>(_readable, _version, name);
    NPrivate::Read<TopicIdMeta>(_readable, _version, topicId);
    NPrivate::Read<IsInternalMeta>(_readable, _version, isInternal);
    NPrivate::Read<PartitionsMeta>(_readable, _version, partitions);
    NPrivate::Read<TopicAuthorizedOperationsMeta>(_readable, _version, topicAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TMetadataResponseData::TMetadataResponseTopic::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TMetadataResponseData::TMetadataResponseTopic";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, errorCode);
    NPrivate::Write<NameMeta>(_collector, _writable, _version, name);
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, topicId);
    NPrivate::Write<IsInternalMeta>(_collector, _writable, _version, isInternal);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, partitions);
    NPrivate::Write<TopicAuthorizedOperationsMeta>(_collector, _writable, _version, topicAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataResponseData::TMetadataResponseTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, errorCode);
    NPrivate::Size<NameMeta>(_collector, _version, name);
    NPrivate::Size<TopicIdMeta>(_collector, _version, topicId);
    NPrivate::Size<IsInternalMeta>(_collector, _version, isInternal);
    NPrivate::Size<PartitionsMeta>(_collector, _version, partitions);
    NPrivate::Size<TopicAuthorizedOperationsMeta>(_collector, _version, topicAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : errorCode(ErrorCodeMeta::Default)
        , partitionIndex(PartitionIndexMeta::Default)
        , leaderId(LeaderIdMeta::Default)
        , leaderEpoch(LeaderEpochMeta::Default)
{}

void TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition";
    }
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, errorCode);
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, partitionIndex);
    NPrivate::Read<LeaderIdMeta>(_readable, _version, leaderId);
    NPrivate::Read<LeaderEpochMeta>(_readable, _version, leaderEpoch);
    NPrivate::Read<ReplicaNodesMeta>(_readable, _version, replicaNodes);
    NPrivate::Read<IsrNodesMeta>(_readable, _version, isrNodes);
    NPrivate::Read<OfflineReplicasMeta>(_readable, _version, offlineReplicas);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, errorCode);
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, partitionIndex);
    NPrivate::Write<LeaderIdMeta>(_collector, _writable, _version, leaderId);
    NPrivate::Write<LeaderEpochMeta>(_collector, _writable, _version, leaderEpoch);
    NPrivate::Write<ReplicaNodesMeta>(_collector, _writable, _version, replicaNodes);
    NPrivate::Write<IsrNodesMeta>(_collector, _writable, _version, isrNodes);
    NPrivate::Write<OfflineReplicasMeta>(_collector, _writable, _version, offlineReplicas);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, errorCode);
    NPrivate::Size<PartitionIndexMeta>(_collector, _version, partitionIndex);
    NPrivate::Size<LeaderIdMeta>(_collector, _version, leaderId);
    NPrivate::Size<LeaderEpochMeta>(_collector, _version, leaderEpoch);
    NPrivate::Size<ReplicaNodesMeta>(_collector, _version, replicaNodes);
    NPrivate::Size<IsrNodesMeta>(_collector, _version, isrNodes);
    NPrivate::Size<OfflineReplicasMeta>(_collector, _version, offlineReplicas);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : clientSoftwareName(ClientSoftwareNameMeta::Default)
        , clientSoftwareVersion(ClientSoftwareVersionMeta::Default)
{}

void TApiVersionsRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsRequestData";
    }
    NPrivate::Read<ClientSoftwareNameMeta>(_readable, _version, clientSoftwareName);
    NPrivate::Read<ClientSoftwareVersionMeta>(_readable, _version, clientSoftwareVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TApiVersionsRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TApiVersionsRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ClientSoftwareNameMeta>(_collector, _writable, _version, clientSoftwareName);
    NPrivate::Write<ClientSoftwareVersionMeta>(_collector, _writable, _version, clientSoftwareVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TApiVersionsRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ClientSoftwareNameMeta>(_collector, _version, clientSoftwareName);
    NPrivate::Size<ClientSoftwareVersionMeta>(_collector, _version, clientSoftwareVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : errorCode(ErrorCodeMeta::Default)
        , throttleTimeMs(ThrottleTimeMsMeta::Default)
        , finalizedFeaturesEpoch(FinalizedFeaturesEpochMeta::Default)
        , zkMigrationReady(ZkMigrationReadyMeta::Default)
{}

void TApiVersionsResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsResponseData";
    }
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, errorCode);
    NPrivate::Read<ApiKeysMeta>(_readable, _version, apiKeys);
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, throttleTimeMs);
    NPrivate::Read<SupportedFeaturesMeta>(_readable, _version, supportedFeatures);
    NPrivate::Read<FinalizedFeaturesEpochMeta>(_readable, _version, finalizedFeaturesEpoch);
    NPrivate::Read<FinalizedFeaturesMeta>(_readable, _version, finalizedFeatures);
    NPrivate::Read<ZkMigrationReadyMeta>(_readable, _version, zkMigrationReady);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                case SupportedFeaturesMeta::Tag:
                    NPrivate::ReadTag<SupportedFeaturesMeta>(_readable, _version, supportedFeatures);
                    break;
                case FinalizedFeaturesEpochMeta::Tag:
                    NPrivate::ReadTag<FinalizedFeaturesEpochMeta>(_readable, _version, finalizedFeaturesEpoch);
                    break;
                case FinalizedFeaturesMeta::Tag:
                    NPrivate::ReadTag<FinalizedFeaturesMeta>(_readable, _version, finalizedFeatures);
                    break;
                case ZkMigrationReadyMeta::Tag:
                    NPrivate::ReadTag<ZkMigrationReadyMeta>(_readable, _version, zkMigrationReady);
                    break;
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TApiVersionsResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TApiVersionsResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, errorCode);
    NPrivate::Write<ApiKeysMeta>(_collector, _writable, _version, apiKeys);
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, throttleTimeMs);
    NPrivate::Write<SupportedFeaturesMeta>(_collector, _writable, _version, supportedFeatures);
    NPrivate::Write<FinalizedFeaturesEpochMeta>(_collector, _writable, _version, finalizedFeaturesEpoch);
    NPrivate::Write<FinalizedFeaturesMeta>(_collector, _writable, _version, finalizedFeatures);
    NPrivate::Write<ZkMigrationReadyMeta>(_collector, _writable, _version, zkMigrationReady);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
        NPrivate::WriteTag<SupportedFeaturesMeta>(_writable, _version, supportedFeatures);
        NPrivate::WriteTag<FinalizedFeaturesEpochMeta>(_writable, _version, finalizedFeaturesEpoch);
        NPrivate::WriteTag<FinalizedFeaturesMeta>(_writable, _version, finalizedFeatures);
        NPrivate::WriteTag<ZkMigrationReadyMeta>(_writable, _version, zkMigrationReady);
    }
}

i32 TApiVersionsResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, errorCode);
    NPrivate::Size<ApiKeysMeta>(_collector, _version, apiKeys);
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, throttleTimeMs);
    NPrivate::Size<SupportedFeaturesMeta>(_collector, _version, supportedFeatures);
    NPrivate::Size<FinalizedFeaturesEpochMeta>(_collector, _version, finalizedFeaturesEpoch);
    NPrivate::Size<FinalizedFeaturesMeta>(_collector, _version, finalizedFeatures);
    NPrivate::Size<ZkMigrationReadyMeta>(_collector, _version, zkMigrationReady);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : apiKey(ApiKeyMeta::Default)
        , minVersion(MinVersionMeta::Default)
        , maxVersion(MaxVersionMeta::Default)
{}

void TApiVersionsResponseData::TApiVersion::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsResponseData::TApiVersion";
    }
    NPrivate::Read<ApiKeyMeta>(_readable, _version, apiKey);
    NPrivate::Read<MinVersionMeta>(_readable, _version, minVersion);
    NPrivate::Read<MaxVersionMeta>(_readable, _version, maxVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TApiVersionsResponseData::TApiVersion::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TApiVersionsResponseData::TApiVersion";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ApiKeyMeta>(_collector, _writable, _version, apiKey);
    NPrivate::Write<MinVersionMeta>(_collector, _writable, _version, minVersion);
    NPrivate::Write<MaxVersionMeta>(_collector, _writable, _version, maxVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TApiVersionsResponseData::TApiVersion::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ApiKeyMeta>(_collector, _version, apiKey);
    NPrivate::Size<MinVersionMeta>(_collector, _version, minVersion);
    NPrivate::Size<MaxVersionMeta>(_collector, _version, maxVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : name(NameMeta::Default)
        , minVersion(MinVersionMeta::Default)
        , maxVersion(MaxVersionMeta::Default)
{}

void TApiVersionsResponseData::TSupportedFeatureKey::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsResponseData::TSupportedFeatureKey";
    }
    NPrivate::Read<NameMeta>(_readable, _version, name);
    NPrivate::Read<MinVersionMeta>(_readable, _version, minVersion);
    NPrivate::Read<MaxVersionMeta>(_readable, _version, maxVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TApiVersionsResponseData::TSupportedFeatureKey::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TApiVersionsResponseData::TSupportedFeatureKey";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, name);
    NPrivate::Write<MinVersionMeta>(_collector, _writable, _version, minVersion);
    NPrivate::Write<MaxVersionMeta>(_collector, _writable, _version, maxVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TApiVersionsResponseData::TSupportedFeatureKey::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, name);
    NPrivate::Size<MinVersionMeta>(_collector, _version, minVersion);
    NPrivate::Size<MaxVersionMeta>(_collector, _version, maxVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : name(NameMeta::Default)
        , maxVersionLevel(MaxVersionLevelMeta::Default)
        , minVersionLevel(MinVersionLevelMeta::Default)
{}

void TApiVersionsResponseData::TFinalizedFeatureKey::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsResponseData::TFinalizedFeatureKey";
    }
    NPrivate::Read<NameMeta>(_readable, _version, name);
    NPrivate::Read<MaxVersionLevelMeta>(_readable, _version, maxVersionLevel);
    NPrivate::Read<MinVersionLevelMeta>(_readable, _version, minVersionLevel);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TApiVersionsResponseData::TFinalizedFeatureKey::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TApiVersionsResponseData::TFinalizedFeatureKey";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<NameMeta>(_collector, _writable, _version, name);
    NPrivate::Write<MaxVersionLevelMeta>(_collector, _writable, _version, maxVersionLevel);
    NPrivate::Write<MinVersionLevelMeta>(_collector, _writable, _version, minVersionLevel);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TApiVersionsResponseData::TFinalizedFeatureKey::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, name);
    NPrivate::Size<MaxVersionLevelMeta>(_collector, _version, maxVersionLevel);
    NPrivate::Size<MinVersionLevelMeta>(_collector, _version, minVersionLevel);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : transactionalId(TransactionalIdMeta::Default)
        , transactionTimeoutMs(TransactionTimeoutMsMeta::Default)
        , producerId(ProducerIdMeta::Default)
        , producerEpoch(ProducerEpochMeta::Default)
{}

void TInitProducerIdRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TInitProducerIdRequestData";
    }
    NPrivate::Read<TransactionalIdMeta>(_readable, _version, transactionalId);
    NPrivate::Read<TransactionTimeoutMsMeta>(_readable, _version, transactionTimeoutMs);
    NPrivate::Read<ProducerIdMeta>(_readable, _version, producerId);
    NPrivate::Read<ProducerEpochMeta>(_readable, _version, producerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TInitProducerIdRequestData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TInitProducerIdRequestData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TransactionalIdMeta>(_collector, _writable, _version, transactionalId);
    NPrivate::Write<TransactionTimeoutMsMeta>(_collector, _writable, _version, transactionTimeoutMs);
    NPrivate::Write<ProducerIdMeta>(_collector, _writable, _version, producerId);
    NPrivate::Write<ProducerEpochMeta>(_collector, _writable, _version, producerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TInitProducerIdRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TransactionalIdMeta>(_collector, _version, transactionalId);
    NPrivate::Size<TransactionTimeoutMsMeta>(_collector, _version, transactionTimeoutMs);
    NPrivate::Size<ProducerIdMeta>(_collector, _version, producerId);
    NPrivate::Size<ProducerEpochMeta>(_collector, _version, producerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : throttleTimeMs(ThrottleTimeMsMeta::Default)
        , errorCode(ErrorCodeMeta::Default)
        , producerId(ProducerIdMeta::Default)
        , producerEpoch(ProducerEpochMeta::Default)
{}

void TInitProducerIdResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TInitProducerIdResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, throttleTimeMs);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, errorCode);
    NPrivate::Read<ProducerIdMeta>(_readable, _version, producerId);
    NPrivate::Read<ProducerEpochMeta>(_readable, _version, producerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    _readable.skip(_size); // skip unknown tag
                    break;
            }
        }
    }
}

void TInitProducerIdResponseData::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TInitProducerIdResponseData";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, throttleTimeMs);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, errorCode);
    NPrivate::Write<ProducerIdMeta>(_collector, _writable, _version, producerId);
    NPrivate::Write<ProducerEpochMeta>(_collector, _writable, _version, producerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TInitProducerIdResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, throttleTimeMs);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, errorCode);
    NPrivate::Size<ProducerIdMeta>(_collector, _version, producerId);
    NPrivate::Size<ProducerEpochMeta>(_collector, _version, producerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}
} //namespace NKafka
