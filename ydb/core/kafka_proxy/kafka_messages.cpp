
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
        : RequestApiKey(RequestApiKeyMeta::Default)
        , RequestApiVersion(RequestApiVersionMeta::Default)
        , CorrelationId(CorrelationIdMeta::Default)
        , ClientId(ClientIdMeta::Default)
{}

void TRequestHeaderData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TRequestHeaderData";
    }
    NPrivate::Read<RequestApiKeyMeta>(_readable, _version, RequestApiKey);
    NPrivate::Read<RequestApiVersionMeta>(_readable, _version, RequestApiVersion);
    NPrivate::Read<CorrelationIdMeta>(_readable, _version, CorrelationId);
    NPrivate::Read<ClientIdMeta>(_readable, _version, ClientId);
    
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
    NPrivate::Write<RequestApiKeyMeta>(_collector, _writable, _version, RequestApiKey);
    NPrivate::Write<RequestApiVersionMeta>(_collector, _writable, _version, RequestApiVersion);
    NPrivate::Write<CorrelationIdMeta>(_collector, _writable, _version, CorrelationId);
    NPrivate::Write<ClientIdMeta>(_collector, _writable, _version, ClientId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TRequestHeaderData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<RequestApiKeyMeta>(_collector, _version, RequestApiKey);
    NPrivate::Size<RequestApiVersionMeta>(_collector, _version, RequestApiVersion);
    NPrivate::Size<CorrelationIdMeta>(_collector, _version, CorrelationId);
    NPrivate::Size<ClientIdMeta>(_collector, _version, ClientId);
    
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
        : CorrelationId(CorrelationIdMeta::Default)
{}

void TResponseHeaderData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TResponseHeaderData";
    }
    NPrivate::Read<CorrelationIdMeta>(_readable, _version, CorrelationId);
    
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
    NPrivate::Write<CorrelationIdMeta>(_collector, _writable, _version, CorrelationId);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TResponseHeaderData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<CorrelationIdMeta>(_collector, _version, CorrelationId);
    
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
        : TransactionalId(TransactionalIdMeta::Default)
        , Acks(AcksMeta::Default)
        , TimeoutMs(TimeoutMsMeta::Default)
{}

void TProduceRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceRequestData";
    }
    NPrivate::Read<TransactionalIdMeta>(_readable, _version, TransactionalId);
    NPrivate::Read<AcksMeta>(_readable, _version, Acks);
    NPrivate::Read<TimeoutMsMeta>(_readable, _version, TimeoutMs);
    NPrivate::Read<TopicDataMeta>(_readable, _version, TopicData);
    
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
    NPrivate::Write<TransactionalIdMeta>(_collector, _writable, _version, TransactionalId);
    NPrivate::Write<AcksMeta>(_collector, _writable, _version, Acks);
    NPrivate::Write<TimeoutMsMeta>(_collector, _writable, _version, TimeoutMs);
    NPrivate::Write<TopicDataMeta>(_collector, _writable, _version, TopicData);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TransactionalIdMeta>(_collector, _version, TransactionalId);
    NPrivate::Size<AcksMeta>(_collector, _version, Acks);
    NPrivate::Size<TimeoutMsMeta>(_collector, _version, TimeoutMs);
    NPrivate::Size<TopicDataMeta>(_collector, _version, TopicData);
    
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
        : Name(NameMeta::Default)
{}

void TProduceRequestData::TTopicProduceData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceRequestData::TTopicProduceData";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionDataMeta>(_readable, _version, PartitionData);
    
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
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionDataMeta>(_collector, _writable, _version, PartitionData);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceRequestData::TTopicProduceData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionDataMeta>(_collector, _version, PartitionData);
    
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
        : Index(IndexMeta::Default)
{}

void TProduceRequestData::TTopicProduceData::TPartitionProduceData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceRequestData::TTopicProduceData::TPartitionProduceData";
    }
    NPrivate::Read<IndexMeta>(_readable, _version, Index);
    NPrivate::Read<RecordsMeta>(_readable, _version, Records);
    
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
    NPrivate::Write<IndexMeta>(_collector, _writable, _version, Index);
    NPrivate::Write<RecordsMeta>(_collector, _writable, _version, Records);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceRequestData::TTopicProduceData::TPartitionProduceData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<IndexMeta>(_collector, _version, Index);
    NPrivate::Size<RecordsMeta>(_collector, _version, Records);
    
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
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
{}

void TProduceResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceResponseData";
    }
    NPrivate::Read<ResponsesMeta>(_readable, _version, Responses);
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    
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
    NPrivate::Write<ResponsesMeta>(_collector, _writable, _version, Responses);
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ResponsesMeta>(_collector, _version, Responses);
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    
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
        : Name(NameMeta::Default)
{}

void TProduceResponseData::TTopicProduceResponse::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceResponseData::TTopicProduceResponse";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<PartitionResponsesMeta>(_readable, _version, PartitionResponses);
    
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
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<PartitionResponsesMeta>(_collector, _writable, _version, PartitionResponses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceResponseData::TTopicProduceResponse::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<PartitionResponsesMeta>(_collector, _version, PartitionResponses);
    
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
        : Index(IndexMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , BaseOffset(BaseOffsetMeta::Default)
        , LogAppendTimeMs(LogAppendTimeMsMeta::Default)
        , LogStartOffset(LogStartOffsetMeta::Default)
        , ErrorMessage(ErrorMessageMeta::Default)
{}

void TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse";
    }
    NPrivate::Read<IndexMeta>(_readable, _version, Index);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<BaseOffsetMeta>(_readable, _version, BaseOffset);
    NPrivate::Read<LogAppendTimeMsMeta>(_readable, _version, LogAppendTimeMs);
    NPrivate::Read<LogStartOffsetMeta>(_readable, _version, LogStartOffset);
    NPrivate::Read<RecordErrorsMeta>(_readable, _version, RecordErrors);
    NPrivate::Read<ErrorMessageMeta>(_readable, _version, ErrorMessage);
    
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
    NPrivate::Write<IndexMeta>(_collector, _writable, _version, Index);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<BaseOffsetMeta>(_collector, _writable, _version, BaseOffset);
    NPrivate::Write<LogAppendTimeMsMeta>(_collector, _writable, _version, LogAppendTimeMs);
    NPrivate::Write<LogStartOffsetMeta>(_collector, _writable, _version, LogStartOffset);
    NPrivate::Write<RecordErrorsMeta>(_collector, _writable, _version, RecordErrors);
    NPrivate::Write<ErrorMessageMeta>(_collector, _writable, _version, ErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : BatchIndex(BatchIndexMeta::Default)
        , BatchIndexErrorMessage(BatchIndexErrorMessageMeta::Default)
{}

void TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage";
    }
    NPrivate::Read<BatchIndexMeta>(_readable, _version, BatchIndex);
    NPrivate::Read<BatchIndexErrorMessageMeta>(_readable, _version, BatchIndexErrorMessage);
    
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
    NPrivate::Write<BatchIndexMeta>(_collector, _writable, _version, BatchIndex);
    NPrivate::Write<BatchIndexErrorMessageMeta>(_collector, _writable, _version, BatchIndexErrorMessage);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<BatchIndexMeta>(_collector, _version, BatchIndex);
    NPrivate::Size<BatchIndexErrorMessageMeta>(_collector, _version, BatchIndexErrorMessage);
    
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
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
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
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
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
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
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
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : Topic(TopicMeta::Default)
        , TopicId(TopicIdMeta::Default)
{}

void TFetchRequestData::TFetchTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchRequestData::TFetchTopic";
    }
    NPrivate::Read<TopicMeta>(_readable, _version, Topic);
    NPrivate::Read<TopicIdMeta>(_readable, _version, TopicId);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
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
    NPrivate::Write<TopicMeta>(_collector, _writable, _version, Topic);
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, TopicId);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchRequestData::TFetchTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicMeta>(_collector, _version, Topic);
    NPrivate::Size<TopicIdMeta>(_collector, _version, TopicId);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
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
        : Partition(PartitionMeta::Default)
        , CurrentLeaderEpoch(CurrentLeaderEpochMeta::Default)
        , FetchOffset(FetchOffsetMeta::Default)
        , LastFetchedEpoch(LastFetchedEpochMeta::Default)
        , LogStartOffset(LogStartOffsetMeta::Default)
        , PartitionMaxBytes(PartitionMaxBytesMeta::Default)
{}

void TFetchRequestData::TFetchTopic::TFetchPartition::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchRequestData::TFetchTopic::TFetchPartition";
    }
    NPrivate::Read<PartitionMeta>(_readable, _version, Partition);
    NPrivate::Read<CurrentLeaderEpochMeta>(_readable, _version, CurrentLeaderEpoch);
    NPrivate::Read<FetchOffsetMeta>(_readable, _version, FetchOffset);
    NPrivate::Read<LastFetchedEpochMeta>(_readable, _version, LastFetchedEpoch);
    NPrivate::Read<LogStartOffsetMeta>(_readable, _version, LogStartOffset);
    NPrivate::Read<PartitionMaxBytesMeta>(_readable, _version, PartitionMaxBytes);
    
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
    NPrivate::Write<PartitionMeta>(_collector, _writable, _version, Partition);
    NPrivate::Write<CurrentLeaderEpochMeta>(_collector, _writable, _version, CurrentLeaderEpoch);
    NPrivate::Write<FetchOffsetMeta>(_collector, _writable, _version, FetchOffset);
    NPrivate::Write<LastFetchedEpochMeta>(_collector, _writable, _version, LastFetchedEpoch);
    NPrivate::Write<LogStartOffsetMeta>(_collector, _writable, _version, LogStartOffset);
    NPrivate::Write<PartitionMaxBytesMeta>(_collector, _writable, _version, PartitionMaxBytes);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : Topic(TopicMeta::Default)
        , TopicId(TopicIdMeta::Default)
{}

void TFetchRequestData::TForgottenTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchRequestData::TForgottenTopic";
    }
    NPrivate::Read<TopicMeta>(_readable, _version, Topic);
    NPrivate::Read<TopicIdMeta>(_readable, _version, TopicId);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
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
    NPrivate::Write<TopicMeta>(_collector, _writable, _version, Topic);
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, TopicId);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchRequestData::TForgottenTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicMeta>(_collector, _version, Topic);
    NPrivate::Size<TopicIdMeta>(_collector, _version, TopicId);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
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
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , SessionId(SessionIdMeta::Default)
{}

void TFetchResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<SessionIdMeta>(_readable, _version, SessionId);
    NPrivate::Read<ResponsesMeta>(_readable, _version, Responses);
    
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
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<SessionIdMeta>(_collector, _writable, _version, SessionId);
    NPrivate::Write<ResponsesMeta>(_collector, _writable, _version, Responses);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<SessionIdMeta>(_collector, _version, SessionId);
    NPrivate::Size<ResponsesMeta>(_collector, _version, Responses);
    
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
        : Topic(TopicMeta::Default)
        , TopicId(TopicIdMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse";
    }
    NPrivate::Read<TopicMeta>(_readable, _version, Topic);
    NPrivate::Read<TopicIdMeta>(_readable, _version, TopicId);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    
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
    NPrivate::Write<TopicMeta>(_collector, _writable, _version, Topic);
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, TopicId);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicMeta>(_collector, _version, Topic);
    NPrivate::Size<TopicIdMeta>(_collector, _version, TopicId);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    
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
        : PartitionIndex(PartitionIndexMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , HighWatermark(HighWatermarkMeta::Default)
        , LastStableOffset(LastStableOffsetMeta::Default)
        , LogStartOffset(LogStartOffsetMeta::Default)
        , PreferredReadReplica(PreferredReadReplicaMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
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
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
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
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
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
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : Epoch(EpochMeta::Default)
        , EndOffset(EndOffsetMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset";
    }
    NPrivate::Read<EpochMeta>(_readable, _version, Epoch);
    NPrivate::Read<EndOffsetMeta>(_readable, _version, EndOffset);
    
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
    NPrivate::Write<EpochMeta>(_collector, _writable, _version, Epoch);
    NPrivate::Write<EndOffsetMeta>(_collector, _writable, _version, EndOffset);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<EpochMeta>(_collector, _version, Epoch);
    NPrivate::Size<EndOffsetMeta>(_collector, _version, EndOffset);
    
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
        : LeaderId(LeaderIdMeta::Default)
        , LeaderEpoch(LeaderEpochMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch";
    }
    NPrivate::Read<LeaderIdMeta>(_readable, _version, LeaderId);
    NPrivate::Read<LeaderEpochMeta>(_readable, _version, LeaderEpoch);
    
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
    NPrivate::Write<LeaderIdMeta>(_collector, _writable, _version, LeaderId);
    NPrivate::Write<LeaderEpochMeta>(_collector, _writable, _version, LeaderEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<LeaderIdMeta>(_collector, _version, LeaderId);
    NPrivate::Size<LeaderEpochMeta>(_collector, _version, LeaderEpoch);
    
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
        : EndOffset(EndOffsetMeta::Default)
        , Epoch(EpochMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId";
    }
    NPrivate::Read<EndOffsetMeta>(_readable, _version, EndOffset);
    NPrivate::Read<EpochMeta>(_readable, _version, Epoch);
    
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
    NPrivate::Write<EndOffsetMeta>(_collector, _writable, _version, EndOffset);
    NPrivate::Write<EpochMeta>(_collector, _writable, _version, Epoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<EndOffsetMeta>(_collector, _version, EndOffset);
    NPrivate::Size<EpochMeta>(_collector, _version, Epoch);
    
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
        : ProducerId(ProducerIdMeta::Default)
        , FirstOffset(FirstOffsetMeta::Default)
{}

void TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction";
    }
    NPrivate::Read<ProducerIdMeta>(_readable, _version, ProducerId);
    NPrivate::Read<FirstOffsetMeta>(_readable, _version, FirstOffset);
    
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
    NPrivate::Write<ProducerIdMeta>(_collector, _writable, _version, ProducerId);
    NPrivate::Write<FirstOffsetMeta>(_collector, _writable, _version, FirstOffset);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ProducerIdMeta>(_collector, _version, ProducerId);
    NPrivate::Size<FirstOffsetMeta>(_collector, _version, FirstOffset);
    
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
        : AllowAutoTopicCreation(AllowAutoTopicCreationMeta::Default)
        , IncludeClusterAuthorizedOperations(IncludeClusterAuthorizedOperationsMeta::Default)
        , IncludeTopicAuthorizedOperations(IncludeTopicAuthorizedOperationsMeta::Default)
{}

void TMetadataRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataRequestData";
    }
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    NPrivate::Read<AllowAutoTopicCreationMeta>(_readable, _version, AllowAutoTopicCreation);
    NPrivate::Read<IncludeClusterAuthorizedOperationsMeta>(_readable, _version, IncludeClusterAuthorizedOperations);
    NPrivate::Read<IncludeTopicAuthorizedOperationsMeta>(_readable, _version, IncludeTopicAuthorizedOperations);
    
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
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    NPrivate::Write<AllowAutoTopicCreationMeta>(_collector, _writable, _version, AllowAutoTopicCreation);
    NPrivate::Write<IncludeClusterAuthorizedOperationsMeta>(_collector, _writable, _version, IncludeClusterAuthorizedOperations);
    NPrivate::Write<IncludeTopicAuthorizedOperationsMeta>(_collector, _writable, _version, IncludeTopicAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    NPrivate::Size<AllowAutoTopicCreationMeta>(_collector, _version, AllowAutoTopicCreation);
    NPrivate::Size<IncludeClusterAuthorizedOperationsMeta>(_collector, _version, IncludeClusterAuthorizedOperations);
    NPrivate::Size<IncludeTopicAuthorizedOperationsMeta>(_collector, _version, IncludeTopicAuthorizedOperations);
    
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
        : TopicId(TopicIdMeta::Default)
        , Name(NameMeta::Default)
{}

void TMetadataRequestData::TMetadataRequestTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataRequestData::TMetadataRequestTopic";
    }
    NPrivate::Read<TopicIdMeta>(_readable, _version, TopicId);
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    
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
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, TopicId);
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataRequestData::TMetadataRequestTopic::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicIdMeta>(_collector, _version, TopicId);
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    
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
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ClusterId(ClusterIdMeta::Default)
        , ControllerId(ControllerIdMeta::Default)
        , ClusterAuthorizedOperations(ClusterAuthorizedOperationsMeta::Default)
{}

void TMetadataResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<BrokersMeta>(_readable, _version, Brokers);
    NPrivate::Read<ClusterIdMeta>(_readable, _version, ClusterId);
    NPrivate::Read<ControllerIdMeta>(_readable, _version, ControllerId);
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    NPrivate::Read<ClusterAuthorizedOperationsMeta>(_readable, _version, ClusterAuthorizedOperations);
    
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
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<BrokersMeta>(_collector, _writable, _version, Brokers);
    NPrivate::Write<ClusterIdMeta>(_collector, _writable, _version, ClusterId);
    NPrivate::Write<ControllerIdMeta>(_collector, _writable, _version, ControllerId);
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    NPrivate::Write<ClusterAuthorizedOperationsMeta>(_collector, _writable, _version, ClusterAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : NodeId(NodeIdMeta::Default)
        , Host(HostMeta::Default)
        , Port(PortMeta::Default)
        , Rack(RackMeta::Default)
{}

void TMetadataResponseData::TMetadataResponseBroker::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataResponseData::TMetadataResponseBroker";
    }
    NPrivate::Read<NodeIdMeta>(_readable, _version, NodeId);
    NPrivate::Read<HostMeta>(_readable, _version, Host);
    NPrivate::Read<PortMeta>(_readable, _version, Port);
    NPrivate::Read<RackMeta>(_readable, _version, Rack);
    
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
    NPrivate::Write<NodeIdMeta>(_collector, _writable, _version, NodeId);
    NPrivate::Write<HostMeta>(_collector, _writable, _version, Host);
    NPrivate::Write<PortMeta>(_collector, _writable, _version, Port);
    NPrivate::Write<RackMeta>(_collector, _writable, _version, Rack);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TMetadataResponseData::TMetadataResponseBroker::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NodeIdMeta>(_collector, _version, NodeId);
    NPrivate::Size<HostMeta>(_collector, _version, Host);
    NPrivate::Size<PortMeta>(_collector, _version, Port);
    NPrivate::Size<RackMeta>(_collector, _version, Rack);
    
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
        : ErrorCode(ErrorCodeMeta::Default)
        , Name(NameMeta::Default)
        , TopicId(TopicIdMeta::Default)
        , IsInternal(IsInternalMeta::Default)
        , TopicAuthorizedOperations(TopicAuthorizedOperationsMeta::Default)
{}

void TMetadataResponseData::TMetadataResponseTopic::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataResponseData::TMetadataResponseTopic";
    }
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<TopicIdMeta>(_readable, _version, TopicId);
    NPrivate::Read<IsInternalMeta>(_readable, _version, IsInternal);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
    NPrivate::Read<TopicAuthorizedOperationsMeta>(_readable, _version, TopicAuthorizedOperations);
    
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
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<TopicIdMeta>(_collector, _writable, _version, TopicId);
    NPrivate::Write<IsInternalMeta>(_collector, _writable, _version, IsInternal);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
    NPrivate::Write<TopicAuthorizedOperationsMeta>(_collector, _writable, _version, TopicAuthorizedOperations);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : ErrorCode(ErrorCodeMeta::Default)
        , PartitionIndex(PartitionIndexMeta::Default)
        , LeaderId(LeaderIdMeta::Default)
        , LeaderEpoch(LeaderEpochMeta::Default)
{}

void TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition";
    }
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<PartitionIndexMeta>(_readable, _version, PartitionIndex);
    NPrivate::Read<LeaderIdMeta>(_readable, _version, LeaderId);
    NPrivate::Read<LeaderEpochMeta>(_readable, _version, LeaderEpoch);
    NPrivate::Read<ReplicaNodesMeta>(_readable, _version, ReplicaNodes);
    NPrivate::Read<IsrNodesMeta>(_readable, _version, IsrNodes);
    NPrivate::Read<OfflineReplicasMeta>(_readable, _version, OfflineReplicas);
    
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
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<PartitionIndexMeta>(_collector, _writable, _version, PartitionIndex);
    NPrivate::Write<LeaderIdMeta>(_collector, _writable, _version, LeaderId);
    NPrivate::Write<LeaderEpochMeta>(_collector, _writable, _version, LeaderEpoch);
    NPrivate::Write<ReplicaNodesMeta>(_collector, _writable, _version, ReplicaNodes);
    NPrivate::Write<IsrNodesMeta>(_collector, _writable, _version, IsrNodes);
    NPrivate::Write<OfflineReplicasMeta>(_collector, _writable, _version, OfflineReplicas);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : ClientSoftwareName(ClientSoftwareNameMeta::Default)
        , ClientSoftwareVersion(ClientSoftwareVersionMeta::Default)
{}

void TApiVersionsRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsRequestData";
    }
    NPrivate::Read<ClientSoftwareNameMeta>(_readable, _version, ClientSoftwareName);
    NPrivate::Read<ClientSoftwareVersionMeta>(_readable, _version, ClientSoftwareVersion);
    
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
    NPrivate::Write<ClientSoftwareNameMeta>(_collector, _writable, _version, ClientSoftwareName);
    NPrivate::Write<ClientSoftwareVersionMeta>(_collector, _writable, _version, ClientSoftwareVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TApiVersionsRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ClientSoftwareNameMeta>(_collector, _version, ClientSoftwareName);
    NPrivate::Size<ClientSoftwareVersionMeta>(_collector, _version, ClientSoftwareVersion);
    
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
        : ErrorCode(ErrorCodeMeta::Default)
        , ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , FinalizedFeaturesEpoch(FinalizedFeaturesEpochMeta::Default)
        , ZkMigrationReady(ZkMigrationReadyMeta::Default)
{}

void TApiVersionsResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsResponseData";
    }
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<ApiKeysMeta>(_readable, _version, ApiKeys);
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<SupportedFeaturesMeta>(_readable, _version, SupportedFeatures);
    NPrivate::Read<FinalizedFeaturesEpochMeta>(_readable, _version, FinalizedFeaturesEpoch);
    NPrivate::Read<FinalizedFeaturesMeta>(_readable, _version, FinalizedFeatures);
    NPrivate::Read<ZkMigrationReadyMeta>(_readable, _version, ZkMigrationReady);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; ++_i) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
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
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
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
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
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
        : ApiKey(ApiKeyMeta::Default)
        , MinVersion(MinVersionMeta::Default)
        , MaxVersion(MaxVersionMeta::Default)
{}

void TApiVersionsResponseData::TApiVersion::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsResponseData::TApiVersion";
    }
    NPrivate::Read<ApiKeyMeta>(_readable, _version, ApiKey);
    NPrivate::Read<MinVersionMeta>(_readable, _version, MinVersion);
    NPrivate::Read<MaxVersionMeta>(_readable, _version, MaxVersion);
    
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
    NPrivate::Write<ApiKeyMeta>(_collector, _writable, _version, ApiKey);
    NPrivate::Write<MinVersionMeta>(_collector, _writable, _version, MinVersion);
    NPrivate::Write<MaxVersionMeta>(_collector, _writable, _version, MaxVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TApiVersionsResponseData::TApiVersion::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ApiKeyMeta>(_collector, _version, ApiKey);
    NPrivate::Size<MinVersionMeta>(_collector, _version, MinVersion);
    NPrivate::Size<MaxVersionMeta>(_collector, _version, MaxVersion);
    
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
        : Name(NameMeta::Default)
        , MinVersion(MinVersionMeta::Default)
        , MaxVersion(MaxVersionMeta::Default)
{}

void TApiVersionsResponseData::TSupportedFeatureKey::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsResponseData::TSupportedFeatureKey";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<MinVersionMeta>(_readable, _version, MinVersion);
    NPrivate::Read<MaxVersionMeta>(_readable, _version, MaxVersion);
    
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
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<MinVersionMeta>(_collector, _writable, _version, MinVersion);
    NPrivate::Write<MaxVersionMeta>(_collector, _writable, _version, MaxVersion);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TApiVersionsResponseData::TSupportedFeatureKey::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<MinVersionMeta>(_collector, _version, MinVersion);
    NPrivate::Size<MaxVersionMeta>(_collector, _version, MaxVersion);
    
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
        : Name(NameMeta::Default)
        , MaxVersionLevel(MaxVersionLevelMeta::Default)
        , MinVersionLevel(MinVersionLevelMeta::Default)
{}

void TApiVersionsResponseData::TFinalizedFeatureKey::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TApiVersionsResponseData::TFinalizedFeatureKey";
    }
    NPrivate::Read<NameMeta>(_readable, _version, Name);
    NPrivate::Read<MaxVersionLevelMeta>(_readable, _version, MaxVersionLevel);
    NPrivate::Read<MinVersionLevelMeta>(_readable, _version, MinVersionLevel);
    
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
    NPrivate::Write<NameMeta>(_collector, _writable, _version, Name);
    NPrivate::Write<MaxVersionLevelMeta>(_collector, _writable, _version, MaxVersionLevel);
    NPrivate::Write<MinVersionLevelMeta>(_collector, _writable, _version, MinVersionLevel);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TApiVersionsResponseData::TFinalizedFeatureKey::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<NameMeta>(_collector, _version, Name);
    NPrivate::Size<MaxVersionLevelMeta>(_collector, _version, MaxVersionLevel);
    NPrivate::Size<MinVersionLevelMeta>(_collector, _version, MinVersionLevel);
    
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
        : TransactionalId(TransactionalIdMeta::Default)
        , TransactionTimeoutMs(TransactionTimeoutMsMeta::Default)
        , ProducerId(ProducerIdMeta::Default)
        , ProducerEpoch(ProducerEpochMeta::Default)
{}

void TInitProducerIdRequestData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TInitProducerIdRequestData";
    }
    NPrivate::Read<TransactionalIdMeta>(_readable, _version, TransactionalId);
    NPrivate::Read<TransactionTimeoutMsMeta>(_readable, _version, TransactionTimeoutMs);
    NPrivate::Read<ProducerIdMeta>(_readable, _version, ProducerId);
    NPrivate::Read<ProducerEpochMeta>(_readable, _version, ProducerEpoch);
    
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
    NPrivate::Write<TransactionalIdMeta>(_collector, _writable, _version, TransactionalId);
    NPrivate::Write<TransactionTimeoutMsMeta>(_collector, _writable, _version, TransactionTimeoutMs);
    NPrivate::Write<ProducerIdMeta>(_collector, _writable, _version, ProducerId);
    NPrivate::Write<ProducerEpochMeta>(_collector, _writable, _version, ProducerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TInitProducerIdRequestData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TransactionalIdMeta>(_collector, _version, TransactionalId);
    NPrivate::Size<TransactionTimeoutMsMeta>(_collector, _version, TransactionTimeoutMs);
    NPrivate::Size<ProducerIdMeta>(_collector, _version, ProducerId);
    NPrivate::Size<ProducerEpochMeta>(_collector, _version, ProducerEpoch);
    
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
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
        , ErrorCode(ErrorCodeMeta::Default)
        , ProducerId(ProducerIdMeta::Default)
        , ProducerEpoch(ProducerEpochMeta::Default)
{}

void TInitProducerIdResponseData::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersionMin, MessageMeta::PresentVersionMax>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TInitProducerIdResponseData";
    }
    NPrivate::Read<ThrottleTimeMsMeta>(_readable, _version, ThrottleTimeMs);
    NPrivate::Read<ErrorCodeMeta>(_readable, _version, ErrorCode);
    NPrivate::Read<ProducerIdMeta>(_readable, _version, ProducerId);
    NPrivate::Read<ProducerEpochMeta>(_readable, _version, ProducerEpoch);
    
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
    NPrivate::Write<ThrottleTimeMsMeta>(_collector, _writable, _version, ThrottleTimeMs);
    NPrivate::Write<ErrorCodeMeta>(_collector, _writable, _version, ErrorCode);
    NPrivate::Write<ProducerIdMeta>(_collector, _writable, _version, ProducerId);
    NPrivate::Write<ProducerEpochMeta>(_collector, _writable, _version, ProducerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
        
    }
}

i32 TInitProducerIdResponseData::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<ThrottleTimeMsMeta>(_collector, _version, ThrottleTimeMs);
    NPrivate::Size<ErrorCodeMeta>(_collector, _version, ErrorCode);
    NPrivate::Size<ProducerIdMeta>(_collector, _version, ProducerId);
    NPrivate::Size<ProducerEpochMeta>(_collector, _version, ProducerEpoch);
    
    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersionMin, MessageMeta::FlexibleVersionMax>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}
} //namespace NKafka
