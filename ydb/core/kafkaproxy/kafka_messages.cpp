
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


class TRequestHeaderData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TRequestHeaderData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TRequestHeaderData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<RequestApiKeyMeta> RequestApiKey;
    NPrivate::TReadStrategy<RequestApiVersionMeta> RequestApiVersion;
    NPrivate::TReadStrategy<CorrelationIdMeta> CorrelationId;
    NPrivate::TReadStrategy<ClientIdMeta> ClientId;
};

TRequestHeaderData::TReadContext::TReadContext(TRequestHeaderData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , RequestApiKey()
    , RequestApiVersion()
    , CorrelationId()
    , ClientId()
{}


TReadDemand TRequestHeaderData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                RequestApiKey.Init<NPrivate::ReadFieldRule<RequestApiKeyMeta>>(Value.RequestApiKey, Version);
            }
            case 1: {
                auto demand = RequestApiKey.Next<NPrivate::ReadFieldRule<RequestApiKeyMeta>>(Value.RequestApiKey, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                RequestApiVersion.Init<NPrivate::ReadFieldRule<RequestApiVersionMeta>>(Value.RequestApiVersion, Version);
            }
            case 3: {
                auto demand = RequestApiVersion.Next<NPrivate::ReadFieldRule<RequestApiVersionMeta>>(Value.RequestApiVersion, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                CorrelationId.Init<NPrivate::ReadFieldRule<CorrelationIdMeta>>(Value.CorrelationId, Version);
            }
            case 5: {
                auto demand = CorrelationId.Next<NPrivate::ReadFieldRule<CorrelationIdMeta>>(Value.CorrelationId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                ClientId.Init<NPrivate::ReadFieldRule<ClientIdMeta>>(Value.ClientId, Version);
            }
            case 7: {
                auto demand = ClientId.Next<NPrivate::ReadFieldRule<ClientIdMeta>>(Value.ClientId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                if (!NPrivate::VersionCheck<TRequestHeaderData::MessageMeta::FlexibleVersionMin, TRequestHeaderData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 9: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 11: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 13: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 10;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TRequestHeaderData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
}


//
// TResponseHeaderData
//
const TResponseHeaderData::CorrelationIdMeta::Type TResponseHeaderData::CorrelationIdMeta::Default = 0;

TResponseHeaderData::TResponseHeaderData() 
        : CorrelationId(CorrelationIdMeta::Default)
{}


class TResponseHeaderData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TResponseHeaderData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TResponseHeaderData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<CorrelationIdMeta> CorrelationId;
};

TResponseHeaderData::TReadContext::TReadContext(TResponseHeaderData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , CorrelationId()
{}


TReadDemand TResponseHeaderData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                CorrelationId.Init<NPrivate::ReadFieldRule<CorrelationIdMeta>>(Value.CorrelationId, Version);
            }
            case 1: {
                auto demand = CorrelationId.Next<NPrivate::ReadFieldRule<CorrelationIdMeta>>(Value.CorrelationId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                if (!NPrivate::VersionCheck<TResponseHeaderData::MessageMeta::FlexibleVersionMin, TResponseHeaderData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 3: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 5: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 7: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 4;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TResponseHeaderData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TProduceRequestData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TProduceRequestData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TProduceRequestData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<TransactionalIdMeta> TransactionalId;
    NPrivate::TReadStrategy<AcksMeta> Acks;
    NPrivate::TReadStrategy<TimeoutMsMeta> TimeoutMs;
    NPrivate::TReadStrategy<TopicDataMeta> TopicData;
};

TProduceRequestData::TReadContext::TReadContext(TProduceRequestData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , TransactionalId()
    , Acks()
    , TimeoutMs()
    , TopicData()
{}


TReadDemand TProduceRequestData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                TransactionalId.Init<NPrivate::ReadFieldRule<TransactionalIdMeta>>(Value.TransactionalId, Version);
            }
            case 1: {
                auto demand = TransactionalId.Next<NPrivate::ReadFieldRule<TransactionalIdMeta>>(Value.TransactionalId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                Acks.Init<NPrivate::ReadFieldRule<AcksMeta>>(Value.Acks, Version);
            }
            case 3: {
                auto demand = Acks.Next<NPrivate::ReadFieldRule<AcksMeta>>(Value.Acks, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                TimeoutMs.Init<NPrivate::ReadFieldRule<TimeoutMsMeta>>(Value.TimeoutMs, Version);
            }
            case 5: {
                auto demand = TimeoutMs.Next<NPrivate::ReadFieldRule<TimeoutMsMeta>>(Value.TimeoutMs, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                TopicData.Init<NPrivate::ReadFieldRule<TopicDataMeta>>(Value.TopicData, Version);
            }
            case 7: {
                auto demand = TopicData.Next<NPrivate::ReadFieldRule<TopicDataMeta>>(Value.TopicData, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                if (!NPrivate::VersionCheck<TProduceRequestData::MessageMeta::FlexibleVersionMin, TProduceRequestData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 9: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 11: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 13: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 10;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TProduceRequestData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
}


//
// TProduceRequestData::TTopicProduceData
//
const TProduceRequestData::TTopicProduceData::NameMeta::Type TProduceRequestData::TTopicProduceData::NameMeta::Default = {""};

TProduceRequestData::TTopicProduceData::TTopicProduceData() 
        : Name(NameMeta::Default)
{}


class TProduceRequestData::TTopicProduceData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TProduceRequestData::TTopicProduceData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TProduceRequestData::TTopicProduceData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<NameMeta> Name;
    NPrivate::TReadStrategy<PartitionDataMeta> PartitionData;
};

TProduceRequestData::TTopicProduceData::TReadContext::TReadContext(TProduceRequestData::TTopicProduceData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , Name()
    , PartitionData()
{}


TReadDemand TProduceRequestData::TTopicProduceData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                Name.Init<NPrivate::ReadFieldRule<NameMeta>>(Value.Name, Version);
            }
            case 1: {
                auto demand = Name.Next<NPrivate::ReadFieldRule<NameMeta>>(Value.Name, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                PartitionData.Init<NPrivate::ReadFieldRule<PartitionDataMeta>>(Value.PartitionData, Version);
            }
            case 3: {
                auto demand = PartitionData.Next<NPrivate::ReadFieldRule<PartitionDataMeta>>(Value.PartitionData, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                if (!NPrivate::VersionCheck<TProduceRequestData::TTopicProduceData::MessageMeta::FlexibleVersionMin, TProduceRequestData::TTopicProduceData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 5: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 7: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 9: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 6;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TProduceRequestData::TTopicProduceData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
}


//
// TProduceRequestData::TTopicProduceData::TPartitionProduceData
//
const TProduceRequestData::TTopicProduceData::TPartitionProduceData::IndexMeta::Type TProduceRequestData::TTopicProduceData::TPartitionProduceData::IndexMeta::Default = 0;

TProduceRequestData::TTopicProduceData::TPartitionProduceData::TPartitionProduceData() 
        : Index(IndexMeta::Default)
{}


class TProduceRequestData::TTopicProduceData::TPartitionProduceData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TProduceRequestData::TTopicProduceData::TPartitionProduceData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TProduceRequestData::TTopicProduceData::TPartitionProduceData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<IndexMeta> Index;
    NPrivate::TReadStrategy<RecordsMeta> Records;
};

TProduceRequestData::TTopicProduceData::TPartitionProduceData::TReadContext::TReadContext(TProduceRequestData::TTopicProduceData::TPartitionProduceData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , Index()
    , Records()
{}


TReadDemand TProduceRequestData::TTopicProduceData::TPartitionProduceData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                Index.Init<NPrivate::ReadFieldRule<IndexMeta>>(Value.Index, Version);
            }
            case 1: {
                auto demand = Index.Next<NPrivate::ReadFieldRule<IndexMeta>>(Value.Index, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                Records.Init<NPrivate::ReadFieldRule<RecordsMeta>>(Value.Records, Version);
            }
            case 3: {
                auto demand = Records.Next<NPrivate::ReadFieldRule<RecordsMeta>>(Value.Records, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                if (!NPrivate::VersionCheck<TProduceRequestData::TTopicProduceData::TPartitionProduceData::MessageMeta::FlexibleVersionMin, TProduceRequestData::TTopicProduceData::TPartitionProduceData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 5: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 7: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 9: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 6;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TProduceRequestData::TTopicProduceData::TPartitionProduceData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
}


//
// TProduceResponseData
//
const TProduceResponseData::ThrottleTimeMsMeta::Type TProduceResponseData::ThrottleTimeMsMeta::Default = 0;

TProduceResponseData::TProduceResponseData() 
        : ThrottleTimeMs(ThrottleTimeMsMeta::Default)
{}


class TProduceResponseData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TProduceResponseData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TProduceResponseData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<ResponsesMeta> Responses;
    NPrivate::TReadStrategy<ThrottleTimeMsMeta> ThrottleTimeMs;
};

TProduceResponseData::TReadContext::TReadContext(TProduceResponseData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , Responses()
    , ThrottleTimeMs()
{}


TReadDemand TProduceResponseData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                Responses.Init<NPrivate::ReadFieldRule<ResponsesMeta>>(Value.Responses, Version);
            }
            case 1: {
                auto demand = Responses.Next<NPrivate::ReadFieldRule<ResponsesMeta>>(Value.Responses, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                ThrottleTimeMs.Init<NPrivate::ReadFieldRule<ThrottleTimeMsMeta>>(Value.ThrottleTimeMs, Version);
            }
            case 3: {
                auto demand = ThrottleTimeMs.Next<NPrivate::ReadFieldRule<ThrottleTimeMsMeta>>(Value.ThrottleTimeMs, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                if (!NPrivate::VersionCheck<TProduceResponseData::MessageMeta::FlexibleVersionMin, TProduceResponseData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 5: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 7: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 9: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 6;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TProduceResponseData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
}


//
// TProduceResponseData::TTopicProduceResponse
//
const TProduceResponseData::TTopicProduceResponse::NameMeta::Type TProduceResponseData::TTopicProduceResponse::NameMeta::Default = {""};

TProduceResponseData::TTopicProduceResponse::TTopicProduceResponse() 
        : Name(NameMeta::Default)
{}


class TProduceResponseData::TTopicProduceResponse::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TProduceResponseData::TTopicProduceResponse& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TProduceResponseData::TTopicProduceResponse& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<NameMeta> Name;
    NPrivate::TReadStrategy<PartitionResponsesMeta> PartitionResponses;
};

TProduceResponseData::TTopicProduceResponse::TReadContext::TReadContext(TProduceResponseData::TTopicProduceResponse& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , Name()
    , PartitionResponses()
{}


TReadDemand TProduceResponseData::TTopicProduceResponse::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                Name.Init<NPrivate::ReadFieldRule<NameMeta>>(Value.Name, Version);
            }
            case 1: {
                auto demand = Name.Next<NPrivate::ReadFieldRule<NameMeta>>(Value.Name, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                PartitionResponses.Init<NPrivate::ReadFieldRule<PartitionResponsesMeta>>(Value.PartitionResponses, Version);
            }
            case 3: {
                auto demand = PartitionResponses.Next<NPrivate::ReadFieldRule<PartitionResponsesMeta>>(Value.PartitionResponses, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                if (!NPrivate::VersionCheck<TProduceResponseData::TTopicProduceResponse::MessageMeta::FlexibleVersionMin, TProduceResponseData::TTopicProduceResponse::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 5: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 7: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 9: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 6;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TProduceResponseData::TTopicProduceResponse::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<IndexMeta> Index;
    NPrivate::TReadStrategy<ErrorCodeMeta> ErrorCode;
    NPrivate::TReadStrategy<BaseOffsetMeta> BaseOffset;
    NPrivate::TReadStrategy<LogAppendTimeMsMeta> LogAppendTimeMs;
    NPrivate::TReadStrategy<LogStartOffsetMeta> LogStartOffset;
    NPrivate::TReadStrategy<RecordErrorsMeta> RecordErrors;
    NPrivate::TReadStrategy<ErrorMessageMeta> ErrorMessage;
};

TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TReadContext::TReadContext(TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , Index()
    , ErrorCode()
    , BaseOffset()
    , LogAppendTimeMs()
    , LogStartOffset()
    , RecordErrors()
    , ErrorMessage()
{}


TReadDemand TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                Index.Init<NPrivate::ReadFieldRule<IndexMeta>>(Value.Index, Version);
            }
            case 1: {
                auto demand = Index.Next<NPrivate::ReadFieldRule<IndexMeta>>(Value.Index, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                ErrorCode.Init<NPrivate::ReadFieldRule<ErrorCodeMeta>>(Value.ErrorCode, Version);
            }
            case 3: {
                auto demand = ErrorCode.Next<NPrivate::ReadFieldRule<ErrorCodeMeta>>(Value.ErrorCode, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                BaseOffset.Init<NPrivate::ReadFieldRule<BaseOffsetMeta>>(Value.BaseOffset, Version);
            }
            case 5: {
                auto demand = BaseOffset.Next<NPrivate::ReadFieldRule<BaseOffsetMeta>>(Value.BaseOffset, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                LogAppendTimeMs.Init<NPrivate::ReadFieldRule<LogAppendTimeMsMeta>>(Value.LogAppendTimeMs, Version);
            }
            case 7: {
                auto demand = LogAppendTimeMs.Next<NPrivate::ReadFieldRule<LogAppendTimeMsMeta>>(Value.LogAppendTimeMs, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                ++Step;
                LogStartOffset.Init<NPrivate::ReadFieldRule<LogStartOffsetMeta>>(Value.LogStartOffset, Version);
            }
            case 9: {
                auto demand = LogStartOffset.Next<NPrivate::ReadFieldRule<LogStartOffsetMeta>>(Value.LogStartOffset, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                RecordErrors.Init<NPrivate::ReadFieldRule<RecordErrorsMeta>>(Value.RecordErrors, Version);
            }
            case 11: {
                auto demand = RecordErrors.Next<NPrivate::ReadFieldRule<RecordErrorsMeta>>(Value.RecordErrors, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                ++Step;
                ErrorMessage.Init<NPrivate::ReadFieldRule<ErrorMessageMeta>>(Value.ErrorMessage, Version);
            }
            case 13: {
                auto demand = ErrorMessage.Next<NPrivate::ReadFieldRule<ErrorMessageMeta>>(Value.ErrorMessage, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 14: {
                if (!NPrivate::VersionCheck<TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::MessageMeta::FlexibleVersionMin, TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 15: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 16: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 17: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 18: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 19: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 16;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<BatchIndexMeta> BatchIndex;
    NPrivate::TReadStrategy<BatchIndexErrorMessageMeta> BatchIndexErrorMessage;
};

TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::TReadContext::TReadContext(TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , BatchIndex()
    , BatchIndexErrorMessage()
{}


TReadDemand TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                BatchIndex.Init<NPrivate::ReadFieldRule<BatchIndexMeta>>(Value.BatchIndex, Version);
            }
            case 1: {
                auto demand = BatchIndex.Next<NPrivate::ReadFieldRule<BatchIndexMeta>>(Value.BatchIndex, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                BatchIndexErrorMessage.Init<NPrivate::ReadFieldRule<BatchIndexErrorMessageMeta>>(Value.BatchIndexErrorMessage, Version);
            }
            case 3: {
                auto demand = BatchIndexErrorMessage.Next<NPrivate::ReadFieldRule<BatchIndexErrorMessageMeta>>(Value.BatchIndexErrorMessage, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                if (!NPrivate::VersionCheck<TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::MessageMeta::FlexibleVersionMin, TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 5: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 7: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 9: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 6;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TProduceResponseData::TTopicProduceResponse::TPartitionProduceResponse::TBatchIndexAndErrorMessage::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TFetchRequestData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TFetchRequestData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TFetchRequestData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<ClusterIdMeta> ClusterId;
    NPrivate::TReadStrategy<ReplicaIdMeta> ReplicaId;
    NPrivate::TReadStrategy<MaxWaitMsMeta> MaxWaitMs;
    NPrivate::TReadStrategy<MinBytesMeta> MinBytes;
    NPrivate::TReadStrategy<MaxBytesMeta> MaxBytes;
    NPrivate::TReadStrategy<IsolationLevelMeta> IsolationLevel;
    NPrivate::TReadStrategy<SessionIdMeta> SessionId;
    NPrivate::TReadStrategy<SessionEpochMeta> SessionEpoch;
    NPrivate::TReadStrategy<TopicsMeta> Topics;
    NPrivate::TReadStrategy<ForgottenTopicsDataMeta> ForgottenTopicsData;
    NPrivate::TReadStrategy<RackIdMeta> RackId;
};

TFetchRequestData::TReadContext::TReadContext(TFetchRequestData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , ClusterId()
    , ReplicaId()
    , MaxWaitMs()
    , MinBytes()
    , MaxBytes()
    , IsolationLevel()
    , SessionId()
    , SessionEpoch()
    , Topics()
    , ForgottenTopicsData()
    , RackId()
{}


TReadDemand TFetchRequestData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                ClusterId.Init<NPrivate::ReadFieldRule<ClusterIdMeta>>(Value.ClusterId, Version);
            }
            case 1: {
                auto demand = ClusterId.Next<NPrivate::ReadFieldRule<ClusterIdMeta>>(Value.ClusterId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                ReplicaId.Init<NPrivate::ReadFieldRule<ReplicaIdMeta>>(Value.ReplicaId, Version);
            }
            case 3: {
                auto demand = ReplicaId.Next<NPrivate::ReadFieldRule<ReplicaIdMeta>>(Value.ReplicaId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                MaxWaitMs.Init<NPrivate::ReadFieldRule<MaxWaitMsMeta>>(Value.MaxWaitMs, Version);
            }
            case 5: {
                auto demand = MaxWaitMs.Next<NPrivate::ReadFieldRule<MaxWaitMsMeta>>(Value.MaxWaitMs, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                MinBytes.Init<NPrivate::ReadFieldRule<MinBytesMeta>>(Value.MinBytes, Version);
            }
            case 7: {
                auto demand = MinBytes.Next<NPrivate::ReadFieldRule<MinBytesMeta>>(Value.MinBytes, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                ++Step;
                MaxBytes.Init<NPrivate::ReadFieldRule<MaxBytesMeta>>(Value.MaxBytes, Version);
            }
            case 9: {
                auto demand = MaxBytes.Next<NPrivate::ReadFieldRule<MaxBytesMeta>>(Value.MaxBytes, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                IsolationLevel.Init<NPrivate::ReadFieldRule<IsolationLevelMeta>>(Value.IsolationLevel, Version);
            }
            case 11: {
                auto demand = IsolationLevel.Next<NPrivate::ReadFieldRule<IsolationLevelMeta>>(Value.IsolationLevel, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                ++Step;
                SessionId.Init<NPrivate::ReadFieldRule<SessionIdMeta>>(Value.SessionId, Version);
            }
            case 13: {
                auto demand = SessionId.Next<NPrivate::ReadFieldRule<SessionIdMeta>>(Value.SessionId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 14: {
                ++Step;
                SessionEpoch.Init<NPrivate::ReadFieldRule<SessionEpochMeta>>(Value.SessionEpoch, Version);
            }
            case 15: {
                auto demand = SessionEpoch.Next<NPrivate::ReadFieldRule<SessionEpochMeta>>(Value.SessionEpoch, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 16: {
                ++Step;
                Topics.Init<NPrivate::ReadFieldRule<TopicsMeta>>(Value.Topics, Version);
            }
            case 17: {
                auto demand = Topics.Next<NPrivate::ReadFieldRule<TopicsMeta>>(Value.Topics, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 18: {
                ++Step;
                ForgottenTopicsData.Init<NPrivate::ReadFieldRule<ForgottenTopicsDataMeta>>(Value.ForgottenTopicsData, Version);
            }
            case 19: {
                auto demand = ForgottenTopicsData.Next<NPrivate::ReadFieldRule<ForgottenTopicsDataMeta>>(Value.ForgottenTopicsData, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 20: {
                ++Step;
                RackId.Init<NPrivate::ReadFieldRule<RackIdMeta>>(Value.RackId, Version);
            }
            case 21: {
                auto demand = RackId.Next<NPrivate::ReadFieldRule<RackIdMeta>>(Value.RackId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 22: {
                if (!NPrivate::VersionCheck<TFetchRequestData::MessageMeta::FlexibleVersionMin, TFetchRequestData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 23: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 24: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 25: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 26: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 27: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    case 0: {
                        if (!TagInitialized_) {
                            TagInitialized_=true;
                            ClusterId.Init<NPrivate::ReadTaggedFieldRule<ClusterIdMeta>>(Value.ClusterId, Version);
                        }
                        demand = ClusterId.Next<NPrivate::ReadTaggedFieldRule<ClusterIdMeta>>(Value.ClusterId, Version);
                        break;
                    }
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 24;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TFetchRequestData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TFetchRequestData::TFetchTopic::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TFetchRequestData::TFetchTopic& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TFetchRequestData::TFetchTopic& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<TopicMeta> Topic;
    NPrivate::TReadStrategy<TopicIdMeta> TopicId;
    NPrivate::TReadStrategy<PartitionsMeta> Partitions;
};

TFetchRequestData::TFetchTopic::TReadContext::TReadContext(TFetchRequestData::TFetchTopic& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , Topic()
    , TopicId()
    , Partitions()
{}


TReadDemand TFetchRequestData::TFetchTopic::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                Topic.Init<NPrivate::ReadFieldRule<TopicMeta>>(Value.Topic, Version);
            }
            case 1: {
                auto demand = Topic.Next<NPrivate::ReadFieldRule<TopicMeta>>(Value.Topic, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                TopicId.Init<NPrivate::ReadFieldRule<TopicIdMeta>>(Value.TopicId, Version);
            }
            case 3: {
                auto demand = TopicId.Next<NPrivate::ReadFieldRule<TopicIdMeta>>(Value.TopicId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                Partitions.Init<NPrivate::ReadFieldRule<PartitionsMeta>>(Value.Partitions, Version);
            }
            case 5: {
                auto demand = Partitions.Next<NPrivate::ReadFieldRule<PartitionsMeta>>(Value.Partitions, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                if (!NPrivate::VersionCheck<TFetchRequestData::TFetchTopic::MessageMeta::FlexibleVersionMin, TFetchRequestData::TFetchTopic::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 7: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 9: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 11: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 8;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TFetchRequestData::TFetchTopic::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TFetchRequestData::TFetchTopic::TFetchPartition::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TFetchRequestData::TFetchTopic::TFetchPartition& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TFetchRequestData::TFetchTopic::TFetchPartition& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<PartitionMeta> Partition;
    NPrivate::TReadStrategy<CurrentLeaderEpochMeta> CurrentLeaderEpoch;
    NPrivate::TReadStrategy<FetchOffsetMeta> FetchOffset;
    NPrivate::TReadStrategy<LastFetchedEpochMeta> LastFetchedEpoch;
    NPrivate::TReadStrategy<LogStartOffsetMeta> LogStartOffset;
    NPrivate::TReadStrategy<PartitionMaxBytesMeta> PartitionMaxBytes;
};

TFetchRequestData::TFetchTopic::TFetchPartition::TReadContext::TReadContext(TFetchRequestData::TFetchTopic::TFetchPartition& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , Partition()
    , CurrentLeaderEpoch()
    , FetchOffset()
    , LastFetchedEpoch()
    , LogStartOffset()
    , PartitionMaxBytes()
{}


TReadDemand TFetchRequestData::TFetchTopic::TFetchPartition::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                Partition.Init<NPrivate::ReadFieldRule<PartitionMeta>>(Value.Partition, Version);
            }
            case 1: {
                auto demand = Partition.Next<NPrivate::ReadFieldRule<PartitionMeta>>(Value.Partition, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                CurrentLeaderEpoch.Init<NPrivate::ReadFieldRule<CurrentLeaderEpochMeta>>(Value.CurrentLeaderEpoch, Version);
            }
            case 3: {
                auto demand = CurrentLeaderEpoch.Next<NPrivate::ReadFieldRule<CurrentLeaderEpochMeta>>(Value.CurrentLeaderEpoch, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                FetchOffset.Init<NPrivate::ReadFieldRule<FetchOffsetMeta>>(Value.FetchOffset, Version);
            }
            case 5: {
                auto demand = FetchOffset.Next<NPrivate::ReadFieldRule<FetchOffsetMeta>>(Value.FetchOffset, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                LastFetchedEpoch.Init<NPrivate::ReadFieldRule<LastFetchedEpochMeta>>(Value.LastFetchedEpoch, Version);
            }
            case 7: {
                auto demand = LastFetchedEpoch.Next<NPrivate::ReadFieldRule<LastFetchedEpochMeta>>(Value.LastFetchedEpoch, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                ++Step;
                LogStartOffset.Init<NPrivate::ReadFieldRule<LogStartOffsetMeta>>(Value.LogStartOffset, Version);
            }
            case 9: {
                auto demand = LogStartOffset.Next<NPrivate::ReadFieldRule<LogStartOffsetMeta>>(Value.LogStartOffset, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                PartitionMaxBytes.Init<NPrivate::ReadFieldRule<PartitionMaxBytesMeta>>(Value.PartitionMaxBytes, Version);
            }
            case 11: {
                auto demand = PartitionMaxBytes.Next<NPrivate::ReadFieldRule<PartitionMaxBytesMeta>>(Value.PartitionMaxBytes, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                if (!NPrivate::VersionCheck<TFetchRequestData::TFetchTopic::TFetchPartition::MessageMeta::FlexibleVersionMin, TFetchRequestData::TFetchTopic::TFetchPartition::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 13: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 14: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 15: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 16: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 17: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 14;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TFetchRequestData::TFetchTopic::TFetchPartition::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TFetchRequestData::TForgottenTopic::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TFetchRequestData::TForgottenTopic& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TFetchRequestData::TForgottenTopic& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<TopicMeta> Topic;
    NPrivate::TReadStrategy<TopicIdMeta> TopicId;
    NPrivate::TReadStrategy<PartitionsMeta> Partitions;
};

TFetchRequestData::TForgottenTopic::TReadContext::TReadContext(TFetchRequestData::TForgottenTopic& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , Topic()
    , TopicId()
    , Partitions()
{}


TReadDemand TFetchRequestData::TForgottenTopic::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                Topic.Init<NPrivate::ReadFieldRule<TopicMeta>>(Value.Topic, Version);
            }
            case 1: {
                auto demand = Topic.Next<NPrivate::ReadFieldRule<TopicMeta>>(Value.Topic, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                TopicId.Init<NPrivate::ReadFieldRule<TopicIdMeta>>(Value.TopicId, Version);
            }
            case 3: {
                auto demand = TopicId.Next<NPrivate::ReadFieldRule<TopicIdMeta>>(Value.TopicId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                Partitions.Init<NPrivate::ReadFieldRule<PartitionsMeta>>(Value.Partitions, Version);
            }
            case 5: {
                auto demand = Partitions.Next<NPrivate::ReadFieldRule<PartitionsMeta>>(Value.Partitions, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                if (!NPrivate::VersionCheck<TFetchRequestData::TForgottenTopic::MessageMeta::FlexibleVersionMin, TFetchRequestData::TForgottenTopic::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 7: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 9: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 11: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 8;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TFetchRequestData::TForgottenTopic::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TFetchResponseData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TFetchResponseData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TFetchResponseData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<ThrottleTimeMsMeta> ThrottleTimeMs;
    NPrivate::TReadStrategy<ErrorCodeMeta> ErrorCode;
    NPrivate::TReadStrategy<SessionIdMeta> SessionId;
    NPrivate::TReadStrategy<ResponsesMeta> Responses;
};

TFetchResponseData::TReadContext::TReadContext(TFetchResponseData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , ThrottleTimeMs()
    , ErrorCode()
    , SessionId()
    , Responses()
{}


TReadDemand TFetchResponseData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                ThrottleTimeMs.Init<NPrivate::ReadFieldRule<ThrottleTimeMsMeta>>(Value.ThrottleTimeMs, Version);
            }
            case 1: {
                auto demand = ThrottleTimeMs.Next<NPrivate::ReadFieldRule<ThrottleTimeMsMeta>>(Value.ThrottleTimeMs, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                ErrorCode.Init<NPrivate::ReadFieldRule<ErrorCodeMeta>>(Value.ErrorCode, Version);
            }
            case 3: {
                auto demand = ErrorCode.Next<NPrivate::ReadFieldRule<ErrorCodeMeta>>(Value.ErrorCode, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                SessionId.Init<NPrivate::ReadFieldRule<SessionIdMeta>>(Value.SessionId, Version);
            }
            case 5: {
                auto demand = SessionId.Next<NPrivate::ReadFieldRule<SessionIdMeta>>(Value.SessionId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                Responses.Init<NPrivate::ReadFieldRule<ResponsesMeta>>(Value.Responses, Version);
            }
            case 7: {
                auto demand = Responses.Next<NPrivate::ReadFieldRule<ResponsesMeta>>(Value.Responses, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                if (!NPrivate::VersionCheck<TFetchResponseData::MessageMeta::FlexibleVersionMin, TFetchResponseData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 9: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 11: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 13: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 10;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TFetchResponseData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TFetchResponseData::TFetchableTopicResponse::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TFetchResponseData::TFetchableTopicResponse& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TFetchResponseData::TFetchableTopicResponse& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<TopicMeta> Topic;
    NPrivate::TReadStrategy<TopicIdMeta> TopicId;
    NPrivate::TReadStrategy<PartitionsMeta> Partitions;
};

TFetchResponseData::TFetchableTopicResponse::TReadContext::TReadContext(TFetchResponseData::TFetchableTopicResponse& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , Topic()
    , TopicId()
    , Partitions()
{}


TReadDemand TFetchResponseData::TFetchableTopicResponse::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                Topic.Init<NPrivate::ReadFieldRule<TopicMeta>>(Value.Topic, Version);
            }
            case 1: {
                auto demand = Topic.Next<NPrivate::ReadFieldRule<TopicMeta>>(Value.Topic, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                TopicId.Init<NPrivate::ReadFieldRule<TopicIdMeta>>(Value.TopicId, Version);
            }
            case 3: {
                auto demand = TopicId.Next<NPrivate::ReadFieldRule<TopicIdMeta>>(Value.TopicId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                Partitions.Init<NPrivate::ReadFieldRule<PartitionsMeta>>(Value.Partitions, Version);
            }
            case 5: {
                auto demand = Partitions.Next<NPrivate::ReadFieldRule<PartitionsMeta>>(Value.Partitions, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                if (!NPrivate::VersionCheck<TFetchResponseData::TFetchableTopicResponse::MessageMeta::FlexibleVersionMin, TFetchResponseData::TFetchableTopicResponse::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 7: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 9: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 11: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 8;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TFetchResponseData::TFetchableTopicResponse::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TFetchResponseData::TFetchableTopicResponse::TPartitionData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TFetchResponseData::TFetchableTopicResponse::TPartitionData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TFetchResponseData::TFetchableTopicResponse::TPartitionData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<PartitionIndexMeta> PartitionIndex;
    NPrivate::TReadStrategy<ErrorCodeMeta> ErrorCode;
    NPrivate::TReadStrategy<HighWatermarkMeta> HighWatermark;
    NPrivate::TReadStrategy<LastStableOffsetMeta> LastStableOffset;
    NPrivate::TReadStrategy<LogStartOffsetMeta> LogStartOffset;
    NPrivate::TReadStrategy<DivergingEpochMeta> DivergingEpoch;
    NPrivate::TReadStrategy<CurrentLeaderMeta> CurrentLeader;
    NPrivate::TReadStrategy<SnapshotIdMeta> SnapshotId;
    NPrivate::TReadStrategy<AbortedTransactionsMeta> AbortedTransactions;
    NPrivate::TReadStrategy<PreferredReadReplicaMeta> PreferredReadReplica;
    NPrivate::TReadStrategy<RecordsMeta> Records;
};

TFetchResponseData::TFetchableTopicResponse::TPartitionData::TReadContext::TReadContext(TFetchResponseData::TFetchableTopicResponse::TPartitionData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , PartitionIndex()
    , ErrorCode()
    , HighWatermark()
    , LastStableOffset()
    , LogStartOffset()
    , DivergingEpoch()
    , CurrentLeader()
    , SnapshotId()
    , AbortedTransactions()
    , PreferredReadReplica()
    , Records()
{}


TReadDemand TFetchResponseData::TFetchableTopicResponse::TPartitionData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                PartitionIndex.Init<NPrivate::ReadFieldRule<PartitionIndexMeta>>(Value.PartitionIndex, Version);
            }
            case 1: {
                auto demand = PartitionIndex.Next<NPrivate::ReadFieldRule<PartitionIndexMeta>>(Value.PartitionIndex, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                ErrorCode.Init<NPrivate::ReadFieldRule<ErrorCodeMeta>>(Value.ErrorCode, Version);
            }
            case 3: {
                auto demand = ErrorCode.Next<NPrivate::ReadFieldRule<ErrorCodeMeta>>(Value.ErrorCode, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                HighWatermark.Init<NPrivate::ReadFieldRule<HighWatermarkMeta>>(Value.HighWatermark, Version);
            }
            case 5: {
                auto demand = HighWatermark.Next<NPrivate::ReadFieldRule<HighWatermarkMeta>>(Value.HighWatermark, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                LastStableOffset.Init<NPrivate::ReadFieldRule<LastStableOffsetMeta>>(Value.LastStableOffset, Version);
            }
            case 7: {
                auto demand = LastStableOffset.Next<NPrivate::ReadFieldRule<LastStableOffsetMeta>>(Value.LastStableOffset, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                ++Step;
                LogStartOffset.Init<NPrivate::ReadFieldRule<LogStartOffsetMeta>>(Value.LogStartOffset, Version);
            }
            case 9: {
                auto demand = LogStartOffset.Next<NPrivate::ReadFieldRule<LogStartOffsetMeta>>(Value.LogStartOffset, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                DivergingEpoch.Init<NPrivate::ReadFieldRule<DivergingEpochMeta>>(Value.DivergingEpoch, Version);
            }
            case 11: {
                auto demand = DivergingEpoch.Next<NPrivate::ReadFieldRule<DivergingEpochMeta>>(Value.DivergingEpoch, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                ++Step;
                CurrentLeader.Init<NPrivate::ReadFieldRule<CurrentLeaderMeta>>(Value.CurrentLeader, Version);
            }
            case 13: {
                auto demand = CurrentLeader.Next<NPrivate::ReadFieldRule<CurrentLeaderMeta>>(Value.CurrentLeader, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 14: {
                ++Step;
                SnapshotId.Init<NPrivate::ReadFieldRule<SnapshotIdMeta>>(Value.SnapshotId, Version);
            }
            case 15: {
                auto demand = SnapshotId.Next<NPrivate::ReadFieldRule<SnapshotIdMeta>>(Value.SnapshotId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 16: {
                ++Step;
                AbortedTransactions.Init<NPrivate::ReadFieldRule<AbortedTransactionsMeta>>(Value.AbortedTransactions, Version);
            }
            case 17: {
                auto demand = AbortedTransactions.Next<NPrivate::ReadFieldRule<AbortedTransactionsMeta>>(Value.AbortedTransactions, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 18: {
                ++Step;
                PreferredReadReplica.Init<NPrivate::ReadFieldRule<PreferredReadReplicaMeta>>(Value.PreferredReadReplica, Version);
            }
            case 19: {
                auto demand = PreferredReadReplica.Next<NPrivate::ReadFieldRule<PreferredReadReplicaMeta>>(Value.PreferredReadReplica, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 20: {
                ++Step;
                Records.Init<NPrivate::ReadFieldRule<RecordsMeta>>(Value.Records, Version);
            }
            case 21: {
                auto demand = Records.Next<NPrivate::ReadFieldRule<RecordsMeta>>(Value.Records, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 22: {
                if (!NPrivate::VersionCheck<TFetchResponseData::TFetchableTopicResponse::TPartitionData::MessageMeta::FlexibleVersionMin, TFetchResponseData::TFetchableTopicResponse::TPartitionData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 23: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 24: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 25: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 26: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 27: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    case 0: {
                        if (!TagInitialized_) {
                            TagInitialized_=true;
                            DivergingEpoch.Init<NPrivate::ReadTaggedFieldRule<DivergingEpochMeta>>(Value.DivergingEpoch, Version);
                        }
                        demand = DivergingEpoch.Next<NPrivate::ReadTaggedFieldRule<DivergingEpochMeta>>(Value.DivergingEpoch, Version);
                        break;
                    }
                    case 1: {
                        if (!TagInitialized_) {
                            TagInitialized_=true;
                            CurrentLeader.Init<NPrivate::ReadTaggedFieldRule<CurrentLeaderMeta>>(Value.CurrentLeader, Version);
                        }
                        demand = CurrentLeader.Next<NPrivate::ReadTaggedFieldRule<CurrentLeaderMeta>>(Value.CurrentLeader, Version);
                        break;
                    }
                    case 2: {
                        if (!TagInitialized_) {
                            TagInitialized_=true;
                            SnapshotId.Init<NPrivate::ReadTaggedFieldRule<SnapshotIdMeta>>(Value.SnapshotId, Version);
                        }
                        demand = SnapshotId.Next<NPrivate::ReadTaggedFieldRule<SnapshotIdMeta>>(Value.SnapshotId, Version);
                        break;
                    }
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 24;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TFetchResponseData::TFetchableTopicResponse::TPartitionData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<EpochMeta> Epoch;
    NPrivate::TReadStrategy<EndOffsetMeta> EndOffset;
};

TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::TReadContext::TReadContext(TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , Epoch()
    , EndOffset()
{}


TReadDemand TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                Epoch.Init<NPrivate::ReadFieldRule<EpochMeta>>(Value.Epoch, Version);
            }
            case 1: {
                auto demand = Epoch.Next<NPrivate::ReadFieldRule<EpochMeta>>(Value.Epoch, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                EndOffset.Init<NPrivate::ReadFieldRule<EndOffsetMeta>>(Value.EndOffset, Version);
            }
            case 3: {
                auto demand = EndOffset.Next<NPrivate::ReadFieldRule<EndOffsetMeta>>(Value.EndOffset, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                if (!NPrivate::VersionCheck<TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::MessageMeta::FlexibleVersionMin, TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 5: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 7: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 9: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 6;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TFetchResponseData::TFetchableTopicResponse::TPartitionData::TEpochEndOffset::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<LeaderIdMeta> LeaderId;
    NPrivate::TReadStrategy<LeaderEpochMeta> LeaderEpoch;
};

TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::TReadContext::TReadContext(TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , LeaderId()
    , LeaderEpoch()
{}


TReadDemand TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                LeaderId.Init<NPrivate::ReadFieldRule<LeaderIdMeta>>(Value.LeaderId, Version);
            }
            case 1: {
                auto demand = LeaderId.Next<NPrivate::ReadFieldRule<LeaderIdMeta>>(Value.LeaderId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                LeaderEpoch.Init<NPrivate::ReadFieldRule<LeaderEpochMeta>>(Value.LeaderEpoch, Version);
            }
            case 3: {
                auto demand = LeaderEpoch.Next<NPrivate::ReadFieldRule<LeaderEpochMeta>>(Value.LeaderEpoch, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                if (!NPrivate::VersionCheck<TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::MessageMeta::FlexibleVersionMin, TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 5: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 7: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 9: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 6;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TFetchResponseData::TFetchableTopicResponse::TPartitionData::TLeaderIdAndEpoch::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<EndOffsetMeta> EndOffset;
    NPrivate::TReadStrategy<EpochMeta> Epoch;
};

TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::TReadContext::TReadContext(TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , EndOffset()
    , Epoch()
{}


TReadDemand TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                EndOffset.Init<NPrivate::ReadFieldRule<EndOffsetMeta>>(Value.EndOffset, Version);
            }
            case 1: {
                auto demand = EndOffset.Next<NPrivate::ReadFieldRule<EndOffsetMeta>>(Value.EndOffset, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                Epoch.Init<NPrivate::ReadFieldRule<EpochMeta>>(Value.Epoch, Version);
            }
            case 3: {
                auto demand = Epoch.Next<NPrivate::ReadFieldRule<EpochMeta>>(Value.Epoch, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                if (!NPrivate::VersionCheck<TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::MessageMeta::FlexibleVersionMin, TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 5: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 7: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 9: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 6;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TFetchResponseData::TFetchableTopicResponse::TPartitionData::TSnapshotId::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<ProducerIdMeta> ProducerId;
    NPrivate::TReadStrategy<FirstOffsetMeta> FirstOffset;
};

TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::TReadContext::TReadContext(TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , ProducerId()
    , FirstOffset()
{}


TReadDemand TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                ProducerId.Init<NPrivate::ReadFieldRule<ProducerIdMeta>>(Value.ProducerId, Version);
            }
            case 1: {
                auto demand = ProducerId.Next<NPrivate::ReadFieldRule<ProducerIdMeta>>(Value.ProducerId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                FirstOffset.Init<NPrivate::ReadFieldRule<FirstOffsetMeta>>(Value.FirstOffset, Version);
            }
            case 3: {
                auto demand = FirstOffset.Next<NPrivate::ReadFieldRule<FirstOffsetMeta>>(Value.FirstOffset, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                if (!NPrivate::VersionCheck<TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::MessageMeta::FlexibleVersionMin, TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 5: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 7: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 9: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 6;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TFetchResponseData::TFetchableTopicResponse::TPartitionData::TAbortedTransaction::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TMetadataRequestData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TMetadataRequestData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TMetadataRequestData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<TopicsMeta> Topics;
    NPrivate::TReadStrategy<AllowAutoTopicCreationMeta> AllowAutoTopicCreation;
    NPrivate::TReadStrategy<IncludeClusterAuthorizedOperationsMeta> IncludeClusterAuthorizedOperations;
    NPrivate::TReadStrategy<IncludeTopicAuthorizedOperationsMeta> IncludeTopicAuthorizedOperations;
};

TMetadataRequestData::TReadContext::TReadContext(TMetadataRequestData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , Topics()
    , AllowAutoTopicCreation()
    , IncludeClusterAuthorizedOperations()
    , IncludeTopicAuthorizedOperations()
{}


TReadDemand TMetadataRequestData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                Topics.Init<NPrivate::ReadFieldRule<TopicsMeta>>(Value.Topics, Version);
            }
            case 1: {
                auto demand = Topics.Next<NPrivate::ReadFieldRule<TopicsMeta>>(Value.Topics, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                AllowAutoTopicCreation.Init<NPrivate::ReadFieldRule<AllowAutoTopicCreationMeta>>(Value.AllowAutoTopicCreation, Version);
            }
            case 3: {
                auto demand = AllowAutoTopicCreation.Next<NPrivate::ReadFieldRule<AllowAutoTopicCreationMeta>>(Value.AllowAutoTopicCreation, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                IncludeClusterAuthorizedOperations.Init<NPrivate::ReadFieldRule<IncludeClusterAuthorizedOperationsMeta>>(Value.IncludeClusterAuthorizedOperations, Version);
            }
            case 5: {
                auto demand = IncludeClusterAuthorizedOperations.Next<NPrivate::ReadFieldRule<IncludeClusterAuthorizedOperationsMeta>>(Value.IncludeClusterAuthorizedOperations, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                IncludeTopicAuthorizedOperations.Init<NPrivate::ReadFieldRule<IncludeTopicAuthorizedOperationsMeta>>(Value.IncludeTopicAuthorizedOperations, Version);
            }
            case 7: {
                auto demand = IncludeTopicAuthorizedOperations.Next<NPrivate::ReadFieldRule<IncludeTopicAuthorizedOperationsMeta>>(Value.IncludeTopicAuthorizedOperations, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                if (!NPrivate::VersionCheck<TMetadataRequestData::MessageMeta::FlexibleVersionMin, TMetadataRequestData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 9: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 11: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 13: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 10;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TMetadataRequestData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TMetadataRequestData::TMetadataRequestTopic::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TMetadataRequestData::TMetadataRequestTopic& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TMetadataRequestData::TMetadataRequestTopic& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<TopicIdMeta> TopicId;
    NPrivate::TReadStrategy<NameMeta> Name;
};

TMetadataRequestData::TMetadataRequestTopic::TReadContext::TReadContext(TMetadataRequestData::TMetadataRequestTopic& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , TopicId()
    , Name()
{}


TReadDemand TMetadataRequestData::TMetadataRequestTopic::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                TopicId.Init<NPrivate::ReadFieldRule<TopicIdMeta>>(Value.TopicId, Version);
            }
            case 1: {
                auto demand = TopicId.Next<NPrivate::ReadFieldRule<TopicIdMeta>>(Value.TopicId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                Name.Init<NPrivate::ReadFieldRule<NameMeta>>(Value.Name, Version);
            }
            case 3: {
                auto demand = Name.Next<NPrivate::ReadFieldRule<NameMeta>>(Value.Name, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                if (!NPrivate::VersionCheck<TMetadataRequestData::TMetadataRequestTopic::MessageMeta::FlexibleVersionMin, TMetadataRequestData::TMetadataRequestTopic::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 5: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 7: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 9: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 6;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TMetadataRequestData::TMetadataRequestTopic::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TMetadataResponseData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TMetadataResponseData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TMetadataResponseData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<ThrottleTimeMsMeta> ThrottleTimeMs;
    NPrivate::TReadStrategy<BrokersMeta> Brokers;
    NPrivate::TReadStrategy<ClusterIdMeta> ClusterId;
    NPrivate::TReadStrategy<ControllerIdMeta> ControllerId;
    NPrivate::TReadStrategy<TopicsMeta> Topics;
    NPrivate::TReadStrategy<ClusterAuthorizedOperationsMeta> ClusterAuthorizedOperations;
};

TMetadataResponseData::TReadContext::TReadContext(TMetadataResponseData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , ThrottleTimeMs()
    , Brokers()
    , ClusterId()
    , ControllerId()
    , Topics()
    , ClusterAuthorizedOperations()
{}


TReadDemand TMetadataResponseData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                ThrottleTimeMs.Init<NPrivate::ReadFieldRule<ThrottleTimeMsMeta>>(Value.ThrottleTimeMs, Version);
            }
            case 1: {
                auto demand = ThrottleTimeMs.Next<NPrivate::ReadFieldRule<ThrottleTimeMsMeta>>(Value.ThrottleTimeMs, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                Brokers.Init<NPrivate::ReadFieldRule<BrokersMeta>>(Value.Brokers, Version);
            }
            case 3: {
                auto demand = Brokers.Next<NPrivate::ReadFieldRule<BrokersMeta>>(Value.Brokers, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                ClusterId.Init<NPrivate::ReadFieldRule<ClusterIdMeta>>(Value.ClusterId, Version);
            }
            case 5: {
                auto demand = ClusterId.Next<NPrivate::ReadFieldRule<ClusterIdMeta>>(Value.ClusterId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                ControllerId.Init<NPrivate::ReadFieldRule<ControllerIdMeta>>(Value.ControllerId, Version);
            }
            case 7: {
                auto demand = ControllerId.Next<NPrivate::ReadFieldRule<ControllerIdMeta>>(Value.ControllerId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                ++Step;
                Topics.Init<NPrivate::ReadFieldRule<TopicsMeta>>(Value.Topics, Version);
            }
            case 9: {
                auto demand = Topics.Next<NPrivate::ReadFieldRule<TopicsMeta>>(Value.Topics, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                ClusterAuthorizedOperations.Init<NPrivate::ReadFieldRule<ClusterAuthorizedOperationsMeta>>(Value.ClusterAuthorizedOperations, Version);
            }
            case 11: {
                auto demand = ClusterAuthorizedOperations.Next<NPrivate::ReadFieldRule<ClusterAuthorizedOperationsMeta>>(Value.ClusterAuthorizedOperations, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                if (!NPrivate::VersionCheck<TMetadataResponseData::MessageMeta::FlexibleVersionMin, TMetadataResponseData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 13: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 14: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 15: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 16: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 17: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 14;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TMetadataResponseData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TMetadataResponseData::TMetadataResponseBroker::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TMetadataResponseData::TMetadataResponseBroker& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TMetadataResponseData::TMetadataResponseBroker& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<NodeIdMeta> NodeId;
    NPrivate::TReadStrategy<HostMeta> Host;
    NPrivate::TReadStrategy<PortMeta> Port;
    NPrivate::TReadStrategy<RackMeta> Rack;
};

TMetadataResponseData::TMetadataResponseBroker::TReadContext::TReadContext(TMetadataResponseData::TMetadataResponseBroker& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , NodeId()
    , Host()
    , Port()
    , Rack()
{}


TReadDemand TMetadataResponseData::TMetadataResponseBroker::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                NodeId.Init<NPrivate::ReadFieldRule<NodeIdMeta>>(Value.NodeId, Version);
            }
            case 1: {
                auto demand = NodeId.Next<NPrivate::ReadFieldRule<NodeIdMeta>>(Value.NodeId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                Host.Init<NPrivate::ReadFieldRule<HostMeta>>(Value.Host, Version);
            }
            case 3: {
                auto demand = Host.Next<NPrivate::ReadFieldRule<HostMeta>>(Value.Host, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                Port.Init<NPrivate::ReadFieldRule<PortMeta>>(Value.Port, Version);
            }
            case 5: {
                auto demand = Port.Next<NPrivate::ReadFieldRule<PortMeta>>(Value.Port, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                Rack.Init<NPrivate::ReadFieldRule<RackMeta>>(Value.Rack, Version);
            }
            case 7: {
                auto demand = Rack.Next<NPrivate::ReadFieldRule<RackMeta>>(Value.Rack, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                if (!NPrivate::VersionCheck<TMetadataResponseData::TMetadataResponseBroker::MessageMeta::FlexibleVersionMin, TMetadataResponseData::TMetadataResponseBroker::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 9: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 11: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 13: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 10;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TMetadataResponseData::TMetadataResponseBroker::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TMetadataResponseData::TMetadataResponseTopic::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TMetadataResponseData::TMetadataResponseTopic& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TMetadataResponseData::TMetadataResponseTopic& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<ErrorCodeMeta> ErrorCode;
    NPrivate::TReadStrategy<NameMeta> Name;
    NPrivate::TReadStrategy<TopicIdMeta> TopicId;
    NPrivate::TReadStrategy<IsInternalMeta> IsInternal;
    NPrivate::TReadStrategy<PartitionsMeta> Partitions;
    NPrivate::TReadStrategy<TopicAuthorizedOperationsMeta> TopicAuthorizedOperations;
};

TMetadataResponseData::TMetadataResponseTopic::TReadContext::TReadContext(TMetadataResponseData::TMetadataResponseTopic& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , ErrorCode()
    , Name()
    , TopicId()
    , IsInternal()
    , Partitions()
    , TopicAuthorizedOperations()
{}


TReadDemand TMetadataResponseData::TMetadataResponseTopic::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                ErrorCode.Init<NPrivate::ReadFieldRule<ErrorCodeMeta>>(Value.ErrorCode, Version);
            }
            case 1: {
                auto demand = ErrorCode.Next<NPrivate::ReadFieldRule<ErrorCodeMeta>>(Value.ErrorCode, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                Name.Init<NPrivate::ReadFieldRule<NameMeta>>(Value.Name, Version);
            }
            case 3: {
                auto demand = Name.Next<NPrivate::ReadFieldRule<NameMeta>>(Value.Name, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                TopicId.Init<NPrivate::ReadFieldRule<TopicIdMeta>>(Value.TopicId, Version);
            }
            case 5: {
                auto demand = TopicId.Next<NPrivate::ReadFieldRule<TopicIdMeta>>(Value.TopicId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                IsInternal.Init<NPrivate::ReadFieldRule<IsInternalMeta>>(Value.IsInternal, Version);
            }
            case 7: {
                auto demand = IsInternal.Next<NPrivate::ReadFieldRule<IsInternalMeta>>(Value.IsInternal, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                ++Step;
                Partitions.Init<NPrivate::ReadFieldRule<PartitionsMeta>>(Value.Partitions, Version);
            }
            case 9: {
                auto demand = Partitions.Next<NPrivate::ReadFieldRule<PartitionsMeta>>(Value.Partitions, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                TopicAuthorizedOperations.Init<NPrivate::ReadFieldRule<TopicAuthorizedOperationsMeta>>(Value.TopicAuthorizedOperations, Version);
            }
            case 11: {
                auto demand = TopicAuthorizedOperations.Next<NPrivate::ReadFieldRule<TopicAuthorizedOperationsMeta>>(Value.TopicAuthorizedOperations, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                if (!NPrivate::VersionCheck<TMetadataResponseData::TMetadataResponseTopic::MessageMeta::FlexibleVersionMin, TMetadataResponseData::TMetadataResponseTopic::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 13: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 14: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 15: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 16: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 17: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 14;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TMetadataResponseData::TMetadataResponseTopic::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<ErrorCodeMeta> ErrorCode;
    NPrivate::TReadStrategy<PartitionIndexMeta> PartitionIndex;
    NPrivate::TReadStrategy<LeaderIdMeta> LeaderId;
    NPrivate::TReadStrategy<LeaderEpochMeta> LeaderEpoch;
    NPrivate::TReadStrategy<ReplicaNodesMeta> ReplicaNodes;
    NPrivate::TReadStrategy<IsrNodesMeta> IsrNodes;
    NPrivate::TReadStrategy<OfflineReplicasMeta> OfflineReplicas;
};

TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::TReadContext::TReadContext(TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , ErrorCode()
    , PartitionIndex()
    , LeaderId()
    , LeaderEpoch()
    , ReplicaNodes()
    , IsrNodes()
    , OfflineReplicas()
{}


TReadDemand TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                ErrorCode.Init<NPrivate::ReadFieldRule<ErrorCodeMeta>>(Value.ErrorCode, Version);
            }
            case 1: {
                auto demand = ErrorCode.Next<NPrivate::ReadFieldRule<ErrorCodeMeta>>(Value.ErrorCode, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                PartitionIndex.Init<NPrivate::ReadFieldRule<PartitionIndexMeta>>(Value.PartitionIndex, Version);
            }
            case 3: {
                auto demand = PartitionIndex.Next<NPrivate::ReadFieldRule<PartitionIndexMeta>>(Value.PartitionIndex, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                LeaderId.Init<NPrivate::ReadFieldRule<LeaderIdMeta>>(Value.LeaderId, Version);
            }
            case 5: {
                auto demand = LeaderId.Next<NPrivate::ReadFieldRule<LeaderIdMeta>>(Value.LeaderId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                LeaderEpoch.Init<NPrivate::ReadFieldRule<LeaderEpochMeta>>(Value.LeaderEpoch, Version);
            }
            case 7: {
                auto demand = LeaderEpoch.Next<NPrivate::ReadFieldRule<LeaderEpochMeta>>(Value.LeaderEpoch, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                ++Step;
                ReplicaNodes.Init<NPrivate::ReadFieldRule<ReplicaNodesMeta>>(Value.ReplicaNodes, Version);
            }
            case 9: {
                auto demand = ReplicaNodes.Next<NPrivate::ReadFieldRule<ReplicaNodesMeta>>(Value.ReplicaNodes, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                IsrNodes.Init<NPrivate::ReadFieldRule<IsrNodesMeta>>(Value.IsrNodes, Version);
            }
            case 11: {
                auto demand = IsrNodes.Next<NPrivate::ReadFieldRule<IsrNodesMeta>>(Value.IsrNodes, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                ++Step;
                OfflineReplicas.Init<NPrivate::ReadFieldRule<OfflineReplicasMeta>>(Value.OfflineReplicas, Version);
            }
            case 13: {
                auto demand = OfflineReplicas.Next<NPrivate::ReadFieldRule<OfflineReplicasMeta>>(Value.OfflineReplicas, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 14: {
                if (!NPrivate::VersionCheck<TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::MessageMeta::FlexibleVersionMin, TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 15: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 16: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 17: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 18: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 19: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 16;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TMetadataResponseData::TMetadataResponseTopic::TMetadataResponsePartition::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TApiVersionsRequestData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TApiVersionsRequestData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TApiVersionsRequestData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<ClientSoftwareNameMeta> ClientSoftwareName;
    NPrivate::TReadStrategy<ClientSoftwareVersionMeta> ClientSoftwareVersion;
};

TApiVersionsRequestData::TReadContext::TReadContext(TApiVersionsRequestData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , ClientSoftwareName()
    , ClientSoftwareVersion()
{}


TReadDemand TApiVersionsRequestData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                ClientSoftwareName.Init<NPrivate::ReadFieldRule<ClientSoftwareNameMeta>>(Value.ClientSoftwareName, Version);
            }
            case 1: {
                auto demand = ClientSoftwareName.Next<NPrivate::ReadFieldRule<ClientSoftwareNameMeta>>(Value.ClientSoftwareName, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                ClientSoftwareVersion.Init<NPrivate::ReadFieldRule<ClientSoftwareVersionMeta>>(Value.ClientSoftwareVersion, Version);
            }
            case 3: {
                auto demand = ClientSoftwareVersion.Next<NPrivate::ReadFieldRule<ClientSoftwareVersionMeta>>(Value.ClientSoftwareVersion, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                if (!NPrivate::VersionCheck<TApiVersionsRequestData::MessageMeta::FlexibleVersionMin, TApiVersionsRequestData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 5: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 7: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 9: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 6;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TApiVersionsRequestData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TApiVersionsResponseData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TApiVersionsResponseData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TApiVersionsResponseData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<ErrorCodeMeta> ErrorCode;
    NPrivate::TReadStrategy<ApiKeysMeta> ApiKeys;
    NPrivate::TReadStrategy<ThrottleTimeMsMeta> ThrottleTimeMs;
    NPrivate::TReadStrategy<SupportedFeaturesMeta> SupportedFeatures;
    NPrivate::TReadStrategy<FinalizedFeaturesEpochMeta> FinalizedFeaturesEpoch;
    NPrivate::TReadStrategy<FinalizedFeaturesMeta> FinalizedFeatures;
    NPrivate::TReadStrategy<ZkMigrationReadyMeta> ZkMigrationReady;
};

TApiVersionsResponseData::TReadContext::TReadContext(TApiVersionsResponseData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , ErrorCode()
    , ApiKeys()
    , ThrottleTimeMs()
    , SupportedFeatures()
    , FinalizedFeaturesEpoch()
    , FinalizedFeatures()
    , ZkMigrationReady()
{}


TReadDemand TApiVersionsResponseData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                ErrorCode.Init<NPrivate::ReadFieldRule<ErrorCodeMeta>>(Value.ErrorCode, Version);
            }
            case 1: {
                auto demand = ErrorCode.Next<NPrivate::ReadFieldRule<ErrorCodeMeta>>(Value.ErrorCode, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                ApiKeys.Init<NPrivate::ReadFieldRule<ApiKeysMeta>>(Value.ApiKeys, Version);
            }
            case 3: {
                auto demand = ApiKeys.Next<NPrivate::ReadFieldRule<ApiKeysMeta>>(Value.ApiKeys, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                ThrottleTimeMs.Init<NPrivate::ReadFieldRule<ThrottleTimeMsMeta>>(Value.ThrottleTimeMs, Version);
            }
            case 5: {
                auto demand = ThrottleTimeMs.Next<NPrivate::ReadFieldRule<ThrottleTimeMsMeta>>(Value.ThrottleTimeMs, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                SupportedFeatures.Init<NPrivate::ReadFieldRule<SupportedFeaturesMeta>>(Value.SupportedFeatures, Version);
            }
            case 7: {
                auto demand = SupportedFeatures.Next<NPrivate::ReadFieldRule<SupportedFeaturesMeta>>(Value.SupportedFeatures, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                ++Step;
                FinalizedFeaturesEpoch.Init<NPrivate::ReadFieldRule<FinalizedFeaturesEpochMeta>>(Value.FinalizedFeaturesEpoch, Version);
            }
            case 9: {
                auto demand = FinalizedFeaturesEpoch.Next<NPrivate::ReadFieldRule<FinalizedFeaturesEpochMeta>>(Value.FinalizedFeaturesEpoch, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                FinalizedFeatures.Init<NPrivate::ReadFieldRule<FinalizedFeaturesMeta>>(Value.FinalizedFeatures, Version);
            }
            case 11: {
                auto demand = FinalizedFeatures.Next<NPrivate::ReadFieldRule<FinalizedFeaturesMeta>>(Value.FinalizedFeatures, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                ++Step;
                ZkMigrationReady.Init<NPrivate::ReadFieldRule<ZkMigrationReadyMeta>>(Value.ZkMigrationReady, Version);
            }
            case 13: {
                auto demand = ZkMigrationReady.Next<NPrivate::ReadFieldRule<ZkMigrationReadyMeta>>(Value.ZkMigrationReady, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 14: {
                if (!NPrivate::VersionCheck<TApiVersionsResponseData::MessageMeta::FlexibleVersionMin, TApiVersionsResponseData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 15: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 16: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 17: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 18: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 19: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    case 0: {
                        if (!TagInitialized_) {
                            TagInitialized_=true;
                            SupportedFeatures.Init<NPrivate::ReadTaggedFieldRule<SupportedFeaturesMeta>>(Value.SupportedFeatures, Version);
                        }
                        demand = SupportedFeatures.Next<NPrivate::ReadTaggedFieldRule<SupportedFeaturesMeta>>(Value.SupportedFeatures, Version);
                        break;
                    }
                    case 1: {
                        if (!TagInitialized_) {
                            TagInitialized_=true;
                            FinalizedFeaturesEpoch.Init<NPrivate::ReadTaggedFieldRule<FinalizedFeaturesEpochMeta>>(Value.FinalizedFeaturesEpoch, Version);
                        }
                        demand = FinalizedFeaturesEpoch.Next<NPrivate::ReadTaggedFieldRule<FinalizedFeaturesEpochMeta>>(Value.FinalizedFeaturesEpoch, Version);
                        break;
                    }
                    case 2: {
                        if (!TagInitialized_) {
                            TagInitialized_=true;
                            FinalizedFeatures.Init<NPrivate::ReadTaggedFieldRule<FinalizedFeaturesMeta>>(Value.FinalizedFeatures, Version);
                        }
                        demand = FinalizedFeatures.Next<NPrivate::ReadTaggedFieldRule<FinalizedFeaturesMeta>>(Value.FinalizedFeatures, Version);
                        break;
                    }
                    case 3: {
                        if (!TagInitialized_) {
                            TagInitialized_=true;
                            ZkMigrationReady.Init<NPrivate::ReadTaggedFieldRule<ZkMigrationReadyMeta>>(Value.ZkMigrationReady, Version);
                        }
                        demand = ZkMigrationReady.Next<NPrivate::ReadTaggedFieldRule<ZkMigrationReadyMeta>>(Value.ZkMigrationReady, Version);
                        break;
                    }
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 16;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TApiVersionsResponseData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TApiVersionsResponseData::TApiVersion::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TApiVersionsResponseData::TApiVersion& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TApiVersionsResponseData::TApiVersion& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<ApiKeyMeta> ApiKey;
    NPrivate::TReadStrategy<MinVersionMeta> MinVersion;
    NPrivate::TReadStrategy<MaxVersionMeta> MaxVersion;
};

TApiVersionsResponseData::TApiVersion::TReadContext::TReadContext(TApiVersionsResponseData::TApiVersion& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , ApiKey()
    , MinVersion()
    , MaxVersion()
{}


TReadDemand TApiVersionsResponseData::TApiVersion::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                ApiKey.Init<NPrivate::ReadFieldRule<ApiKeyMeta>>(Value.ApiKey, Version);
            }
            case 1: {
                auto demand = ApiKey.Next<NPrivate::ReadFieldRule<ApiKeyMeta>>(Value.ApiKey, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                MinVersion.Init<NPrivate::ReadFieldRule<MinVersionMeta>>(Value.MinVersion, Version);
            }
            case 3: {
                auto demand = MinVersion.Next<NPrivate::ReadFieldRule<MinVersionMeta>>(Value.MinVersion, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                MaxVersion.Init<NPrivate::ReadFieldRule<MaxVersionMeta>>(Value.MaxVersion, Version);
            }
            case 5: {
                auto demand = MaxVersion.Next<NPrivate::ReadFieldRule<MaxVersionMeta>>(Value.MaxVersion, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                if (!NPrivate::VersionCheck<TApiVersionsResponseData::TApiVersion::MessageMeta::FlexibleVersionMin, TApiVersionsResponseData::TApiVersion::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 7: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 9: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 11: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 8;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TApiVersionsResponseData::TApiVersion::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TApiVersionsResponseData::TSupportedFeatureKey::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TApiVersionsResponseData::TSupportedFeatureKey& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TApiVersionsResponseData::TSupportedFeatureKey& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<NameMeta> Name;
    NPrivate::TReadStrategy<MinVersionMeta> MinVersion;
    NPrivate::TReadStrategy<MaxVersionMeta> MaxVersion;
};

TApiVersionsResponseData::TSupportedFeatureKey::TReadContext::TReadContext(TApiVersionsResponseData::TSupportedFeatureKey& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , Name()
    , MinVersion()
    , MaxVersion()
{}


TReadDemand TApiVersionsResponseData::TSupportedFeatureKey::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                Name.Init<NPrivate::ReadFieldRule<NameMeta>>(Value.Name, Version);
            }
            case 1: {
                auto demand = Name.Next<NPrivate::ReadFieldRule<NameMeta>>(Value.Name, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                MinVersion.Init<NPrivate::ReadFieldRule<MinVersionMeta>>(Value.MinVersion, Version);
            }
            case 3: {
                auto demand = MinVersion.Next<NPrivate::ReadFieldRule<MinVersionMeta>>(Value.MinVersion, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                MaxVersion.Init<NPrivate::ReadFieldRule<MaxVersionMeta>>(Value.MaxVersion, Version);
            }
            case 5: {
                auto demand = MaxVersion.Next<NPrivate::ReadFieldRule<MaxVersionMeta>>(Value.MaxVersion, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                if (!NPrivate::VersionCheck<TApiVersionsResponseData::TSupportedFeatureKey::MessageMeta::FlexibleVersionMin, TApiVersionsResponseData::TSupportedFeatureKey::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 7: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 9: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 11: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 8;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TApiVersionsResponseData::TSupportedFeatureKey::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TApiVersionsResponseData::TFinalizedFeatureKey::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TApiVersionsResponseData::TFinalizedFeatureKey& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TApiVersionsResponseData::TFinalizedFeatureKey& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<NameMeta> Name;
    NPrivate::TReadStrategy<MaxVersionLevelMeta> MaxVersionLevel;
    NPrivate::TReadStrategy<MinVersionLevelMeta> MinVersionLevel;
};

TApiVersionsResponseData::TFinalizedFeatureKey::TReadContext::TReadContext(TApiVersionsResponseData::TFinalizedFeatureKey& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , Name()
    , MaxVersionLevel()
    , MinVersionLevel()
{}


TReadDemand TApiVersionsResponseData::TFinalizedFeatureKey::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                Name.Init<NPrivate::ReadFieldRule<NameMeta>>(Value.Name, Version);
            }
            case 1: {
                auto demand = Name.Next<NPrivate::ReadFieldRule<NameMeta>>(Value.Name, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                MaxVersionLevel.Init<NPrivate::ReadFieldRule<MaxVersionLevelMeta>>(Value.MaxVersionLevel, Version);
            }
            case 3: {
                auto demand = MaxVersionLevel.Next<NPrivate::ReadFieldRule<MaxVersionLevelMeta>>(Value.MaxVersionLevel, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                MinVersionLevel.Init<NPrivate::ReadFieldRule<MinVersionLevelMeta>>(Value.MinVersionLevel, Version);
            }
            case 5: {
                auto demand = MinVersionLevel.Next<NPrivate::ReadFieldRule<MinVersionLevelMeta>>(Value.MinVersionLevel, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                if (!NPrivate::VersionCheck<TApiVersionsResponseData::TFinalizedFeatureKey::MessageMeta::FlexibleVersionMin, TApiVersionsResponseData::TFinalizedFeatureKey::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 7: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 9: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 11: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 8;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TApiVersionsResponseData::TFinalizedFeatureKey::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TInitProducerIdRequestData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TInitProducerIdRequestData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TInitProducerIdRequestData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<TransactionalIdMeta> TransactionalId;
    NPrivate::TReadStrategy<TransactionTimeoutMsMeta> TransactionTimeoutMs;
    NPrivate::TReadStrategy<ProducerIdMeta> ProducerId;
    NPrivate::TReadStrategy<ProducerEpochMeta> ProducerEpoch;
};

TInitProducerIdRequestData::TReadContext::TReadContext(TInitProducerIdRequestData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , TransactionalId()
    , TransactionTimeoutMs()
    , ProducerId()
    , ProducerEpoch()
{}


TReadDemand TInitProducerIdRequestData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                TransactionalId.Init<NPrivate::ReadFieldRule<TransactionalIdMeta>>(Value.TransactionalId, Version);
            }
            case 1: {
                auto demand = TransactionalId.Next<NPrivate::ReadFieldRule<TransactionalIdMeta>>(Value.TransactionalId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                TransactionTimeoutMs.Init<NPrivate::ReadFieldRule<TransactionTimeoutMsMeta>>(Value.TransactionTimeoutMs, Version);
            }
            case 3: {
                auto demand = TransactionTimeoutMs.Next<NPrivate::ReadFieldRule<TransactionTimeoutMsMeta>>(Value.TransactionTimeoutMs, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                ProducerId.Init<NPrivate::ReadFieldRule<ProducerIdMeta>>(Value.ProducerId, Version);
            }
            case 5: {
                auto demand = ProducerId.Next<NPrivate::ReadFieldRule<ProducerIdMeta>>(Value.ProducerId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                ProducerEpoch.Init<NPrivate::ReadFieldRule<ProducerEpochMeta>>(Value.ProducerEpoch, Version);
            }
            case 7: {
                auto demand = ProducerEpoch.Next<NPrivate::ReadFieldRule<ProducerEpochMeta>>(Value.ProducerEpoch, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                if (!NPrivate::VersionCheck<TInitProducerIdRequestData::MessageMeta::FlexibleVersionMin, TInitProducerIdRequestData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 9: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 11: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 13: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 10;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TInitProducerIdRequestData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
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


class TInitProducerIdResponseData::TReadContext : public NKafka::TReadContext {
public:
    TReadContext(TInitProducerIdResponseData& value, TKafkaVersion version);
    TReadDemand Next() override;
private:
    TInitProducerIdResponseData& Value;
    TKafkaVersion Version;
    size_t Step;
    
    NPrivate::ReadUnsignedVarintStrategy NumTaggedFields_;
    NPrivate::ReadUnsignedVarintStrategy Tag_;
    NPrivate::ReadUnsignedVarintStrategy TagSize_;
    bool TagInitialized_;
    
    NPrivate::TReadStrategy<ThrottleTimeMsMeta> ThrottleTimeMs;
    NPrivate::TReadStrategy<ErrorCodeMeta> ErrorCode;
    NPrivate::TReadStrategy<ProducerIdMeta> ProducerId;
    NPrivate::TReadStrategy<ProducerEpochMeta> ProducerEpoch;
};

TInitProducerIdResponseData::TReadContext::TReadContext(TInitProducerIdResponseData& value, TKafkaVersion version)
    : Value(value)
    , Version(version)
    , Step(0)
    , ThrottleTimeMs()
    , ErrorCode()
    , ProducerId()
    , ProducerEpoch()
{}


TReadDemand TInitProducerIdResponseData::TReadContext::Next() {
    while(true) {
        switch(Step) {
            case 0: {
                ++Step;
                ThrottleTimeMs.Init<NPrivate::ReadFieldRule<ThrottleTimeMsMeta>>(Value.ThrottleTimeMs, Version);
            }
            case 1: {
                auto demand = ThrottleTimeMs.Next<NPrivate::ReadFieldRule<ThrottleTimeMsMeta>>(Value.ThrottleTimeMs, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 2: {
                ++Step;
                ErrorCode.Init<NPrivate::ReadFieldRule<ErrorCodeMeta>>(Value.ErrorCode, Version);
            }
            case 3: {
                auto demand = ErrorCode.Next<NPrivate::ReadFieldRule<ErrorCodeMeta>>(Value.ErrorCode, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 4: {
                ++Step;
                ProducerId.Init<NPrivate::ReadFieldRule<ProducerIdMeta>>(Value.ProducerId, Version);
            }
            case 5: {
                auto demand = ProducerId.Next<NPrivate::ReadFieldRule<ProducerIdMeta>>(Value.ProducerId, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 6: {
                ++Step;
                ProducerEpoch.Init<NPrivate::ReadFieldRule<ProducerEpochMeta>>(Value.ProducerEpoch, Version);
            }
            case 7: {
                auto demand = ProducerEpoch.Next<NPrivate::ReadFieldRule<ProducerEpochMeta>>(Value.ProducerEpoch, Version);
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 8: {
                if (!NPrivate::VersionCheck<TInitProducerIdResponseData::MessageMeta::FlexibleVersionMin, TInitProducerIdResponseData::MessageMeta::FlexibleVersionMax>(Version)) return NoDemand;
                ++Step;
                NumTaggedFields_.Init();
            }
            case 9: {
                auto demand = NumTaggedFields_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 10: {
                ++Step;
                if (NumTaggedFields_.Value <= 0) return NoDemand;
                --NumTaggedFields_.Value;
                Tag_.Init();
                TagSize_.Init();
                TagInitialized_=false;
            }
            case 11: {
                auto demand = Tag_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 12: {
                auto demand = TagSize_.Next();
                if (demand) {
                    return demand;
                } else {
                    ++Step;
                }
            }
            case 13: {
                TReadDemand demand;
                switch(Tag_.Value) {
                    default: {
                        if (!TagInitialized_) {
                            TagInitialized_ = true;
                            demand = TReadDemand(TagSize_.Value);
                        } else {
                            demand = NoDemand;
                        }
                    }
                }
                if (demand) {
                    return demand;
                } else {
                    Step = 10;
                    break;
                }
            }
            default:
                return NoDemand;
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
std::unique_ptr<NKafka::TReadContext> TInitProducerIdResponseData::CreateReadContext(TKafkaVersion _version) {
    return std::unique_ptr<NKafka::TReadContext>(new TReadContext(*this, _version));
}
} //namespace NKafka
