#include "kafka_messages_int.h"


namespace NKafka {

//
// TConsumerProtocolSubscription
//
const TConsumerProtocolSubscription::GenerationIdMeta::Type TConsumerProtocolSubscription::GenerationIdMeta::Default = -1;
const TConsumerProtocolSubscription::RackIdMeta::Type TConsumerProtocolSubscription::RackIdMeta::Default = std::nullopt;

TConsumerProtocolSubscription::TConsumerProtocolSubscription()
    : GenerationId(GenerationIdMeta::Default), RackId(std::nullopt)
{}

void TConsumerProtocolSubscription::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TConsumerProtocolSubscription";
    }
    NPrivate::Read<TopicsMeta>(_readable, _version, Topics);
    NPrivate::Read<UserDataMeta>(_readable, _version, UserData);

    if (NPrivate::VersionCheck<OwnedPartitionsMeta::PresentVersions.Min, OwnedPartitionsMeta::PresentVersions.Max>(_version)) {
        NPrivate::Read<OwnedPartitionsMeta>(_readable, _version, OwnedPartitions);
    }

    if (NPrivate::VersionCheck<GenerationIdMeta::PresentVersions.Min, GenerationIdMeta::PresentVersions.Max>(_version)) {
        NPrivate::Read<GenerationIdMeta>(_readable, _version, GenerationId);
    }

    if (NPrivate::VersionCheck<RackIdMeta::PresentVersions.Min, RackIdMeta::PresentVersions.Max>(_version)) {
        NPrivate::Read<RackIdMeta>(_readable, _version, RackId);
    }
}

void TConsumerProtocolSubscription::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TConsumerProtocolSubscription";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicsMeta>(_collector, _writable, _version, Topics);
    NPrivate::Write<UserDataMeta>(_collector, _writable, _version, UserData);

    if (NPrivate::VersionCheck<OwnedPartitionsMeta::PresentVersions.Min, OwnedPartitionsMeta::PresentVersions.Max>(_version)) {
        NPrivate::Write<OwnedPartitionsMeta>(_collector, _writable, _version, OwnedPartitions);
    }

    if (NPrivate::VersionCheck<GenerationIdMeta::PresentVersions.Min, GenerationIdMeta::PresentVersions.Max>(_version)) {
        NPrivate::Write<GenerationIdMeta>(_collector, _writable, _version, GenerationId);
    }

    if (NPrivate::VersionCheck<RackIdMeta::PresentVersions.Min, RackIdMeta::PresentVersions.Max>(_version)) {
        NPrivate::Write<RackIdMeta>(_collector, _writable, _version, RackId);
    }
}

i32 TConsumerProtocolSubscription::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicsMeta>(_collector, _version, Topics);
    NPrivate::Size<UserDataMeta>(_collector, _version, UserData);

    if (NPrivate::VersionCheck<OwnedPartitionsMeta::PresentVersions.Min, OwnedPartitionsMeta::PresentVersions.Max>(_version)) {
        NPrivate::Size<OwnedPartitionsMeta>(_collector, _version, OwnedPartitions);
    }

    if (NPrivate::VersionCheck<GenerationIdMeta::PresentVersions.Min, GenerationIdMeta::PresentVersions.Max>(_version)) {
        NPrivate::Size<GenerationIdMeta>(_collector, _version, GenerationId);
    }

    if (NPrivate::VersionCheck<RackIdMeta::PresentVersions.Min, RackIdMeta::PresentVersions.Max>(_version)) {
        NPrivate::Size<RackIdMeta>(_collector, _version, RackId);
    }
    return _collector.Size;
}



//
// TConsumerProtocolSubscription::TopicPartition
//
const TConsumerProtocolSubscription::TopicPartition::TopicPartition::TopicMeta::Type TConsumerProtocolSubscription::TopicPartition::TopicPartition::TopicMeta::Default = {""};

TConsumerProtocolSubscription::TopicPartition::TopicPartition()
    : Topic(TopicMeta::Default)
{}

void TConsumerProtocolSubscription::TopicPartition::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<TopicMeta::PresentVersions.Min, TopicMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TConsumerProtocolSubscription::TopicPartition";
    }
    NPrivate::Read<TopicMeta>(_readable, _version, Topic);
    NPrivate::Read<PartitionsMeta>(_readable, _version, Partitions);
}

void TConsumerProtocolSubscription::TopicPartition::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<TopicMeta::PresentVersions.Min, TopicMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TConsumerProtocolSubscription::TopicPartition";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicMeta>(_collector, _writable, _version, Topic);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);
}

i32 TConsumerProtocolSubscription::TopicPartition::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicMeta>(_collector, _version, Topic);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);
    return _collector.Size;
}



//
// TConsumerProtocolAssignment
//

const TConsumerProtocolSubscription::TopicPartition::TopicPartition::TopicMeta::Type TConsumerProtocolAssignment::TopicPartition::TopicPartition::TopicMeta::Default = {""};
TConsumerProtocolAssignment::TConsumerProtocolAssignment()
{}

void TConsumerProtocolAssignment::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TConsumerProtocolAssignment";
    }
    NPrivate::Read<AssignedPartitionsMeta>(_readable, _version, AssignedPartitions);
    NPrivate::Read<UserDataMeta>(_readable, _version, UserData);

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

void TConsumerProtocolAssignment::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TConsumerProtocolAssignment";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<AssignedPartitionsMeta>(_collector, _writable, _version, AssignedPartitions);
    NPrivate::Write<UserDataMeta>(_collector, _writable, _version, UserData);

    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
    }
}

i32 TConsumerProtocolAssignment::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<AssignedPartitionsMeta>(_collector, _version, AssignedPartitions);
    NPrivate::Size<UserDataMeta>(_collector, _version, UserData);

    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}



//
// TConsumerProtocolAssignment::TopicPartition
//
TConsumerProtocolAssignment::TopicPartition::TopicPartition()
{}

void TConsumerProtocolAssignment::TopicPartition::Read(TKafkaReadable& _readable, TKafkaVersion _version) {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't read version " << _version << " of TConsumerProtocolAssignment::TopicPartition";
    }
    NPrivate::Read<TopicMeta>(_readable, _version, Topic);
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

void TConsumerProtocolAssignment::TopicPartition::Write(TKafkaWritable& _writable, TKafkaVersion _version) const {
    if (!NPrivate::VersionCheck<MessageMeta::PresentVersions.Min, MessageMeta::PresentVersions.Max>(_version)) {
        ythrow yexception() << "Can't write version " << _version << " of TConsumerProtocolAssignment::TopicPartition";
    }
    NPrivate::TWriteCollector _collector;
    NPrivate::Write<TopicMeta>(_collector, _writable, _version, Topic);
    NPrivate::Write<PartitionsMeta>(_collector, _writable, _version, Partitions);

    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _writable.writeUnsignedVarint(_collector.NumTaggedFields);
    }
}

i32 TConsumerProtocolAssignment::TopicPartition::Size(TKafkaVersion _version) const {
    NPrivate::TSizeCollector _collector;
    NPrivate::Size<TopicMeta>(_collector, _version, Topic);
    NPrivate::Size<PartitionsMeta>(_collector, _version, Partitions);

    if (NPrivate::VersionCheck<MessageMeta::FlexibleVersions.Min, MessageMeta::FlexibleVersions.Max>(_version)) {
        _collector.Size += NPrivate::SizeOfUnsignedVarint(_collector.NumTaggedFields);
    }
    return _collector.Size;
}

} //namespace NKafka
