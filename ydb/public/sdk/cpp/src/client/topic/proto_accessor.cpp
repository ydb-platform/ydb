#include <ydb-cpp-sdk/client/proto/accessor.h>

namespace NYdb::inline V3 {
    const NYdbProtos::Topic::DescribeTopicResult& TProtoAccessor::GetProto(const NTopic::TTopicDescription& topicDescription) {
        return topicDescription.GetProto();
    }

    const NYdbProtos::Topic::DescribeConsumerResult& TProtoAccessor::GetProto(const NTopic::TConsumerDescription& consumerDescription) {
        return consumerDescription.GetProto();
    }

    NYdbProtos::Topic::MeteringMode TProtoAccessor::GetProto(NTopic::EMeteringMode mode) {
        switch (mode) {
        case NTopic::EMeteringMode::Unspecified:
            return NYdbProtos::Topic::METERING_MODE_UNSPECIFIED;
        case NTopic::EMeteringMode::RequestUnits:
            return NYdbProtos::Topic::METERING_MODE_REQUEST_UNITS;
        case NTopic::EMeteringMode::ReservedCapacity:
            return NYdbProtos::Topic::METERING_MODE_RESERVED_CAPACITY;
        case NTopic::EMeteringMode::Unknown:
            return NYdbProtos::Topic::METERING_MODE_UNSPECIFIED;
        }
    }

    NTopic::EMeteringMode TProtoAccessor::FromProto(NYdbProtos::Topic::MeteringMode mode) {
        switch (mode) {
        case NYdbProtos::Topic::MeteringMode::METERING_MODE_UNSPECIFIED:
            return NTopic::EMeteringMode::Unspecified;
        case NYdbProtos::Topic::MeteringMode::METERING_MODE_REQUEST_UNITS:
            return NTopic::EMeteringMode::RequestUnits;
        case NYdbProtos::Topic::MeteringMode::METERING_MODE_RESERVED_CAPACITY:
            return NTopic::EMeteringMode::ReservedCapacity;
        default:
            return NTopic::EMeteringMode::Unknown;
        }
    }
}// namespace NYdb

