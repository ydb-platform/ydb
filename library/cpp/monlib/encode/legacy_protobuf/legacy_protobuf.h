#pragma once

#include <google/protobuf/message.h>
#include <util/datetime/base.h>

namespace NMonitoring {
    // Unsupported features of the original format:
    // - histograms;
    // - memOnly;
    // - dropHost/ignorePath

    void DecodeLegacyProto(const NProtoBuf::Message& data, class IMetricConsumer* c, TInstant ts = TInstant::Zero()); 

    /// Does not open/close consumer stream unlike the above function.
    void DecodeLegacyProtoToStream(const NProtoBuf::Message& data, class IMetricConsumer* c, TInstant ts = TInstant::Zero()); 
}
