#pragma once

#include <util/datetime/base.h>

namespace google::protobuf {
    class Duration;
    class Timestamp;
}

namespace NProtoInterop {
    google::protobuf::Duration CastToProto(TDuration duration);
    google::protobuf::Timestamp CastToProto(TInstant instant);
    TDuration CastFromProto(const google::protobuf::Duration& message);
    TInstant CastFromProto(const google::protobuf::Timestamp& message);
}
