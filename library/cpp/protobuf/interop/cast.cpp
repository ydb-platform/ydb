#include <library/cpp/protobuf/interop/cast.h>

#include <google/protobuf/duration.pb.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/time_util.h>

namespace NProtoInterop {
    google::protobuf::Duration CastToProto(TDuration duration) {
        return google::protobuf::util::TimeUtil::MicrosecondsToDuration(duration.MicroSeconds());
    }

    google::protobuf::Timestamp CastToProto(TInstant instant) {
        return google::protobuf::util::TimeUtil::MicrosecondsToTimestamp(instant.MicroSeconds());
    }

    TDuration CastFromProto(const google::protobuf::Duration& duration) {
        return TDuration::MicroSeconds(google::protobuf::util::TimeUtil::DurationToMicroseconds(duration));
    }

    TInstant CastFromProto(const google::protobuf::Timestamp& timestamp) {
        return TInstant::MicroSeconds(google::protobuf::util::TimeUtil::TimestampToMicroseconds(timestamp));
    }
}
