#pragma once

#include <util/datetime/base.h>
#include <google/protobuf/duration.pb.h>

namespace NKikimr {

inline
TDuration GetDuration(const google::protobuf::Duration& duration) {
    ui32 ns = std::max((i32)duration.nanos(), (i32)0);
    ui32 ms = ns / 1000000;
    ms += ns % 1000000 > 0 ? 1 : 0; // ceil to millisecond

    ui64 seconds = std::max((i64)duration.seconds(), (i64)0);

    return TDuration::Seconds(seconds) + TDuration::MilliSeconds(ms);
}

inline
void SetDuration(const TDuration& duration, google::protobuf::Duration& protoValue) {
    protoValue.set_seconds(duration.Seconds());
    protoValue.set_nanos(duration.NanoSecondsOfSecond());
}

}
