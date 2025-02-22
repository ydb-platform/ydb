#define INCLUDE_YDB_INTERNAL_H
#include "make.h"


namespace NYdb::inline V2 {

void SetDuration(const TDuration& duration, google::protobuf::Duration& protoValue) {
    protoValue.set_seconds(duration.Seconds());
    protoValue.set_nanos(duration.NanoSecondsOfSecond());
}

} // namespace NYdb
