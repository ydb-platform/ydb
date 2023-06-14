#include <ydb/core/protos/cms.pb.h>

#include <util/stream/output.h>

Y_DECLARE_OUT_SPEC(, NKikimrCms::EAvailabilityMode, stream, value) {
    stream << NKikimrCms::EAvailabilityMode_Name(value);
}
