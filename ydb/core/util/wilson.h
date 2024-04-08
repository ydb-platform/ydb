#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/library/actors/wilson/wilson_span.h>

namespace NWilson {

inline void EndSpanWithStatus(NWilson::TSpan& span, Ydb::StatusIds::StatusCode statusCode) {
    if (statusCode == Ydb::StatusIds::SUCCESS) {
        span.EndOk();
    } else {
        span.EndError(Ydb::StatusIds_StatusCode_Name(statusCode));
    }
}

} // namespace NWilson
