#include <ydb/library/actors/wilson/wilson_trace.h>

#include <util/stream/output.h>

Y_DECLARE_OUT_SPEC(, NWilson::TTraceId, stream, value) {
    value.OutputSpanId(stream);
}
