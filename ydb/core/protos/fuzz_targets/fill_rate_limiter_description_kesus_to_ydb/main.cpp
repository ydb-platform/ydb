#include <ydb/core/protos/fuzz_targets/common.h>
#include <ydb/core/ydb_convert/kesus_description.h>
#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    NKikimrFuzz::TFuzzInput input;
    if (!input.ParseFromArray(data, size)) {
        return 0;
    }

    if (!input.has_streaming_quoter_resource()) {
        return 0;
    }

    Ydb::RateLimiter::Resource result;
    try {
        NKikimr::FillRateLimiterDescription(
            result,
            input.streaming_quoter_resource()
        );
    } catch (...) {
    }

    return 0;
}
