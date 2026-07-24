#include <ydb/core/protos/fuzz_targets/common.h>
#include <ydb/core/protos/kesus.pb.h>
#include <ydb/core/ydb_convert/kesus_description.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    NKikimrFuzz::TFuzzInput input;
    if (!input.ParseFromArray(data, size)) {
        return 0;
    }

    if (!input.has_rate_limiter_resource()) {
        return 0;
    }

    NKikimrKesus::TStreamingQuoterResource result;
    try {
        NKikimr::FillRateLimiterDescription(
            result,
            input.rate_limiter_resource()
        );
    } catch (...) {
    }

    return 0;
}
