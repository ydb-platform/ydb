#include <ydb/core/protos/fuzz_targets/common.h>
#include <ydb/core/protos/fuzz_inputs.pb.h>
#include <ydb/core/health_check/health_check.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    NKikimrFuzz::TFuzzInput input;
    if (!input.ParseFromArray(data, size)) {
        return 0;
    }

    Ydb::Monitoring::SelfCheckRequest req;
    req.set_return_verbose_status(input.is_container());

    Ydb::Monitoring::SelfCheckResult res;
    // We can parse a dummy result or just use the whole data for req and result.
    if (!res.ParseFromString(input.parse_string())) {
        return 0;
    }

    NKikimr::NHealthCheck::RemoveUnrequestedEntries(res, req);

    return 0;
}
