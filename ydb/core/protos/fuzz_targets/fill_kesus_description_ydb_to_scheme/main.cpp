#include <ydb/core/protos/fuzz_targets/common.h>
#include <ydb/core/ydb_convert/kesus_description.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    NKikimrFuzz::TFuzzInput input;
    if (!input.ParseFromArray(data, size)) {
        return 0;
    }

    if (!input.has_coordination_config() || input.owner().empty()) {
        return 0;
    }

    NKikimrSchemeOp::TKesusDescription result;
    try {
        NKikimr::FillKesusDescription(
            result,
            input.coordination_config(),
            input.owner()
        );
    } catch (...) {
    }

    return 0;
}
