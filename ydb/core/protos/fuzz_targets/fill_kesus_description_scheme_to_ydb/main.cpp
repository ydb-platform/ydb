#include <ydb/core/protos/fuzz_targets/common.h>
#include <ydb/core/ydb_convert/kesus_description.h>
#include <ydb/public/api/protos/ydb_coordination.pb.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    NKikimrFuzz::TFuzzInput input;
    if (!input.ParseFromArray(data, size)) {
        return 0;
    }

    if (!input.has_kesus_description() || !input.has_dir_entry()) {
        return 0;
    }

    Ydb::Coordination::DescribeNodeResult result;
    try {
        NKikimr::FillKesusDescription(
            result,
            input.kesus_description(),
            input.dir_entry()
        );
    } catch (...) {
    }

    return 0;
}
