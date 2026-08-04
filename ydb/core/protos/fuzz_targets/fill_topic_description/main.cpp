#include <ydb/core/protos/fuzz_targets/common.h>
#include <ydb/core/ydb_convert/topic_description.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    NKikimrFuzz::TFuzzInput input;
    if (!input.ParseFromArray(data, size)) {
        return 0;
    }

    if (!input.has_pq_group_description() || !input.has_dir_entry()) {
        return 0;
    }

    Ydb::Topic::DescribeTopicResult result;
    Ydb::StatusIds::StatusCode status;
    TString error;

    try {
        NKikimr::FillTopicDescription(
            result,
            input.pq_group_description(),
            input.dir_entry(),
            Nothing(), // cdcName
            status,
            error
        );
    } catch (...) {
    }

    return 0;
}
