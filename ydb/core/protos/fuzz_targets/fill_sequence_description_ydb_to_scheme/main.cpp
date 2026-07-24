#include <ydb/core/protos/fuzz_targets/common.h>
#include <ydb/core/ydb_convert/table_description.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    NKikimrFuzz::TFuzzInput input;
    if (!input.ParseFromArray(data, size)) {
        return 0;
    }

    if (!input.has_sequence_description()) {
        return 0;
    }

    NKikimrSchemeOp::TSequenceDescription result;
    Ydb::StatusIds::StatusCode status;
    TString error;
    try {
        NKikimr::FillSequenceDescription(
            result,
            input.sequence_description(),
            status,
            error
        );
    } catch (...) {
    }

    return 0;
}
