#include <ydb/core/protos/fuzz_targets/common.h>
#include <ydb/core/ydb_convert/table_settings.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    NKikimrFuzz::TFuzzInput input;
    if (!input.ParseFromArray(data, size)) {
        return 0;
    }

    if (!input.has_alter_table_request()) {
        return 0;
    }

    NKikimrSchemeOp::TTableDescription result;
    Ydb::StatusIds::StatusCode status;
    TString error;

    try {
        NKikimr::FillAlterTableSettingsDesc(
            result,
            input.alter_table_request(),
            status,
            error,
            true // changed
        );
    } catch (...) {
    }

    return 0;
}
