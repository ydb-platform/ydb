#include <ydb/core/protos/fuzz_targets/common.h>
#include <ydb/core/ydb_convert/table_settings.h>
#include <util/generic/list.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    NKikimrFuzz::TFuzzInput input;
    if (!input.ParseFromArray(data, size)) {
        return 0;
    }

    if (!input.has_create_table_request()) {
        return 0;
    }

    NKikimrSchemeOp::TTableDescription result;
    Ydb::StatusIds::StatusCode status;
    TString error;
    TList<TString> warnings;

    try {
        NKikimr::FillCreateTableSettingsDesc(
            result,
            input.create_table_request(),
            status,
            error,
            warnings,
            false // tableProfileSet
        );
    } catch (...) {
    }

    return 0;
}
