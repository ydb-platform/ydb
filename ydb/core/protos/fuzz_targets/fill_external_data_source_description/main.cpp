#include <ydb/core/protos/fuzz_targets/common.h>
#include <ydb/core/ydb_convert/external_data_source_description.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    NKikimrFuzz::TFuzzInput input;
    if (!input.ParseFromArray(data, size)) {
        return 0;
    }

    if (!input.has_external_data_source_description() || !input.has_dir_entry()) {
        return 0;
    }

    Ydb::Table::DescribeExternalDataSourceResult result;
    try {
        NKikimr::FillExternalDataSourceDescription(
            result,
            input.external_data_source_description(),
            input.dir_entry()
        );
    } catch (...) {
    }

    return 0;
}
