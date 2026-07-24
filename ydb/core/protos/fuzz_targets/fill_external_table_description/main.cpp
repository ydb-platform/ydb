#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/ydb_convert/external_table_description.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    const size_t split = size / 2;

    NKikimrSchemeOp::TExternalTableDescription description;
    if (!description.ParseFromArray(data, split)) {
        return 0;
    }

    NKikimrSchemeOp::TDirEntry dirEntry;
    if (!dirEntry.ParseFromArray(data + split, size - split)) {
        return 0;
    }

    Ydb::Table::DescribeExternalTableResult result;
    Ydb::StatusIds::StatusCode status;
    TString error;

    try {
        NKikimr::FillExternalTableDescription(
            result,
            description,
            dirEntry,
            status,
            error
        );
    } catch (...) {
    }

    return 0;
}
