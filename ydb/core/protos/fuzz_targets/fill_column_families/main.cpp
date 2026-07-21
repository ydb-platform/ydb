#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) {
        return 0;
    }

    NKikimrSchemeOp::TTableDescription input;
    if (!input.ParseFromArray(data, size)) {
        return 0;
    }

    Ydb::Table::DescribeTableResult output;
    try {
        NKikimr::FillColumnFamilies(output, input);
    } catch (...) {
    }

    return 0;
}
