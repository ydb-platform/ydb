#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    Ydb::Table::ColumnMeta out;
    NKikimrSchemeOp::TColumnDescription in;

    if (!in.ParseFromArray(data, size)) {
        in.SetName("column");
        in.SetType("Utf8");
    } else {
        if (in.GetName().empty()) {
            in.SetName("column");
        }
        if (in.GetType().empty()) {
            in.SetType("Utf8");
        }
    }

    try {
        NKikimr::FillColumnDescription(out, in);
    } catch (...) {
    }

    return 0;
}
