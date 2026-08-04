#include <ydb/core/ydb_convert/table_settings.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    NKikimrSchemeOp::TTableDescription out;
    Ydb::Table::AlterTableRequest in;

    (void)in.ParseFromArray(data, size);

    if (in.path().empty()) {
        in.set_path("/Root/table");
    }

    if (in.add_columns().empty() && in.alter_columns().empty()) {
        auto* column = in.add_add_columns();
        column->set_name("column");
        column->mutable_type()->set_type_id(Ydb::Type::UTF8);
    }

    Ydb::StatusIds::StatusCode code = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    TString error;

    try {
        NKikimr::FillAlterTableSettingsDesc(out, in, code, error, false);
    } catch (...) {
    }

    return 0;
}
