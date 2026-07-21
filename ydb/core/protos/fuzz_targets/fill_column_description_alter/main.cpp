#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    Ydb::Table::AlterTableRequest request;
    (void)request.ParseFromArray(data, size);

    if (request.path().empty()) {
        request.set_path("/Root/table");
    }

    if (request.alter_columns().empty()) {
        auto* column = request.add_alter_columns();
        column->set_name("column");
        column->mutable_type()->set_type_id(Ydb::Type::UTF8);
    }

    NKikimrSchemeOp::TAlterColumnTable result;
    Ydb::StatusIds::StatusCode status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    TString error;

    try {
        NKikimr::FillColumnDescription(result, request.alter_columns(), status, error);
    } catch (...) {
    }

    return 0;
}
