#include <ydb/core/protos/fuzz_targets/fuzz_actor_utils.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    Ydb::Table::CreateTableRequest request;
    (void)request.ParseFromArray(data, size);

    if (request.path().empty()) {
        request.set_path("/Root/column_table");
    }

    if (request.columns().empty()) {
        auto* column = request.add_columns();
        column->set_name("column");
        column->mutable_type()->set_type_id(Ydb::Type::UTF8);
    }

    RunWithMockedActorSystem([request]() mutable {
        NKikimrSchemeOp::TModifyScheme result;
        Ydb::StatusIds::StatusCode status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        TString error;

        NKikimr::FillColumnTableDescription(result, request, status, error);
    });

    return 0;
}
