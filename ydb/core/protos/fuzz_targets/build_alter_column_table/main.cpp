#include <ydb/core/protos/fuzz_targets/common.h>
#include <ydb/core/protos/fuzz_targets/fuzz_actor_utils.h>
#include <ydb/core/kqp/provider/yql_kikimr_gateway.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/scheme/scheme_pathid.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size == 0) {
        return 0;
    }

    Ydb::Table::AlterTableRequest request;
    if (!request.ParseFromArray(data, size)) {
        return 0;
    }

    RunWithMockedActorSystem([request]() mutable {
        NKikimrSchemeOp::TModifyScheme modifyScheme;
        auto metadata = MakeIntrusive<NYql::TKikimrTableMetadata>();
        Ydb::StatusIds::StatusCode status;
        TString error;

        NKikimr::BuildAlterColumnTableModifyScheme(
            "/Root/ColumnTable", &request, &modifyScheme, metadata, status, error
        );
    });

    return 0;
}
