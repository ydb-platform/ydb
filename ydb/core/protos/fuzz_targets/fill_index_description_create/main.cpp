#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/protos/fuzz_targets/fuzz_actor_utils.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    Ydb::Table::CreateTableRequest request;
    (void)request.ParseFromArray(data, size);

    if (request.path().empty()) {
        request.set_path("/Root/table");
    }

    if (request.columns().empty()) {
        auto* column = request.add_columns();
        column->set_name("key");
        column->mutable_type()->set_type_id(Ydb::Type::UINT64);
    }

    if (request.primary_key_size() == 0) {
        request.add_primary_key(request.columns(0).name());
    }

    if (request.indexes().empty()) {
        auto* index = request.add_indexes();
        index->set_name("by_key");
        index->add_index_columns(request.primary_key(0));
        index->mutable_global_index();
    }

    RunWithMockedActorSystem([&]() {
        try {
            NKikimrSchemeOp::TIndexedTableCreationConfig out;
            Ydb::StatusIds::StatusCode status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
            TString error;

            NKikimr::FillIndexDescription(out, request, status, error);
        } catch (...) {
        }
    });

    return 0;
}
