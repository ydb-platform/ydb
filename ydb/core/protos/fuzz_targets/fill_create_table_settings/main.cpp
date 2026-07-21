#include <ydb/core/ydb_convert/table_settings.h>
#include <ydb/core/protos/fuzz_targets/fuzz_actor_utils.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <util/generic/list.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    NKikimrSchemeOp::TTableDescription out;
    Ydb::Table::CreateTableRequest in;

    (void)in.ParseFromArray(data, size);

    if (in.path().empty()) {
        in.set_path("/Root/table");
    }

    if (in.columns().empty()) {
        auto* col = in.add_columns();
        col->set_name("key");
        col->mutable_type()->set_type_id(Ydb::Type::UTF8);
    }

    if (in.primary_key_size() == 0) {
        in.add_primary_key(in.columns(0).name());
    }

    Ydb::StatusIds::StatusCode code = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    TString error;
    TList<TString> warnings;

    RunWithMockedActorSystem([&]() {
        try {
            NKikimr::FillCreateTableSettingsDesc(out, in, code, error, warnings, false);
        } catch (...) {
        }
    });

    return 0;
}
