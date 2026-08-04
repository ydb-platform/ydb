#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    Ydb::Table::ChangefeedDescription input;
    if (!input.ParseFromArray(data, size)) {
        input.set_name(TString(reinterpret_cast<const char*>(data), size));
        input.set_mode(Ydb::Table::ChangefeedMode::MODE_KEYS_ONLY);
    }

    if (input.name().empty()) {
        input.set_name("f");
    }

    NKikimrSchemeOp::TCdcStreamDescription result;
    Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS;
    TString error;
    try {
        NKikimr::FillChangefeedDescription(
            result,
            input,
            status,
            error
        );
    } catch (...) {
        return 0;
    }

    return 0;
}
